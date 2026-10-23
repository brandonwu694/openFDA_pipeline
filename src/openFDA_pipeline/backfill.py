"""Download one complete device enforcement snapshot into the Raw layer."""

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tempfile
from urllib.parse import urlsplit
from uuid import uuid4
from zipfile import BadZipFile, ZipFile
import zlib

import requests


MANIFEST_URL = "https://api.fda.gov/download.json"
ENDPOINT = "/device/enforcement"
TIMEOUT = (10, 120)


def validate_manifest(manifest):
    """Return the endpoint entry after checking fields required for ingestion."""
    try:
        entry = manifest["results"]["device"]["enforcement"]
        date.fromisoformat(entry["export_date"])
        partitions = entry["partitions"]
        total_records = entry["total_records"]
        if type(total_records) is not int or total_records < 0:
            raise ValueError("total_records must be a nonnegative integer")
        if not isinstance(partitions, list) or not partitions:
            raise ValueError("partitions must be a nonempty list")
        for partition in partitions:
            url = urlsplit(partition["file"])
            if url.scheme != "https" or not url.hostname:
                raise ValueError("partition file must be an absolute HTTPS URL")
            count = partition["records"]
            if type(count) is not int or count < 0:
                raise ValueError("partition records must be a nonnegative integer")
        if sum(part["records"] for part in partitions) != total_records:
            raise ValueError("partition counts do not match total_records")
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise ValueError(f"Invalid device enforcement download manifest: {error}") from error
    return entry


def download_partition(session, url, destination):
    """Stream a partition to disk without buffering the archive in memory."""
    with session.get(url, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)


def _validate_archive(archive):
    """Reject unsafe member paths and verify ZIP integrity."""
    for member in archive.infolist():
        path = PurePosixPath(member.filename)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in member.filename
            or ":" in member.filename
            or stat.S_ISLNK(member.external_attr >> 16)
        ):
            raise ValueError(f"Unsafe archive path: {member.filename!r}")
    corrupt_member = archive.testzip()
    if corrupt_member is not None:
        raise ValueError(f"ZIP integrity check failed: {corrupt_member}")


def _inspect_json_file(path):
    """Validate the source envelope and return record count and checksum metadata."""
    with path.open("rb") as source:
        payload = json.load(source)
    if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
        raise ValueError(f"{path.name}: expected a top-level results list")
    record_count = len(payload["results"])
    del payload
    with path.open("rb") as source:
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
    return {
        "filename": path.name,
        "sha256": checksum,
        "actual_records": record_count,
    }


def extract_partition(archive_path, destination, used_filenames):
    """Validate an archive and preserve JSON bytes under unique, safe names."""
    files = []
    try:
        with ZipFile(archive_path) as archive:
            _validate_archive(archive)
            for member in archive.infolist():
                path = PurePosixPath(member.filename)
                if member.is_dir() or path.suffix.lower() != ".json":
                    continue
                filename = path.name
                name_key = filename.casefold()
                if name_key in used_filenames:
                    raise ValueError(f"Duplicate or reserved JSON filename: {filename}")
                used_filenames.add(name_key)
                output_path = destination / filename
                with archive.open(member) as source, output_path.open("xb") as output:
                    shutil.copyfileobj(source, output)
                files.append(_inspect_json_file(output_path))
    except (BadZipFile, EOFError, RuntimeError, NotImplementedError, zlib.error) as error:
        raise ValueError(f"Cannot read ZIP archive {archive_path.name}: {error}") from error
    if not files:
        raise ValueError(f"No JSON data files found in {archive_path.name}")
    return files


def _ingest_partitions(session, partitions, temporary_dir, staging_dir):
    """Download and validate all partitions, returning their ingestion metadata."""
    used_filenames = {"ingestion_metadata.json"}
    partition_metadata = []
    for index, partition in enumerate(partitions, start=1):
        url = partition["file"]
        print(f"Downloading partition {index}/{len(partitions)}: {url}", flush=True)
        archive_path = temporary_dir / f"partition-{index}.zip"
        download_partition(session, url, archive_path)
        files = extract_partition(archive_path, staging_dir, used_filenames)
        actual_records = sum(file["actual_records"] for file in files)
        if actual_records != partition["records"]:
            raise ValueError(
                f"Partition {index} record count mismatch: expected "
                f"{partition['records']}, found {actual_records}"
            )
        partition_metadata.append({
            "source_url": url,
            "expected_records": partition["records"],
            "actual_records": actual_records,
            "files": files,
        })
        archive_path.unlink()
    return partition_metadata


def run_backfill(output_dir):
    """Publish a validated snapshot and return its batch directory."""
    started_at = datetime.now(timezone.utc)
    batch_id = f"{started_at:%Y%m%dT%H%M%S%fZ}_{uuid4().hex}"
    endpoint_dir = Path(output_dir) / "device_enforcement"
    final_dir = (
        endpoint_dir
        / f"ingestion_date={started_at.date().isoformat()}"
        / f"batch_id={batch_id}"
    )
    with requests.Session() as session:
        print(f"Fetching download manifest: {MANIFEST_URL}", flush=True)
        with session.get(MANIFEST_URL, timeout=TIMEOUT) as response:
            response.raise_for_status()
            entry = validate_manifest(response.json())
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".backfill-", dir=endpoint_dir) as temporary:
            staging_dir = Path(temporary) / "batch"
            staging_dir.mkdir()
            partition_metadata = _ingest_partitions(
                session, entry["partitions"], Path(temporary), staging_dir,
            )
            actual_total = sum(part["actual_records"] for part in partition_metadata)
            if actual_total != entry["total_records"]:
                raise ValueError(
                    f"Total record count mismatch: expected {entry['total_records']}, "
                    f"found {actual_total}"
                )
            metadata = {
                "endpoint": ENDPOINT,
                "manifest_url": MANIFEST_URL,
                "export_date": entry["export_date"],
                "batch_id": batch_id,
                "ingestion_started_at": started_at.isoformat(),
                "ingestion_completed_at": datetime.now(timezone.utc).isoformat(),
                "expected_records": entry["total_records"],
                "actual_records": actual_total,
                "partitions": partition_metadata,
            }
            with (staging_dir / "ingestion_metadata.json").open("x", encoding="utf-8") as output:
                json.dump(metadata, output, indent=2)
                output.write("\n")
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            staging_dir.rename(final_dir)
    print(f"Backfill complete: {actual_total} records in {final_dir.resolve()}", flush=True)
    return final_dir


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/raw"),
        help="Raw data root (default: data/raw relative to the current directory)",
    )
    args = parser.parse_args(argv)
    try:
        run_backfill(args.output_dir)
    except (requests.RequestException, OSError, ValueError) as error:
        print(
            f"Backfill failed: {error}. Check network access, download contents, "
            "and output permissions, then rerun; existing batches are preserved.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
