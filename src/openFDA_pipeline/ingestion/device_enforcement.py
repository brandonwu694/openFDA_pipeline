"""Ingest the latest device enforcement bulk export into the Raw layer."""

import argparse
import hashlib
import json
import shutil
import stat
import sys
import tempfile
import zlib
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

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
        partition_metadata.append(
            {
                "source_url": url,
                "expected_records": partition["records"],
                "actual_records": actual_records,
                "files": files,
            }
        )
        archive_path.unlink()
    return partition_metadata


def _verify_existing_export(metadata_path, metadata):
    """Check local source files before treating an advertised export as ingested."""
    batch_dir = metadata_path.parent.resolve()
    if batch_dir.name != f"batch_id={metadata['batch_id']}":
        raise ValueError("batch ID does not match its directory")
    if metadata["actual_records"] != metadata["expected_records"]:
        raise ValueError("batch counts do not match")
    seen_filenames = set()
    total = 0
    for partition in metadata["partitions"]:
        files = partition["files"]
        if not files:
            raise ValueError("partition has no source files")
        partition_total = 0
        for file_info in files:
            filename = file_info["filename"]
            if (
                not isinstance(filename, str)
                or Path(filename).name != filename
                or "\\" in filename
                or filename.casefold() == "ingestion_metadata.json"
                or filename.casefold() in seen_filenames
            ):
                raise ValueError(f"invalid or duplicate source filename: {filename!r}")
            seen_filenames.add(filename.casefold())
            source_path = batch_dir / filename
            if source_path.resolve().parent != batch_dir:
                raise ValueError(f"source file is outside its batch: {filename}")
            with source_path.open("rb") as source:
                checksum = hashlib.file_digest(source, "sha256").hexdigest()
            if checksum != file_info["sha256"]:
                raise ValueError(f"checksum mismatch: {filename}")
            count = file_info["actual_records"]
            if type(count) is not int or count < 0:
                raise ValueError(f"invalid record count: {filename}")
            partition_total += count
        if (
            partition_total != partition["expected_records"]
            or partition_total != partition["actual_records"]
        ):
            raise ValueError("partition counts do not match")
        total += partition_total
    if total != metadata["actual_records"]:
        raise ValueError("source file counts do not match the batch")


def _find_existing_export(endpoint_dir, entry):
    """Match published batches by export date, source URLs, and record counts."""
    signature = [(part["file"], part["records"]) for part in entry["partitions"]]
    metadata_paths = endpoint_dir.glob("ingestion_date=*/batch_id=*/ingestion_metadata.json")
    for metadata_path in sorted(metadata_paths, reverse=True):
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata["endpoint"] != ENDPOINT:
                raise ValueError(f"expected endpoint {ENDPOINT}")
            if metadata["export_date"] != entry["export_date"]:
                continue
            stored_signature = [
                (part["source_url"], part["expected_records"]) for part in metadata["partitions"]
            ]
            if (
                metadata["expected_records"] != entry["total_records"]
                or stored_signature != signature
            ):
                continue
            _verify_existing_export(metadata_path, metadata)
            return metadata_path.parent
        except (KeyError, TypeError, OSError, ValueError) as error:
            raise ValueError(
                f"Cannot verify existing Raw batch {metadata_path.parent}: {error}. "
                "Inspect the batch or use --force to download a separate snapshot"
            ) from error
    return None


def ingest_device_enforcement(
    output_dir: Path = Path("data/raw"), *, force: bool = False
) -> Path | None:
    """Publish a new Raw snapshot; return None when the advertised export is already stored."""
    started_at = datetime.now(timezone.utc)
    batch_id = f"{started_at:%Y%m%dT%H%M%S%fZ}_{uuid4().hex}"
    endpoint_dir = Path(output_dir) / "device_enforcement"
    final_dir = (
        endpoint_dir / f"ingestion_date={started_at.date().isoformat()}" / f"batch_id={batch_id}"
    )
    with requests.Session() as session:
        print(f"Fetching download manifest: {MANIFEST_URL}", flush=True)
        with session.get(MANIFEST_URL, timeout=TIMEOUT) as response:
            response.raise_for_status()
            entry = validate_manifest(response.json())
        if not force:
            existing_batch = _find_existing_export(endpoint_dir, entry)
            if existing_batch is not None:
                print(
                    f"Export {entry['export_date']} is already ingested at {existing_batch}; "
                    "no new Raw batch created.",
                    flush=True,
                )
                return None
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".ingest-", dir=endpoint_dir) as temporary:
            staging_dir = Path(temporary) / "batch"
            staging_dir.mkdir()
            partition_metadata = _ingest_partitions(
                session,
                entry["partitions"],
                Path(temporary),
                staging_dir,
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
    print(f"Ingestion complete: {actual_total} records in {final_dir.resolve()}", flush=True)
    return final_dir


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Raw data root (default: data/raw relative to the current directory)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download a fresh snapshot even if this export is already stored",
    )
    args = parser.parse_args(argv)
    try:
        ingest_device_enforcement(args.output_dir, force=args.force)
    except (requests.RequestException, OSError, ValueError) as error:
        print(
            f"Ingestion failed: {error}. Check network access, download contents, "
            "and output permissions, then rerun; existing batches are preserved.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
