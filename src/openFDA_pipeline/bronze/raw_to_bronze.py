"""Convert one completed device enforcement Raw batch to Parquet."""

import argparse
from datetime import date, datetime, timezone
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


ENDPOINT = "/device/enforcement"
SCHEMA_VERSION = 1
SOURCE_STRING_FIELDS = (
    "address_1", "address_2", "center_classification_date", "city",
    "classification", "code_info", "country", "distribution_pattern", "event_id",
    "initial_firm_notification", "more_code_info", "postal_code",
    "product_description", "product_quantity", "product_type", "reason_for_recall",
    "recall_initiation_date", "recall_number", "recalling_firm", "report_date",
    "state", "status", "termination_date", "voluntary_mandated",
)
SOURCE_FIELDS = frozenset((*SOURCE_STRING_FIELDS, "openfda"))
BRONZE_SCHEMA = pa.schema(
    [pa.field(name, pa.string()) for name in SOURCE_STRING_FIELDS]
    + [
        pa.field("openfda", pa.string(), metadata={b"encoding": b"json"}),
        pa.field("_source_batch_id", pa.string(), nullable=False),
        pa.field("_source_file", pa.string(), nullable=False),
        pa.field("_source_record_index", pa.int64(), nullable=False),
    ],
    metadata={b"bronze_schema_version": str(SCHEMA_VERSION).encode("ascii")},
)


def _record_count(value, label):
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _validate_batch_identity(metadata, raw_batch_dir):
    """Check the endpoint, batch directory identity, and source dates."""
    if metadata["endpoint"] != ENDPOINT:
        raise ValueError(f"expected endpoint {ENDPOINT}")
    batch_id = metadata["batch_id"]
    if (
        not isinstance(batch_id, str)
        or not batch_id
        or raw_batch_dir.name != f"batch_id={batch_id}"
    ):
        raise ValueError("directory name does not match batch_id")
    ingestion_directory = raw_batch_dir.parent.name
    if not ingestion_directory.startswith("ingestion_date="):
        raise ValueError("batch must be inside an ingestion_date=YYYY-MM-DD directory")
    ingestion_date = ingestion_directory.removeprefix("ingestion_date=")
    if date.fromisoformat(ingestion_date).isoformat() != ingestion_date:
        raise ValueError("ingestion date must use YYYY-MM-DD")
    date.fromisoformat(metadata["export_date"])


def _validate_source_file_metadata(file_info, raw_batch_dir, seen_filenames):
    """Validate a unique source reference and return its declared record count."""
    filename = file_info["filename"]
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or "\\" in filename
        or ":" in filename
        or not filename.endswith(".json")
        or filename.casefold() == "ingestion_metadata.json"
    ):
        raise ValueError(f"invalid source filename: {filename!r}")
    if filename.casefold() in seen_filenames:
        raise ValueError(f"duplicate source filename: {filename}")
    seen_filenames.add(filename.casefold())
    source_path = raw_batch_dir / filename
    if source_path.resolve().parent != raw_batch_dir:
        raise ValueError(f"source file must remain inside the Raw batch: {filename}")
    checksum = file_info["sha256"]
    if (
        not isinstance(checksum, str)
        or len(checksum) != 64
        or any(character not in "0123456789abcdef" for character in checksum)
    ):
        raise ValueError(f"invalid SHA-256 checksum for {filename}")
    return _record_count(file_info["actual_records"], f"{filename} actual_records")


def _validate_partitions(partitions, raw_batch_dir, expected_total):
    """Reconcile file and partition counts with the declared batch total."""
    if not isinstance(partitions, list) or not partitions:
        raise ValueError("partitions must be a nonempty list")
    seen_filenames = set()
    partition_total = 0
    for partition in partitions:
        expected_records = _record_count(partition["expected_records"], "partition expected_records")
        actual_records = _record_count(partition["actual_records"], "partition actual_records")
        if actual_records != expected_records:
            raise ValueError("partition expected and actual record counts differ")
        files = partition["files"]
        if not isinstance(files, list) or not files:
            raise ValueError("partition files must be a nonempty list")
        file_total = sum(
            _validate_source_file_metadata(file_info, raw_batch_dir, seen_filenames)
            for file_info in files
        )
        if file_total != expected_records:
            raise ValueError("file counts do not match partition count")
        partition_total += expected_records
    if partition_total != expected_total:
        raise ValueError("partition counts do not match batch count")


def _load_batch_metadata(raw_batch_dir):
    """Load metadata and report validation failures with the manifest path."""
    metadata_path = raw_batch_dir / "ingestion_metadata.json"
    with metadata_path.open(encoding="utf-8") as source:
        metadata = json.load(source)
    try:
        _validate_batch_identity(metadata, raw_batch_dir)
        expected_records = _record_count(metadata["expected_records"], "expected_records")
        actual_records = _record_count(metadata["actual_records"], "actual_records")
        if actual_records != expected_records:
            raise ValueError("batch expected and actual record counts differ")
        _validate_partitions(metadata["partitions"], raw_batch_dir, expected_records)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid Raw batch metadata in {metadata_path}: {error}") from error
    return metadata


def _load_records(source_path, file_info):
    with source_path.open("rb") as source:
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
        if checksum != file_info["sha256"]:
            raise ValueError(f"{source_path.name}: SHA-256 checksum mismatch")
        source.seek(0)
        try:
            payload = json.load(source)
        except (ValueError, UnicodeError) as error:
            raise ValueError(f"{source_path.name}: invalid JSON: {error}") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
        raise ValueError(f"{source_path.name}: expected a top-level results list")
    records = payload["results"]
    if len(records) != file_info["actual_records"]:
        raise ValueError(
            f"{source_path.name}: record count mismatch: expected "
            f"{file_info['actual_records']}, found {len(records)}"
        )
    return records


def _records_to_table(records, source_filename, batch_id):
    """Preserve source values under the explicit schema and attach provenance."""
    rows = []
    for index, record in enumerate(records):
        location = f"{source_filename}, record {index}"
        if not isinstance(record, dict):
            raise ValueError(f"{location}: expected an object")
        unknown_fields = set(record) - SOURCE_FIELDS
        if unknown_fields:
            raise ValueError(f"{location}: unexpected fields {sorted(unknown_fields)}; review the schema")
        row = dict(record)
        for field, value in record.items():
            if value is None:
                continue
            if field == "openfda":
                if not isinstance(value, dict):
                    raise ValueError(f"{location}, field {field}: expected an object or null")
                try:
                    row[field] = json.dumps(value, ensure_ascii=False, allow_nan=False)
                except ValueError as error:
                    raise ValueError(f"{location}, field {field}: invalid JSON object: {error}") from error
            elif not isinstance(value, str):
                raise ValueError(f"{location}, field {field}: expected a string or null")
        row.update({
            "_source_batch_id": batch_id,
            "_source_file": source_filename,
            "_source_record_index": index,
        })
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)


def _convert_file(raw_batch_dir, file_info, output_path, batch_id):
    source_filename = file_info["filename"]
    records = _load_records(raw_batch_dir / source_filename, file_info)
    table = _records_to_table(records, source_filename, batch_id)
    pq.write_table(table, output_path, compression="snappy")
    # Read the file directly: dataset discovery would add ingestion_date/batch_id columns.
    restored = pq.ParquetFile(output_path).read()
    restored.validate(full=True)
    if not restored.schema.equals(BRONZE_SCHEMA, check_metadata=True):
        raise ValueError(f"{output_path.name}: Parquet schema does not match Bronze schema")
    if restored.num_rows != len(records):
        raise ValueError(f"{output_path.name}: Parquet row count does not match source")
    return {
        "source_file": source_filename,
        "source_sha256": file_info["sha256"],
        "output_file": output_path.name,
        "expected_records": file_info["actual_records"],
        "actual_records": restored.num_rows,
    }


def _build_conversion_metadata(raw_batch_dir, raw_metadata, converted_files, started_at):
    """Reconcile converted counts and describe the completed conversion."""
    total_records = sum(file["actual_records"] for file in converted_files)
    if total_records != raw_metadata["expected_records"]:
        raise ValueError(
            f"Converted count {total_records} does not match Raw count "
            f"{raw_metadata['expected_records']}"
        )
    try:
        converter_version = version("openFDA-pipeline")
    except PackageNotFoundError:
        converter_version = "uninstalled"
    return {
        "endpoint": ENDPOINT,
        "source_batch_id": raw_metadata["batch_id"],
        "source_ingestion_date": raw_batch_dir.parent.name.removeprefix("ingestion_date="),
        "source_export_date": raw_metadata["export_date"],
        "conversion_started_at": started_at.isoformat(),
        "conversion_completed_at": datetime.now(timezone.utc).isoformat(),
        "expected_records": raw_metadata["expected_records"],
        "actual_records": total_records,
        "schema_version": SCHEMA_VERSION,
        "arrow_schema": str(BRONZE_SCHEMA),
        "openfda_encoding": "json",
        "converter_version": converter_version,
        "pyarrow_version": pa.__version__,
        "files": converted_files,
    }


def convert_batch(raw_batch_dir: Path, output_dir: Path = Path("data/bronze")) -> Path:
    """Publish a validated Bronze batch; reject existing output without overwriting."""
    started_at = datetime.now(timezone.utc)
    raw_batch_dir = Path(raw_batch_dir).resolve()
    metadata = _load_batch_metadata(raw_batch_dir)
    endpoint_dir = Path(output_dir) / "device_enforcement"
    final_dir = endpoint_dir / raw_batch_dir.parent.name / raw_batch_dir.name
    if final_dir.exists():
        raise FileExistsError(
            f"Bronze batch already exists: {final_dir}; choose a separate output root to reprocess"
        )
    if final_dir.resolve().is_relative_to(raw_batch_dir):
        raise ValueError("Bronze output must be outside the immutable Raw batch")
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".bronze-", dir=endpoint_dir) as temporary:
        staging_dir = Path(temporary) / "batch"
        staging_dir.mkdir()
        converted_files = []
        for partition in metadata["partitions"]:
            for file_info in partition["files"]:
                output_path = staging_dir / f"part-{len(converted_files) + 1:04d}.parquet"
                print(f"Converting {file_info['filename']} → {output_path.name}", flush=True)
                converted_files.append(
                    _convert_file(raw_batch_dir, file_info, output_path, metadata["batch_id"])
                )
        conversion_metadata = _build_conversion_metadata(
            raw_batch_dir, metadata, converted_files, started_at,
        )
        with (staging_dir / "conversion_metadata.json").open("x", encoding="utf-8") as output:
            json.dump(conversion_metadata, output, indent=2)
            output.write("\n")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            raise FileExistsError(f"Bronze batch already exists: {final_dir}")
        staging_dir.rename(final_dir)
    print(
        f"Bronze conversion complete: {conversion_metadata['actual_records']} records "
        f"in {final_dir.resolve()}",
        flush=True,
    )
    return final_dir


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-batch", type=Path, required=True, help="Completed Raw batch directory")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/bronze"),
        help="Bronze root (default: data/bronze relative to the current directory)",
    )
    args = parser.parse_args(argv)
    try:
        convert_batch(args.raw_batch, args.output_dir)
    except (OSError, ValueError, pa.ArrowException) as error:
        print(f"Bronze conversion failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
