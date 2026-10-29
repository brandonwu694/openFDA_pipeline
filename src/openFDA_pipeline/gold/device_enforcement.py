"""Build recall analytics from one completed device enforcement Silver snapshot."""

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import date, datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from openFDA_pipeline.bronze.raw_to_bronze import BRONZE_SCHEMA, ENDPOINT
from openFDA_pipeline.silver.device_enforcement import SCHEMA_VERSION as SILVER_SCHEMA_VERSION

SCHEMA_VERSION = 1
RULES_VERSION = 1
SQL_STEPS = ("recall_detail.sql", "summaries.sql")
OUTPUTS = {
    "recall_detail": "recall_number",
    "recall_monthly": "report_month, classification",
    "recall_by_firm": "recalling_firm_normalized, classification",
    "recall_by_status": "status",
}
DATE_FIELDS = (
    "report_date",
    "recall_initiation_date",
    "center_classification_date",
    "termination_date",
)
# Silver v1 retains Bronze column order and appends raw dates, firm grouping, and codes.
SILVER_FIELDS = (
    [
        (field.name, pa.date32() if field.name in DATE_FIELDS else field.type)
        for field in BRONZE_SCHEMA
    ]
    + [(f"{name}_raw", pa.string()) for name in DATE_FIELDS]
    + [
        ("recalling_firm_normalized", pa.string()),
        ("_error_codes", pa.list_(pa.string())),
        ("_warning_codes", pa.list_(pa.string())),
    ]
)


def _sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _record_count(value, label):
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _load_batch_metadata(batch_dir):
    path = batch_dir / "silver_metadata.json"
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
        if metadata["endpoint"] != ENDPOINT:
            raise ValueError(f"expected endpoint {ENDPOINT}")
        batch_id = metadata["source_batch_id"]
        if (
            not isinstance(batch_id, str)
            or not batch_id
            or batch_dir.name != f"batch_id={batch_id}"
        ):
            raise ValueError("directory name does not match source_batch_id")
        ingestion_date = metadata["source_ingestion_date"]
        if date.fromisoformat(ingestion_date).isoformat() != ingestion_date:
            raise ValueError("source_ingestion_date must use YYYY-MM-DD")
        if batch_dir.parent.name != f"ingestion_date={ingestion_date}":
            raise ValueError("directory name does not match source_ingestion_date")
        date.fromisoformat(metadata["source_export_date"])
        if (
            type(metadata["schema_version"]) is not int
            or metadata["schema_version"] != SILVER_SCHEMA_VERSION
        ):
            raise ValueError(f"expected Silver schema version {SILVER_SCHEMA_VERSION}")
        total = 0
        for name in ("accepted", "quarantine", "duplicates"):
            output = metadata["outputs"][name]
            if output["filename"] != f"{name}.parquet":
                raise ValueError(f"unexpected {name} filename")
            total += _record_count(output["records"], f"{name} records")
        if total != _record_count(metadata["input_records"], "input_records"):
            raise ValueError("Silver disposition counts do not reconcile")
        checksum = metadata["outputs"]["accepted"]["sha256"]
        if (
            not isinstance(checksum, str)
            or len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
        ):
            raise ValueError("invalid accepted SHA-256")
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid Silver metadata in {path}: {error}") from error
    return metadata


def _load_input(connection, batch_dir, metadata):
    path = batch_dir / "accepted.parquet"
    if path.resolve().parent != batch_dir:
        raise ValueError("accepted.parquet must remain inside the Silver batch")
    checksum = _sha256(path)
    declared = metadata["outputs"]["accepted"]
    if checksum != declared["sha256"]:
        raise ValueError("accepted.parquet: SHA-256 mismatch")
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    if [(field.name, field.type) for field in schema] != SILVER_FIELDS:
        raise ValueError("accepted.parquet: schema does not match Silver v1")
    if parquet.metadata.num_rows != declared["records"]:
        raise ValueError("accepted.parquet: row count does not match Silver metadata")
    connection.read_parquet(str(path), hive_partitioning=False).create_view("accepted")
    invalid = connection.execute(
        """SELECT count(*) FROM accepted
           WHERE recall_number IS NULL OR trim(recall_number) = '' OR report_date IS NULL
              OR _source_batch_id IS DISTINCT FROM ?
              OR _source_file IS NULL OR _source_file = ''
              OR _source_record_index IS NULL OR _source_record_index < 0
              OR _error_codes IS NULL OR len(_error_codes) > 0 OR _warning_codes IS NULL
              OR (openfda IS NOT NULL AND
                  (NOT json_valid(openfda)
                   OR json_type(TRY_CAST(openfda AS JSON)) IS DISTINCT FROM 'OBJECT'))""",
        [metadata["source_batch_id"]],
    ).fetchall()[0][0]
    if invalid:
        raise ValueError(
            "accepted.parquet: invalid accepted keys, dates, provenance, or quality codes"
        )
    for columns in ("recall_number", "_source_file, _source_record_index"):
        if connection.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM accepted GROUP BY ALL HAVING count(*) > 1)"
        ).fetchall()[0][0]:
            raise ValueError(f"accepted.parquet: duplicate {columns}")
    return [{"filename": path.name, "records": declared["records"], "sha256": checksum}]


def _run_sql_steps(connection):
    hashes = {}
    for name in SQL_STEPS:
        sql = files("openFDA_pipeline.gold").joinpath("sql", name).read_text(encoding="utf-8")
        print(f"Gold step: {name}", flush=True)
        connection.execute(sql).fetchall()
        hashes[name] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    return hashes


def _write_outputs(connection, staging_dir, input_count):
    outputs = {}
    for name, ordering in OUTPUTS.items():
        path = staging_dir / f"{name}.parquet"
        expected_schema = connection.sql(f"SELECT * FROM {name} LIMIT 0").to_arrow_table().schema
        expected_count = connection.execute(f"SELECT count(*) FROM {name}").fetchall()[0][0]
        connection.execute(
            f"COPY (SELECT * FROM {name} ORDER BY {ordering} NULLS FIRST) "
            "TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)",
            [str(path)],
        ).fetchall()
        parquet = pq.ParquetFile(path)
        actual_schema = parquet.schema_arrow
        if [(f.name, f.type) for f in actual_schema] != [(f.name, f.type) for f in expected_schema]:
            raise ValueError(f"{path.name}: output schema mismatch")
        count = 0
        for batch in parquet.iter_batches():
            batch.validate(full=True)
            count += batch.num_rows
        if count != expected_count:
            raise ValueError(f"{path.name}: output row count mismatch")
        connection.read_parquet(str(path), hive_partitioning=False).create_view(f"{name}_readback")
        if connection.execute(
            f"SELECT count(*) FROM (SELECT {ordering} FROM {name}_readback "
            "GROUP BY ALL HAVING count(*) > 1)"
        ).fetchall()[0][0]:
            raise ValueError(f"{path.name}: duplicate output keys")
        reconciled = (
            count
            if name == "recall_detail"
            else connection.execute(
                f"SELECT coalesce(sum(recall_count), 0) FROM {name}_readback"
            ).fetchall()[0][0]
        )
        if reconciled != input_count:
            raise ValueError(f"{path.name}: recall counts do not reconcile with Silver accepted")
        outputs[name] = {
            "filename": path.name,
            "records": count,
            "sha256": _sha256(path),
            "arrow_schema": str(actual_schema),
        }
    return outputs


def _quality_summary(connection):
    counts = connection.execute(
        """SELECT code, count(*) FROM recall_detail, unnest(_warning_codes) codes(code)
           GROUP BY code ORDER BY code"""
    ).fetchall()
    summary = {"warnings": dict(counts)}
    for flag in ("initiation_to_report_flag", "initiation_to_classification_flag"):
        summary[flag] = dict(
            connection.execute(
                f"SELECT {flag}, count(*) FROM recall_detail GROUP BY {flag} ORDER BY {flag}"
            ).fetchall()
        )
    for name, condition in {
        "accepted_warning_records": "len(_warning_codes) > 0",
        "missing_event_id_records": "event_id IS NULL",
        "unknown_classification_records": "is_class_i IS NULL",
        "missing_firm_records": "recalling_firm_normalized IS NULL",
    }.items():
        summary[name] = connection.execute(
            f"SELECT count(*) FROM recall_detail WHERE {condition}"
        ).fetchall()[0][0]
    return summary


def build_gold(silver_batch_dir: Path, output_dir: Path = Path("data/gold")) -> Path:
    """Publish analytics for one Silver snapshot, preserving all earlier batches."""
    started_at = datetime.now(timezone.utc)
    silver_batch_dir = Path(silver_batch_dir).resolve()
    metadata = _load_batch_metadata(silver_batch_dir)
    endpoint_dir = Path(output_dir) / "device_enforcement"
    final_dir = endpoint_dir / silver_batch_dir.parent.name / silver_batch_dir.name
    if final_dir.exists():
        raise FileExistsError(f"Gold batch already exists: {final_dir}; choose another output root")
    if final_dir.resolve().is_relative_to(silver_batch_dir):
        raise ValueError("Gold output must be outside the immutable Silver batch")
    input_hash = _sha256(silver_batch_dir / "silver_metadata.json")
    with duckdb.connect() as connection:
        inputs = _load_input(connection, silver_batch_dir, metadata)
        hashes = _run_sql_steps(connection)
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".gold-", dir=endpoint_dir) as temporary:
            staging_dir = Path(temporary) / "batch"
            staging_dir.mkdir()
            outputs = _write_outputs(connection, staging_dir, inputs[0]["records"])
            try:
                builder_version = version("openFDA-pipeline")
            except PackageNotFoundError:
                builder_version = "uninstalled"
            gold_metadata = {
                "endpoint": ENDPOINT,
                "source_batch_id": metadata["source_batch_id"],
                "source_ingestion_date": metadata["source_ingestion_date"],
                "source_export_date": metadata["source_export_date"],
                "source_silver_schema_version": metadata["schema_version"],
                "input_metadata_sha256": input_hash,
                "inputs": inputs,
                "input_records": inputs[0]["records"],
                "schema_version": SCHEMA_VERSION,
                "rules_version": RULES_VERSION,
                "sql_sha256": hashes,
                "builder_version": builder_version,
                "duckdb_version": duckdb.__version__,
                "pyarrow_version": pa.__version__,
                "build_started_at": started_at.isoformat(),
                "build_completed_at": datetime.now(timezone.utc).isoformat(),
                "outputs": outputs,
                "quality_summary": _quality_summary(connection),
            }
            (staging_dir / "gold_metadata.json").write_text(
                json.dumps(gold_metadata, indent=2) + "\n", encoding="utf-8"
            )
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            if final_dir.exists():
                raise FileExistsError(f"Gold batch already exists: {final_dir}")
            staging_dir.rename(final_dir)
    print(
        f"Gold build complete: {inputs[0]['records']} recalls in {final_dir.resolve()}", flush=True
    )
    return final_dir


def _find_pending_batch(silver_root, output_dir):
    pending = []
    for path in sorted(
        (silver_root / "device_enforcement").glob(
            "ingestion_date=*/batch_id=*/silver_metadata.json"
        )
    ):
        batch_dir = path.parent
        _load_batch_metadata(batch_dir.resolve())
        destination = output_dir / "device_enforcement" / batch_dir.parent.name / batch_dir.name
        if (destination / "gold_metadata.json").is_file():
            continue
        if destination.exists():
            raise ValueError(
                f"Existing Gold path has no gold_metadata.json: {destination}; inspect it"
            )
        pending.append(batch_dir)
    if len(pending) > 1:
        choices = "\n".join(f"  {batch}" for batch in pending)
        raise ValueError(
            f"Multiple pending Silver batches. Select one with --silver-batch:\n{choices}"
        )
    return pending[0] if pending else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-batch", type=Path, help="Completed Silver batch")
    parser.add_argument("--output-dir", type=Path, default=Path("data/gold"), help="Gold root")
    args = parser.parse_args(argv)
    try:
        batch = args.silver_batch
        if batch is None:
            batch = _find_pending_batch(Path("data/silver"), args.output_dir)
            if batch is None:
                print(
                    "No completed Silver batches need Gold processing under data/silver/device_enforcement."
                )
                return 0
        build_gold(batch, args.output_dir)
    except (OSError, ValueError, duckdb.Error, pa.ArrowException) as error:
        print(f"Gold build failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
