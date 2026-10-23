"""Clean and validate one completed device enforcement Bronze snapshot."""

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
from openFDA_pipeline.bronze.raw_to_bronze import SCHEMA_VERSION as BRONZE_SCHEMA_VERSION

SCHEMA_VERSION = 1
RULES_VERSION = 1
SQL_STEPS = ("standardize.sql", "enforce_types.sql", "validate.sql", "deduplicate.sql")
DISPOSITIONS = ("accepted", "quarantine", "duplicates")


def _sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _record_count(value, label):
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _validate_filename(filename, suffix):
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or "\\" in filename
        or ":" in filename
        or not filename.endswith(suffix)
    ):
        raise ValueError(f"invalid source filename: {filename!r}")


def _load_batch_metadata(batch_dir):
    metadata_path = batch_dir / "conversion_metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata["endpoint"] != ENDPOINT:
            raise ValueError(f"expected endpoint {ENDPOINT}")
        if not isinstance(metadata["source_batch_id"], str) or not metadata["source_batch_id"]:
            raise ValueError("source_batch_id must be a nonempty string")
        if batch_dir.name != f"batch_id={metadata['source_batch_id']}":
            raise ValueError("directory name does not match source_batch_id")
        ingestion_date = metadata["source_ingestion_date"]
        if date.fromisoformat(ingestion_date).isoformat() != ingestion_date:
            raise ValueError("source_ingestion_date must use YYYY-MM-DD")
        if batch_dir.parent.name != f"ingestion_date={ingestion_date}":
            raise ValueError("directory name does not match source_ingestion_date")
        date.fromisoformat(metadata["source_export_date"])
        if (
            type(metadata["schema_version"]) is not int
            or metadata["schema_version"] != BRONZE_SCHEMA_VERSION
        ):
            raise ValueError(f"expected Bronze schema version {BRONZE_SCHEMA_VERSION}")
        expected = _record_count(metadata["expected_records"], "expected_records")
        if _record_count(metadata["actual_records"], "actual_records") != expected:
            raise ValueError("batch expected and actual counts differ")
        source_files = metadata["files"]
        if not isinstance(source_files, list) or not source_files:
            raise ValueError("files must be a nonempty list")
        output_names = set()
        source_names = set()
        file_total = 0
        for file_info in source_files:
            filename = file_info["output_file"]
            _validate_filename(filename, ".parquet")
            _validate_filename(file_info["source_file"], ".json")
            if (
                filename.casefold() in output_names
                or file_info["source_file"].casefold() in source_names
            ):
                raise ValueError("duplicate file reference in conversion metadata")
            output_names.add(filename.casefold())
            source_names.add(file_info["source_file"].casefold())
            if (batch_dir / filename).resolve().parent != batch_dir:
                raise ValueError(f"source file is outside the Bronze batch: {filename}")
            count = _record_count(file_info["expected_records"], f"{filename} expected_records")
            if _record_count(file_info["actual_records"], f"{filename} actual_records") != count:
                raise ValueError(f"{filename}: expected and actual counts differ")
            file_total += count
        if file_total != expected:
            raise ValueError("file counts do not match batch count")
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid Bronze metadata in {metadata_path}: {error}") from error
    return metadata


def _load_inputs(connection, batch_dir, metadata):
    """Verify input schemas, counts, and provenance before exposing the Bronze view."""
    inputs = []
    paths = []
    for file_info in metadata["files"]:
        path = batch_dir / file_info["output_file"]
        parquet = pq.ParquetFile(path)
        if not parquet.schema_arrow.equals(BRONZE_SCHEMA, check_metadata=True):
            raise ValueError(
                f"{path.name}: schema does not match Bronze version {BRONZE_SCHEMA_VERSION}"
            )
        if parquet.metadata.num_rows != file_info["actual_records"]:
            raise ValueError(f"{path.name}: Parquet row count does not match conversion metadata")
        connection.read_parquet(str(path), hive_partitioning=False).create_view(
            "input_file", replace=True
        )
        invalid_provenance = connection.execute(
            """SELECT count(*) FROM input_file
               WHERE _source_batch_id IS DISTINCT FROM ?
                  OR _source_file IS DISTINCT FROM ?
                  OR _source_record_index IS NULL
                  OR _source_record_index < 0 OR _source_record_index >= ?""",
            [metadata["source_batch_id"], file_info["source_file"], file_info["actual_records"]],
        ).fetchall()[0][0]
        if invalid_provenance:
            raise ValueError(f"{path.name}: invalid source provenance")
        invalid_annotations = connection.execute(
            """SELECT count(*) FROM input_file WHERE openfda IS NOT NULL AND
               (NOT json_valid(openfda) OR
                json_type(TRY_CAST(openfda AS JSON)) IS DISTINCT FROM 'OBJECT')"""
        ).fetchall()[0][0]
        if invalid_annotations:
            raise ValueError(f"{path.name}: openfda must contain JSON objects or nulls")
        paths.append(str(path))
        inputs.append(
            {
                "filename": path.name,
                "sha256": _sha256(path),
                "records": file_info["actual_records"],
            }
        )
    connection.read_parquet(paths, hive_partitioning=False).create_view("bronze")
    repeated_positions = connection.execute(
        """SELECT count(*) FROM (
               SELECT _source_file, _source_record_index FROM bronze
               GROUP BY ALL HAVING count(*) > 1
           )"""
    ).fetchall()[0][0]
    if repeated_positions:
        raise ValueError("Bronze source-file/record-position pairs must be unique")
    if (
        connection.execute("SELECT count(*) FROM bronze").fetchall()[0][0]
        != metadata["actual_records"]
    ):
        raise ValueError("Bronze input count does not match batch metadata")
    return inputs


def _run_sql_steps(connection):
    sql_hashes = {}
    for name in SQL_STEPS:
        sql = files("openFDA_pipeline.silver").joinpath("sql", name).read_text(encoding="utf-8")
        print(f"Silver step: {name}", flush=True)
        connection.execute(sql).fetchall()
        sql_hashes[name] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    return sql_hashes


def _write_outputs(connection, staging_dir, input_count):
    """Write typed outputs and reconcile their readback schemas and row counts."""
    outputs = {}
    for disposition in DISPOSITIONS:
        path = staging_dir / f"{disposition}.parquet"
        query = f"SELECT * FROM {disposition} ORDER BY _source_file, _source_record_index"
        expected_schema = (
            connection.sql(f"SELECT * FROM {disposition} LIMIT 0").to_arrow_table().schema
        )
        expected_count = connection.execute(f"SELECT count(*) FROM {disposition}").fetchall()[0][0]
        connection.execute(
            f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)", [str(path)]
        ).fetchall()
        parquet = pq.ParquetFile(path)
        # DuckDB's Parquet writer can infer stronger NOT NULL constraints than its Arrow export.
        actual_schema = parquet.schema_arrow
        if actual_schema.names != expected_schema.names or any(
            not actual.type.equals(expected.type)
            for actual, expected in zip(actual_schema, expected_schema)
        ):
            raise ValueError(f"{path.name}: output schema mismatch")
        actual_count = 0
        for record_batch in parquet.iter_batches():
            record_batch.validate(full=True)
            actual_count += record_batch.num_rows
        if actual_count != expected_count:
            raise ValueError(f"{path.name}: output row count mismatch")
        outputs[disposition] = {
            "filename": path.name,
            "records": actual_count,
            "sha256": _sha256(path),
            "arrow_schema": str(parquet.schema_arrow),
        }
    if sum(output["records"] for output in outputs.values()) != input_count:
        raise ValueError("Silver disposition counts do not reconcile with input records")
    connection.read_parquet(
        str(staging_dir / "accepted.parquet"),
        hive_partitioning=False,
    ).create_view("accepted_readback")
    invalid_keys = connection.execute(
        """SELECT count(*) FROM (
               SELECT recall_number FROM accepted_readback
               GROUP BY recall_number HAVING recall_number IS NULL OR count(*) > 1
           )"""
    ).fetchall()[0][0]
    if invalid_keys:
        raise ValueError("Accepted recall numbers must be non-null and unique")
    if connection.execute(
        "SELECT count(*) FROM accepted_readback WHERE len(_error_codes) > 0"
    ).fetchall()[0][0]:
        raise ValueError("Accepted rows must not contain critical errors")
    return outputs


def _quality_summary(connection):
    summary = {}
    for label, column in (("errors", "_error_codes"), ("warnings", "_warning_codes")):
        counts = connection.execute(
            f"""SELECT code, count(*) FROM dispositioned, unnest({column}) codes(code)
                GROUP BY code ORDER BY code"""
        ).fetchall()
        summary[label] = dict(counts)
    summary["accepted_warning_records"] = connection.execute(
        "SELECT count(*) FROM accepted WHERE len(_warning_codes) > 0"
    ).fetchall()[0][0]
    return summary


def clean_batch(bronze_batch_dir: Path, output_dir: Path = Path("data/silver")) -> Path:
    """Publish one cleaned snapshot without changing Bronze or earlier Silver batches."""
    started_at = datetime.now(timezone.utc)
    bronze_batch_dir = Path(bronze_batch_dir).resolve()
    metadata = _load_batch_metadata(bronze_batch_dir)
    endpoint_dir = Path(output_dir) / "device_enforcement"
    final_dir = endpoint_dir / bronze_batch_dir.parent.name / bronze_batch_dir.name
    if final_dir.exists():
        raise FileExistsError(
            f"Silver batch already exists: {final_dir}; choose a separate output root to reprocess"
        )
    if final_dir.resolve().is_relative_to(bronze_batch_dir):
        raise ValueError("Silver output must be outside the immutable Bronze batch")
    input_metadata_sha256 = _sha256(bronze_batch_dir / "conversion_metadata.json")
    with duckdb.connect() as connection:
        inputs = _load_inputs(connection, bronze_batch_dir, metadata)
        sql_hashes = _run_sql_steps(connection)
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".silver-", dir=endpoint_dir) as temporary:
            staging_dir = Path(temporary) / "batch"
            staging_dir.mkdir()
            outputs = _write_outputs(connection, staging_dir, metadata["actual_records"])
            try:
                cleaner_version = version("openFDA-pipeline")
            except PackageNotFoundError:
                cleaner_version = "uninstalled"
            silver_metadata = {
                "endpoint": ENDPOINT,
                "source_batch_id": metadata["source_batch_id"],
                "source_ingestion_date": metadata["source_ingestion_date"],
                "source_export_date": metadata["source_export_date"],
                "source_bronze_schema_version": metadata["schema_version"],
                "input_metadata_sha256": input_metadata_sha256,
                "inputs": inputs,
                "input_records": metadata["actual_records"],
                "schema_version": SCHEMA_VERSION,
                "rules_version": RULES_VERSION,
                "sql_sha256": sql_hashes,
                "cleaner_version": cleaner_version,
                "duckdb_version": duckdb.__version__,
                "pyarrow_version": pa.__version__,
                "cleaning_started_at": started_at.isoformat(),
                "cleaning_completed_at": datetime.now(timezone.utc).isoformat(),
                "outputs": outputs,
                "quality_summary": _quality_summary(connection),
            }
            with (staging_dir / "silver_metadata.json").open("x", encoding="utf-8") as output:
                json.dump(silver_metadata, output, indent=2)
                output.write("\n")
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            if final_dir.exists():
                raise FileExistsError(f"Silver batch already exists: {final_dir}")
            staging_dir.rename(final_dir)
    counts = ", ".join(f"{name}={outputs[name]['records']}" for name in DISPOSITIONS)
    print(f"Silver cleaning complete: {counts} in {final_dir.resolve()}", flush=True)
    return final_dir


def _find_pending_batch(bronze_root, output_dir):
    pending = []
    metadata_paths = (bronze_root / "device_enforcement").glob(
        "ingestion_date=*/batch_id=*/conversion_metadata.json"
    )
    for metadata_path in sorted(metadata_paths):
        batch_dir = metadata_path.parent
        _load_batch_metadata(batch_dir.resolve())
        destination = output_dir / "device_enforcement" / batch_dir.parent.name / batch_dir.name
        if (destination / "silver_metadata.json").is_file():
            continue
        if destination.exists():
            raise ValueError(
                f"Existing Silver path has no silver_metadata.json: {destination}; inspect it or choose a separate --output-dir"
            )
        pending.append(batch_dir)
    if len(pending) > 1:
        choices = "\n".join(f"  {batch}" for batch in pending)
        raise ValueError(
            f"Multiple pending Bronze batches found. Select one with --bronze-batch:\n{choices}"
        )
    return pending[0] if pending else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-batch",
        type=Path,
        help="Completed Bronze batch (default: discover the sole pending batch under data/bronze)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/silver"),
        help="Silver root (default: data/silver)",
    )
    args = parser.parse_args(argv)
    try:
        batch_dir = args.bronze_batch
        if batch_dir is None:
            batch_dir = _find_pending_batch(Path("data/bronze"), args.output_dir)
            if batch_dir is None:
                print(
                    "No completed Bronze batches need cleaning under data/bronze/device_enforcement."
                )
                return 0
        clean_batch(batch_dir, args.output_dir)
    except (OSError, ValueError, duckdb.Error, pa.ArrowException) as error:
        print(f"Silver cleaning failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
