"""Read-only checks for published pipeline batches and their source lineage."""

import json
from datetime import date

import duckdb
import pyarrow.parquet as pq

from openFDA_pipeline.bronze import raw_to_bronze as bronze
from openFDA_pipeline.gold import device_enforcement as gold
from openFDA_pipeline.ingestion import device_enforcement as ingestion
from openFDA_pipeline.silver import device_enforcement as silver

STAGES = ("raw", "bronze", "silver", "gold")
METADATA_FILES = {
    "raw": "ingestion_metadata.json",
    "bronze": "conversion_metadata.json",
    "silver": "silver_metadata.json",
    "gold": "gold_metadata.json",
}


def _check_output_files(batch_dir, metadata, names):
    """Check recorded counts, schemas, and checksums without changing publications."""
    for name in names:
        output = metadata["outputs"][name]
        if output["filename"] != f"{name}.parquet":
            raise ValueError(f"unexpected output filename for {name}")
        path = batch_dir / output["filename"]
        if path.resolve().parent != batch_dir:
            raise ValueError(f"output must remain inside its batch: {path}")
        if gold._sha256(path) != output["sha256"]:
            raise ValueError(f"{path}: SHA-256 mismatch")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != gold._record_count(output["records"], name):
            raise ValueError(f"{path}: row count mismatch")
        if str(parquet.schema_arrow) != output["arrow_schema"]:
            raise ValueError(f"{path}: schema mismatch")


def _validate_gold(batch_dir):
    metadata = json.loads((batch_dir / METADATA_FILES["gold"]).read_text(encoding="utf-8"))
    if metadata["endpoint"] != ingestion.ENDPOINT:
        raise ValueError("Gold endpoint mismatch")
    if batch_dir.name != f"batch_id={metadata['source_batch_id']}":
        raise ValueError("Gold batch ID mismatch")
    ingestion_date = metadata["source_ingestion_date"]
    if date.fromisoformat(ingestion_date).isoformat() != ingestion_date:
        raise ValueError("Gold ingestion date must use YYYY-MM-DD")
    if batch_dir.parent.name != f"ingestion_date={ingestion_date}":
        raise ValueError("Gold ingestion date mismatch")
    date.fromisoformat(metadata["source_export_date"])
    if (
        type(metadata["schema_version"]) is not int
        or metadata["schema_version"] != gold.SCHEMA_VERSION
    ):
        raise ValueError("unsupported Gold schema version")
    _check_output_files(batch_dir, metadata, gold.OUTPUTS)
    input_count = gold._record_count(metadata["input_records"], "Gold input_records")
    if metadata["outputs"]["recall_detail"]["records"] != input_count:
        raise ValueError("Gold detail count does not reconcile")
    with duckdb.connect() as connection:
        for name, keys in gold.OUTPUTS.items():
            connection.read_parquet(
                str(batch_dir / f"{name}.parquet"), hive_partitioning=False
            ).create_view("output", replace=True)
            if connection.execute(
                f"SELECT count(*) FROM (SELECT {keys} FROM output GROUP BY ALL HAVING count(*) > 1)"
            ).fetchall()[0][0]:
                raise ValueError(f"{name}: duplicate output keys")
            if connection.execute(
                "SELECT count(*) FROM output WHERE _source_batch_id IS DISTINCT FROM ?",
                [metadata["source_batch_id"]],
            ).fetchall()[0][0]:
                raise ValueError(f"{name}: source batch mismatch")
            if (
                name != "recall_detail"
                and connection.execute(
                    "SELECT coalesce(sum(recall_count), 0) FROM output"
                ).fetchall()[0][0]
                != input_count
            ):
                raise ValueError(f"{name}: recall counts do not reconcile")
    return metadata


def validate_completed(stage, batch_dir):
    metadata_path = batch_dir / METADATA_FILES[stage]
    if not metadata_path.is_file():
        raise ValueError(
            f"Incomplete {stage} destination has no {metadata_path.name}: {batch_dir}; inspect it"
        )
    if stage == "raw":
        metadata = bronze._load_batch_metadata(batch_dir)
        ingestion._verify_existing_export(metadata_path, metadata)
        return metadata
    if stage == "bronze":
        metadata = silver._load_batch_metadata(batch_dir)
        with duckdb.connect() as connection:
            silver._load_inputs(connection, batch_dir, metadata)
        return metadata
    if stage == "silver":
        metadata = gold._load_batch_metadata(batch_dir)
        _check_output_files(batch_dir, metadata, silver.DISPOSITIONS)
        with duckdb.connect() as connection:
            gold._load_input(connection, batch_dir, metadata)
        return metadata
    return _validate_gold(batch_dir)


def check_lineage(stage, metadata, previous_dir, previous_metadata):
    if stage == "bronze":
        sources = {
            f["filename"]: f["sha256"]
            for part in previous_metadata["partitions"]
            for f in part["files"]
        }
        if {f["source_file"]: f["source_sha256"] for f in metadata["files"]} != sources:
            raise ValueError("Bronze source hashes do not match Raw")
    else:
        previous_stage = STAGES[STAGES.index(stage) - 1]
        if metadata["input_metadata_sha256"] != gold._sha256(
            previous_dir / METADATA_FILES[previous_stage]
        ):
            raise ValueError(f"{stage}: upstream metadata SHA-256 mismatch")
        expected_files = (
            {info["output_file"]: info["actual_records"] for info in previous_metadata["files"]}
            if stage == "silver"
            else {"accepted.parquet": previous_metadata["outputs"]["accepted"]["records"]}
        )
        inputs = metadata["inputs"]
        if (
            len(inputs) != len(expected_files)
            or {info["filename"]: info["records"] for info in inputs} != expected_files
        ):
            raise ValueError(f"{stage}: upstream input files or counts mismatch")
        for info in inputs:
            if gold._sha256(previous_dir / info["filename"]) != info["sha256"]:
                raise ValueError(f"{stage}: upstream input SHA-256 mismatch: {info['filename']}")
    expected_count = (
        previous_metadata["outputs"]["accepted"]["records"]
        if stage == "gold"
        else previous_metadata["actual_records"]
    )
    actual_count = metadata["actual_records"] if stage == "bronze" else metadata["input_records"]
    if actual_count != expected_count:
        raise ValueError(f"{stage}: upstream record count mismatch")
    previous_export = previous_metadata.get(
        "source_export_date", previous_metadata.get("export_date")
    )
    if metadata["source_export_date"] != previous_export:
        raise ValueError(f"{stage}: upstream export date mismatch")
