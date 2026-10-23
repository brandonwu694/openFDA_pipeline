import hashlib
import json
from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openFDA_pipeline.bronze.raw_to_bronze import BRONZE_SCHEMA
from openFDA_pipeline.silver import device_enforcement as silver


def make_record(**changes):
    return {
        "recall_number": "Z-1",
        "event_id": "000123",
        "report_date": "20261014",
        "recall_initiation_date": "20261001",
        "classification": "Class II",
        "status": "Ongoing",
        "openfda": "{}",
        **changes,
    }


@pytest.fixture
def bronze_batch_factory(tmp_path):
    def create(records=None, *, file_records=None, batch_id="example"):
        if file_records is None:
            file_records = [("source.json", [make_record()] if records is None else records)]
        batch = (
            tmp_path
            / "data/bronze/device_enforcement/ingestion_date=2026-10-23"
            / f"batch_id={batch_id}"
        )
        batch.mkdir(parents=True)
        file_metadata = []
        for file_index, (source_filename, source_records) in enumerate(file_records, start=1):
            rows = [
                {
                    **record,
                    "_source_batch_id": batch_id,
                    "_source_file": source_filename,
                    "_source_record_index": record_index,
                }
                for record_index, record in enumerate(source_records)
            ]
            filename = f"part-{file_index:04d}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA), batch / filename)
            file_metadata.append(
                {
                    "source_file": source_filename,
                    "output_file": filename,
                    "source_sha256": "a" * 64,
                    "expected_records": len(rows),
                    "actual_records": len(rows),
                }
            )
        count = sum(file["actual_records"] for file in file_metadata)
        metadata = {
            "endpoint": "/device/enforcement",
            "source_batch_id": batch_id,
            "source_ingestion_date": "2026-10-23",
            "source_export_date": "2026-10-21",
            "schema_version": 1,
            "expected_records": count,
            "actual_records": count,
            "files": file_metadata,
        }
        (batch / "conversion_metadata.json").write_text(json.dumps(metadata))
        return batch

    return create


@pytest.fixture
def output_root(tmp_path):
    return tmp_path / "silver"


def read_rows(batch, disposition):
    return pq.ParquetFile(batch / f"{disposition}.parquet").read().to_pylist()


def read_metadata(batch):
    return json.loads((batch / "silver_metadata.json").read_text())


def assert_no_outputs(output_root):
    assert not list(output_root.rglob("batch_id=*"))
    assert not list(output_root.rglob(".silver-*"))
    assert not list(output_root.rglob("*.parquet"))


def rewrite_parquet(batch, transform):
    path = batch / "part-0001.parquet"
    table = pq.ParquetFile(path).read()
    pq.write_table(transform(table), path)


def test_cleaning_preserves_unknowns_text_and_provenance(bronze_batch_factory, output_root):
    source = make_record(
        recall_number="  Z-1\t",
        event_id=" 000123 ",
        postal_code=" 00100 ",
        state=" CA ",
        classification=" Class II ",
        status=" Ongoing ",
        recalling_firm="  Acme\t MEDICAL  ",
        product_description="  Device — café\n ",
        reason_for_recall="\t\n",
        code_info="  0001, 0002  ",
        product_quantity=" N/A ",
        voluntary_mandated="N/A",
        report_date=" 20261014 ",
    )
    bronze_batch = bronze_batch_factory([source])
    original = {path.name: path.read_bytes() for path in bronze_batch.iterdir()}
    batch = silver.clean_batch(bronze_batch, output_root)
    row = read_rows(batch, "accepted")[0]
    assert row["recall_number"] == "Z-1"
    assert row["event_id"] == "000123"
    assert row["postal_code"] == "00100"
    assert row["state"] == "CA"
    assert row["classification"] == "Class II"
    assert row["status"] == "Ongoing"
    assert row["recalling_firm"] == source["recalling_firm"]
    assert row["recalling_firm_normalized"] == "acme medical"
    assert row["product_description"] == source["product_description"]
    assert row["code_info"] == source["code_info"]
    assert row["reason_for_recall"] is None
    assert row["product_quantity"] == " N/A "
    assert row["voluntary_mandated"] == "N/A"
    assert row["termination_date"] is None
    assert row["report_date"] == date(2026, 10, 14)
    assert row["report_date_raw"] == " 20261014 "
    assert row["_error_codes"] == row["_warning_codes"] == []
    assert (row["_source_batch_id"], row["_source_file"], row["_source_record_index"]) == (
        "example",
        "source.json",
        0,
    )
    assert {path.name: path.read_bytes() for path in bronze_batch.iterdir()} == original
    assert read_rows(batch, "quarantine") == read_rows(batch, "duplicates") == []
    metadata = read_metadata(batch)
    assert metadata["input_records"] == metadata["outputs"]["accepted"]["records"] == 1
    assert (
        metadata["inputs"][0]["sha256"] == hashlib.sha256(original["part-0001.parquet"]).hexdigest()
    )
    assert (
        metadata["input_metadata_sha256"]
        == hashlib.sha256(original["conversion_metadata.json"]).hexdigest()
    )
    assert set(metadata["sql_sha256"]) == set(silver.SQL_STEPS)
    assert all(len(checksum) == 64 for checksum in metadata["sql_sha256"].values())
    assert metadata["schema_version"] == metadata["rules_version"] == 1
    assert metadata["duckdb_version"] == duckdb.__version__
    assert metadata["cleaning_started_at"].endswith("+00:00")
    for disposition in silver.DISPOSITIONS:
        path = batch / metadata["outputs"][disposition]["filename"]
        assert (
            hashlib.sha256(path.read_bytes()).hexdigest()
            == metadata["outputs"][disposition]["sha256"]
        )
    assert not list(output_root.rglob(".silver-*"))


@pytest.mark.parametrize("annotations", [None, "{}", '{"device_name": ["A"]}'])
def test_annotation_text_is_preserved(bronze_batch_factory, output_root, annotations):
    batch = silver.clean_batch(
        bronze_batch_factory([make_record(openfda=annotations)]), output_root
    )
    assert read_rows(batch, "accepted")[0]["openfda"] == annotations


@pytest.mark.parametrize("value", [None, "", " \t\n ", "\u00a0", "\u2003", "\u0085", "\v"])
def test_missing_recall_number_is_quarantined(bronze_batch_factory, output_root, value):
    batch = silver.clean_batch(
        bronze_batch_factory([make_record(recall_number=value)]), output_root
    )
    assert read_rows(batch, "accepted") == []
    assert "missing_recall_number" in read_rows(batch, "quarantine")[0]["_error_codes"]


@pytest.mark.parametrize(
    "whitespace", ["\u00a0", "\u2003", "\u0085", "\v", " \t\u00a0\u2003\u0085\v"]
)
def test_unicode_whitespace_standardization(bronze_batch_factory, output_root, whitespace):
    firm = f"{whitespace}Acme{whitespace}MEDICAL{whitespace}"
    description = f"{whitespace}Device{whitespace}"
    record = make_record(
        recall_number=f"{whitespace}Z-1{whitespace}",
        event_id=whitespace,
        postal_code=f"{whitespace}00100{whitespace}",
        classification=f"{whitespace}Class II{whitespace}",
        recalling_firm=firm,
        product_description=description,
        reason_for_recall=whitespace,
        report_date=f"{whitespace}20261014{whitespace}",
        termination_date=whitespace,
    )
    batch = silver.clean_batch(bronze_batch_factory([record]), output_root)
    row = read_rows(batch, "accepted")[0]
    assert row["recall_number"] == "Z-1"
    assert row["postal_code"] == "00100"
    assert row["classification"] == "Class II"
    assert row["event_id"] is None
    assert row["_warning_codes"] == ["missing_event_id"]
    assert row["_error_codes"] == []
    assert row["recalling_firm"] == firm
    assert row["recalling_firm_normalized"] == "acme medical"
    assert row["product_description"] == description
    assert row["reason_for_recall"] is None
    assert row["report_date"] == date(2026, 10, 14)
    assert row["report_date_raw"] == record["report_date"]
    assert row["termination_date"] is None
    assert row["termination_date_raw"] == whitespace


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("report_date", None, "missing_report_date"),
        ("report_date", " \t", "missing_report_date"),
        ("report_date", "\u00a0\u2003", "missing_report_date"),
        ("report_date", "20260230", "invalid_report_date"),
        ("report_date", "2026101", "invalid_report_date"),
        ("report_date", "2026-10-14", "invalid_report_date"),
        ("recall_initiation_date", "junk", "invalid_recall_initiation_date"),
        ("center_classification_date", "20261301", "invalid_center_classification_date"),
        ("termination_date", "20260229", "invalid_termination_date"),
    ],
)
def test_invalid_and_missing_dates_have_distinct_errors(
    bronze_batch_factory, output_root, field, value, error
):
    batch = silver.clean_batch(bronze_batch_factory([make_record(**{field: value})]), output_root)
    assert read_rows(batch, "accepted") == []
    row = read_rows(batch, "quarantine")[0]
    assert row[field] is None
    assert row[f"{field}_raw"] == value
    assert error in row["_error_codes"]
    assert len(row["_error_codes"]) == 1


def test_date_types_leap_day_and_optional_missing_values(bronze_batch_factory, output_root):
    record = make_record(
        report_date="20240229",
        recall_initiation_date="20240228",
        termination_date="20240301",
        center_classification_date="\t",
    )
    batch = silver.clean_batch(bronze_batch_factory([record]), output_root)
    row = read_rows(batch, "accepted")[0]
    assert row["report_date"] == date(2024, 2, 29)
    assert row["recall_initiation_date"] == date(2024, 2, 28)
    assert row["termination_date"] == date(2024, 3, 1)
    assert row["center_classification_date"] is None
    table = pq.ParquetFile(batch / "accepted.parquet").read()
    assert pa.types.is_date32(table.schema.field("report_date").type)
    assert pa.types.is_list(table.schema.field("_error_codes").type)


@pytest.mark.parametrize(
    ("changes", "warning"),
    [
        ({"recall_initiation_date": "19301211"}, "initiation_before_2004"),
        ({"recall_initiation_date": "20261015"}, "report_before_initiation"),
        ({"event_id": "\t"}, "missing_event_id"),
        ({"classification": "Future Class"}, "unfamiliar_classification"),
        ({"status": "Future Status"}, "unfamiliar_status"),
    ],
)
def test_suspicious_records_are_accepted_with_warnings(
    bronze_batch_factory, output_root, changes, warning
):
    batch = silver.clean_batch(bronze_batch_factory([make_record(**changes)]), output_root)
    row = read_rows(batch, "accepted")[0]
    assert row["_error_codes"] == []
    assert warning in row["_warning_codes"]
    assert read_metadata(batch)["quality_summary"]["warnings"][warning] == 1
    assert read_metadata(batch)["quality_summary"]["accepted_warning_records"] == 1


@pytest.mark.parametrize(
    "classification", ["Class I", "Class II", "Class III", "Not Yet Classified", None]
)
def test_known_and_missing_classifications_are_not_rejected(
    bronze_batch_factory, output_root, classification
):
    batch = silver.clean_batch(
        bronze_batch_factory([make_record(classification=classification)]), output_root
    )
    assert read_rows(batch, "accepted")[0]["_warning_codes"] == []


def test_exact_duplicates_across_files_keep_deterministic_representative(
    bronze_batch_factory, output_root
):
    record = make_record()
    batch = silver.clean_batch(
        bronze_batch_factory(
            file_records=[
                ("z.json", [record]),
                ("a.json", [record, record]),
            ]
        ),
        output_root,
    )
    accepted = read_rows(batch, "accepted")
    assert len(accepted) == 1
    assert accepted[0]["_source_file"] == "a.json"
    assert accepted[0]["_source_record_index"] == 0
    duplicates = read_rows(batch, "duplicates")
    assert len(duplicates) == 2
    assert {(row["_source_file"], row["_source_record_index"]) for row in duplicates} == {
        ("a.json", 1),
        ("z.json", 0),
    }
    assert all(
        row["_kept_source_file"] == "a.json" and row["_kept_source_record_index"] == 0
        for row in duplicates
    )
    assert read_rows(batch, "quarantine") == []
    metadata = read_metadata(batch)
    assert (
        sum(item["records"] for item in metadata["outputs"].values())
        == metadata["input_records"]
        == 3
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"reason_for_recall": "Different reason"},
        {"recall_number": " Z-1 "},
        {"report_date": " 20261014 "},
        {"termination_date": "invalid"},
    ],
)
def test_conflicting_source_versions_quarantine_all_rows(
    bronze_batch_factory, output_root, changed
):
    batch = silver.clean_batch(
        bronze_batch_factory([make_record(), make_record(**changed)]), output_root
    )
    assert read_rows(batch, "accepted") == read_rows(batch, "duplicates") == []
    quarantined = read_rows(batch, "quarantine")
    assert len(quarantined) == 2
    assert all("conflicting_recall_number" in row["_error_codes"] for row in quarantined)


def test_identical_critical_failures_stay_quarantined(bronze_batch_factory, output_root):
    record = make_record(report_date="junk")
    batch = silver.clean_batch(bronze_batch_factory([record, record]), output_root)
    assert len(read_rows(batch, "quarantine")) == 2
    assert read_rows(batch, "duplicates") == []


def test_empty_snapshot_produces_typed_empty_outputs(bronze_batch_factory, output_root):
    batch = silver.clean_batch(bronze_batch_factory([]), output_root)
    for disposition in silver.DISPOSITIONS:
        assert read_rows(batch, disposition) == []
    assert pa.types.is_date32(
        pq.ParquetFile(batch / "accepted.parquet").schema_arrow.field("report_date").type
    )
    assert read_metadata(batch)["input_records"] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("endpoint", "/device/recall"),
        ("source_batch_id", "wrong"),
        ("schema_version", 2),
        ("schema_version", True),
        ("source_ingestion_date", "2026-10-22"),
        ("source_export_date", "bad"),
        ("expected_records", 2),
        ("actual_records", True),
        ("files", []),
    ],
)
def test_invalid_input_metadata_fails_batch(bronze_batch_factory, output_root, field, value):
    batch = bronze_batch_factory()
    path = batch / "conversion_metadata.json"
    metadata = json.loads(path.read_text())
    metadata[field] = value
    path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="Invalid Bronze metadata"):
        silver.clean_batch(batch, output_root)
    assert_no_outputs(output_root)


@pytest.mark.parametrize(
    "damage",
    [
        "schema",
        "count",
        "batch",
        "filename",
        "position",
        "repeated-position",
        "annotations",
        "missing-file",
    ],
)
def test_broken_bronze_contract_fails_batch(bronze_batch_factory, output_root, damage):
    batch = bronze_batch_factory([make_record(), make_record(recall_number="Z-2")])
    if damage == "missing-file":
        (batch / "part-0001.parquet").unlink()
    elif damage == "schema":
        rewrite_parquet(batch, lambda table: table.drop(["city"]))
    elif damage == "count":
        rewrite_parquet(batch, lambda table: table.slice(0, 1))
    else:

        def corrupt(table):
            rows = table.to_pylist()
            field, value = {
                "batch": ("_source_batch_id", "other"),
                "filename": ("_source_file", "other.json"),
                "position": ("_source_record_index", -1),
                "repeated-position": ("_source_record_index", 1),
                "annotations": ("openfda", "not JSON"),
            }[damage]
            rows[0][field] = value
            return pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)

        rewrite_parquet(batch, corrupt)
    with pytest.raises((OSError, ValueError, pa.ArrowException)):
        silver.clean_batch(batch, output_root)
    assert_no_outputs(output_root)


def test_rerun_preserves_existing_silver(bronze_batch_factory, output_root):
    bronze = bronze_batch_factory()
    batch = silver.clean_batch(bronze, output_root)
    original = {file.name: file.read_bytes() for file in batch.iterdir()}
    with pytest.raises(FileExistsError, match="already exists"):
        silver.clean_batch(bronze, output_root)
    assert {file.name: file.read_bytes() for file in batch.iterdir()} == original


def test_output_cannot_be_inside_bronze(bronze_batch_factory):
    bronze = bronze_batch_factory()
    with pytest.raises(ValueError, match="outside the immutable Bronze batch"):
        silver.clean_batch(bronze, bronze / "nested")
    assert not (bronze / "nested").exists()


def test_failed_output_write_removes_staging(bronze_batch_factory, output_root, monkeypatch):
    bronze = bronze_batch_factory()

    def fail_write(connection, staging_dir, input_count):
        (staging_dir / "accepted.parquet").write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(silver, "_write_outputs", fail_write)
    with pytest.raises(OSError, match="disk full"):
        silver.clean_batch(bronze, output_root)
    assert_no_outputs(output_root)


def test_output_readback_failure_removes_staging(
    bronze_batch_factory, output_root, tmp_path, monkeypatch
):
    bronze = bronze_batch_factory()
    bad_path = tmp_path / "wrong-schema.parquet"
    pq.write_table(pa.table({"bad": [1]}), bad_path)
    original_reader = pq.ParquetFile

    def bad_readback(path, *args, **kwargs):
        return original_reader(
            bad_path if path.name == "accepted.parquet" else path, *args, **kwargs
        )

    monkeypatch.setattr(silver.pq, "ParquetFile", bad_readback)
    with pytest.raises(ValueError, match="schema mismatch"):
        silver.clean_batch(bronze, output_root)
    assert_no_outputs(output_root)


def test_default_cli_discovery_and_noop(bronze_batch_factory, tmp_path, monkeypatch, capsys):
    bronze = bronze_batch_factory()
    monkeypatch.chdir(tmp_path)
    assert silver.main([]) == 0
    output = tmp_path / "data/silver/device_enforcement" / bronze.parent.name / bronze.name
    assert (output / "silver_metadata.json").is_file()
    assert silver.main([]) == 0
    assert "No completed Bronze batches need cleaning" in capsys.readouterr().out


def test_multiple_pending_batches_require_selection(
    bronze_batch_factory, output_root, tmp_path, monkeypatch, capsys
):
    first = bronze_batch_factory(batch_id="first")
    second = bronze_batch_factory(batch_id="second")
    monkeypatch.chdir(tmp_path)
    assert silver.main([]) == 1
    error = capsys.readouterr().err
    assert first.name in error and second.name in error and "--bronze-batch" in error
    assert silver.main(["--bronze-batch", str(second), "--output-dir", str(output_root)]) == 0
    assert silver.main(["--output-dir", str(output_root)]) == 0
    assert len(list(output_root.rglob("silver_metadata.json"))) == 2


def test_discovery_ignores_staging_and_requires_completion_metadata(tmp_path, monkeypatch, capsys):
    path = tmp_path / "data/bronze/device_enforcement/.bronze-leftover/batch"
    path.mkdir(parents=True)
    (path / "conversion_metadata.json").write_text("not JSON")
    (path.parent.parent / "ingestion_date=2026-10-23/batch_id=incomplete").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    assert silver.main([]) == 0
    assert "No completed Bronze batches" in capsys.readouterr().out
    assert not (tmp_path / "data/silver").exists()


def test_discovery_rejects_incomplete_silver_output(
    bronze_batch_factory, tmp_path, monkeypatch, capsys
):
    bronze = bronze_batch_factory()
    destination = tmp_path / "data/silver/device_enforcement" / bronze.parent.name / bronze.name
    destination.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    assert silver.main([]) == 1
    assert "no silver_metadata.json" in capsys.readouterr().err
