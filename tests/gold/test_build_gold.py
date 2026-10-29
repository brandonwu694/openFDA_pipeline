import hashlib
import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openFDA_pipeline.bronze.raw_to_bronze import BRONZE_SCHEMA
from openFDA_pipeline.gold import device_enforcement as gold
from openFDA_pipeline.silver.device_enforcement import clean_batch


@pytest.fixture
def silver_factory(tmp_path):
    def create(records=None, batch_id="example"):
        if records is None:
            records = [{}]
        bronze = tmp_path / "bronze/ingestion_date=2026-10-23" / f"batch_id={batch_id}"
        bronze.mkdir(parents=True)
        rows = [
            {
                "recall_number": f"Z-{i}",
                "event_id": "shared",
                "report_date": "20261014",
                "recall_initiation_date": "20261001",
                "center_classification_date": "20261010",
                "classification": "Class II",
                "status": "Ongoing",
                "openfda": "{}",
                "recalling_firm": "Acme",
                **record,
                "_source_batch_id": batch_id,
                "_source_file": "source.json",
                "_source_record_index": i,
            }
            for i, record in enumerate(records)
        ]
        pq.write_table(pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA), bronze / "part.parquet")
        (bronze / "conversion_metadata.json").write_text(
            json.dumps(
                {
                    "endpoint": "/device/enforcement",
                    "source_batch_id": batch_id,
                    "source_ingestion_date": "2026-10-23",
                    "source_export_date": "2026-10-21",
                    "schema_version": 1,
                    "expected_records": len(rows),
                    "actual_records": len(rows),
                    "files": [
                        {
                            "source_file": "source.json",
                            "output_file": "part.parquet",
                            "expected_records": len(rows),
                            "actual_records": len(rows),
                        }
                    ],
                }
            )
        )
        return clean_batch(bronze, tmp_path / "data/silver")

    return create


def read_rows(batch, name):
    return pq.ParquetFile(batch / f"{name}.parquet").read().to_pylist()


def test_detail_features_and_lineage(silver_factory, tmp_path):
    source = silver_factory(
        [
            {
                "product_description": "  Device\t café\u00a0\nX ",
                "reason_for_recall": "  Seal\n broken ",
                "product_quantity": " 100 units ",
                "recalling_firm": " ACME\tMedical ",
                "classification": "Class I",
                "openfda": '{"device_name": ["Test"]}',
            }
        ]
    )
    original = {p.name: p.read_bytes() for p in source.iterdir()}
    batch = gold.build_gold(source, tmp_path / "gold")
    row = read_rows(batch, "recall_detail")[0]
    for key, value in read_rows(source, "accepted")[0].items():
        assert row[key] == value
    assert row["product_description_normalized"] == "Device café X"
    assert row["product_description_char_count"] == len(row["product_description"])
    assert row["reason_for_recall_normalized"] == "Seal broken"
    assert row["product_quantity_normalized"] == "100 units"
    assert row["recalling_firm_normalized"] == "acme medical"
    assert row["is_class_i"] is True
    assert (row["report_year"], row["report_quarter"], row["report_month"]) == (
        2026,
        4,
        date(2026, 10, 1),
    )
    assert row["initiation_month"] == row["classification_month"] == date(2026, 10, 1)
    assert row["initiation_to_report_days"] == 13
    assert row["initiation_to_classification_days"] == 9
    assert row["initiation_to_report_flag"] == row["initiation_to_classification_flag"] == "valid"
    assert row["openfda_missing"] is row["openfda_empty"] is False
    assert {p.name: p.read_bytes() for p in source.iterdir()} == original
    metadata = json.loads((batch / "gold_metadata.json").read_text())
    assert metadata["input_records"] == 1
    assert (
        metadata["input_metadata_sha256"]
        == hashlib.sha256(original["silver_metadata.json"]).hexdigest()
    )
    assert (
        metadata["inputs"][0]["sha256"] == hashlib.sha256(original["accepted.parquet"]).hexdigest()
    )
    assert set(metadata["sql_sha256"]) == set(gold.SQL_STEPS)
    assert metadata["build_started_at"].endswith("+00:00")
    for name, output in metadata["outputs"].items():
        assert output["records"] == len(read_rows(batch, name))
        assert (
            output["sha256"]
            == hashlib.sha256((batch / output["filename"]).read_bytes()).hexdigest()
        )


def test_summary_grains_unknowns_events_and_shares(silver_factory, tmp_path):
    source = silver_factory(
        [
            {"classification": "Class I"},
            {"classification": "Class II"},
            {"classification": "Class I", "event_id": None},
            {"classification": "Future", "recalling_firm": None, "status": None},
            {"classification": None, "report_date": "20260901", "status": "Future"},
            {"classification": "Not Yet Classified", "event_id": "other"},
            {"classification": "Class III"},
        ]
    )
    batch = gold.build_gold(source, tmp_path / "gold")
    detail = read_rows(batch, "recall_detail")
    assert len(detail) == 7
    assert detail[3]["_warning_codes"] == ["unfamiliar_classification"]
    assert detail[3]["is_class_i"] is detail[4]["is_class_i"] is detail[5]["is_class_i"] is None
    assert detail[6]["is_class_i"] is False
    for name in ("recall_monthly", "recall_by_firm", "recall_by_status"):
        assert sum(row["recall_count"] for row in read_rows(batch, name)) == 7
    monthly = read_rows(batch, "recall_monthly")
    october = [r for r in monthly if r["report_month"] == date(2026, 10, 1)]
    assert all(r["class_i_share"] == 0.5 and r["classified_recall_count"] == 4 for r in october)
    september = next(r for r in monthly if r["report_month"] == date(2026, 9, 1))
    assert september["class_i_share"] is None
    class_i = next(r for r in october if r["classification"] == "Class I")
    assert (class_i["recall_count"], class_i["event_count"], class_i["missing_event_id_count"]) == (
        2,
        1,
        1,
    )
    ongoing = next(r for r in read_rows(batch, "recall_by_status") if r["status"] == "Ongoing")
    assert (ongoing["recall_count"], ongoing["event_count"]) == (5, 2)
    assert ongoing["class_i_share"] == 0.5
    assert any(r["status"] is None for r in read_rows(batch, "recall_by_status"))
    assert any(r["recalling_firm_normalized"] is None for r in read_rows(batch, "recall_by_firm"))
    quality = json.loads((batch / "gold_metadata.json").read_text())["quality_summary"]
    assert quality["accepted_warning_records"] == 3
    assert quality["missing_event_id_records"] == 1


@pytest.mark.parametrize(
    ("changes", "report_flag", "classification_flag"),
    [
        ({"recall_initiation_date": None}, "missing_date", "missing_date"),
        ({"recall_initiation_date": "20261015"}, "negative_interval", "negative_interval"),
        ({"center_classification_date": None}, "valid", "missing_date"),
        ({"center_classification_date": "20260930"}, "valid", "negative_interval"),
        (
            {"recall_initiation_date": "20261014", "center_classification_date": "20261014"},
            "valid",
            "valid",
        ),
    ],
)
def test_date_intervals(silver_factory, tmp_path, changes, report_flag, classification_flag):
    batch = gold.build_gold(silver_factory([changes]), tmp_path / "gold")
    row = read_rows(batch, "recall_detail")[0]
    for prefix, flag in (
        ("initiation_to_report", report_flag),
        ("initiation_to_classification", classification_flag),
    ):
        assert row[f"{prefix}_flag"] == flag
        if flag != "valid":
            assert row[f"{prefix}_days"] is None
        else:
            assert row[f"{prefix}_days"] >= 0


@pytest.mark.parametrize(
    ("annotations", "missing", "empty"),
    [
        (None, True, None),
        ("{ }", False, True),
        ('{"device_name": []}', False, False),
    ],
)
def test_missing_features_and_annotations(silver_factory, tmp_path, annotations, missing, empty):
    batch = gold.build_gold(silver_factory([{"openfda": annotations}]), tmp_path / "gold")
    row = read_rows(batch, "recall_detail")[0]
    for field in ("product_description", "reason_for_recall", "product_quantity"):
        assert row[f"{field}_normalized"] is row[f"{field}_char_count"] is None
        assert row[f"{field}_missing"] is True
    assert row["openfda"] == annotations
    assert row["openfda_missing"] is missing
    assert row["openfda_empty"] is empty


@pytest.mark.parametrize("records", [[], [{"report_date": "invalid"}]])
def test_empty_accepted_writes_typed_outputs(silver_factory, tmp_path, records):
    batch = gold.build_gold(silver_factory(records), tmp_path / "gold")
    for name in gold.OUTPUTS:
        assert read_rows(batch, name) == []
    schema = pq.ParquetFile(batch / "recall_detail.parquet").schema_arrow
    assert schema.field("report_month").type == pa.date32()
    assert schema.field("is_class_i").type == pa.bool_()
    assert schema.field("initiation_to_report_days").type == pa.int64()


def test_silver_dispositions_are_excluded(silver_factory, tmp_path):
    source = silver_factory(
        [
            {"recall_number": "same"},
            {"recall_number": "same"},
            {"report_date": "invalid"},
        ]
    )
    assert len(read_rows(source, "duplicates")) == len(read_rows(source, "quarantine")) == 1
    batch = gold.build_gold(source, tmp_path / "gold")
    assert len(read_rows(batch, "recall_detail")) == 1


def test_snapshots_stay_separate_and_reruns_preserve_outputs(silver_factory, tmp_path):
    first, second = silver_factory(batch_id="first"), silver_factory(batch_id="second")
    output = tmp_path / "gold"
    a, b = gold.build_gold(first, output), gold.build_gold(second, output)
    assert a != b
    assert read_rows(a, "recall_detail")[0]["_source_batch_id"] == "first"
    assert read_rows(b, "recall_detail")[0]["_source_batch_id"] == "second"
    original = {p.name: p.read_bytes() for p in a.iterdir()}
    with pytest.raises(FileExistsError):
        gold.build_gold(first, output)
    assert {p.name: p.read_bytes() for p in a.iterdir()} == original


@pytest.mark.parametrize("damage", ["hash", "schema", "count", "batch", "duplicate", "error"])
def test_broken_input_is_rejected(silver_factory, tmp_path, damage):
    source = silver_factory([{}, {}])
    path = source / "accepted.parquet"
    table = pq.ParquetFile(path).read()
    if damage == "schema":
        table = table.drop(["city"])
    elif damage == "count":
        table = table.slice(0, 1)
    elif damage in ("batch", "duplicate", "error"):
        rows = table.to_pylist()
        field, value = {
            "batch": ("_source_batch_id", "other"),
            "duplicate": ("recall_number", "Z-1"),
            "error": ("_error_codes", ["invalid"]),
        }[damage]
        rows[0][field] = value
        table = pa.Table.from_pylist(rows, schema=table.schema)
    pq.write_table(table, path)
    if damage != "hash":
        metadata_path = source / "silver_metadata.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["outputs"]["accepted"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        metadata_path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError):
        gold.build_gold(source, tmp_path / "gold")
    assert not list((tmp_path / "gold").rglob("*.parquet"))


def test_failed_publication_cleans_staging(silver_factory, tmp_path, monkeypatch):
    source = silver_factory()

    def fail(connection, staging_dir, input_count):
        (staging_dir / "recall_detail.parquet").write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(gold, "_write_outputs", fail)
    with pytest.raises(OSError, match="disk full"):
        gold.build_gold(source, tmp_path / "gold")
    assert not list((tmp_path / "gold").rglob("batch_id=*"))
    assert not list((tmp_path / "gold").rglob(".gold-*"))


def test_readback_failure_cleans_staging(silver_factory, tmp_path, monkeypatch):
    source = silver_factory()
    wrong = tmp_path / "wrong-schema.parquet"
    pq.write_table(pa.table({"wrong": [1]}), wrong)
    original_reader = pq.ParquetFile

    def corrupt_readback(path, *args, **kwargs):
        return original_reader(
            wrong if path.name == "recall_monthly.parquet" else path, *args, **kwargs
        )

    monkeypatch.setattr(gold.pq, "ParquetFile", corrupt_readback)
    with pytest.raises(ValueError, match="schema mismatch"):
        gold.build_gold(source, tmp_path / "gold")
    assert not list((tmp_path / "gold").rglob("batch_id=*"))
    assert not list((tmp_path / "gold").rglob(".gold-*"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("endpoint", "/device/recall"),
        ("schema_version", True),
        ("schema_version", 2),
        ("source_batch_id", "other"),
        ("source_ingestion_date", "2026-10-22"),
        ("input_records", 2),
    ],
)
def test_invalid_silver_metadata_fails(silver_factory, tmp_path, field, value):
    source = silver_factory()
    path = source / "silver_metadata.json"
    metadata = json.loads(path.read_text())
    metadata[field] = value
    path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="Invalid Silver metadata"):
        gold.build_gold(source, tmp_path / "gold")
    assert not (tmp_path / "gold").exists()


def test_output_cannot_be_inside_silver(silver_factory):
    source = silver_factory()
    with pytest.raises(ValueError, match="outside the immutable Silver"):
        gold.build_gold(source, source / "nested")
    assert not (source / "nested").exists()


def test_cli_discovery_noop_and_selection(silver_factory, tmp_path, monkeypatch, capsys):
    first = silver_factory(batch_id="first")
    second = silver_factory(batch_id="second")
    monkeypatch.chdir(tmp_path)
    assert gold.main([]) == 1
    assert "--silver-batch" in capsys.readouterr().err
    assert gold.main(["--silver-batch", str(second)]) == 0
    assert gold.main([]) == 0
    destination = tmp_path / "data/gold/device_enforcement" / first.parent.name / first.name
    assert (destination / "gold_metadata.json").exists()
    assert gold.main([]) == 0
    assert "No completed Silver batches" in capsys.readouterr().out
    assert (
        gold.main(["--silver-batch", str(first), "--output-dir", str(tmp_path / "alternate")]) == 0
    )


def test_cli_ignores_staging_and_rejects_incomplete_destination(
    silver_factory, tmp_path, monkeypatch, capsys
):
    staging = tmp_path / "data/silver/device_enforcement/.silver-leftover/batch"
    staging.mkdir(parents=True)
    (staging / "silver_metadata.json").write_text("invalid")
    monkeypatch.chdir(tmp_path)
    assert gold.main([]) == 0
    source = silver_factory()
    destination = tmp_path / "data/gold/device_enforcement" / source.parent.name / source.name
    destination.mkdir(parents=True)
    assert gold.main([]) == 1
    assert "no gold_metadata.json" in capsys.readouterr().err
