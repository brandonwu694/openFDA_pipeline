import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openFDA_pipeline.bronze import raw_to_bronze as bronze


@pytest.fixture
def raw_batch_factory(tmp_path):
    def create(groups=None):
        if groups is None:
            groups = [[[{
                "recall_number": "Z-0001-2026", "event_id": "00123",
                "report_date": "20261014", "openfda": {},
            }]]]
        batch_id = "example-batch"
        batch = tmp_path / "raw/device_enforcement/ingestion_date=2026-10-23" / f"batch_id={batch_id}"
        batch.mkdir(parents=True)
        partitions = []
        index = 0
        for group in groups:
            files = []
            for records in group:
                index += 1
                filename = f"source-{index}.json"
                content = json.dumps({"meta": {}, "results": records}).encode("utf-8")
                (batch / filename).write_bytes(content)
                files.append({
                    "filename": filename, "sha256": hashlib.sha256(content).hexdigest(),
                    "actual_records": len(records),
                })
            count = sum(file["actual_records"] for file in files)
            partitions.append({"files": files, "expected_records": count, "actual_records": count})
        total = sum(partition["actual_records"] for partition in partitions)
        metadata = {
            "endpoint": "/device/enforcement", "batch_id": batch_id,
            "export_date": "2026-10-21", "expected_records": total,
            "actual_records": total, "partitions": partitions,
        }
        (batch / "ingestion_metadata.json").write_text(json.dumps(metadata))
        return batch
    return create


@pytest.fixture
def raw_batch(raw_batch_factory):
    return raw_batch_factory()


@pytest.fixture
def output_root(tmp_path):
    return tmp_path / "bronze"


def load_metadata(batch):
    return json.loads((batch / "ingestion_metadata.json").read_text())


def write_metadata(batch, metadata):
    (batch / "ingestion_metadata.json").write_text(json.dumps(metadata))


def replace_source(batch, content):
    """Change a source fixture while keeping its declared checksum in sync."""
    (batch / "source-1.json").write_bytes(content)
    metadata = load_metadata(batch)
    metadata["partitions"][0]["files"][0]["sha256"] = hashlib.sha256(content).hexdigest()
    write_metadata(batch, metadata)


def assert_no_published_batch(output_root):
    assert not list(output_root.rglob("batch_id=*"))
    assert not list(output_root.rglob(".bronze-*"))
    assert not list(output_root.rglob("*.parquet"))
    assert not list(output_root.rglob("conversion_metadata.json"))


def test_preserves_values_order_and_provenance(raw_batch_factory, output_root):
    record = {
        "recall_number": "  Z-1  ", "event_id": "000123", "postal_code": "00100",
        "report_date": "19301211", "reason_for_recall": "", "city": None,
        "product_description": "Device — café\nsecond line", "openfda": {},
    }
    later_record = {"recall_number": "Z-2", "more_code_info": "0001, 0002", "termination_date": "20261021"}
    raw_batch = raw_batch_factory([[[record, record, later_record]]])
    original = {file.name: file.read_bytes() for file in raw_batch.iterdir()}
    batch = bronze.convert_batch(raw_batch, output_root)
    assert batch == output_root / "device_enforcement" / raw_batch.parent.name / raw_batch.name
    table = pq.ParquetFile(batch / "part-0001.parquet").read()
    assert table.schema.equals(bronze.BRONZE_SCHEMA, check_metadata=True)
    rows = table.to_pylist()
    assert len(rows) == 3
    for index, source in enumerate([record, record, later_record]):
        for field in bronze.SOURCE_FIELDS:
            expected = source.get(field)
            if field == "openfda" and expected is not None:
                assert json.loads(rows[index][field]) == expected
            else:
                assert rows[index][field] == expected
        assert rows[index]["_source_batch_id"] == "example-batch"
        assert rows[index]["_source_file"] == "source-1.json"
        assert rows[index]["_source_record_index"] == index
    assert rows[0]["openfda"] == "{}"
    assert {file.name: file.read_bytes() for file in raw_batch.iterdir()} == original
    parquet = pq.ParquetFile(batch / "part-0001.parquet")
    assert parquet.metadata.row_group(0).column(0).compression == "SNAPPY"
    metadata = json.loads((batch / "conversion_metadata.json").read_text())
    assert metadata["source_batch_id"] == "example-batch"
    assert metadata["source_ingestion_date"] == "2026-10-23"
    assert metadata["source_export_date"] == "2026-10-21"
    assert metadata["schema_version"] == bronze.SCHEMA_VERSION
    assert metadata["arrow_schema"] == str(bronze.BRONZE_SCHEMA)
    assert metadata["pyarrow_version"] == pa.__version__
    assert metadata["converter_version"]
    assert metadata["conversion_started_at"].endswith("+00:00")
    assert metadata["conversion_completed_at"].endswith("+00:00")
    assert metadata["expected_records"] == metadata["actual_records"] == 3
    assert metadata["files"][0] == {
        "source_file": "source-1.json",
        "source_sha256": hashlib.sha256(original["source-1.json"]).hexdigest(),
        "output_file": "part-0001.parquet", "expected_records": 3, "actual_records": 3,
    }
    assert not list(output_root.rglob(".bronze-*"))


@pytest.mark.parametrize("annotations", [{}, {"device_name": ["A", "B"], "nested": {"flag": True}}, None])
def test_openfda_encoding(raw_batch_factory, output_root, annotations):
    raw_batch = raw_batch_factory([[[{"openfda": annotations}, {}]]])
    batch = bronze.convert_batch(raw_batch, output_root)
    rows = pq.ParquetFile(batch / "part-0001.parquet").read().to_pylist()
    assert (json.loads(rows[0]["openfda"]) if rows[0]["openfda"] is not None else None) == annotations
    assert rows[1]["openfda"] is None


def test_multiple_files_and_partitions_preserve_manifest_order(raw_batch_factory, output_root):
    raw_batch = raw_batch_factory([
        [[{"event_id": "first"}], [{"event_id": "second"}, {"event_id": "second"}]],
        [[{"event_id": "third"}]],
    ])
    batch = bronze.convert_batch(raw_batch, output_root)
    metadata = json.loads((batch / "conversion_metadata.json").read_text())
    assert metadata["actual_records"] == 4
    assert [item["source_file"] for item in metadata["files"]] == [
        "source-1.json", "source-2.json", "source-3.json",
    ]
    for index, expected in enumerate(["first", "second", "third"], start=1):
        rows = pq.ParquetFile(batch / f"part-{index:04d}.parquet").read().to_pylist()
        assert all(row["event_id"] == expected for row in rows)
        assert [row["_source_record_index"] for row in rows] == list(range(len(rows)))


def test_empty_file_retains_schema(raw_batch_factory, output_root):
    raw_batch = raw_batch_factory([[[]]])
    batch = bronze.convert_batch(raw_batch, output_root)
    table = pq.ParquetFile(batch / "part-0001.parquet").read()
    assert table.num_rows == 0
    assert table.schema.equals(bronze.BRONZE_SCHEMA, check_metadata=True)


@pytest.mark.parametrize(("record", "message"), [
    ({"future_field": "x"}, "unexpected fields.*future_field"),
    ({"event_id": 123}, "field event_id"),
    ({"report_date": False}, "field report_date"),
    ({"classification": ["Class I"]}, "field classification"),
    ({"openfda": []}, "field openfda"),
    ({"openfda": "{}"}, "field openfda"),
    ([], "expected an object"),
    (None, "expected an object"),
])
def test_rejects_schema_changes_with_record_context(raw_batch_factory, output_root, record, message):
    raw_batch = raw_batch_factory([[[record]]])
    with pytest.raises(ValueError, match=f"source-1.json, record 0.*{message}"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


@pytest.mark.parametrize("content", [b"not JSON", b"[]", b'{"results": {}}', b'{}'])
def test_malformed_json_and_envelopes(raw_batch, output_root, content):
    replace_source(raw_batch, content)
    with pytest.raises(ValueError, match="source-1.json"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


def test_checksum_mismatch(raw_batch, output_root):
    (raw_batch / "source-1.json").write_text('{"results": []}')
    with pytest.raises(ValueError, match="checksum mismatch"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


def test_source_count_mismatch(raw_batch, output_root):
    replace_source(raw_batch, b'{"results": []}')
    with pytest.raises(ValueError, match="record count mismatch"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


@pytest.mark.parametrize(("field", "value"), [
    ("endpoint", "/device/recall"), ("batch_id", "another-batch"),
    ("expected_records", 2), ("actual_records", True),
    ("partitions", []), ("export_date", "invalid"),
])
def test_invalid_batch_metadata(raw_batch, output_root, field, value):
    metadata = load_metadata(raw_batch)
    metadata[field] = value
    write_metadata(raw_batch, metadata)
    with pytest.raises(ValueError, match="Invalid Raw batch metadata"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


@pytest.mark.parametrize("level", ["partition", "file"])
def test_inconsistent_declared_counts(raw_batch, output_root, level):
    metadata = load_metadata(raw_batch)
    partition = metadata["partitions"][0]
    if level == "partition":
        partition["expected_records"] = partition["actual_records"] = 2
    else:
        partition["files"][0]["actual_records"] = 2
    write_metadata(raw_batch, metadata)
    with pytest.raises(ValueError, match="counts do not match"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


@pytest.mark.parametrize("filename", ["../outside.json", "/outside.json", "folder\\file.json", "ingestion_metadata.json"])
def test_unsafe_source_references(raw_batch, output_root, filename):
    metadata = load_metadata(raw_batch)
    metadata["partitions"][0]["files"][0]["filename"] = filename
    write_metadata(raw_batch, metadata)
    with pytest.raises(ValueError, match="invalid source filename"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


def test_duplicate_source_reference(raw_batch, output_root):
    metadata = load_metadata(raw_batch)
    metadata["partitions"][0]["files"] *= 2
    write_metadata(raw_batch, metadata)
    with pytest.raises(ValueError, match="duplicate source filename"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


def test_failed_later_write_cleans_entire_batch(raw_batch_factory, output_root, monkeypatch):
    raw_batch = raw_batch_factory([[[{}], [{}]]])
    original_write = pq.write_table
    calls = 0

    def fail_second_write(table, path, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            path.write_bytes(b"partial parquet")
            raise OSError("disk full")
        return original_write(table, path, **kwargs)

    monkeypatch.setattr(bronze.pq, "write_table", fail_second_write)
    with pytest.raises(OSError, match="disk full"):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


@pytest.mark.parametrize("failure", ["unreadable", "schema", "count"])
def test_failed_readback_cleans_batch(raw_batch, output_root, monkeypatch, failure):
    original_write = pq.write_table

    def write_bad_output(table, path, **kwargs):
        if failure == "unreadable":
            path.write_bytes(b"not parquet")
            return
        if failure == "schema":
            table = table.drop(["openfda"])
        else:
            table = table.slice(0, 0)
        return original_write(table, path, **kwargs)

    monkeypatch.setattr(bronze.pq, "write_table", write_bad_output)
    with pytest.raises((ValueError, pa.ArrowException)):
        bronze.convert_batch(raw_batch, output_root)
    assert_no_published_batch(output_root)


def test_rerun_rejects_and_preserves_existing_output(raw_batch, output_root):
    batch = bronze.convert_batch(raw_batch, output_root)
    original = {file.name: file.read_bytes() for file in batch.iterdir()}
    with pytest.raises(FileExistsError, match="already exists"):
        bronze.convert_batch(raw_batch, output_root)
    assert {file.name: file.read_bytes() for file in batch.iterdir()} == original
    assert not list(output_root.rglob(".bronze-*"))


def test_rejects_output_inside_raw_batch(raw_batch):
    with pytest.raises(ValueError, match="outside the immutable Raw batch"):
        bronze.convert_batch(raw_batch, raw_batch / "nested-output")
    assert not (raw_batch / "nested-output").exists()


def test_cli_success_and_failure(raw_batch, output_root, capsys):
    args = ["--raw-batch", str(raw_batch), "--output-dir", str(output_root)]
    assert bronze.main(args) == 0
    assert "Bronze conversion complete" in capsys.readouterr().out
    assert bronze.main(args) == 1
    assert "already exists" in capsys.readouterr().err
