import errno
import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openFDA_pipeline.pipeline import pipeline
from openFDA_pipeline.pipeline.validation import METADATA_FILES


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(
        pipeline.ingestion, "ingest_device_enforcement", lambda *args, **kwargs: None
    )
    return tmp_path / "custom-data"


@pytest.fixture
def batch_factory(data_root):
    def create(batch_id="example", ingestion_date="2026-10-23", through="raw"):
        batch = (
            data_root
            / "raw/device_enforcement"
            / f"ingestion_date={ingestion_date}"
            / f"batch_id={batch_id}"
        )
        batch.mkdir(parents=True)
        payload = json.dumps(
            {
                "results": [
                    {
                        "recall_number": "Z-1",
                        "report_date": "20261014",
                        "classification": "Class II",
                        "openfda": {},
                    }
                ]
            }
        ).encode()
        (batch / "source.json").write_bytes(payload)
        metadata = {
            "endpoint": "/device/enforcement",
            "batch_id": batch_id,
            "export_date": "2026-10-21",
            "actual_records": 1,
            "expected_records": 1,
            "partitions": [
                {
                    "source_url": "https://example.test/part.zip",
                    "expected_records": 1,
                    "actual_records": 1,
                    "files": [
                        {
                            "filename": "source.json",
                            "actual_records": 1,
                            "sha256": hashlib.sha256(payload).hexdigest(),
                        }
                    ],
                }
            ],
        }
        (batch / "ingestion_metadata.json").write_text(json.dumps(metadata))
        paths = {"raw": batch}
        for stage, function in (
            ("bronze", pipeline.bronze.convert_batch),
            ("silver", pipeline.silver.clean_batch),
            ("gold", pipeline.gold.build_gold),
        ):
            if pipeline.STAGES.index(stage) > pipeline.STAGES.index(through):
                break
            paths[stage] = function(list(paths.values())[-1], data_root / stage)
        return paths

    return create


def snapshot(root):
    return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def test_new_export_runs_all_stages(data_root, batch_factory, monkeypatch):
    calls = []

    def ingest(output_dir, *, force):
        calls.append((output_dir, force))
        return batch_factory()["raw"]

    monkeypatch.setattr(pipeline.ingestion, "ingest_device_enforcement", ingest)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert calls == [(data_root / "raw", False)]
    assert result.status == "completed"
    assert result.new_raw_batch == result.batches[0].paths["raw"]
    batch = result.batches[0]
    assert batch.completed_stages == ["raw", "bronze", "silver", "gold"]
    assert batch.skipped_stages == []
    assert batch.batch_id == "example" and batch.ingestion_date == "2026-10-23"
    for stage, path in batch.paths.items():
        assert (
            path
            == data_root / stage / "device_enforcement/ingestion_date=2026-10-23/batch_id=example"
        )
        assert (path / METADATA_FILES[stage]).is_file()


def test_complete_snapshot_is_read_only_noop(data_root, batch_factory, monkeypatch):
    batch_factory(through="gold")
    before = snapshot(data_root)

    def unexpected(*args, **kwargs):
        pytest.fail("Completed stages must not run")

    monkeypatch.setattr(pipeline.bronze, "convert_batch", unexpected)
    monkeypatch.setattr(pipeline.silver, "clean_batch", unexpected)
    monkeypatch.setattr(pipeline.gold, "build_gold", unexpected)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert result.status == "noop" and result.new_raw_batch is None
    assert result.batches[0].skipped_stages == list(pipeline.STAGES)
    assert result.batches[0].completed_stages == []
    assert snapshot(data_root) == before


@pytest.mark.parametrize("through", ["raw", "bronze", "silver"])
def test_no_new_export_resumes_missing_stages(data_root, batch_factory, through):
    paths = batch_factory(through=through)
    before = snapshot(data_root)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert result.status == "completed" and result.new_raw_batch is None
    assert result.batches[0].skipped_stages == list(paths)
    assert result.batches[0].completed_stages == list(pipeline.STAGES)[len(paths) :]
    after = snapshot(data_root)
    assert all(after[path] == contents for path, contents in before.items())


@pytest.mark.parametrize("earliest", ["bronze", "silver", "gold"])
def test_recognizes_standalone_downstream_batches(data_root, batch_factory, earliest):
    paths = batch_factory(through=earliest)
    for stage in list(paths)[:-1]:
        shutil.rmtree(data_root / stage)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert result.batches[0].skipped_stages == [earliest]
    assert set(result.batches[0].paths) == set(pipeline.STAGES[pipeline.STAGES.index(earliest) :])
    assert result.status == ("noop" if earliest == "gold" else "completed")


def test_backlog_runs_oldest_first(data_root, batch_factory, monkeypatch):
    batch_factory("new", "2026-10-24", through="silver")
    batch_factory("b", through="bronze")
    batch_factory("a")
    order = []
    original = pipeline.gold.build_gold

    def track(source, output_dir):
        order.append(source.name)
        return original(source, output_dir)

    monkeypatch.setattr(pipeline.gold, "build_gold", track)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert order == ["batch_id=a", "batch_id=b", "batch_id=new"]
    assert [batch.batch_id for batch in result.batches] == ["a", "b", "new"]


@pytest.mark.parametrize(
    ("stage", "module", "function"),
    [
        ("ingestion", pipeline.ingestion, "ingest_device_enforcement"),
        ("bronze", pipeline.bronze, "convert_batch"),
        ("silver", pipeline.silver, "clean_batch"),
        ("gold", pipeline.gold, "build_gold"),
    ],
)
def test_failure_stops_and_next_run_recovers(
    data_root, batch_factory, monkeypatch, stage, module, function
):
    batch_factory("first")
    later = batch_factory("later")["raw"]
    original = getattr(module, function)
    failure = OSError("disk or network failure")

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(module, function, fail)
    with pytest.raises(pipeline.PipelineRunError) as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    error = raised.value
    assert error.stage == stage and error.__cause__ is failure
    assert error.result.status is None
    if stage == "ingestion":
        assert error.batch_path is None and error.result.batches == []
    else:
        assert error.batch_path.name == "batch_id=first"
        assert (
            error.result.batches[0].completed_stages
            == list(pipeline.STAGES)[1 : pipeline.STAGES.index(stage)]
        )
    assert not (data_root / "bronze/device_enforcement" / later.parent.name / later.name).exists()
    before = snapshot(data_root)
    monkeypatch.setattr(module, function, original)
    result = pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert result.status == "completed"
    assert all(snapshot(data_root)[path] == content for path, content in before.items())


@pytest.mark.parametrize("stage", pipeline.STAGES)
@pytest.mark.parametrize("damage", ["identity", "missing-file", "checksum", "incomplete"])
def test_invalid_publications_fail_without_replacement(data_root, batch_factory, stage, damage):
    paths = batch_factory(through=stage)
    batch = paths[stage]
    metadata_path = batch / METADATA_FILES[stage]
    metadata = json.loads(metadata_path.read_text())
    if damage == "incomplete":
        metadata_path.unlink()
    elif damage == "identity":
        metadata["batch_id" if stage == "raw" else "source_batch_id"] = "wrong"
        metadata_path.write_text(json.dumps(metadata))
    else:
        filename = {
            "raw": "source.json",
            "bronze": "part-0001.parquet",
            "silver": "accepted.parquet",
            "gold": "recall_detail.parquet",
        }[stage]
        path = batch / filename
        if damage == "missing-file":
            path.unlink()
        else:
            path.write_bytes(b"damaged")
    before = snapshot(data_root)
    with pytest.raises(pipeline.PipelineRunError) as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert raised.value.stage == stage
    assert snapshot(data_root) == before


def test_lineage_mismatch_is_not_skipped(data_root, batch_factory):
    paths = batch_factory(through="gold")
    metadata_path = paths["silver"] / "silver_metadata.json"
    metadata_path.write_text(metadata_path.read_text() + "\n")
    with pytest.raises(pipeline.PipelineRunError, match="gold") as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert "upstream metadata SHA-256 mismatch" in str(raised.value.__cause__)


def test_recorded_silver_input_hash_is_checked(data_root, batch_factory):
    paths = batch_factory(through="silver")
    path = paths["bronze"] / "part-0001.parquet"
    table = pq.ParquetFile(path).read()
    rows = table.to_pylist()
    rows[0]["reason_for_recall"] = "Altered text with unchanged schema and count"
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    before = snapshot(data_root)
    with pytest.raises(pipeline.PipelineRunError) as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert raised.value.stage == "silver"
    assert "upstream input SHA-256 mismatch" in str(raised.value.__cause__)
    assert snapshot(data_root) == before


def test_gap_is_reported_before_rebuilding(data_root, batch_factory):
    paths = batch_factory(through="silver")
    shutil.rmtree(paths["bronze"])
    before = snapshot(data_root)
    with pytest.raises(pipeline.PipelineRunError) as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert "gap" in str(raised.value.__cause__)
    assert snapshot(data_root) == before


def test_empty_and_staging_only_are_noops(data_root):
    runner = pipeline.OpenFDADeviceEnforcementPipeline(data_root)
    assert runner.run().status == "noop"
    assert not data_root.exists()
    for stage in pipeline.STAGES:
        path = data_root / stage / "device_enforcement/.leftover/batch"
        path.mkdir(parents=True)
        (path / METADATA_FILES[stage]).write_text("invalid")
    before = snapshot(data_root)
    result = runner.run()
    assert result.status == "noop" and result.batches == []
    assert snapshot(data_root) == before


@pytest.mark.parametrize("level", ["layer", "ingestion_date"])
@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EIO])
def test_discovery_reports_directory_errors(
    data_root, batch_factory, monkeypatch, capsys, level, error_number
):
    paths = batch_factory(through="gold")
    inaccessible = paths["silver"].parent
    if level == "layer":
        inaccessible = inaccessible.parent
    before = snapshot(data_root)
    failure = OSError(error_number, "Cannot list directory", str(inaccessible))
    original_iterdir = Path.iterdir

    def fail_listing(path):
        if path == inaccessible:
            raise failure
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_listing)
    with pytest.raises(pipeline.PipelineRunError) as raised:
        pipeline.OpenFDADeviceEnforcementPipeline(data_root).run()
    assert raised.value.stage == "discovery"
    assert raised.value.__cause__ is failure
    assert raised.value.result.status is None
    assert snapshot(data_root) == before
    assert pipeline.main(["--data-root", str(data_root)]) == 1
    message = capsys.readouterr().err
    assert "discovery" in message and str(inaccessible) in message


def test_cli_custom_root_noop_completion_and_failure(data_root, batch_factory, capsys):
    assert pipeline.main(["--data-root", str(data_root)]) == 0
    assert "noop" in capsys.readouterr().out
    batch_factory()
    assert pipeline.main(["--data-root", str(data_root)]) == 0
    assert "completed" in capsys.readouterr().out
    batch_factory("broken")["raw"].joinpath("ingestion_metadata.json").unlink()
    assert pipeline.main(["--data-root", str(data_root)]) == 1
    error = capsys.readouterr().err
    assert "raw" in error and "batch_id=broken" in error and "inspect" in error
