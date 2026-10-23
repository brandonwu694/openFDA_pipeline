import io
import json
from datetime import datetime
from unittest.mock import MagicMock
from zipfile import ZipFile

import pytest
import requests

from openFDA_pipeline.bronze.raw_to_bronze import convert_batch
from openFDA_pipeline.ingestion import device_enforcement as ingestion


@pytest.fixture
def mock_download(monkeypatch):
    session_factory = MagicMock()
    monkeypatch.setattr(ingestion.requests, "Session", session_factory)
    session = session_factory.return_value.__enter__.return_value

    def configure(*, export_date="2026-10-21", counts=(1,), url_suffix="", records=None):
        manifest = {
            "results": {
                "device": {
                    "enforcement": {
                        "export_date": export_date,
                        "total_records": sum(counts),
                        "partitions": [
                            {
                                "file": f"https://download.open.fda.gov/part-{index}{url_suffix}.json.zip",
                                "records": count,
                            }
                            for index, count in enumerate(counts, start=1)
                        ],
                    }
                }
            }
        }
        response = MagicMock()
        response.__enter__.return_value = response
        response.json.return_value = manifest
        responses = [response]
        for index, count in enumerate(counts, start=1):
            buffer = io.BytesIO()
            payload = {
                "results": records
                if records is not None
                else [
                    {"recall_number": f"Z-{index}-{row}", "report_date": "20261014", "openfda": {}}
                    for row in range(count)
                ]
            }
            with ZipFile(buffer, "w") as archive:
                archive.writestr(f"records-{index}.json", json.dumps(payload))
            response = MagicMock()
            response.__enter__.return_value = response
            response.iter_content.return_value = [buffer.getvalue()]
            responses.append(response)
        session.reset_mock()
        session.get.side_effect = responses
        return session

    return configure


def test_initial_ingestion_stamps_utc_and_hands_batch_to_bronze(tmp_path, mock_download):
    mock_download(counts=(1, 2))
    raw_root = tmp_path / "raw"
    batch = ingestion.ingest_device_enforcement(raw_root)
    assert batch is not None
    metadata = json.loads((batch / "ingestion_metadata.json").read_text())
    started_at = datetime.fromisoformat(metadata["ingestion_started_at"])
    assert started_at.utcoffset().total_seconds() == 0
    assert batch.parent.name == f"ingestion_date={started_at.date()}"
    assert batch.name == f"batch_id={metadata['batch_id']}"
    assert metadata["actual_records"] == metadata["expected_records"] == 3
    assert not list(raw_root.rglob(".ingest-*"))
    assert not list(raw_root.rglob("*.zip"))
    assert not (tmp_path / "bronze").exists()
    bronze_batch = convert_batch(batch, tmp_path / "bronze")
    converted = json.loads((bronze_batch / "conversion_metadata.json").read_text())
    assert converted["actual_records"] == 3
    assert converted["source_batch_id"] == metadata["batch_id"]


def test_repeat_export_skips_download_and_leaves_raw_unchanged(tmp_path, mock_download, capsys):
    raw_root = tmp_path / "raw"
    mock_download()
    batch = ingestion.ingest_device_enforcement(raw_root)
    original = {file.name: file.read_bytes() for file in batch.iterdir()}
    session = mock_download()
    assert ingestion.ingest_device_enforcement(raw_root) is None
    session.get.assert_called_once_with(ingestion.MANIFEST_URL, timeout=ingestion.TIMEOUT)
    assert "no new Raw batch created" in capsys.readouterr().out
    assert list(raw_root.rglob("batch_id=*")) == [batch]
    assert {file.name: file.read_bytes() for file in batch.iterdir()} == original


@pytest.mark.parametrize("change", ["date", "count", "url", "partitions"])
def test_changed_manifest_downloads_full_snapshot(tmp_path, mock_download, change):
    raw_root = tmp_path / "raw"
    mock_download()
    first = ingestion.ingest_device_enforcement(raw_root)
    changes = {
        "date": {"export_date": "2026-10-22"},
        "count": {"counts": (2,)},
        "url": {"url_suffix": "-revised"},
        "partitions": {"counts": (1, 1)},
    }
    session = mock_download(**changes[change])
    second = ingestion.ingest_device_enforcement(raw_root)
    assert first != second
    metadata = json.loads((second / "ingestion_metadata.json").read_text())
    assert session.get.call_count == len(metadata["partitions"]) + 1
    assert first.is_dir() and second.is_dir()


def test_force_creates_fresh_batch_for_same_export(tmp_path, mock_download):
    mock_download()
    first = ingestion.ingest_device_enforcement(tmp_path)
    session = mock_download()
    second = ingestion.ingest_device_enforcement(tmp_path, force=True)
    assert first != second
    assert session.get.call_count == 2


@pytest.mark.parametrize("damage", ["missing-file", "checksum", "metadata", "counts"])
def test_damaged_existing_batch_fails_and_force_can_publish_replacement(
    tmp_path, mock_download, damage
):
    mock_download()
    first = ingestion.ingest_device_enforcement(tmp_path)
    if damage == "missing-file":
        (first / "records-1.json").unlink()
    elif damage == "checksum":
        (first / "records-1.json").write_text('{"results": []}')
    elif damage == "metadata":
        (first / "ingestion_metadata.json").write_text("not JSON")
    else:
        path = first / "ingestion_metadata.json"
        metadata = json.loads(path.read_text())
        metadata["actual_records"] = 2
        path.write_text(json.dumps(metadata))
    session = mock_download()
    with pytest.raises(ValueError, match="Cannot verify existing Raw batch.*--force"):
        ingestion.ingest_device_enforcement(tmp_path)
    assert session.get.call_count == 1
    assert len(list(tmp_path.rglob("batch_id=*"))) == 1
    mock_download()
    second = ingestion.ingest_device_enforcement(tmp_path, force=True)
    assert second != first
    assert first.is_dir()


def test_staging_and_incomplete_batches_do_not_suppress_ingestion(tmp_path, mock_download):
    endpoint_dir = tmp_path / "device_enforcement"
    staging = endpoint_dir / ".ingest-abandoned/batch"
    staging.mkdir(parents=True)
    (staging / "ingestion_metadata.json").write_text("not JSON")
    (endpoint_dir / "ingestion_date=2026-10-21/batch_id=incomplete").mkdir(parents=True)
    mock_download()
    assert ingestion.ingest_device_enforcement(tmp_path) is not None
    assert (staging / "ingestion_metadata.json").read_text() == "not JSON"


def test_failed_updated_export_preserves_earlier_snapshot(tmp_path, mock_download):
    mock_download()
    first = ingestion.ingest_device_enforcement(tmp_path)
    original = {file.name: file.read_bytes() for file in first.iterdir()}
    mock_download(export_date="2026-10-22", records=[])
    with pytest.raises(ValueError, match="record count mismatch"):
        ingestion.ingest_device_enforcement(tmp_path)
    assert list(tmp_path.rglob("batch_id=*")) == [first]
    assert not list(tmp_path.rglob(".ingest-*"))
    assert {file.name: file.read_bytes() for file in first.iterdir()} == original


def test_ingest_cli_defaults_and_force(tmp_path, mock_download, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    mock_download()
    assert ingestion.main([]) == 0
    assert len(list((tmp_path / "data/raw").rglob("batch_id=*"))) == 1
    assert not (tmp_path / "data/bronze").exists()
    mock_download()
    assert ingestion.main([]) == 0
    assert "no new Raw batch created" in capsys.readouterr().out
    mock_download()
    assert ingestion.main(["--force"]) == 0
    assert len(list((tmp_path / "data/raw").rglob("batch_id=*"))) == 2


def test_ingest_cli_network_error_does_not_create_output(tmp_path, mock_download, capsys):
    session = mock_download()
    session.get.side_effect = requests.Timeout("manifest timed out")
    assert ingestion.main(["--output-dir", str(tmp_path / "raw")]) == 1
    assert "Ingestion failed" in capsys.readouterr().err
    assert not (tmp_path / "raw").exists()
