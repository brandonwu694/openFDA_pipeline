import hashlib
import io
import json
from unittest.mock import MagicMock
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

import pytest
import requests

from openFDA_pipeline import backfill
from openFDA_pipeline.ingestion import device_enforcement as ingestion


def make_zip(members):
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_STORED) as archive:
        for filename, content in members:
            archive.writestr(filename, content)
    return buffer.getvalue()


def make_manifest(counts):
    return {
        "results": {
            "device": {
                "enforcement": {
                    "export_date": "2026-10-21",
                    "total_records": sum(counts),
                    "partitions": [
                        {
                            "file": f"https://download.open.fda.gov/part-{index}.json.zip",
                            "records": count,
                        }
                        for index, count in enumerate(counts, start=1)
                    ],
                }
            }
        }
    }


def make_response(payload=None, content=None):
    response = MagicMock()
    response.__enter__.return_value = response
    response.json.return_value = payload
    if content is not None:
        response.iter_content.return_value = [content[:10], b"", content[10:]]
    return response


@pytest.fixture
def session(monkeypatch):
    session_factory = MagicMock()
    monkeypatch.setattr(ingestion.requests, "Session", session_factory)
    return session_factory.return_value.__enter__.return_value


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "raw"


def configure_downloads(session, counts, archives):
    session.get.side_effect = [
        make_response(payload=make_manifest(counts)),
        *(make_response(content=archive) for archive in archives),
    ]


def assert_no_partial_batch(output_dir):
    assert not list(output_dir.rglob("batch_id=*"))
    assert not list(output_dir.rglob(".ingest-*"))
    assert not list(output_dir.rglob("*.zip"))
    assert not list(output_dir.rglob("*.json"))


def test_preserves_bytes_and_records_metadata(session, output_dir):
    content = b'{\n  "meta": {}, "results": [{"recall_number": "Z-1"}]\n}\n'
    configure_downloads(
        session,
        [1],
        [
            make_zip([("records.json", content), ("notes.txt", b"ignored")]),
        ],
    )
    batch = backfill.run_backfill(output_dir)
    assert (batch / "records.json").read_bytes() == content
    assert {file.name for file in batch.iterdir()} == {
        "records.json",
        "ingestion_metadata.json",
    }
    metadata = json.loads((batch / "ingestion_metadata.json").read_text())
    assert metadata["endpoint"] == "/device/enforcement"
    assert metadata["manifest_url"] == ingestion.MANIFEST_URL
    assert metadata["export_date"] == "2026-10-21"
    assert metadata["actual_records"] == 1
    assert metadata["expected_records"] == 1
    assert batch.name == f"batch_id={metadata['batch_id']}"
    assert metadata["ingestion_started_at"].endswith("+00:00")
    assert metadata["ingestion_completed_at"].endswith("+00:00")
    assert metadata["partitions"][0] == {
        "source_url": "https://download.open.fda.gov/part-1.json.zip",
        "expected_records": 1,
        "actual_records": 1,
        "files": [
            {
                "filename": "records.json",
                "actual_records": 1,
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    assert not list(output_dir.rglob("*.zip"))
    assert not list(output_dir.rglob(".ingest-*"))
    session.get.assert_any_call(ingestion.MANIFEST_URL, timeout=ingestion.TIMEOUT)
    session.get.assert_any_call(
        "https://download.open.fda.gov/part-1.json.zip",
        stream=True,
        timeout=ingestion.TIMEOUT,
    )


def test_multiple_partitions_and_files(session, output_dir):
    record = b'{"results": [{}]}'
    configure_downloads(
        session,
        [2, 1],
        [
            make_zip([("folder/a.json", record), ("b.json", record)]),
            make_zip([("c.json", record)]),
        ],
    )
    batch = backfill.run_backfill(output_dir)
    metadata = json.loads((batch / "ingestion_metadata.json").read_text())
    assert metadata["actual_records"] == 3
    assert len(metadata["partitions"]) == 2
    assert (batch / "a.json").is_file()


@pytest.mark.parametrize("field", ["export_date", "partitions", "total_records"])
def test_missing_manifest_fields(field):
    manifest = make_manifest([1])
    del manifest["results"]["device"]["enforcement"][field]
    with pytest.raises(ValueError, match="Invalid.*manifest"):
        ingestion.validate_manifest(manifest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("export_date", "yesterday", id="invalid-date"),
        pytest.param("partitions", [], id="empty-partitions"),
        pytest.param("total_records", True, id="boolean-total"),
        pytest.param("total_records", 2, id="inconsistent-total"),
    ],
)
def test_invalid_manifest_fields(field, value):
    manifest = make_manifest([1])
    manifest["results"]["device"]["enforcement"][field] = value
    with pytest.raises(ValueError):
        ingestion.validate_manifest(manifest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("file", None, id="missing-url"),
        pytest.param("file", "file:///tmp/data.zip", id="non-https-url"),
        pytest.param("records", -1, id="negative-count"),
        pytest.param("records", True, id="boolean-count"),
    ],
)
def test_invalid_partition_fields(field, value):
    manifest = make_manifest([1])
    manifest["results"]["device"]["enforcement"]["partitions"][0][field] = value
    with pytest.raises(ValueError):
        ingestion.validate_manifest(manifest)


@pytest.mark.parametrize("stage", ["manifest", "download", "stream"])
@pytest.mark.parametrize("error_type", [requests.HTTPError, requests.Timeout])
def test_network_errors_exit_nonzero_and_cleanup(session, output_dir, capsys, stage, error_type):
    error = error_type("test network error")
    response = make_response(content=b"partial")
    if stage == "stream":

        def fail_midstream(chunk_size):
            yield b"partial"
            raise error

        response.iter_content.side_effect = fail_midstream
    else:
        response.raise_for_status.side_effect = error
    session.get.side_effect = (
        [response] if stage == "manifest" else [make_response(payload=make_manifest([1])), response]
    )
    assert backfill.main(["--output-dir", str(output_dir)]) == 1
    assert "Backfill failed" in capsys.readouterr().err
    assert_no_partial_batch(output_dir)


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param(b"not a zip", id="invalid-zip"),
        pytest.param(
            make_zip([("a.json", b'{"results": [{}]}')]).replace(
                b'{"results": [{}]}',
                b"x" * len(b'{"results": [{}]}'),
                1,
            ),
            id="corrupt-crc",
        ),
        pytest.param(make_zip([("a.json", b"not JSON")]), id="invalid-json"),
        pytest.param(make_zip([("a.json", b'{"results": {}}')]), id="results-not-list"),
        pytest.param(make_zip([("a.json", b"[]")]), id="envelope-not-object"),
        pytest.param(make_zip([("a.json", b'{"results": []}')]), id="count-mismatch"),
        pytest.param(make_zip([("notes.txt", b"no data")]), id="no-json"),
        pytest.param(make_zip([("../a.json", b"{}")]), id="parent-path"),
        pytest.param(make_zip([("/a.json", b"{}")]), id="absolute-path"),
        pytest.param(make_zip([("folder\\a.json", b"{}")]), id="backslash-path"),
        pytest.param(make_zip([("ingestion_metadata.json", b"{}")]), id="reserved-name"),
        pytest.param(
            make_zip(
                [
                    ("folder/a.json", b'{"results": [{}]}'),
                    ("other/a.json", b'{"results": [{}]}'),
                ]
            ),
            id="duplicate-basename",
        ),
    ],
)
def test_invalid_archives_and_json_never_publish(session, output_dir, archive):
    configure_downloads(session, [1], [archive])
    with pytest.raises(ValueError):
        backfill.run_backfill(output_dir)
    assert_no_partial_batch(output_dir)


def test_duplicate_archive_member(session, output_dir):
    record = b'{"results": [{}]}'
    with pytest.warns(UserWarning, match="Duplicate name"):
        archive = make_zip([("a.json", record), ("a.json", record)])
    configure_downloads(session, [1], [archive])
    with pytest.raises(ValueError, match="Duplicate"):
        backfill.run_backfill(output_dir)
    assert_no_partial_batch(output_dir)


def test_duplicate_filename_across_partitions(session, output_dir):
    archive = make_zip([("a.json", b'{"results": [{}]}')])
    configure_downloads(session, [1, 1], [archive, archive])
    with pytest.raises(ValueError, match="Duplicate"):
        backfill.run_backfill(output_dir)
    assert_no_partial_batch(output_dir)


def test_corrupt_deflate_stream_exits_cleanly(session, output_dir, capsys):
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("a.json", b'{"results": [{}]}')
    content = bytearray(buffer.getvalue())
    # The local header is 30 bytes, followed by the six-byte filename.
    content[36] = 0xFF  # Invalid DEFLATE block type.
    configure_downloads(session, [1], [bytes(content)])
    assert backfill.main(["--output-dir", str(output_dir)]) == 1
    assert "Cannot read ZIP archive" in capsys.readouterr().err
    assert_no_partial_batch(output_dir)


def test_reruns_and_later_failure_preserve_existing_batch(session, output_dir):
    archive = make_zip([("a.json", b'{"results": [{}]}')])
    batches = []
    for _ in range(2):
        configure_downloads(session, [1], [archive])
        batches.append(backfill.run_backfill(output_dir))
    assert batches[0] != batches[1]
    original = {path: path.read_bytes() for batch in batches for path in batch.iterdir()}
    configure_downloads(session, [1, 1], [archive, b"invalid zip"])
    with pytest.raises(ValueError):
        backfill.run_backfill(output_dir)
    assert set(output_dir.rglob("batch_id=*")) == set(batches)
    assert not list(output_dir.rglob(".ingest-*"))
    for path, content in original.items():
        assert path.read_bytes() == content
