import errno
from datetime import date
from io import BytesIO
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from openFDA_pipeline.dashboard.data import (  # noqa: E402
    DashboardDataError,
    Filters,
    RecallData,
    discover_snapshots,
)


def files(root):
    return {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_snapshot_order_and_staging(snapshot_factory, gold_root):
    snapshot_factory(batch_id="a")
    snapshot_factory(batch_id="b")
    snapshot_factory(batch_id="older", export="2026-10-01", ingested="2026-10-25")
    snapshot_factory(batch_id="later", ingested="2026-10-24")
    staging = gold_root / "device_enforcement/.gold-leftover/batch"
    staging.mkdir(parents=True)
    (staging / "gold_metadata.json").write_text("invalid")
    assert [s.batch_id for s in discover_snapshots(gold_root)] == ["later", "b", "a", "older"]


def test_empty_root_and_invalid_publications(snapshot_factory, gold_root):
    assert discover_snapshots(gold_root) == []
    batch = snapshot_factory()
    (batch / "gold_metadata.json").unlink()
    with pytest.raises(DashboardDataError, match="gold_metadata.json"):
        discover_snapshots(gold_root)


def test_metadata_and_output_corruption(snapshot_factory, gold_root):
    batch = snapshot_factory()
    snapshot = discover_snapshots(gold_root)[0]
    (batch / "recall_detail.parquet").write_bytes(b"broken")
    with pytest.raises(DashboardDataError, match="SHA-256"):
        with RecallData(snapshot):
            pass
    (batch / "gold_metadata.json").write_text("invalid")
    with pytest.raises(DashboardDataError, match="Cannot read completed"):
        discover_snapshots(gold_root)


@pytest.mark.parametrize("nested", [False, True])
def test_discovery_access_errors(snapshot_factory, gold_root, monkeypatch, nested):
    batch = snapshot_factory()
    target = batch.parent if nested else batch.parent.parent
    original = Path.iterdir

    def fail(path):
        if path == target:
            raise PermissionError(errno.EACCES, "Denied", str(path))
        return original(path)

    monkeypatch.setattr(Path, "iterdir", fail)
    with pytest.raises(DashboardDataError, match="Cannot discover"):
        discover_snapshots(gold_root)


def test_metrics_filters_trends_and_csv_agree(snapshot_factory, gold_root):
    snapshot_factory()
    original = files(gold_root)
    snapshot = discover_snapshots(gold_root)[0]
    with RecallData(snapshot) as data:
        assert data.metrics() == {
            "recall_count": 3,
            "event_count": 1,
            "class_i_count": 1,
            "classified_recall_count": 2,
            "missing_event_id_count": 1,
            "class_i_share": 0.5,
        }
        assert data.options()["years"] == [2026, 2025]
        data.apply_filters(Filters(date(2025, 1, 1), date(2025, 12, 31)))
        assert data.metrics()["recall_count"] == 2
        monthly = data.monthly()
        assert len(monthly) == 12 and monthly.iloc[1].sum() == 0
        assert monthly.to_numpy().sum() == 2
        assert data.firms().recall_count.sum() == 2
        exported = pd.read_csv(BytesIO(data.csv()))
        assert set(exported.recall_number) == set(data.records().recall_number)
        data.apply_filters(
            Filters(date(2025, 1, 1), date(2025, 3, 31), ("Class I",), ("acme",), "SEAL")
        )
        assert data.metrics()["recall_count"] == len(data.records()) == 1
        assert data.metrics()["class_i_share"] == 1
        assert len(pd.read_csv(BytesIO(data.csv()))) == 1
        data.apply_filters(Filters(classifications=(None,), firms=(None,)))
        assert data.metrics()["recall_count"] == 1
        assert data.metrics()["class_i_share"] is None
        assert data.firms().iloc[0].recalling_firm == "Missing firm"
    assert files(gold_root) == original


@pytest.mark.parametrize(
    "search, count", [("100%_", 1), ("100X_", 0), ("' OR true --", 0), ("z-0001", 1), ("ACME", 2)]
)
def test_literal_search(snapshot_factory, gold_root, search, count):
    snapshot_factory()
    with RecallData(discover_snapshots(gold_root)[0], Filters(search=search)) as data:
        assert data.metrics()["recall_count"] == count


@pytest.mark.parametrize(
    "filters", [Filters(start=date(2025, 1, 1)), Filters(date(2026, 1, 1), date(2025, 1, 1))]
)
def test_invalid_dates(snapshot_factory, gold_root, filters):
    snapshot_factory()
    with RecallData(discover_snapshots(gold_root)[0]) as data:
        with pytest.raises(DashboardDataError):
            data.apply_filters(filters)


def test_pagination_and_inclusive_dates(snapshot_factory, gold_root):
    snapshot_factory([{"report_date": "20250101"} for _ in range(205)])
    with RecallData(
        discover_snapshots(gold_root)[0], Filters(date(2025, 1, 1), date(2025, 1, 1))
    ) as data:
        pages = [data.records(page) for page in range(3)]
        assert [len(frame) for frame in pages] == [100, 100, 5]
        assert pd.concat(pages).recall_number.tolist() == [f"Z-{i:04}" for i in range(205)]
        assert len(pd.read_csv(BytesIO(data.csv()))) == 205


def test_typed_empty_snapshot(snapshot_factory, gold_root):
    snapshot_factory([])
    with RecallData(discover_snapshots(gold_root)[0]) as data:
        assert data.metrics()["recall_count"] == 0
        assert data.metrics()["class_i_share"] is None
        assert data.options()["start"] is None
        assert data.records().empty and data.monthly().empty and data.firms().empty
        assert pd.read_csv(BytesIO(data.csv())).empty
