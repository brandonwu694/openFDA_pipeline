import shutil
from io import BytesIO

import pytest

pytest.importorskip("streamlit")

from streamlit.testing.v1 import AppTest  # noqa: E402

from openFDA_pipeline.dashboard.data import RecallData  # noqa: E402


def app_for(gold_root):
    def run(root):
        import pathlib

        from openFDA_pipeline.dashboard.app import main

        main(pathlib.Path(root))

    return AppTest.from_function(run, args=(str(gold_root),), default_timeout=20).run()


def button(app, label):
    return next(b for b in app.button if b.label == label)


def test_empty_root_is_actionable(gold_root):
    app = app_for(gold_root)
    assert not app.exception
    assert "Run the pipeline first" in app.info[0].value


def test_filters_apply_together_and_reset(snapshot_factory, gold_root):
    snapshot_factory()
    app = app_for(gold_root)
    assert not app.exception
    assert [m.value for m in app.metric] == ["3", "1", "1", "50.0%"]
    app.text_input(key="filter_search").set_value("100%_")
    assert app.metric[0].value == "3"
    button(app, "Apply filters").click().run()
    assert not app.exception
    assert app.metric[0].value == "1"
    button(app, "Reset filters").click().run()
    assert app.metric[0].value == "3"
    assert app.text_input(key="filter_search").value == ""
    app.text_input(key="filter_search").set_value("no matches")
    button(app, "Apply filters").click().run()
    assert app.metric[0].value == "0"
    assert any("No recalls match" in item.value for item in app.info)


def test_snapshot_change_resets_filters_and_page(snapshot_factory, gold_root):
    snapshot_factory(batch_id="older", export="2026-10-01")
    snapshot_factory(batch_id="newer")
    app = app_for(gold_root)
    assert app.selectbox(key="snapshot").value.endswith("batch_id=newer")
    app.text_input(key="filter_search").set_value("no matches")
    button(app, "Apply filters").click().run()
    app.session_state.page = 2
    older = next(path for path in app.selectbox(key="snapshot").options if "older" in path)
    # Selectbox values are paths; options are formatted labels.
    app.selectbox(key="snapshot").select(
        str(next((gold_root / "device_enforcement").glob("ingestion_date=*/batch_id=older")))
    ).run()
    assert older and not app.exception
    assert app.metric[0].value == "3"
    assert app.session_state.page == 0


def test_year_and_incomplete_range(snapshot_factory, gold_root):
    snapshot_factory()
    app = app_for(gold_root)
    app.selectbox(key="filter_mode").select("Report year").run()
    app.selectbox(key="filter_year").select(2025)
    button(app, "Apply filters").click().run()
    assert app.metric[0].value == "2"
    app.selectbox(key="filter_mode").select("Custom date range").run()
    app.date_input(key="filter_dates").set_value(("2025-01-01",))
    button(app, "Apply filters").click().run()
    assert not app.exception
    assert any("both a start date" in warning.value for warning in app.warning)


def test_corrupt_and_empty_snapshots(snapshot_factory, gold_root):
    batch = snapshot_factory([])
    app = app_for(gold_root)
    assert not app.exception
    assert "no accepted recall" in app.info[0].value
    (batch / "recall_detail.parquet").write_bytes(b"corrupt")
    app.run()
    assert not app.exception
    assert "SHA-256" in app.error[0].value


def test_missing_categories_and_accessible_chart_tables(snapshot_factory, gold_root):
    snapshot_factory()
    app = app_for(gold_root)
    assert len(app.table) == 2
    app.multiselect(key="filter_classifications").select(None)
    app.multiselect(key="filter_firms").select(None)
    button(app, "Apply filters").click().run()
    assert not app.exception
    assert app.metric[0].value == "1"
    assert app.metric[3].value == "N/A"


def test_pagination_controls_and_filter_reset(snapshot_factory, gold_root):
    snapshot_factory([{} for _ in range(205)])
    app = app_for(gold_root)
    assert not app.error, [e.value for e in app.error]
    assert button(app, "Previous page").disabled
    button(app, "Next page").click().run()
    assert not app.exception
    assert app.session_state.page == 1
    assert not button(app, "Previous page").disabled
    button(app, "Next page").click().run()
    assert app.session_state.page == 2
    assert button(app, "Next page").disabled
    assert len(app.dataframe[0].value) == 5
    button(app, "Previous page").click().run()
    assert app.session_state.page == 1
    app.text_input(key="filter_search").set_value("Z-0001")
    button(app, "Apply filters").click().run()
    assert app.session_state.page == 0
    assert app.metric[0].value == "1"


def test_refresh_discovers_new_snapshots(snapshot_factory, gold_root):
    snapshot_factory(batch_id="first")
    app = app_for(gold_root)
    snapshot_factory(batch_id="second", export="2026-10-28")
    button(app, "Refresh snapshots").click().run()
    assert not app.exception
    assert len(app.selectbox(key="snapshot").options) == 2
    assert any("second" in label for label in app.selectbox(key="snapshot").options)


def test_single_unfamiliar_classification_chart(snapshot_factory, gold_root):
    snapshot_factory([{"classification": "Future: [I].", "recalling_firm": "Firm: [A]."}])
    app = app_for(gold_root)
    assert not app.exception
    assert not app.error, [e.value for e in app.error]
    assert len(app.table) == 2
    assert len(app.dataframe[0].value) == 1


def test_csv_is_explicit_reused_and_invalidated(snapshot_factory, gold_root, monkeypatch):
    import pandas as pd

    snapshot_factory([{} for _ in range(105)])
    calls = []
    original = RecallData.csv

    def track(data):
        calls.append(data.filters)
        return original(data)

    monkeypatch.setattr(RecallData, "csv", track)
    app = app_for(gold_root)
    assert not calls
    button(app, "Next page").click().run()
    assert not calls
    button(app, "Prepare filtered CSV").click().run()
    assert not app.exception and not app.error
    assert len(calls) == 1
    assert len(pd.read_csv(BytesIO(app.session_state.prepared_export["contents"]))) == 105
    assert len(app.get("download_button")) == 1
    button(app, "Previous page").click().run()
    assert len(calls) == 1
    app.text_input(key="filter_search").set_value("Z-0001")
    button(app, "Apply filters").click().run()
    assert "prepared_export" not in app.session_state
    assert len(app.get("download_button")) == 0
    assert len(calls) == 1
    button(app, "Prepare filtered CSV").click().run()
    assert len(calls) == 2
    assert len(pd.read_csv(BytesIO(app.session_state.prepared_export["contents"]))) == 1
    button(app, "Reset filters").click().run()
    assert "prepared_export" not in app.session_state
    assert len(calls) == 2


@pytest.mark.parametrize("has_replacement", [False, True])
def test_removed_snapshot_requires_explicit_replacement(
    snapshot_factory, gold_root, has_replacement
):
    older = snapshot_factory(batch_id="older", export="2026-10-01")
    newer = snapshot_factory(batch_id="newer") if has_replacement else None
    app = app_for(gold_root)
    if has_replacement:
        app.selectbox(key="snapshot").select(str(older)).run()
    app.text_input(key="filter_search").set_value("100%_")
    button(app, "Apply filters").click().run()
    button(app, "Prepare filtered CSV").click().run()
    previous_filters = app.session_state.applied_filters
    shutil.rmtree(older)
    button(app, "Refresh snapshots").click().run()
    assert not app.exception and not app.error
    assert any("no longer available" in item.value for item in app.warning)
    assert len(app.metric) == 0
    assert "prepared_export" not in app.session_state
    assert app.session_state.selected_snapshot == str(older)
    assert app.session_state.applied_filters == previous_filters
    button(app, "Refresh snapshots").click().run()
    assert any("no longer available" in item.value for item in app.warning)
    if has_replacement:
        assert app.selectbox(key="replacement_snapshot").value is None
        assert button(app, "Use selected snapshot").disabled
        app.selectbox(key="replacement_snapshot").select(str(newer)).run()
        assert len(app.metric) == 0
        button(app, "Use selected snapshot").click().run()
        assert not app.exception and not app.error
        assert app.selectbox(key="snapshot").value == str(newer)
        assert app.metric[0].value == "3"
        assert app.session_state.applied_filters.search == ""
    else:
        assert any("No replacement snapshots" in item.value for item in app.info)
