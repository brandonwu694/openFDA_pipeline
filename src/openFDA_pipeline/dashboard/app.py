"""Local dashboard: streamlit run app.py -- --gold-root data/gold."""

import argparse
from datetime import date
from pathlib import Path

import altair as alt
import duckdb
import pyarrow as pa
import streamlit as st

from openFDA_pipeline.dashboard.data import (
    PAGE_SIZE,
    DashboardDataError,
    Filters,
    RecallData,
    category_label,
    discover_snapshots,
)


def reset_filters():
    for key in list(st.session_state):
        if key.startswith("filter_"):
            del st.session_state[key]
    st.session_state.applied_filters = Filters()
    st.session_state.page = 0
    st.session_state.pop("prepared_export", None)


def change_page(direction):
    st.session_state.page += direction


def render_filters(options):
    st.subheader("Explore recalls")
    st.caption("Choose filters, then select Apply filters. Dates refer to the recall report date.")
    # Mode configures the form; it does not change the applied dataset until submission.
    mode = st.selectbox(
        "Date filter", ["All dates", "Report year", "Custom date range"], key="filter_mode"
    )
    with st.form("recall_filters"):
        year, dates = None, ()
        if mode == "Report year":
            year = st.selectbox("Report year", options["years"], key="filter_year")
        elif mode == "Custom date range":
            dates = st.date_input(
                "Report date range",
                value=(options["start"], options["end"]),
                format="YYYY-MM-DD",
                key="filter_dates",
            )
        classes = st.multiselect(
            "Classification",
            options["classifications"],
            key="filter_classifications",
            format_func=lambda value: category_label(value, "classification"),
            help="Leave empty to include every classification, including missing values.",
        )
        firms = st.multiselect(
            "Recalling firm",
            options["firms"],
            key="filter_firms",
            format_func=lambda value: category_label(value, "firm"),
            help="Groups normalized firm names, not corporate entities. Empty includes all firms.",
        )
        search = st.text_input(
            "Search recalls",
            key="filter_search",
            help="Literal, case-insensitive search of recall number, firm, description, and reason.",
        )
        submitted = st.form_submit_button("Apply filters", type="primary")
    st.button("Reset filters", on_click=reset_filters)
    if submitted:
        start, end = None, None
        if mode == "Report year":
            start, end = date(year, 1, 1), date(year, 12, 31)
        elif mode == "Custom date range":
            if len(dates) != 2:
                st.warning("Select both a start date and an end date, then apply filters.")
                return False
            start, end = dates
        if start is not None and end < start:
            st.warning("End date must be on or after start date.")
            return False
        st.session_state.applied_filters = Filters(start, end, tuple(classes), tuple(firms), search)
        st.session_state.page = 0


def render_metrics(metrics, filters):
    st.subheader("Recall overview")
    interval = f"{filters.start} to {filters.end}" if filters.start else "All report dates"
    classifications = ", ".join(
        category_label(v, "classification") for v in filters.classifications
    )
    firms = ", ".join(category_label(v, "firm") for v in filters.firms)
    st.caption(
        f"Applied filters: {interval} · Classifications: {classifications or 'all'} · "
        f"Firms: {firms or 'all'} · Search: {filters.search or 'none'}"
    )
    # Two columns per row keep values legible in narrow windows and at high zoom.
    for row in (
        (
            ("Recall count", f"{metrics['recall_count']:,}"),
            ("Event count", f"{metrics['event_count']:,}"),
        ),
        (
            ("Class I count", f"{metrics['class_i_count']:,}"),
            (
                "Class I share",
                "N/A" if metrics["class_i_share"] is None else f"{metrics['class_i_share']:.1%}",
            ),
        ),
    ):
        for column, (label, value) in zip(st.columns(2), row):
            column.metric(label, value)
    st.caption(
        "Recall count counts recall numbers; event count counts distinct non-missing event IDs. "
        f"{metrics['missing_event_id_count']:,} matching recalls have no event ID. "
        f"Class I share uses {metrics['classified_recall_count']:,} Class I/II/III recalls "
        "in the filtered dataset; N/A means there is no eligible denominator."
    )


def render_charts(data):
    st.subheader("Monthly recall trends")
    st.caption("Recall count by report month and classification. Months without records show zero.")
    monthly = data.monthly()
    palette = {
        "Classification: Class I": "#334155",
        "Classification: Class II": "#64748B",
        "Classification: Class III": "#57534E",
        "Classification: Not Yet Classified": "#71717A",
        "Missing classification": "#475569",
    }
    # Explicit fields keep source category labels out of Altair's field/type shorthand.
    trend = monthly.reset_index().melt(
        id_vars="report_month", var_name="classification", value_name="recall_count"
    )
    chart = (
        alt.Chart(trend)
        .mark_line(point=True)
        .encode(
            x=alt.X(field="report_month", type="temporal", title="Report month"),
            y=alt.Y(field="recall_count", type="quantitative", title="Recall count"),
            color=alt.Color(
                field="classification",
                type="nominal",
                title="Classification",
                scale=alt.Scale(
                    domain=list(monthly.columns),
                    range=[palette.get(column, "#78716C") for column in monthly.columns],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    field="report_month", type="temporal", title="Report month", format="%Y-%m"
                ),
                alt.Tooltip(field="classification", type="nominal", title="Classification"),
                alt.Tooltip(field="recall_count", type="quantitative", title="Recall count"),
            ],
        )
        .properties(description="Monthly recall counts by report month and classification.")
    )
    st.altair_chart(chart, width="stretch")
    with st.expander("Monthly trend data table"):
        st.table(monthly)
    st.subheader("Top 10 recalling firms")
    st.caption("Reported recall activity, not comparative safety or device failure rates.")
    firms = data.firms()
    firm_chart = (
        alt.Chart(firms)
        .mark_bar(color="#475569")
        .encode(
            x=alt.X(field="recall_count", type="quantitative", title="Recall count"),
            y=alt.Y(
                field="recalling_firm",
                type="nominal",
                title="Recalling firm",
                sort=firms["recalling_firm"].tolist(),
            ),
            tooltip=[
                alt.Tooltip(field="recalling_firm", type="nominal", title="Recalling firm"),
                alt.Tooltip(field="recall_count", type="quantitative", title="Recall count"),
            ],
        )
        .properties(description="Top ten recalling firms ranked by reported recall count.")
    )
    st.altair_chart(firm_chart, width="stretch")
    with st.expander("Firm counts data table"):
        st.table(firms)


def render_details(data, count):
    st.subheader("Recall details")
    st.caption("Reported status does not establish whether a recall is currently active.")
    pages = max(1, (count + PAGE_SIZE - 1) // PAGE_SIZE)
    st.session_state.page = min(st.session_state.page, pages - 1)
    previous, following = st.columns(2)
    previous.button(
        "Previous page", disabled=st.session_state.page == 0, on_click=change_page, args=(-1,)
    )
    following.button(
        "Next page", disabled=st.session_state.page >= pages - 1, on_click=change_page, args=(1,)
    )
    st.caption(f"Page {st.session_state.page + 1} of {pages} · up to {PAGE_SIZE} rows per page")
    st.dataframe(
        data.records(st.session_state.page),
        hide_index=True,
        width="stretch",
        column_config={
            "report_date": st.column_config.DateColumn("Report date", format="YYYY-MM-DD"),
            "recall_initiation_date": st.column_config.DateColumn(
                "Initiation date", format="YYYY-MM-DD"
            ),
            "recall_number": "Recall number",
            "recalling_firm": "Recalling firm",
            "classification": "Classification",
            "status": "Reported status",
            "product_description": "Product description",
            "reason_for_recall": "Recall reason",
        },
    )
    export_key = (str(data.snapshot.path), data.filters)
    prepared = st.session_state.get("prepared_export")
    if prepared is not None and prepared["key"] != export_key:
        st.session_state.pop("prepared_export")
        prepared = None
    if st.button(
        "Prepare filtered CSV", help="Generate all matching rows and columns for download."
    ):
        with st.spinner("Preparing filtered CSV…"):
            prepared = {"key": export_key, "contents": data.csv()}
            st.session_state.prepared_export = prepared
    if prepared is not None:
        st.download_button(
            "Download filtered CSV",
            data=prepared["contents"],
            mime="text/csv",
            file_name=f"recalls_{data.snapshot.batch_id}.csv",
            on_click="ignore",
            help="All matching rows and all detail columns, not just the displayed page.",
        )


def main(gold_root=Path("data/gold")):
    st.set_page_config(page_title="Medical Device Recall Explorer", layout="centered")
    st.title("Medical Device Recall Explorer")
    st.caption("Explore one published snapshot of FDA medical device recall enforcement records.")
    st.button("Refresh snapshots", help="Discover newly completed Gold batches and reload data.")
    try:
        with st.spinner("Discovering snapshots…"):
            snapshots = discover_snapshots(gold_root)
        paths = [str(snapshot.path) for snapshot in snapshots]
        by_path = {str(snapshot.path): snapshot for snapshot in snapshots}
        previous = st.session_state.get("snapshot", st.session_state.get("selected_snapshot"))
        if previous is not None and previous not in paths:
            st.session_state.pop("prepared_export", None)
            st.warning(
                f"The previously selected snapshot is no longer available: {previous}. "
                "Choose a replacement explicitly to continue."
            )
            if paths:
                if st.session_state.get("replacement_snapshot") not in paths:
                    st.session_state.replacement_snapshot = None
                replacement = st.selectbox(
                    "Replacement snapshot",
                    paths,
                    index=None,
                    key="replacement_snapshot",
                    format_func=lambda path: by_path[path].label,
                )
                if st.button("Use selected snapshot", disabled=replacement is None):
                    st.session_state.snapshot = replacement
                    st.rerun()
            else:
                st.info(
                    "No replacement snapshots are available. Restore the snapshot or run the pipeline, then refresh."
                )
            return
        if not snapshots:
            st.info(
                "No completed Gold snapshots found. Run the pipeline first, then refresh snapshots."
            )
            st.code("python -m openFDA_pipeline.pipeline --data-root data", language="shell")
            st.caption(f"Dashboard Gold root: {Path(gold_root).resolve()}")
            return
        if "snapshot" not in st.session_state:
            st.session_state.snapshot = previous if previous is not None else paths[0]
        selected = st.selectbox(
            "Snapshot", paths, key="snapshot", format_func=lambda path: by_path[path].label
        )
        snapshot = by_path[selected]
        st.caption(
            f"Export date: {snapshot.export_date} · Ingestion date (UTC): {snapshot.ingestion_date}"
        )
        if st.session_state.get("selected_snapshot") != selected:
            reset_filters()
            st.session_state.selected_snapshot = selected
        with st.spinner("Validating and loading the selected snapshot…"):
            data = RecallData(snapshot)
            with data:
                options = data.options()
                if options["start"] is None:
                    st.info("This snapshot contains no accepted recall records.")
                    return
                if render_filters(options) is False:
                    return
                filters = st.session_state.applied_filters
                data.apply_filters(filters)
                metrics = data.metrics()
                st.write(f"**{metrics['recall_count']:,} matching recall records**")
                render_metrics(metrics, filters)
                if metrics["recall_count"]:
                    render_charts(data)
                else:
                    st.info(
                        "No recalls match the applied filters. Adjust them or select Reset filters."
                    )
                render_details(data, metrics["recall_count"])
    except (DashboardDataError, OSError, ValueError, duckdb.Error, pa.ArrowException) as error:
        st.error(str(error))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", type=Path, default=Path("data/gold"))
    main(parser.parse_args().gold_root)
