"""Discover Gold snapshots and query one snapshot with shared analytical filters."""

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from stat import S_ISDIR

import duckdb
import pandas as pd

from openFDA_pipeline.pipeline.validation import validate_completed

PAGE_SIZE = 100
CLASSIFICATIONS = ("Class I", "Class II", "Class III", "Not Yet Classified")
DETAIL_COLUMNS = (
    "recall_number",
    "recalling_firm",
    "classification",
    "status",
    "report_date",
    "recall_initiation_date",
    "product_description",
    "reason_for_recall",
)


class DashboardDataError(ValueError):
    """An actionable discovery, validation, or query failure."""


@dataclass(frozen=True)
class Snapshot:
    path: Path
    export_date: date
    ingestion_date: date
    batch_id: str

    @property
    def label(self):
        return f"Export {self.export_date} · ingested {self.ingestion_date} · {self.batch_id}"


@dataclass(frozen=True)
class Filters:
    start: date | None = None
    end: date | None = None
    classifications: tuple[str | None, ...] = ()
    firms: tuple[str | None, ...] = ()
    search: str = ""


def discover_snapshots(gold_root: Path) -> list[Snapshot]:
    """List complete snapshots newest first; report invalid publications explicitly."""
    endpoint = Path(gold_root).resolve() / "device_enforcement"
    try:
        directories = sorted(endpoint.iterdir())
    except FileNotFoundError:
        return []
    except OSError as error:
        raise DashboardDataError(f"Cannot discover snapshots in {endpoint}: {error}") from error
    snapshots = []
    try:
        for directory in directories:
            if not directory.name.startswith("ingestion_date="):
                continue
            if not S_ISDIR(directory.stat().st_mode):
                continue
            for batch in sorted(directory.iterdir()):
                if not batch.name.startswith("batch_id="):
                    continue
                metadata_path = batch / "gold_metadata.json"
                try:
                    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                    export = date.fromisoformat(metadata["source_export_date"])
                    ingested = date.fromisoformat(metadata["source_ingestion_date"])
                    batch_id = metadata["source_batch_id"]
                    if (
                        metadata["endpoint"] != "/device/enforcement"
                        or not isinstance(batch_id, str)
                        or not batch_id
                        or batch.name != f"batch_id={batch_id}"
                        or directory.name != f"ingestion_date={ingested}"
                    ):
                        raise ValueError("endpoint or batch directory identity mismatch")
                except (OSError, ValueError, TypeError, KeyError) as error:
                    raise DashboardDataError(
                        f"Cannot read completed Gold snapshot {batch}: {error}. "
                        "Inspect its gold_metadata.json and published files."
                    ) from error
                snapshots.append(Snapshot(batch, export, ingested, batch_id))
    except OSError as error:
        raise DashboardDataError(f"Cannot discover snapshots in {endpoint}: {error}") from error
    return sorted(
        snapshots, key=lambda s: (s.export_date, s.ingestion_date, s.batch_id), reverse=True
    )


def category_label(value, kind):
    # Explicit prefixes keep a missing category distinct from literal source labels.
    return f"Missing {kind}" if pd.isna(value) else f"{kind.capitalize()}: {value}"


def _where(filters):
    clauses, parameters = [], []
    if (filters.start is None) != (filters.end is None):
        raise DashboardDataError("Select both a start date and an end date.")
    if filters.start is not None:
        if filters.end < filters.start:
            raise DashboardDataError("End date must be on or after start date.")
        clauses.append("report_date BETWEEN ? AND ?")
        parameters.extend([filters.start, filters.end])
    for column, values in (
        ("classification", filters.classifications),
        ("recalling_firm_normalized", filters.firms),
    ):
        if values:
            nonnull = [value for value in values if value is not None]
            choices = []
            if nonnull:
                choices.append(f"{column} IN ({', '.join('?' for _ in nonnull)})")
                parameters.extend(nonnull)
            if None in values:
                choices.append(f"{column} IS NULL")
            clauses.append("(" + " OR ".join(choices) + ")")
    if filters.search:
        columns = ("recall_number", "recalling_firm", "product_description", "reason_for_recall")
        clauses.append(
            "("
            + " OR ".join(
                f"contains(lower(coalesce({column}, '')), lower(?))" for column in columns
            )
            + ")"
        )
        parameters.extend([filters.search] * len(columns))
    return " AND ".join(clauses) or "true", parameters


class RecallData:
    """One validated snapshot and one filtered table; use as a context manager."""

    def __init__(self, snapshot: Snapshot, filters: Filters = Filters()):
        self.snapshot = snapshot
        self.filters = filters
        self.connection = None

    def __enter__(self):
        try:
            validate_completed("gold", self.snapshot.path)
            self.connection = duckdb.connect()
            self.connection.read_parquet(
                str(self.snapshot.path / "recall_detail.parquet"), hive_partitioning=False
            ).create_view("recalls")
            self.apply_filters(self.filters)
        except Exception as error:
            self.__exit__(None, None, None)
            raise DashboardDataError(
                f"Cannot open Gold snapshot {self.snapshot.path}: {error}. "
                "Inspect the publication; the dashboard will not rebuild or replace it."
            ) from error
        return self

    def __exit__(self, *args):
        if self.connection is not None:
            self.connection.close()

    def apply_filters(self, filters):
        where, parameters = _where(filters)
        self.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE filtered AS SELECT * FROM recalls WHERE {where}",
            parameters,
        ).fetchall()
        self.filters = filters

    def options(self):
        start, end = self.connection.execute(
            "SELECT min(report_date), max(report_date) FROM recalls"
        ).fetchall()[0]
        years = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT year(report_date)::INTEGER FROM recalls ORDER BY 1 DESC"
            ).fetchall()
        ]
        categories = {}
        for key, column in (
            ("classifications", "classification"),
            ("firms", "recalling_firm_normalized"),
        ):
            categories[key] = [
                row[0]
                for row in self.connection.execute(
                    f"SELECT DISTINCT {column} FROM recalls ORDER BY {column} NULLS LAST"
                ).fetchall()
            ]
        categories["classifications"].sort(
            key=lambda v: (
                CLASSIFICATIONS.index(v) if v in CLASSIFICATIONS else len(CLASSIFICATIONS),
                v is None,
                v or "",
            )
        )
        return {"start": start, "end": end, "years": years, **categories}

    def metrics(self):
        counts = self.connection.execute(
            """SELECT count(*), count(DISTINCT event_id),
                      count(*) FILTER (WHERE is_class_i), count(is_class_i),
                      count(*) FILTER (WHERE event_id IS NULL)
               FROM filtered"""
        ).fetchall()[0]
        return dict(
            zip(
                (
                    "recall_count",
                    "event_count",
                    "class_i_count",
                    "classified_recall_count",
                    "missing_event_id_count",
                    "class_i_share",
                ),
                (*counts, counts[2] / counts[3] if counts[3] else None),
            )
        )

    def monthly(self):
        frame = self.connection.execute(
            """SELECT date_trunc('month', report_date)::DATE AS report_month,
                      classification, count(*) AS recall_count
               FROM filtered GROUP BY ALL ORDER BY report_month, classification"""
        ).df()
        if frame.empty:
            return frame
        start, end = self.filters.start, self.filters.end
        if start is None:
            start, end = self.connection.execute(
                "SELECT min(report_date), max(report_date) FROM recalls"
            ).fetchall()[0]
        months = pd.date_range(
            pd.Timestamp(start).replace(day=1), pd.Timestamp(end).replace(day=1), freq="MS"
        )
        frame["classification"] = frame["classification"].map(
            lambda value: category_label(value, "classification")
        )
        wide = frame.pivot(index="report_month", columns="classification", values="recall_count")
        return wide.reindex(months).fillna(0).astype("int64").rename_axis("report_month")

    def firms(self):
        frame = self.connection.execute(
            """SELECT recalling_firm_normalized, count(*) AS recall_count
               FROM filtered GROUP BY recalling_firm_normalized
               ORDER BY recall_count DESC, recalling_firm_normalized NULLS LAST LIMIT 10"""
        ).df()
        frame["recalling_firm"] = frame.pop("recalling_firm_normalized").map(
            lambda value: category_label(value, "firm")
        )
        return frame[["recalling_firm", "recall_count"]]

    def records(self, page=0):
        if type(page) is not int or page < 0:
            raise DashboardDataError("Page must be a nonnegative integer.")
        return self.connection.execute(
            f"SELECT {', '.join(DETAIL_COLUMNS)} FROM filtered "
            "ORDER BY report_date DESC, recall_number LIMIT ? OFFSET ?",
            [PAGE_SIZE, page * PAGE_SIZE],
        ).df()

    def csv(self):
        return (
            self.connection.execute(
                "SELECT * FROM filtered ORDER BY report_date DESC, recall_number"
            )
            .df()
            .to_csv(index=False)
            .encode("utf-8")
        )
