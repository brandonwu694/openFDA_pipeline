"""Coordinate snapshot ingestion and resumable device enforcement transformations."""

import argparse
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from stat import S_ISDIR
from typing import Literal

from openFDA_pipeline.bronze import raw_to_bronze as bronze
from openFDA_pipeline.gold import device_enforcement as gold
from openFDA_pipeline.ingestion import device_enforcement as ingestion
from openFDA_pipeline.pipeline.validation import (
    STAGES,
    check_lineage,
    validate_completed,
)
from openFDA_pipeline.silver import device_enforcement as silver


@dataclass
class BatchRunResult:
    """Published paths and progress for one snapshot, including partial failures."""

    ingestion_date: str
    batch_id: str
    paths: dict[str, Path] = field(default_factory=dict)
    completed_stages: list[str] = field(default_factory=list)
    skipped_stages: list[str] = field(default_factory=list)


@dataclass
class PipelineRunResult:
    """A successful status, or status=None when attached to a failed run."""

    status: Literal["completed", "noop"] | None = None
    new_raw_batch: Path | None = None
    batches: list[BatchRunResult] = field(default_factory=list)


class PipelineRunError(RuntimeError):
    """A stage failure with original cause and all progress made before failure."""

    def __init__(self, stage: str, batch_path: Path | None, result: PipelineRunResult):
        self.stage = stage
        self.batch_path = batch_path
        self.result = result
        location = f" at {batch_path}" if batch_path is not None else ""
        super().__init__(
            f"Pipeline failed during {stage}{location}; existing batches are preserved"
        )


@contextmanager
def _stage_context(stage, batch_path, result):
    """Attach stage context to failures while preserving their original cause."""
    try:
        yield
    except Exception as error:
        raise PipelineRunError(stage, batch_path, result) from error


class OpenFDADeviceEnforcementPipeline:
    """Run sequentially; use one active runner per data root."""

    def __init__(self, data_root: Path = Path("data")):
        self.data_root = Path(data_root).resolve()

    def _run_ingestion(self) -> Path | None:
        return ingestion.ingest_device_enforcement(self.data_root / "raw", force=False)

    def _run_bronze(self, raw_batch_dir: Path) -> Path:
        return bronze.convert_batch(raw_batch_dir, self.data_root / "bronze")

    def _run_silver(self, bronze_batch_dir: Path) -> Path:
        return silver.clean_batch(bronze_batch_dir, self.data_root / "silver")

    def _run_gold(self, silver_batch_dir: Path) -> Path:
        return gold.build_gold(silver_batch_dir, self.data_root / "gold")

    def _batch_paths(self):
        batches = {}
        for stage in STAGES:
            root = self.data_root / stage / "device_enforcement"
            try:
                ingestion_dirs = sorted(root.iterdir())
            except FileNotFoundError:
                continue
            for ingestion_dir in ingestion_dirs:
                if not ingestion_dir.name.startswith("ingestion_date="):
                    continue
                if not S_ISDIR(ingestion_dir.stat().st_mode):
                    continue
                for path in sorted(ingestion_dir.iterdir()):
                    if path.name.startswith("batch_id="):
                        key = (ingestion_dir.name, path.name)
                        batches.setdefault(key, {})[stage] = path
        return batches

    def _validate_existing_stages(self, existing, batch_result, result):
        metadata_by_stage = {}
        for stage in STAGES:
            if stage not in existing:
                continue
            batch_path = existing[stage]
            with _stage_context(stage, batch_path, result):
                metadata_by_stage[stage] = validate_completed(stage, batch_path)
            batch_result.paths[stage] = batch_path
            if stage == "raw" and batch_path == result.new_raw_batch:
                batch_result.completed_stages.append(stage)
            else:
                batch_result.skipped_stages.append(stage)
        return metadata_by_stage

    def _validate_batch_lineage(self, existing, metadata_by_stage, result):
        first = min(STAGES.index(name) for name in existing)
        for index in range(first + 1, len(STAGES)):
            stage = STAGES[index]
            if stage not in existing:
                continue
            with _stage_context(stage, existing[stage], result):
                previous = STAGES[index - 1]
                if previous not in existing:
                    raise ValueError(
                        f"Published {stage} batch has a gap in its upstream chain; inspect it"
                    )
                check_lineage(
                    stage,
                    metadata_by_stage[stage],
                    existing[previous],
                    metadata_by_stage[previous],
                )

    def _run_missing_stages(self, existing, batch_result, result):
        stage_runners = {
            "bronze": self._run_bronze,
            "silver": self._run_silver,
            "gold": self._run_gold,
        }
        first = min(STAGES.index(name) for name in existing)
        for index in range(first + 1, len(STAGES)):
            stage = STAGES[index]
            if stage in existing:
                continue
            batch_path = (
                self.data_root
                / stage
                / "device_enforcement"
                / f"ingestion_date={batch_result.ingestion_date}"
                / f"batch_id={batch_result.batch_id}"
            )
            with _stage_context(stage, batch_path, result):
                previous = STAGES[index - 1]
                published = stage_runners[stage](batch_result.paths[previous])
            batch_result.paths[stage] = published
            batch_result.completed_stages.append(stage)

    def _process_batch(self, existing, result):
        batch_path = next(iter(existing.values()))
        batch_result = BatchRunResult(
            batch_path.parent.name.removeprefix("ingestion_date="),
            batch_path.name.removeprefix("batch_id="),
        )
        result.batches.append(batch_result)
        # Validate the entire existing chain before creating anything for this batch.
        metadata_by_stage = self._validate_existing_stages(existing, batch_result, result)
        self._validate_batch_lineage(existing, metadata_by_stage, result)
        self._run_missing_stages(existing, batch_result, result)

    def run(self) -> PipelineRunResult:
        result = PipelineRunResult()
        with _stage_context("ingestion", None, result):
            result.new_raw_batch = self._run_ingestion()
        with _stage_context("discovery", None, result):
            batches = self._batch_paths()
        for _, existing in sorted(batches.items()):
            self._process_batch(existing, result)
        result.status = (
            "completed"
            if result.new_raw_batch is not None
            or any(batch.completed_stages for batch in result.batches)
            else "noop"
        )
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    try:
        result = OpenFDADeviceEnforcementPipeline(args.data_root).run()
    except PipelineRunError as error:
        print(f"{error}: {error.__cause__}", file=sys.stderr)
        return 1
    print(f"Pipeline {result.status}: {len(result.batches)} batches checked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
