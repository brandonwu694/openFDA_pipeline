"""Run a one-time device enforcement backfill using shared ingestion logic."""

import argparse
import sys
from pathlib import Path

import requests

from openFDA_pipeline.ingestion.device_enforcement import ingest_device_enforcement


def run_backfill(output_dir):
    """Download a fresh complete snapshot, preserving any earlier batches."""
    return ingest_device_enforcement(output_dir, force=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Raw data root (default: data/raw relative to the current directory)",
    )
    args = parser.parse_args(argv)
    try:
        run_backfill(args.output_dir)
    except (requests.RequestException, OSError, ValueError) as error:
        print(
            f"Backfill failed: {error}. Check network access, download contents, "
            "and output permissions, then rerun; existing batches are preserved.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
