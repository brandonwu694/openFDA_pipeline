# openFDA Pipeline

## Project Overview

The goal of this project is to develop a pipeline that ingests, processes, and makes openFDA medical device recall data ready for analysis and machine learning.

## One-time backfill

Requires Python 3.11+. Activate your Python environment and run from the
repository root. Install once (or reinstall to register the new `backfill` command):

```sh
python -m pip install -e .
```

Then run:

```sh
backfill
```

Downloads all `/device/enforcement` partitions from the
[openFDA manifest](https://api.fda.gov/download.json) into a current historical
snapshot. FDA describes the endpoint as covering 2004–present, but the verified
October 21, 2026 export contains 40,300 records with `report_date` values from
June 20, 2012 through October 14, 2026. This is the observed report-date range for
that snapshot, not a filter applied by the script or a guarantee for future exports.

The default output is `data/raw/`, relative to the current working directory.
To use a different location:

```sh
backfill --output-dir /path/to/raw
```

```text
data/raw/device_enforcement/
  ingestion_date=YYYY-MM-DD/
    batch_id=<UTC-timestamp>_<UUID>/
      device-enforcement-0001-of-0001.json
      ingestion_metadata.json
```

Preserves JSON bytes, validates ZIP integrity and record counts, then discards
ZIPs. Separate metadata records provenance, timestamps, counts, and SHA-256
checksums. Only validated batches are published; failed runs clean up temporary
files and exit with a nonzero status. Fix the reported error and rerun to start
again. Successful reruns create new batches without overwriting earlier ones.

Downstream processing should select one completed batch and read the data files
listed in its metadata. Ignore hidden `.backfill-*` staging directories, which
may remain after forced termination. Raw data is excluded from Git; cleaning and
Parquet conversion follow in the [downstream layers](docs/PIPELINE.md).

## Tests

Install the test dependencies and run the offline pytest suite from the repository root:

```sh
python -m pip install -e ".[test]"
python -m pytest -v
```
