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

## Bronze conversion

Install or reinstall the package to register the `bronze` command:

```sh
python -m pip install .
bronze --raw-batch data/raw/device_enforcement/ingestion_date=YYYY-MM-DD/batch_id=YOUR_BATCH_ID
```

Replace the date and batch ID with a completed Raw batch. Use `--output-dir` to
override the default `data/bronze/` root. Each source JSON file produces one
Snappy-compressed Parquet file in manifest order:

```text
data/bronze/device_enforcement/
  ingestion_date=YYYY-MM-DD/
    batch_id=YOUR_BATCH_ID/
      part-0001.parquet
      conversion_metadata.json
```

Bronze verifies source checksums and counts, preserves strings and row order,
and adds source batch/file/record-position columns. `openfda` objects are stored
as JSON text, including `{}`; missing keys and explicit nulls become Parquet
nulls. Unknown source fields or incompatible types require schema review.
Cleaning and date parsing remain in Silver.

Outputs are published only after Parquet readback validates schema and counts.
Failed runs remove staging files; reruns reject an existing completed batch.
Use a separate output root for intentional reprocessing. For programmatic DAG
use, call `convert_batch(raw_batch_dir, output_dir)` from
`openFDA_pipeline.bronze.raw_to_bronze`; it returns the completed batch path.
Ignore hidden `.bronze-*` staging directories, which may remain after forced
termination. Select one completed Bronze batch for downstream processing.

## Exploratory notebook

With your Python environment activated, run from the repository root:

```sh
python -m pip install ".[notebook]"
python -m jupyterlab notebooks/01_device_enforcement_eda.ipynb
```

The notebook selects one explicit completed Raw batch in its first code cell.
Update that relative path to explore another batch, then run all cells in order.
It examines structure, missing values, identifiers, dates, and distributions
without changing Raw files or writing datasets. Use the kernel from the same
Python environment. Clear cell outputs before committing the notebook.

## Tests

Install the test dependencies and run the offline pytest suite from the repository root:

```sh
python -m pip install -e ".[test]"
python -m pytest -v
```
