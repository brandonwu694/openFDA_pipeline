# openFDA Pipeline

## Project Overview

The goal of this project is to develop a pipeline that ingests, processes, and makes openFDA medical device recall data ready for analysis and machine learning.

## Ingestion

With your Python environment activated, install the package from the repository
root and run the ingestion node:

```sh
python -m pip install .
ingest
```

`ingest` checks FDA's bulk-download manifest for `/device/enforcement`. It
downloads every partition for a newly advertised export and publishes unchanged
JSON under `data/raw/device_enforcement/ingestion_date=YYYY-MM-DD/batch_id=.../`.
The ingestion date and metadata timestamps use UTC. Files and metadata follow
the same contract as the initial backfill.

If the export date, partition URLs, and counts match a completed local batch,
ingestion verifies its source checksums and exits successfully without creating
another batch. Use `ingest --force` to capture a fresh snapshot, including when
you suspect a source revision under the same advertised export identity. A
damaged matching local batch fails explicitly; `--force` downloads a separate
replacement while preserving existing files. Use `--output-dir /path/to/raw`
to select another Raw root.

openFDA bulk updates are complete snapshots that may contain historical
corrections; ingestion downloads all partitions rather than appending only
recently dated recalls. See [FDA's download guidance](https://open.fda.gov/apis/downloads/).

Ingestion stops after Raw publication. Run `bronze` separately to convert a
pending batch. For a DAG, call `ingest_device_enforcement()` from
`openFDA_pipeline.ingestion.device_enforcement`; it returns a new Raw batch
`Path`, or `None` when the advertised export is already stored. Pass a returned
path directly to `convert_batch()`. Retry a failed downstream task against its
existing Raw batch. No scheduler or automatic downstream execution is installed.

## One-time backfill

Requires Python 3.11+. Activate your Python environment and run from the
repository root. Install once (or reinstall to register the new `backfill` command):

```sh
python -m pip install .
```

Then run:

```sh
backfill
```

`backfill` uses the shared ingestion implementation and always downloads a fresh
snapshot. Use `ingest` for subsequent export checks.

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
listed in its metadata. Ignore hidden `.ingest-*` (and older `.backfill-*`)
staging directories, which may remain after forced termination. Raw data is
excluded from Git; cleaning and Parquet conversion follow in the
[downstream layers](docs/PIPELINE.md).

## Bronze conversion

Install or reinstall the package to register the `bronze` command:

```sh
python -m pip install .
bronze
```

From the repository root, `bronze` discovers completed batches under
`data/raw/device_enforcement/` and converts the sole batch without a published
Bronze counterpart. If none are pending, it reports that and exits successfully.
If several are pending, it lists them and requires an explicit selection:

```sh
bronze --raw-batch data/raw/device_enforcement/ingestion_date=YYYY-MM-DD/batch_id=YOUR_BATCH_ID
```

Replace the date and batch ID with the chosen batch. Use `--output-dir` to
override the default `data/bronze/` root; discovery checks for existing outputs
under that root. Each source JSON file produces one Snappy-compressed Parquet
file in manifest order:

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
Failed runs remove staging files. Automatic discovery skips published batches;
explicit `--raw-batch` reruns reject an existing output. A destination without
conversion metadata requires inspection and is never silently skipped.
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
