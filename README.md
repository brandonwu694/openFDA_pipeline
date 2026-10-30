# openFDA Pipeline

Process openFDA medical device recall enforcement data through
**Raw → Bronze → Silver → Gold** for analysis and future machine learning projects.

## Run the complete pipeline

Requires Python 3.11+. Activate your Python environment and run from the
repository root:

```sh
python -m pip install .
python -m openFDA_pipeline.pipeline --data-root data
```

This runs the orchestration code in
[`pipeline/pipeline.py`](src/openFDA_pipeline/pipeline/pipeline.py). The equivalent
installed command is:

```sh
openfda-pipeline --data-root data
```

The runner checks FDA's bulk-download manifest for a new `/device/enforcement`
export, then processes all unfinished snapshots oldest first through Gold.
The first run downloads the full available snapshot and builds all layers;
you do not need to run a separate backfill or call each layer manually.
Subsequent runs check for updates and resume any unfinished downstream work.
Use `--data-root /path/to/data` to change the root for all four layers.

## Reruns and recovery

- **No new export and no unfinished work:** return `noop` without writing data
  or metadata. Existing publications are validated and skipped.
- **Unfinished processing:** reuse completed stages and run only missing
  downstream stages, even if FDA has no new export.
- **Failure:** stop dependent stages and later batches, preserve completed
  outputs, and exit nonzero with stage/batch context. Rerun the same command
  after resolving the error; there are no automatic retries.
- **Damaged or incomplete publications:** report the problem for inspection;
  never overwrite, delete, or silently rebuild existing outputs. Directory access
  errors are reported as discovery failures.

Hidden staging directories are ignored. The runner recognizes batches created
with standalone commands, including those whose earlier layers are absent;
a gap between existing published layers causes an error. Run one instance per
root. Scheduling is external; there is no separate progress database or
persistent run log.

## Output datasets

Each layer keeps snapshots under
`data/<layer>/device_enforcement/ingestion_date=YYYY-MM-DD/batch_id=.../`.
Dates and metadata timestamps use UTC.

| Layer | Published output |
| --- | --- |
| Raw | Original JSON responses and `ingestion_metadata.json` |
| Bronze | Source-preserving Parquet files and `conversion_metadata.json` |
| Silver | `accepted.parquet`, `quarantine.parquet`, `duplicates.parquet`, and `silver_metadata.json` |
| Gold | `recall_detail.parquet`, `recall_monthly.parquet`, `recall_by_firm.parquet`, `recall_by_status.parquet`, and `gold_metadata.json` |

Gold uses Silver accepted records and preserves narratives, warnings, and source
provenance. It adds date dimensions, flagged date intervals, classification
indicators, text features, and summary counts. Targets and training datasets
remain deferred.

Select **one snapshot per analysis**. Exports are complete snapshots and can
include historical corrections; combining batches multiplies recall records.
Event counts are not additive, and monthly/firm Class I shares repeat across
classification rows. Reported status is not a live recall lifecycle measure;
firm addresses do not describe affected geography.

See the [pipeline architecture](docs/PIPELINE.md) and
[Gold contract](docs/PIPELINE.md#device-enforcement-gold-contract) for layer
responsibilities, validation rules, table grains, and metric definitions.

## Explore Gold in a local dashboard

Build Gold with the pipeline first, then install the optional dashboard dependencies
and launch from the repository root:

```sh
python -m pip install -e ".[dashboard]"
python -m streamlit run src/openFDA_pipeline/dashboard/app.py -- --gold-root data/gold
```

Open the local URL printed by Streamlit. The repository's `.streamlit/config.toml`
binds the server to `127.0.0.1` and supplies a neutral, high-contrast light theme.
`--gold-root` selects a different Gold root; it defaults to `data/gold`.
This dashboard runs locally and reads existing files only. It does not run the
pipeline, upload datasets, or rebuild invalid publications. If Gold is empty,
it shows instructions to run the pipeline first.

Choose a snapshot, then use report year or an inclusive report-date range,
classification, recalling firm, and literal text search to explore it. Date mode
chooses which controls appear; results change only when you select **Apply filters**.
Empty classification/firm selections include every category. Snapshot changes and
**Reset filters** restore all records. **Refresh snapshots** discovers new exports
and reloads the selected snapshot without caching query results.

Cards show recall count, distinct event count, Class I count, and Class I share.
Charts show monthly trends and the top ten recalling firms, with expandable data
tables as alternatives. The detail table uses 100-row pages; **Download filtered
CSV** includes all matching records and all detail columns. Select **Prepare
filtered CSV** first to generate the file on demand; navigation reuses it until
the applied filters or snapshot change. Every view uses the
same filters and one snapshot. Definitions and reporting limits appear alongside
the metrics. Snapshot comparison and text-to-SQL are not included.

If the selected snapshot disappears, the dashboard stops showing results and
asks you to select and confirm a replacement rather than switching automatically.

The UI is in `src/openFDA_pipeline/dashboard/app.py`; `dashboard/data.py` owns
snapshot discovery, publication validation, and parameterized DuckDB queries.
Existing Gold schemas and pipeline behavior are unchanged. Dashboard tests use
synthetic snapshots and Streamlit AppTest:

```sh
python -m pip install -e ".[dashboard,test]"
python -m pytest tests/dashboard -q
```

## Use the runner from Python

```python
from pathlib import Path
from openFDA_pipeline.pipeline import OpenFDADeviceEnforcementPipeline, PipelineRunError

pipeline = OpenFDADeviceEnforcementPipeline(data_root=Path("data"))
try:
    result = pipeline.run()
except PipelineRunError as error:
    print(error.stage, error.batch_path, error.result)
    raise
```

`PipelineRunResult` contains `status` (`completed` or `noop`), `new_raw_batch`
(`Path` or `None`), and `batches`. Each batch result includes `ingestion_date`,
`batch_id`, published `paths` by layer, `completed_stages`, and `skipped_stages`.
The newly ingested Raw stage counts as completed. On failure, the exception's
`result` preserves partial progress with `status=None`, and its cause is the
original error.

## Individual stage commands (advanced)

Use these for isolated processing, troubleshooting, or intentional reprocessing.
Normal operation uses the complete pipeline runner above. Each command stops
at its own layer.

| Command | Purpose | Explicit batch selection |
| --- | --- | --- |
| `ingest` | Check for a new export and publish Raw | Not applicable |
| `bronze` | Convert Raw JSON to Parquet | `--raw-batch <raw-batch-directory>` |
| `silver` | Clean, validate, and deduplicate Bronze | `--bronze-batch <bronze-batch-directory>` |
| `gold` | Build analytical tables from Silver | `--silver-batch <silver-batch-directory>` |

Bronze, Silver, and Gold discover the sole pending batch by default. No pending
batch is a successful no-op; several require explicit selection. Unlike the
full runner, these commands do not process the entire backlog. `--output-dir`
changes an individual command's destination root. Existing output batches are
never overwritten; use a separate output root for intentional reprocessing.

`ingest --force` or `backfill` always downloads a fresh Raw snapshot, preserving
older batches. These are optional tools for capturing a fresh copy, including
suspected source corrections under an unchanged advertised export identity.
When using the default Raw root, rerun the full pipeline afterward to process
the new batch through Gold.

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
