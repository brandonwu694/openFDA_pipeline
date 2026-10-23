# openFDA Pipeline Design Doc

## Architecture Overview

The pipeline follows a four-stage data processing architecture: **Raw → Bronze → Silver → Gold**.

Each stage has a distinct responsibility, separating source preservation, storage optimization, data cleaning, and application-specific transformations.

### Raw Layer — Source Preservation

**Purpose:** Preserve original openFDA API responses exactly as received to ensure reproducibility, traceability, and recoverability.

**Responsibilities:**
- Store unmodified JSON responses from the openFDA API.
- Organize files by endpoint, ingestion date, and batch.
- Maintain ingestion metadata separately, including timestamps, source endpoints, and batch identifiers.
- Retain historical source responses to support reprocessing and auditing.

**Output:** Immutable, original JSON files.

### Bronze Layer — Format Conversion

**Purpose:** Convert raw JSON records into a structured, columnar storage format for efficient downstream processing.

**Responsibilities:**
- Parse raw JSON responses and extract individual records.
- Convert JSON records into Apache Parquet format.
- Preserve source fields and values without applying business logic or data cleaning.
- Retain source identifiers and references to original raw files.
- Record schema information and conversion metadata.

**Output:** Parquet datasets representing the original source records.

### Silver Layer — Data Cleaning and Standardization

**Purpose:** Transform Bronze records into consistent, validated, analysis-ready datasets.

**Responsibilities:**
- Deduplicate records using appropriate source identifiers and versioning rules.
- Enforce data types, including dates, numeric fields, and categorical attributes.
- Handle missing values through documented, field-specific policies.
- Standardize manufacturer names, device categories, and relevant text fields.
- Validate data integrity, schema consistency, and accepted values.
- Flag or quarantine records that fail critical data quality checks.

**Output:** Cleaned and standardized Parquet tables.

### Gold Layer — Analytics and Business Logic

**Purpose:** Create curated datasets supporting domain-specific analytics, machine learning, and application features.

**Responsibilities:**
- Aggregate recall records by manufacturer, device category, classification, and time period.
- Calculate metrics and trends in reported recall activity.
- Generate machine learning features from structured and unstructured fields.
- Prepare model-ready datasets, including target variables and temporally appropriate training splits.
- Store model predictions and associated metadata.
- Create application-specific tables for dashboards and analytical queries.

**Output:** Curated analytical tables, ML feature datasets, and prediction results.