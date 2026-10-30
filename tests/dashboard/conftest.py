import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openFDA_pipeline.bronze.raw_to_bronze import BRONZE_SCHEMA
from openFDA_pipeline.gold.device_enforcement import build_gold
from openFDA_pipeline.silver.device_enforcement import clean_batch


@pytest.fixture
def gold_root(tmp_path):
    return tmp_path / "gold"


@pytest.fixture
def snapshot_factory(tmp_path, gold_root):
    def create(records=None, batch_id="example", export="2026-10-21", ingested="2026-10-23"):
        if records is None:
            records = [
                {
                    "classification": "Class I",
                    "report_date": "20250101",
                    "product_description": "Seal 100%_test",
                    "event_id": "shared",
                },
                {"classification": "Class II", "report_date": "20250331", "event_id": "shared"},
                {
                    "classification": None,
                    "report_date": "20260101",
                    "recalling_firm": None,
                    "event_id": None,
                },
            ]
        bronze = tmp_path / "bronze" / f"ingestion_date={ingested}" / f"batch_id={batch_id}"
        bronze.mkdir(parents=True)
        rows = [
            {
                "recall_number": f"Z-{i:04}",
                "event_id": str(i),
                "report_date": "20250101",
                "recall_initiation_date": "20241201",
                "classification": "Class II",
                "status": "Ongoing",
                "openfda": "{}",
                "recalling_firm": "Acme",
                **record,
                "_source_batch_id": batch_id,
                "_source_file": "source.json",
                "_source_record_index": i,
            }
            for i, record in enumerate(records)
        ]
        pq.write_table(pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA), bronze / "part.parquet")
        (bronze / "conversion_metadata.json").write_text(
            json.dumps(
                {
                    "endpoint": "/device/enforcement",
                    "source_batch_id": batch_id,
                    "source_ingestion_date": ingested,
                    "source_export_date": export,
                    "schema_version": 1,
                    "expected_records": len(rows),
                    "actual_records": len(rows),
                    "files": [
                        {
                            "source_file": "source.json",
                            "output_file": "part.parquet",
                            "expected_records": len(rows),
                            "actual_records": len(rows),
                        }
                    ],
                }
            )
        )
        silver = clean_batch(bronze, tmp_path / "silver")
        return build_gold(silver, gold_root)

    return create
