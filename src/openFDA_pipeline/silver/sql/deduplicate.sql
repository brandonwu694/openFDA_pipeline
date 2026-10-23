-- Compare original source values only for repeated keys; provenance identifies copies.
CREATE TEMP VIEW duplicate_key_stats AS
SELECT
    trim_scalar(b.recall_number) AS recall_number,
    count(DISTINCT struct_pack(
        address_1 := b.address_1,
        address_2 := b.address_2,
        center_classification_date := b.center_classification_date,
        city := b.city,
        classification := b.classification,
        code_info := b.code_info,
        country := b.country,
        distribution_pattern := b.distribution_pattern,
        event_id := b.event_id,
        initial_firm_notification := b.initial_firm_notification,
        more_code_info := b.more_code_info,
        postal_code := b.postal_code,
        product_description := b.product_description,
        product_quantity := b.product_quantity,
        product_type := b.product_type,
        reason_for_recall := b.reason_for_recall,
        recall_initiation_date := b.recall_initiation_date,
        recall_number := b.recall_number,
        recalling_firm := b.recalling_firm,
        report_date := b.report_date,
        state := b.state,
        status := b.status,
        termination_date := b.termination_date,
        voluntary_mandated := b.voluntary_mandated,
        openfda := b.openfda
    )) AS source_versions
FROM bronze b
JOIN (
    SELECT recall_number
    FROM validated
    WHERE recall_number IS NOT NULL
    GROUP BY recall_number
    HAVING count(*) > 1
) repeated ON trim_scalar(b.recall_number) = repeated.recall_number
GROUP BY trim_scalar(b.recall_number);

CREATE TEMP TABLE dispositioned AS
WITH ranked AS (
    SELECT
        v.* REPLACE (
            CASE WHEN d.source_versions > 1
                 THEN list_append(v._error_codes, 'conflicting_recall_number')
                 ELSE v._error_codes END AS _error_codes
        ),
        row_number() OVER copies AS _duplicate_rank,
        first_value(v._source_file) OVER copies AS _kept_source_file,
        first_value(v._source_record_index) OVER copies AS _kept_source_record_index
    FROM validated v
    LEFT JOIN duplicate_key_stats d ON v.recall_number = d.recall_number
    WINDOW copies AS (
        PARTITION BY v.recall_number
        ORDER BY v._source_file, v._source_record_index
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT
    *,
    CASE
        WHEN len(_error_codes) > 0 THEN 'quarantine'
        WHEN _duplicate_rank > 1 THEN 'duplicates'
        ELSE 'accepted'
    END AS _disposition
FROM ranked;

CREATE TEMP VIEW accepted AS
SELECT * EXCLUDE (_duplicate_rank, _kept_source_file, _kept_source_record_index, _disposition)
FROM dispositioned WHERE _disposition = 'accepted';

CREATE TEMP VIEW quarantine AS
SELECT * EXCLUDE (_duplicate_rank, _kept_source_file, _kept_source_record_index, _disposition)
FROM dispositioned WHERE _disposition = 'quarantine';

CREATE TEMP VIEW duplicates AS
SELECT
    _source_batch_id, _source_file, _source_record_index, recall_number,
    _kept_source_file, _kept_source_record_index
FROM dispositioned WHERE _disposition = 'duplicates';
