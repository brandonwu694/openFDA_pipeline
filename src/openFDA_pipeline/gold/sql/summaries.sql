-- Event counts are computed directly at each grain; they are not additive.
CREATE TEMP TABLE recall_monthly AS
WITH counts AS (
    SELECT _source_batch_id, report_month, classification,
        count(*) AS recall_count,
        count(DISTINCT event_id) AS event_count,
        count(*) FILTER (WHERE event_id IS NULL) AS missing_event_id_count,
        count(*) FILTER (WHERE is_class_i) AS class_i_count,
        count(is_class_i) AS classified_recall_count
    FROM recall_detail GROUP BY ALL
), cohorts AS (
    SELECT * EXCLUDE (class_i_count, classified_recall_count),
        (sum(class_i_count) OVER cohort)::BIGINT AS class_i_count,
        (sum(classified_recall_count) OVER cohort)::BIGINT AS classified_recall_count
    FROM counts WINDOW cohort AS (PARTITION BY _source_batch_id, report_month)
)
SELECT *, class_i_count::DOUBLE / nullif(classified_recall_count, 0) AS class_i_share
FROM cohorts;

CREATE TEMP TABLE recall_by_firm AS
WITH counts AS (
    SELECT _source_batch_id, recalling_firm_normalized, classification,
        count(*) AS recall_count,
        count(DISTINCT event_id) AS event_count,
        count(*) FILTER (WHERE event_id IS NULL) AS missing_event_id_count,
        count(*) FILTER (WHERE is_class_i) AS class_i_count,
        count(is_class_i) AS classified_recall_count
    FROM recall_detail GROUP BY ALL
), cohorts AS (
    SELECT * EXCLUDE (class_i_count, classified_recall_count),
        (sum(class_i_count) OVER cohort)::BIGINT AS class_i_count,
        (sum(classified_recall_count) OVER cohort)::BIGINT AS classified_recall_count
    FROM counts WINDOW cohort AS (PARTITION BY _source_batch_id, recalling_firm_normalized)
)
SELECT *, class_i_count::DOUBLE / nullif(classified_recall_count, 0) AS class_i_share
FROM cohorts;

CREATE TEMP TABLE recall_by_status AS
SELECT _source_batch_id, status,
    count(*) AS recall_count,
    count(DISTINCT event_id) AS event_count,
    count(*) FILTER (WHERE event_id IS NULL) AS missing_event_id_count,
    count(*) FILTER (WHERE is_class_i) AS class_i_count,
    count(is_class_i) AS classified_recall_count,
    class_i_count::DOUBLE / nullif(classified_recall_count, 0) AS class_i_share
FROM recall_detail GROUP BY _source_batch_id, status;
