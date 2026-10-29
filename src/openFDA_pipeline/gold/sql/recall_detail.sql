CREATE TEMP MACRO normalized_text(value) AS
    nullif(trim(regexp_replace(value, '[\p{Z}\x{0009}-\x{000D}\x{0085}]+', ' ', 'g')), '');

CREATE TEMP MACRO interval_flag(start_date, end_date) AS
    CASE WHEN start_date IS NULL OR end_date IS NULL THEN 'missing_date'
         WHEN end_date < start_date THEN 'negative_interval'
         ELSE 'valid' END;

CREATE TEMP MACRO interval_days(start_date, end_date) AS
    CASE WHEN end_date >= start_date THEN date_diff('day', start_date, end_date)
         ELSE NULL::BIGINT END;

CREATE TEMP TABLE recall_detail AS
SELECT
    *,
    year(report_date)::INTEGER AS report_year,
    quarter(report_date)::INTEGER AS report_quarter,
    date_trunc('month', report_date)::DATE AS report_month,
    date_trunc('month', recall_initiation_date)::DATE AS initiation_month,
    date_trunc('month', center_classification_date)::DATE AS classification_month,
    interval_days(recall_initiation_date, report_date) AS initiation_to_report_days,
    interval_flag(recall_initiation_date, report_date) AS initiation_to_report_flag,
    interval_days(recall_initiation_date, center_classification_date)
        AS initiation_to_classification_days,
    interval_flag(recall_initiation_date, center_classification_date)
        AS initiation_to_classification_flag,
    CASE WHEN classification = 'Class I' THEN true
         WHEN classification IN ('Class II', 'Class III') THEN false
         ELSE NULL::BOOLEAN END AS is_class_i,
    normalized_text(product_description) AS product_description_normalized,
    length(product_description) AS product_description_char_count,
    product_description IS NULL AS product_description_missing,
    normalized_text(reason_for_recall) AS reason_for_recall_normalized,
    length(reason_for_recall) AS reason_for_recall_char_count,
    reason_for_recall IS NULL AS reason_for_recall_missing,
    normalized_text(product_quantity) AS product_quantity_normalized,
    length(product_quantity) AS product_quantity_char_count,
    product_quantity IS NULL AS product_quantity_missing,
    length(openfda) AS openfda_char_count,
    openfda IS NULL AS openfda_missing,
    CASE WHEN openfda IS NULL THEN NULL::BOOLEAN
         ELSE len(json_keys(openfda)) = 0 END AS openfda_empty
FROM accepted;
