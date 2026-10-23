-- Unicode White_Space includes separators, tab/newline controls, and next line.
CREATE TEMP MACRO whitespace_pattern() AS
    '[\p{Z}\x{0009}-\x{000D}\x{0085}]';

CREATE TEMP MACRO blank_to_null(value) AS
    CASE WHEN regexp_full_match(value, whitespace_pattern() || '*') THEN NULL ELSE value END;

CREATE TEMP MACRO trim_scalar(value) AS
    nullif(regexp_replace(
        value, '^' || whitespace_pattern() || '+|' || whitespace_pattern() || '+$', '', 'g'
    ), '');

CREATE TEMP VIEW standardized AS
SELECT
    * REPLACE (
        trim_scalar(address_1) AS address_1,
        trim_scalar(address_2) AS address_2,
        trim_scalar(center_classification_date) AS center_classification_date,
        trim_scalar(city) AS city,
        trim_scalar(classification) AS classification,
        blank_to_null(code_info) AS code_info,
        trim_scalar(country) AS country,
        blank_to_null(distribution_pattern) AS distribution_pattern,
        trim_scalar(event_id) AS event_id,
        trim_scalar(initial_firm_notification) AS initial_firm_notification,
        blank_to_null(more_code_info) AS more_code_info,
        trim_scalar(postal_code) AS postal_code,
        blank_to_null(product_description) AS product_description,
        blank_to_null(product_quantity) AS product_quantity,
        trim_scalar(product_type) AS product_type,
        blank_to_null(reason_for_recall) AS reason_for_recall,
        trim_scalar(recall_initiation_date) AS recall_initiation_date,
        trim_scalar(recall_number) AS recall_number,
        blank_to_null(recalling_firm) AS recalling_firm,
        trim_scalar(report_date) AS report_date,
        trim_scalar(state) AS state,
        trim_scalar(status) AS status,
        trim_scalar(termination_date) AS termination_date,
        trim_scalar(voluntary_mandated) AS voluntary_mandated
    ),
    report_date AS report_date_raw,
    recall_initiation_date AS recall_initiation_date_raw,
    center_classification_date AS center_classification_date_raw,
    termination_date AS termination_date_raw,
    lower(regexp_replace(trim_scalar(recalling_firm), whitespace_pattern() || '+', ' ', 'g'))
        AS recalling_firm_normalized
FROM bronze;
