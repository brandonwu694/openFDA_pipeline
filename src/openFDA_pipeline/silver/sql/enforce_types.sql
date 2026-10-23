CREATE TEMP MACRO parse_source_date(value) AS
    CASE
        WHEN regexp_full_match(value, '[0-9]{8}')
        THEN CAST(try_strptime(value, '%Y%m%d') AS DATE)
        ELSE NULL::DATE
    END;

CREATE TEMP VIEW typed AS
SELECT * REPLACE (
    parse_source_date(report_date) AS report_date,
    parse_source_date(recall_initiation_date) AS recall_initiation_date,
    parse_source_date(center_classification_date) AS center_classification_date,
    parse_source_date(termination_date) AS termination_date
)
FROM standardized;
