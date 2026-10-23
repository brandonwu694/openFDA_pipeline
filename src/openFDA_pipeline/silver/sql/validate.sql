CREATE TEMP VIEW validated AS
SELECT
    *,
    list_filter([
        CASE WHEN recall_number IS NULL THEN 'missing_recall_number' END,
        CASE WHEN trim_scalar(report_date_raw) IS NULL THEN 'missing_report_date' END,
        CASE WHEN trim_scalar(report_date_raw) IS NOT NULL AND report_date IS NULL
             THEN 'invalid_report_date' END,
        CASE WHEN trim_scalar(recall_initiation_date_raw) IS NOT NULL
                  AND recall_initiation_date IS NULL THEN 'invalid_recall_initiation_date' END,
        CASE WHEN trim_scalar(center_classification_date_raw) IS NOT NULL
                  AND center_classification_date IS NULL THEN 'invalid_center_classification_date' END,
        CASE WHEN trim_scalar(termination_date_raw) IS NOT NULL AND termination_date IS NULL
             THEN 'invalid_termination_date' END
    ]::VARCHAR[], lambda code: code IS NOT NULL) AS _error_codes,
    list_filter([
        CASE WHEN recall_initiation_date < DATE '2004-01-01'
             THEN 'initiation_before_2004' END,
        CASE WHEN report_date < recall_initiation_date
             THEN 'report_before_initiation' END,
        CASE WHEN event_id IS NULL THEN 'missing_event_id' END,
        CASE WHEN classification IS NOT NULL
                  AND classification NOT IN ('Class I', 'Class II', 'Class III', 'Not Yet Classified')
             THEN 'unfamiliar_classification' END,
        CASE WHEN status IS NOT NULL AND status NOT IN ('Ongoing', 'Completed', 'Terminated')
             THEN 'unfamiliar_status' END
    ]::VARCHAR[], lambda code: code IS NOT NULL) AS _warning_codes
FROM typed;
