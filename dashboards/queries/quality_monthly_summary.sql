-- Quality dashboard (ADR 0010): one row per month.
-- Rows checked come from the rule counts table, rows quarantined from the quarantine table.
WITH checked AS (
    SELECT
        data_month,
        MAX(total_rows) AS rows_checked
    FROM workspace.mobility.quality_fhvhv_rule_counts
    GROUP BY data_month
),

quarantined AS (
    SELECT
        data_month,
        COUNT(*) AS rows_quarantined
    FROM workspace.mobility.quarantine_fhvhv_trips
    GROUP BY data_month
)

SELECT
    c.data_month,
    c.rows_checked,
    COALESCE(q.rows_quarantined, 0) AS rows_quarantined,
    c.rows_checked - COALESCE(q.rows_quarantined, 0) AS rows_in_silver,
    TRY_DIVIDE(COALESCE(q.rows_quarantined, 0), c.rows_checked) AS quarantine_rate
FROM checked AS c
LEFT JOIN quarantined AS q
    ON c.data_month = q.data_month
ORDER BY c.data_month
