-- Quality dashboard (ADR 0010): one row per month and rule, with the change against the previous month.
-- A rule is flagged as a spike when its rate is at least twice the previous month's rate and it matched at
-- least 100 rows, so that a jump from 1 row to 3 rows is not reported.
WITH rates AS (
    SELECT
        data_month,
        rule_id,
        severity,
        description,
        matched_rows,
        total_rows,
        TRY_DIVIDE(matched_rows, total_rows) AS match_rate
    FROM workspace.mobility.quality_fhvhv_rule_counts
),

with_previous AS (
    SELECT
        data_month,
        rule_id,
        severity,
        description,
        matched_rows,
        total_rows,
        match_rate,
        LAG(match_rate) OVER (PARTITION BY rule_id ORDER BY data_month) AS previous_match_rate
    FROM rates
)

SELECT
    data_month,
    rule_id,
    severity,
    description,
    matched_rows,
    total_rows,
    match_rate,
    previous_match_rate,
    TRY_DIVIDE(match_rate, previous_match_rate) AS rate_vs_previous_month,
    COALESCE(
        matched_rows >= 100 AND match_rate >= 2 * previous_match_rate,
        FALSE
    ) AS is_spike
FROM with_previous
ORDER BY data_month, rule_id
