-- Quality dashboard (ADR 0010): quarantined rows per month, reject rule and company.
-- A row that fails two rules is counted once for each rule.
-- Company names follow the gold mapping (ADR 0008); unknown codes are kept as "Other (<code>)".
SELECT
    data_month,
    failed_rule AS rule_id,
    CASE hvfhs_license_num
        WHEN 'HV0002' THEN 'Juno'
        WHEN 'HV0003' THEN 'Uber'
        WHEN 'HV0004' THEN 'Via'
        WHEN 'HV0005' THEN 'Lyft'
        ELSE CONCAT('Other (', hvfhs_license_num, ')')
    END AS company,
    COUNT(*) AS rows_quarantined
FROM workspace.mobility.quarantine_fhvhv_trips
LATERAL VIEW EXPLODE(failed_rules) AS failed_rule
GROUP BY data_month, failed_rule, hvfhs_license_num
ORDER BY data_month, rule_id, company
