-- One row per cpv_code, aggregated from opportunity_cpv.
--
-- Amount attribution is per category: an opportunity carrying N CPV codes
-- contributes its full estimated_value to each of the N categories. This is
-- the documented semantics of total_estimated_value / avg_estimated_value
-- here and must NOT be used to reconcile global totals with Gold; the base
-- open_opportunities view remains the only global grain. distinct_buyers
-- uses count(DISTINCT buyer_id), so opportunities without buyer_id are not
-- counted there (they remain visible in buyer_summary).
CREATE OR REPLACE VIEW cpv_summary AS
SELECT
    c.cpv_code,
    max(c.label_es) AS label_es,
    max(c.label_en) AS label_en,
    count(DISTINCT c.procedure_id) AS opportunities_count,
    count(DISTINCT c.buyer_id) AS distinct_buyers,
    sum(c.estimated_value) AS total_estimated_value,
    CAST(avg(c.estimated_value) AS DECIMAL(38, 6)) AS avg_estimated_value
FROM opportunity_cpv c
GROUP BY c.cpv_code;
