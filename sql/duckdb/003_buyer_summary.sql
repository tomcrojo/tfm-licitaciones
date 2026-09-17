-- One row per analytical buyer identity, aggregated exclusively from
-- open_opportunities (Gold).
--
-- Explicit identity policy (no invented IDs, no accidental merging):
--   * rows with buyer_id group by buyer_id (identity_basis = 'buyer_id');
--     buyer_name is the lexicographic max of the names published for that
--     id, which is deterministic and does not rewrite Gold;
--   * rows without buyer_id group by the published buyer_name
--     (identity_basis = 'buyer_name'), so different unnamed buyers are never
--     collapsed into one another; opportunities with neither id nor name
--     stay observable as the (null, null) bucket.
-- avg_estimated_value is rounded to 6 fractional digits (DECIMAL(38,6)) so
-- monetary aggregates never silently become floating point.
CREATE OR REPLACE VIEW buyer_summary AS
SELECT
    buyer_id,
    max(buyer_name) AS buyer_name,
    'buyer_id' AS identity_basis,
    count(*) AS opportunities_count,
    sum(estimated_value) AS total_estimated_value,
    CAST(avg(estimated_value) AS DECIMAL(38, 6)) AS avg_estimated_value,
    min(publication_date) AS first_publication_date,
    max(publication_date) AS last_publication_date,
    min(deadline) AS earliest_deadline,
    len(list_distinct(flatten(list(cpv_codes)))) AS distinct_cpv_count
FROM open_opportunities
WHERE buyer_id IS NOT NULL
GROUP BY buyer_id
UNION ALL
SELECT
    CAST(NULL AS VARCHAR) AS buyer_id,
    buyer_name,
    'buyer_name' AS identity_basis,
    count(*) AS opportunities_count,
    sum(estimated_value) AS total_estimated_value,
    CAST(avg(estimated_value) AS DECIMAL(38, 6)) AS avg_estimated_value,
    min(publication_date) AS first_publication_date,
    max(publication_date) AS last_publication_date,
    min(deadline) AS earliest_deadline,
    len(list_distinct(flatten(list(cpv_codes)))) AS distinct_cpv_count
FROM open_opportunities
WHERE buyer_id IS NULL
GROUP BY buyer_name;
