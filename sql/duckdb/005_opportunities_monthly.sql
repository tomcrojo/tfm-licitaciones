-- One row per (month, source) over publication_date.
--
-- Opportunities without publication_date are excluded here and stay
-- observable in opportunities_without_publication_date; they never receive
-- an invented date. month is the first day of the publication month, cast
-- back to DATE because DuckDB date_trunc returns TIMESTAMP.
CREATE OR REPLACE VIEW opportunities_monthly AS
SELECT
    CAST(date_trunc('month', publication_date) AS DATE) AS month,
    source,
    count(*) AS opportunities_count,
    sum(estimated_value) AS total_estimated_value,
    CAST(avg(estimated_value) AS DECIMAL(38, 6)) AS avg_estimated_value,
    count(DISTINCT buyer_id) AS distinct_buyers
FROM open_opportunities
WHERE publication_date IS NOT NULL
GROUP BY month, source;
