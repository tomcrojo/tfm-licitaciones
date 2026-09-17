-- Auxiliary observability view for opportunities without publication_date.
--
-- One row per source with the count and attributed value of undated rows.
-- Paired with opportunities_monthly, the two counts always reconcile with
-- the base open_opportunities grain without any invented dates.
CREATE OR REPLACE VIEW opportunities_without_publication_date AS
SELECT
    source,
    count(*) AS opportunities_count,
    sum(estimated_value) AS total_estimated_value
FROM open_opportunities
WHERE publication_date IS NULL
GROUP BY source;
