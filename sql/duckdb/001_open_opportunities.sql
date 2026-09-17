-- Base analytical view over the principal Gold dataset.
--
-- Grain: one row per procedure_id (exactly the Gold open_opportunities
-- grain). This is a 1:1 projection: every Gold field is preserved with its
-- meaning and type unchanged. The view never filters, never joins and never
-- derives business columns; open/actionable semantics live only in Gold
-- (tfm_licitaciones.gold_open_opportunities). The explicit column list is a
-- contract guard: schema drift in Gold fails here instead of propagating
-- silently.
CREATE OR REPLACE VIEW open_opportunities AS
SELECT
    procedure_id,
    event_id,
    source,
    buyer_id,
    buyer_name,
    title,
    description,
    cpv_codes,
    estimated_value,
    awarded_value,
    currency,
    publication_date,
    source_updated_at,
    deadline,
    status,
    nuts_code,
    country,
    source_url,
    ingested_at
FROM read_parquet($gold_glob);
