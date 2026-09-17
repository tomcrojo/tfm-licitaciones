-- One row per (procedure_id, cpv_code, cpv_position) occurrence.
--
-- cpv_position is the 0-based index inside the canonical cpv_codes array,
-- so the original publication order is preserved and the explosion is
-- deterministic. Every published code survives: the LEFT JOIN against
-- dim_cpv keeps unmatched codes with cpv_matched = false and null official
-- attributes. A procedure with several CPV codes produces several rows here
-- and only here; the base open_opportunities view is never exploded.
CREATE OR REPLACE VIEW opportunity_cpv AS
WITH exploded AS (
    SELECT
        o.procedure_id,
        o.source,
        o.buyer_id,
        o.publication_date,
        o.estimated_value,
        o.currency,
        pos.i AS cpv_position,
        o.cpv_codes[pos.i + 1] AS cpv_code
    FROM open_opportunities o
    CROSS JOIN range(len(o.cpv_codes)) AS pos(i)
)
SELECT
    e.procedure_id,
    e.source,
    e.buyer_id,
    e.publication_date,
    e.estimated_value,
    e.currency,
    e.cpv_position,
    e.cpv_code,
    COALESCE(d.cpv_matched, FALSE) AS cpv_matched,
    d.label_es,
    d.label_en,
    d.level,
    d.parent_code,
    d.is_leaf
FROM exploded e
LEFT JOIN dim_cpv d ON d.cpv_code = e.cpv_code;
