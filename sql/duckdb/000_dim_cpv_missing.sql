-- Typed empty stub used when data/reference/cpv_codes.parquet is not built.
--
-- Explicit fallback policy (docs/analytics.md): the analytical layer stays
-- functional without the CPV dimension. opportunity_cpv keeps every
-- published code with cpv_matched = false and null official attributes
-- (label_es, label_en, level, parent_code, is_leaf). No labels are guessed
-- and no code is dropped; rebuilding after the dimension exists restores the
-- official attributes.
CREATE OR REPLACE VIEW dim_cpv AS
SELECT
    CAST(NULL AS VARCHAR) AS cpv_code,
    CAST(NULL AS BOOLEAN) AS cpv_matched,
    CAST(NULL AS VARCHAR) AS label_es,
    CAST(NULL AS VARCHAR) AS label_en,
    CAST(NULL AS TINYINT) AS level,
    CAST(NULL AS VARCHAR) AS parent_code,
    CAST(NULL AS BOOLEAN) AS is_leaf
WHERE 1 = 0;
