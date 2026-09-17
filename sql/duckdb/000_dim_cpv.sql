-- CPV reference dimension consumed by opportunity_cpv and cpv_summary.
--
-- This is a view over the official reference Parquet built from the CPV 2008
-- table (docs/cpv-reference.md); the reference is never copied into the
-- DuckDB file. `cpv_matched` marks dimension presence so an official code
-- with a null attribute (e.g. a missing label_en) is distinguishable from a
-- code absent from the vocabulary.
CREATE OR REPLACE VIEW dim_cpv AS
SELECT
    cpv_code,
    TRUE AS cpv_matched,
    label_es,
    label_en,
    level,
    parent_code,
    is_leaf
FROM read_parquet($cpv_dimension);
