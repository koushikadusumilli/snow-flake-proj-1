-- =============================================================================
-- Warehouse bootstrap. Postgres runs this once, when the warehouse volume is
-- first created. Every statement is idempotent, so re-running it is harmless.
--
--   raw      Landing zone written by src/extract.py. Source-shaped, untouched.
--   staging  dbt views: deduplicated, typed, renamed (stg_*).
--   marts    dbt models for analysts (mart_*).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw     IS 'Landing zone loaded by Airflow via src/extract.py';
COMMENT ON SCHEMA staging IS 'dbt staging views (stg_*)';
COMMENT ON SCHEMA marts   IS 'dbt analytics models (mart_*)';
