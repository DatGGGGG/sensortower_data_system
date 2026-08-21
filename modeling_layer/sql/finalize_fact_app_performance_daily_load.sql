-- finalize_fact_app_performance_daily_load.sql
-- Recreate constraints/indexes after fast full load.
\set ON_ERROR_STOP on
\pset pager off

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pk_fact_app_performance_daily'
      AND conrelid = 'core.fact_app_performance_daily'::regclass
  ) THEN
    ALTER TABLE core.fact_app_performance_daily
      ADD CONSTRAINT pk_fact_app_performance_daily
      PRIMARY KEY (app_id, "date", country_android, country_ios);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_fact_app_performance_daily_1'
      AND conrelid = 'core.fact_app_performance_daily'::regclass
  ) THEN
    ALTER TABLE core.fact_app_performance_daily
      ADD CONSTRAINT fk_fact_app_performance_daily_1
      FOREIGN KEY (app_id)
      REFERENCES core.dim_app_info(app_id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_date
  ON core.fact_app_performance_daily("date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_app_id
  ON core.fact_app_performance_daily(app_id);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_app_date
  ON core.fact_app_performance_daily(app_id, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_country_date
  ON core.fact_app_performance_daily(country_android, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_country_ios_date
  ON core.fact_app_performance_daily(country_ios, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_android_date_app
  ON core.fact_app_performance_daily(country_android, "date", app_id);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_ios_date_app
  ON core.fact_app_performance_daily(country_ios, "date", app_id);

ALTER TABLE core.fact_app_performance_daily SET (autovacuum_enabled = true);

ANALYZE core.fact_app_performance_daily;

SELECT COUNT(*) AS missing_app_rows
FROM raw.fact_app_performance_daily_missing_app;

SELECT reltuples::bigint AS estimated_fact_rows
FROM pg_class
WHERE oid = 'core.fact_app_performance_daily'::regclass;

SELECT MIN(date) AS min_date, MAX(date) AS max_date
FROM core.fact_app_performance_daily;
