\set ON_ERROR_STOP on
\pset pager off

\echo Creating optional CN/WW core partial indexes. This can take a while on full fact tables.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_app_perf_daily_cnww_android_nonzero_v2
ON core.fact_app_performance_daily (country_android, date, app_id)
WHERE country_android IN ('CN', 'WW')
  AND (COALESCE(downloads_android, 0) <> 0 OR COALESCE(revenue_android, 0) <> 0);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_app_perf_daily_cnww_iphone_nonzero_v2
ON core.fact_app_performance_daily (country_ios, date, app_id)
WHERE country_ios IN ('CN', 'WW')
  AND (COALESCE(downloads_iphone, 0) <> 0 OR COALESCE(revenue_iphone, 0) <> 0);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_app_perf_daily_cnww_ipad_nonzero_v2
ON core.fact_app_performance_daily (country_ios, date, app_id)
WHERE country_ios IN ('CN', 'WW')
  AND (COALESCE(downloads_ipad, 0) <> 0 OR COALESCE(revenue_ipad, 0) <> 0);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_app_perf_au_cnww_country_date_app_v2
ON core.fact_app_performance_active_users (country, date, app_id)
WHERE country IN ('CN', 'WW');
