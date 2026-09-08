\set ON_ERROR_STOP on
\pset pager off

DROP TABLE IF EXISTS analytics.repair_agg_subgenre_performance_yearly;
DROP TABLE IF EXISTS analytics.repair_agg_subgenre_performance_monthly;
DROP TABLE IF EXISTS analytics.repair_agg_game_performance_yearly;
DROP TABLE IF EXISTS analytics.repair_agg_game_performance_monthly;
DROP TABLE IF EXISTS analytics.repair_agg_game_performance_daily;
DROP TABLE IF EXISTS analytics.repair_mobile_app_yearly_active_users_cache;
DROP TABLE IF EXISTS analytics.repair_mobile_app_monthly_active_users_cache;
DROP TABLE IF EXISTS analytics.repair_mobile_app_yearly_performance_cache;
DROP TABLE IF EXISTS analytics.repair_mobile_app_monthly_performance_cache;
DROP TABLE IF EXISTS analytics.analytics_repair_progress;
DROP TABLE IF EXISTS analytics.analytics_repair_runs;
