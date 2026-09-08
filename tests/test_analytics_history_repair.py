import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AnalyticsHistoryRepairContractTests(unittest.TestCase):
    def test_history_repair_uses_global_lock_and_explicit_cutoff(self) -> None:
        script = (ROOT / "scripts/ops/run_analytics_history_repair.sh").read_text(encoding="utf-8")
        self.assertIn('ops_acquire_pipeline_lock', script)
        self.assertIn('REPAIR_END_DATE is required', script)
        self.assertIn('REPAIR_PHASE must be build, validate, publish, all, or cleanup', script)
        self.assertIn('Repair cutoff is not stable', script)

    def test_history_repair_has_country_specific_defaults(self) -> None:
        script = (ROOT / "scripts/ops/run_analytics_history_repair.sh").read_text(encoding="utf-8")
        self.assertIn("VN) printf '2014-01-01", script)
        self.assertIn("CN|WW) printf '2025-01-01", script)
        self.assertIn('REPAIR_START_DATE_${country}', script)

    def test_publication_is_month_sliced_and_explicit(self) -> None:
        script = (ROOT / "scripts/ops/run_analytics_history_repair.sh").read_text(encoding="utf-8")
        publish_sql = (ROOT / "modeling_layer/sql/analytics_history_repair_publish_daily_month_slice.sql").read_text(encoding="utf-8")
        self.assertIn('publish_daily_month', script)
        self.assertNotIn('day_cursor=', script)
        self.assertIn('INSERT INTO analytics.agg_game_performance_daily (', publish_sql)
        self.assertNotIn('INSERT INTO analytics.agg_game_performance_daily\nSELECT *', publish_sql)

    def test_validation_checks_core_facts_and_active_user_only_keys(self) -> None:
        sql = (ROOT / "modeling_layer/sql/analytics_history_repair_validate_country.sql").read_text(encoding="utf-8")
        self.assertIn('core.fact_app_performance_daily', sql)
        self.assertIn('core.fact_app_performance_active_users', sql)
        self.assertIn('SELECT date, country, unified_app_id FROM perf_game UNION SELECT', sql)
        self.assertIn('daily_fact_mismatch_count', sql)
        self.assertIn('monthly_perf_mismatch_count', sql)
        self.assertIn('monthly_au_mismatch_count', sql)

    def test_legacy_publish_sql_does_not_depend_on_column_order(self) -> None:
        for filename in (
            "analytics_cnww_repair_publish_month_slice.sql",
            "analytics_cnww_repair_publish_year_slice.sql",
        ):
            sql = (ROOT / "modeling_layer/sql" / filename).read_text(encoding="utf-8")
            self.assertNotIn('INSERT INTO analytics.mobile_app_monthly_performance_cache\nSELECT *', sql)
            self.assertNotIn('INSERT INTO analytics.mobile_app_yearly_performance_cache\nSELECT *', sql)


if __name__ == "__main__":
    unittest.main()
