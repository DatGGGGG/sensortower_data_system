\set ON_ERROR_STOP on
\pset pager off

\if :{?run_id}
\else
  \echo Missing required psql variable: run_id
  \quit 64
\endif

\if :{?country_code}
\else
  \echo Missing required psql variable: country_code
  \quit 64
\endif

\if :{?period_start}
\else
  \echo Missing required psql variable: period_start
  \quit 64
\endif

\if :{?period_end}
\else
  \echo Missing required psql variable: period_end
  \quit 64
\endif

\echo [:country_code :period_start daily_game] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'daily_game', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

\set target_table analytics.repair_agg_game_performance_daily
\set delete_existing true
\set refresh_start_date :period_start
\set refresh_end_date :period_end
\set refresh_country_codes :country_code
\include /sql/insert_agg_game_performance_daily_window.sql

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_agg_game_performance_daily
        WHERE date >= :'period_start'::date
          AND date <= :'period_end'::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'daily_game'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start daily_game] done
