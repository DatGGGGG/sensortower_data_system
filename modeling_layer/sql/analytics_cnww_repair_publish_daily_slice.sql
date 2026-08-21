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

\echo [:country_code :period_start publish_daily] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'publish_daily', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

BEGIN;

DELETE FROM analytics.agg_game_performance_daily
WHERE date >= :'period_start'::date
  AND date <= :'period_end'::date
  AND country = :'country_code';

INSERT INTO analytics.agg_game_performance_daily
SELECT *
FROM analytics.repair_agg_game_performance_daily
WHERE date >= :'period_start'::date
  AND date <= :'period_end'::date
  AND country = :'country_code';

COMMIT;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.agg_game_performance_daily
        WHERE date >= :'period_start'::date
          AND date <= :'period_end'::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'publish_daily'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start publish_daily] done
