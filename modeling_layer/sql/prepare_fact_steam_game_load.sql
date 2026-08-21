-- prepare_fact_steam_game_load.sql  (DROP-IN)
\set ON_ERROR_STOP on
\pset pager off

-- 0) Ensure schemas exist
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS steam;

-- 1) Full refresh (fact only) + clear missing-app bucket
TRUNCATE TABLE steam.fact_steam_game_performance_monthly;

DROP TABLE IF EXISTS raw.fact_steam_game_performance_monthly_missing_app;
CREATE TABLE raw.fact_steam_game_performance_monthly_missing_app (
  line_no           BIGINT,
  app_id            INTEGER,
  month             DATE,
  peak_ccu          BIGINT,
  raw_line          TEXT
);

-- 2) Staging table with line numbers (identity)
DROP TABLE IF EXISTS raw.fact_steam_game_performance_monthly_lines;
CREATE TABLE raw.fact_steam_game_performance_monthly_lines (
  line_no BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  line    TEXT
);

-- (Optional) reduce noise / avoid autovacuum stealing cycles on staging during the big load
ALTER TABLE raw.fact_steam_game_performance_monthly_lines SET (autovacuum_enabled = false);

-- 3) Progress table
DROP TABLE IF EXISTS raw.fact_steam_game_performance_monthly_load_progress;
CREATE TABLE raw.fact_steam_game_performance_monthly_load_progress (
  started_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  total_lines     BIGINT,
  processed_lines BIGINT NOT NULL DEFAULT 0,
  inserted_rows   BIGINT NOT NULL DEFAULT 0
);

INSERT INTO raw.fact_steam_game_performance_monthly_load_progress(total_lines, processed_lines, inserted_rows)
VALUES (NULL, 0, 0);

-- 4) Stored procedure: loads from staging in batches and updates progress
DROP PROCEDURE IF EXISTS steam.load_fact_steam_game_performance_monthly_from_staging(BIGINT);

CREATE PROCEDURE steam.load_fact_steam_game_performance_monthly_from_staging(batch_lines BIGINT DEFAULT 500000)
LANGUAGE plpgsql
AS $$
DECLARE
  v_total    BIGINT;
  v_from     BIGINT := 1;
  v_to       BIGINT;
  v_ins      BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_steam_game_performance_monthly_lines;

  UPDATE raw.fact_steam_game_performance_monthly_load_progress
  SET total_lines = v_total,
      updated_at  = now();

  IF v_total IS NULL OR v_total = 0 THEN
    RAISE NOTICE 'No staging lines to load.';
    RETURN;
  END IF;

  -- Make per-batch inserts faster; adjust if memory is tight
  PERFORM set_config('work_mem', '256MB', true);

  WHILE v_from <= v_total LOOP
    v_to := LEAST(v_from + batch_lines - 1, v_total);

    -- 1) Divert rows whose app_id is missing in dim_steam_game_info
    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_steam_game_performance_monthly_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        line_no,
        line AS raw_line,
        (j->>'app_id')::int AS app_id,
        CASE
          WHEN (j->>'month') ~ '^\d{4}-\d{2}$'
            THEN to_date((j->>'month') || '-01', 'YYYY-MM-DD')
          ELSE NULL
        END AS month,
        CASE WHEN (j->>'peak_ccu') ~ '^-?\d+$' THEN (j->>'peak_ccu')::bigint ELSE NULL END AS peak_ccu
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE app_id IS NOT NULL
        AND month IS NOT NULL
    )
    INSERT INTO raw.fact_steam_game_performance_monthly_missing_app (
      line_no, app_id, month, peak_ccu, raw_line
    )
    SELECT
      g.line_no, 
      g.app_id,
      g.month, 
      g.peak_ccu,
      g.raw_line
    FROM good g
    LEFT JOIN steam.dim_steam_game_info a
      ON a.app_id = g.app_id
    WHERE a.app_id IS NULL;

    -- 2) Load only FK-safe rows into the fact table
    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_steam_game_performance_monthly_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        (j->>'app_id')::int AS app_id,
        CASE
          WHEN (j->>'month') ~ '^\d{4}-\d{2}$'
            THEN to_date((j->>'month') || '-01', 'YYYY-MM-DD')
          ELSE NULL
        END AS month,
        CASE WHEN (j->>'peak_ccu') ~ '^-?\d+$' THEN (j->>'peak_ccu')::bigint ELSE NULL END AS peak_ccu
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE app_id IS NOT NULL
        AND month IS NOT NULL
    )
    INSERT INTO steam.fact_steam_game_performance_monthly (
      app_id,
      month,
      peak_ccu
    )
    SELECT
      g.app_id,
      g.month,
      g.peak_ccu
    FROM good g
    JOIN steam.dim_steam_game_info a
      ON a.app_id = g.app_id
    ON CONFLICT (app_id, month) DO UPDATE SET
      month = EXCLUDED.month;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_steam_game_performance_monthly_load_progress
    SET processed_lines = v_to,
        inserted_rows   = inserted_rows + v_ins,
        updated_at      = now();

    COMMIT;

    RAISE NOTICE 'Processed % / % lines (%.2f%%). Fact affected rows so far: %. Missing-app rows so far: %',
      v_to,
      v_total,
      (v_to::numeric * 100.0 / v_total::numeric),
      (SELECT inserted_rows FROM raw.fact_steam_game_performance_monthly_load_progress),
      (SELECT COUNT(*) FROM raw.fact_steam_game_performance_monthly_missing_app);

    v_from := v_to + 1;
  END LOOP;


  COMMIT;

  -- re-enable autovacuum for staging (optional)
  ALTER TABLE raw.fact_steam_game_performance_monthly_lines SET (autovacuum_enabled = true);

END $$;
