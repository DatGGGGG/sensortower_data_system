-- prepare_fact_active_users_load.sql
\set ON_ERROR_STOP on
\pset pager off
\if :{?full_refresh}
\else
  \set full_refresh true
\endif

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

\if :full_refresh
TRUNCATE TABLE core.fact_app_performance_active_users;
\else
\echo Incremental active-users fact load: preserving existing core.fact_app_performance_active_users rows.
\endif

DROP TABLE IF EXISTS raw.fact_app_performance_active_users_missing_app;
CREATE TABLE raw.fact_app_performance_active_users_missing_app (
  line_no BIGINT,
  app_id TEXT,
  country TEXT,
  "date" TIMESTAMPTZ,
  raw_line TEXT
);

DROP TABLE IF EXISTS raw.fact_app_performance_active_users_lines;
CREATE TABLE raw.fact_app_performance_active_users_lines (
  line_no BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  line TEXT
);

ALTER TABLE raw.fact_app_performance_active_users_lines SET (autovacuum_enabled = false);

DROP TABLE IF EXISTS raw.fact_app_performance_active_users_load_progress;
CREATE TABLE raw.fact_app_performance_active_users_load_progress (
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  total_lines BIGINT,
  processed_lines BIGINT NOT NULL DEFAULT 0,
  inserted_rows BIGINT NOT NULL DEFAULT 0
);

INSERT INTO raw.fact_app_performance_active_users_load_progress(total_lines, processed_lines, inserted_rows)
VALUES (NULL, 0, 0);

DROP PROCEDURE IF EXISTS core.load_fact_app_performance_active_users_from_staging(BIGINT);

CREATE PROCEDURE core.load_fact_app_performance_active_users_from_staging(batch_lines BIGINT DEFAULT 50000)
LANGUAGE plpgsql
AS $$
DECLARE
  v_total BIGINT;
  v_from BIGINT := 1;
  v_to BIGINT;
  v_ins BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_app_performance_active_users_lines;

  UPDATE raw.fact_app_performance_active_users_load_progress
  SET total_lines = v_total,
      updated_at = now();

  IF v_total IS NULL OR v_total = 0 THEN
    RAISE NOTICE 'No active-user staging lines to load.';
    RETURN;
  END IF;

  PERFORM set_config('work_mem', '64MB', true);

  WHILE v_from <= v_total LOOP
    v_to := LEAST(v_from + batch_lines - 1, v_total);

    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_app_performance_active_users_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        line_no,
        line AS raw_line,
        j->>'app_id' AS app_id,
        j->>'country' AS country,
        core.to_timestamptz_loose(j->>'date') AS "date",
        core.to_bigint_loose(j->>'dau_android') AS dau_android,
        core.to_bigint_loose(j->>'dau_iphone') AS dau_iphone,
        core.to_bigint_loose(j->>'dau_ipad') AS dau_ipad,
        core.to_bigint_loose(j->>'wau_android') AS wau_android,
        core.to_bigint_loose(j->>'wau_iphone') AS wau_iphone,
        core.to_bigint_loose(j->>'wau_ipad') AS wau_ipad,
        core.to_bigint_loose(j->>'mau_android') AS mau_android,
        core.to_bigint_loose(j->>'mau_iphone') AS mau_iphone,
        core.to_bigint_loose(j->>'mau_ipad') AS mau_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id, '')) <> ''
        AND btrim(coalesce(country, '')) <> ''
        AND "date" IS NOT NULL
    )
    INSERT INTO raw.fact_app_performance_active_users_missing_app (
      line_no, app_id, country, "date", raw_line
    )
    SELECT g.line_no, g.app_id, g.country, g."date", g.raw_line
    FROM good g
    LEFT JOIN core.dim_app_info a
      ON a.app_id = g.app_id
    WHERE a.app_id IS NULL;

    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_app_performance_active_users_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        line_no,
        j->>'app_id' AS app_id,
        j->>'country' AS country,
        core.to_timestamptz_loose(j->>'date') AS "date",
        core.to_bigint_loose(j->>'dau_android') AS dau_android,
        core.to_bigint_loose(j->>'dau_iphone') AS dau_iphone,
        core.to_bigint_loose(j->>'dau_ipad') AS dau_ipad,
        core.to_bigint_loose(j->>'wau_android') AS wau_android,
        core.to_bigint_loose(j->>'wau_iphone') AS wau_iphone,
        core.to_bigint_loose(j->>'wau_ipad') AS wau_ipad,
        core.to_bigint_loose(j->>'mau_android') AS mau_android,
        core.to_bigint_loose(j->>'mau_iphone') AS mau_iphone,
        core.to_bigint_loose(j->>'mau_ipad') AS mau_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id, '')) <> ''
        AND btrim(coalesce(country, '')) <> ''
        AND "date" IS NOT NULL
    ),
    dedup AS (
      SELECT DISTINCT ON (g.app_id, g.country, g."date")
        g.*
      FROM good g
      JOIN core.dim_app_info a
        ON a.app_id = g.app_id
      ORDER BY g.app_id, g.country, g."date", g.line_no DESC
    )
    INSERT INTO core.fact_app_performance_active_users (
      app_id, country, "date",
      dau_android, dau_iphone, dau_ipad,
      wau_android, wau_iphone, wau_ipad,
      mau_android, mau_iphone, mau_ipad
    )
    SELECT
      app_id, country, "date",
      dau_android, dau_iphone, dau_ipad,
      wau_android, wau_iphone, wau_ipad,
      mau_android, mau_iphone, mau_ipad
    FROM dedup
    ON CONFLICT (app_id, country, "date") DO UPDATE SET
      dau_android = COALESCE(EXCLUDED.dau_android, core.fact_app_performance_active_users.dau_android),
      dau_iphone = COALESCE(EXCLUDED.dau_iphone, core.fact_app_performance_active_users.dau_iphone),
      dau_ipad = COALESCE(EXCLUDED.dau_ipad, core.fact_app_performance_active_users.dau_ipad),
      wau_android = COALESCE(EXCLUDED.wau_android, core.fact_app_performance_active_users.wau_android),
      wau_iphone = COALESCE(EXCLUDED.wau_iphone, core.fact_app_performance_active_users.wau_iphone),
      wau_ipad = COALESCE(EXCLUDED.wau_ipad, core.fact_app_performance_active_users.wau_ipad),
      mau_android = COALESCE(EXCLUDED.mau_android, core.fact_app_performance_active_users.mau_android),
      mau_iphone = COALESCE(EXCLUDED.mau_iphone, core.fact_app_performance_active_users.mau_iphone),
      mau_ipad = COALESCE(EXCLUDED.mau_ipad, core.fact_app_performance_active_users.mau_ipad);

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_app_performance_active_users_load_progress
    SET processed_lines = v_to,
        inserted_rows = inserted_rows + v_ins,
        updated_at = now();

    RAISE NOTICE 'Loaded active-user lines %-% of %, inserted/upserted % rows',
      v_from, v_to, v_total, v_ins;

    v_from := v_to + 1;
    COMMIT;
  END LOOP;

  COMMIT;

  ALTER TABLE raw.fact_app_performance_active_users_lines SET (autovacuum_enabled = true);
END;
$$;

SELECT 'prepared_active_users_staging' AS status;
