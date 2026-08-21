-- upsert_fact_steam_game_performance_monthly_batched.sql
\set ON_ERROR_STOP on
\pset pager off

DO $$
DECLARE
  v_total   BIGINT;
  v_from    BIGINT := 1;
  v_to      BIGINT;
  v_batch   BIGINT := 200000; -- adjust
  v_ins     BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_steam_game_perf_monthly_lines;

  UPDATE raw.fact_steam_game_perf_monthly_progress
  SET total_lines = v_total, updated_at = now();

  IF v_total IS NULL OR v_total = 0 THEN
    RAISE NOTICE 'No staging lines to load.';
    RETURN;
  END IF;

  WHILE v_from <= v_total LOOP
    v_to := LEAST(v_from + v_batch - 1, v_total);

    WITH parsed AS (
      SELECT line::jsonb AS j
      FROM raw.fact_steam_game_perf_monthly_lines
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
      FROM parsed
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE app_id IS NOT NULL AND month IS NOT NULL
    )
    INSERT INTO steam.fact_steam_game_performance_monthly(app_id, month, peak_ccu)
    SELECT g.app_id, g.month, g.peak_ccu
    FROM good g
    JOIN steam.dim_steam_game_info d ON d.app_id = g.app_id
    ON CONFLICT (app_id, month) DO UPDATE SET
      peak_ccu = EXCLUDED.peak_ccu;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_steam_game_perf_monthly_progress
    SET processed_lines = v_to,
        upserted_rows   = upserted_rows + v_ins,
        updated_at      = now();

    RAISE NOTICE 'FACT steam monthly: processed %/% lines (%.2f%%), affected rows so far: %',
      v_to, v_total, (v_to::numeric*100.0/v_total::numeric),
      (SELECT upserted_rows FROM raw.fact_steam_game_perf_monthly_progress);

    v_from := v_to + 1;
  END LOOP;
END $$;

SELECT COUNT(*) AS fact_steam_rows FROM steam.fact_steam_game_performance_monthly;
SELECT * FROM steam.fact_steam_game_performance_monthly ORDER BY month, app_id LIMIT 10;
