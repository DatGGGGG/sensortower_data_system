BEGIN;

CREATE SCHEMA IF NOT EXISTS steam;

CREATE TABLE IF NOT EXISTS steam.dim_steam_game_info (
  app_id        INTEGER PRIMARY KEY,
  name          TEXT,
  game_class    TEXT,
  game_genre    TEXT,
  game_subgenre TEXT,
  developer     TEXT,
  publisher     TEXT,
  language      TEXT,
  initial_price DOUBLE PRECISION,
  release_date  DATE,
  steam_genres  TEXT,
  steam_tags    TEXT,
  description   TEXT
);

CREATE TABLE IF NOT EXISTS steam.fact_steam_game_performance_monthly (
  app_id    INTEGER NOT NULL,
  month     DATE    NOT NULL,
  peak_ccu  BIGINT,
  PRIMARY KEY (app_id, month),
  CONSTRAINT fk_steam_fact_app
    FOREIGN KEY (app_id) REFERENCES steam.dim_steam_game_info(app_id)
);

CREATE INDEX IF NOT EXISTS idx_steam_fact_month ON steam.fact_steam_game_performance_monthly(month);
CREATE INDEX IF NOT EXISTS idx_steam_fact_app   ON steam.fact_steam_game_performance_monthly(app_id);

COMMIT;
