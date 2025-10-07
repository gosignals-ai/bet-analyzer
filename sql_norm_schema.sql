CREATE SCHEMA IF NOT EXISTS odds_norm;

-- =========================
-- games: one row per game
-- =========================
CREATE TABLE IF NOT EXISTS odds_norm.games (
  game_uid         text PRIMARY KEY,              -- Odds API game id
  sport_key        text        NOT NULL,          -- e.g. americanfootball_nfl
  commence_time    timestamptz NOT NULL,
  home_team        text,
  away_team        text,
  status           text         DEFAULT NULL,     -- scheduled, completed, etc.
  created_at       timestamptz  DEFAULT now(),
  updated_at       timestamptz  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_games_sport_key ON odds_norm.games (sport_key);
CREATE INDEX IF NOT EXISTS ix_games_commence_time ON odds_norm.games (commence_time);

-- =========================
-- markets: one row per (game, market_key, book)
-- =========================
CREATE TABLE IF NOT EXISTS odds_norm.markets (
  game_uid         text        NOT NULL REFERENCES odds_norm.games(game_uid) ON DELETE CASCADE,
  market_key       text        NOT NULL,          -- e.g. 'h2h', 'spreads', 'totals'
  book_key         text        NOT NULL,          -- sportsbook key from Odds API
  market_ref       text        DEFAULT NULL,      -- optional external id
  last_update      timestamptz DEFAULT NULL,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now(),
  CONSTRAINT markets_natural_uk UNIQUE (game_uid, market_key, book_key)
);

CREATE INDEX IF NOT EXISTS ix_markets_game ON odds_norm.markets (game_uid);
CREATE INDEX IF NOT EXISTS ix_markets_last_update ON odds_norm.markets (last_update);

-- =========================
-- odds: time-series quotes; one row per (game, market_key, book, side, last_update)
-- =========================
CREATE TABLE IF NOT EXISTS odds_norm.odds (
  id               bigserial   PRIMARY KEY,
  game_uid         text        NOT NULL REFERENCES odds_norm.games(game_uid) ON DELETE CASCADE,
  market_key       text        NOT NULL,      -- 'h2h'/'spreads'/'totals'
  book_key         text        NOT NULL,
  side             text        NOT NULL CHECK (side IN ('home','away','draw')),
  price            integer     NOT NULL,      -- American price (e.g., -110, +150)
  point            numeric(10,2),            -- line/total for spreads/totals; null for h2h
  last_update      timestamptz NOT NULL,      -- from Odds API
  observed_at      timestamptz NOT NULL DEFAULT now(), -- when we saw this row
  created_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT odds_natural_uk UNIQUE (game_uid, market_key, book_key, side, last_update)
);

CREATE INDEX IF NOT EXISTS ix_odds_game ON odds_norm.odds (game_uid);
CREATE INDEX IF NOT EXISTS ix_odds_market ON odds_norm.odds (market_key);
CREATE INDEX IF NOT EXISTS ix_odds_book ON odds_norm.odds (book_key);
CREATE INDEX IF NOT EXISTS ix_odds_side ON odds_norm.odds (side);
CREATE INDEX IF NOT EXISTS ix_odds_last_update_desc ON odds_norm.odds (last_update DESC);

-- lightweight trigger to keep updated_at fresh (optional but handy)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'touch_updated_at_norm') THEN
    CREATE OR REPLACE FUNCTION touch_updated_at_norm() RETURNS trigger AS $f$
    BEGIN
      NEW.updated_at := now();
      RETURN NEW;
    END
    $f$ LANGUAGE plpgsql;
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tg_games_touch_updated') THEN
    CREATE TRIGGER tg_games_touch_updated BEFORE UPDATE ON odds_norm.games
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at_norm();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tg_markets_touch_updated') THEN
    CREATE TRIGGER tg_markets_touch_updated BEFORE UPDATE ON odds_norm.markets
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at_norm();
  END IF;
END$$;