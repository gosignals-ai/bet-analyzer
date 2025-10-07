-- Latest quote per (game, market, book, side)
CREATE SCHEMA IF NOT EXISTS odds_norm;

CREATE OR REPLACE VIEW odds_norm.v_odds_latest AS
SELECT DISTINCT ON (o.game_uid, o.market_key, o.book_key, o.side)
       o.game_uid, o.market_key, o.book_key, o.side,
       o.price, o.point, o.last_update, o.observed_at
FROM odds_norm.odds o
ORDER BY o.game_uid, o.market_key, o.book_key, o.side, o.last_update DESC;

-- Best moneyline per game (one row per game)
CREATE OR REPLACE VIEW odds_norm.v_moneyline_game_best_norm AS
WITH latest_ml AS (
  SELECT l.*
  FROM odds_norm.v_odds_latest l
  WHERE l.market_key = 'h2h'
),
best_home AS (
  SELECT DISTINCT ON (game_uid)
         game_uid, book_key AS home_book, price AS home_best_price, last_update AS home_last_update
  FROM latest_ml
  WHERE side = 'home'
  ORDER BY game_uid, price DESC, last_update DESC
),
best_away AS (
  SELECT DISTINCT ON (game_uid)
         game_uid, book_key AS away_book, price AS away_best_price, last_update AS away_last_update
  FROM latest_ml
  WHERE side = 'away'
  ORDER BY game_uid, price DESC, last_update DESC
)
SELECT g.sport_key,
       g.game_uid,
       g.away_team,
       g.home_team,
       g.commence_time AS commence_time_utc,
       ba.away_best_price,
       ba.away_book,
       bh.home_best_price,
       bh.home_book
FROM odds_norm.games g
LEFT JOIN best_home bh USING (game_uid)
LEFT JOIN best_away ba USING (game_uid);