-- === odds_norm upsert/insert functions ===
CREATE OR REPLACE FUNCTION odds_norm.upsert_game(
  p_game_uid text, p_sport_key text, p_commence_time timestamptz,
  p_home_team text, p_away_team text, p_status text
) RETURNS void AS $$
BEGIN
  INSERT INTO odds_norm.games(game_uid, sport_key, commence_time, home_team, away_team, status)
  VALUES(p_game_uid, p_sport_key, p_commence_time, p_home_team, p_away_team, p_status)
  ON CONFLICT (game_uid) DO UPDATE
    SET sport_key = EXCLUDED.sport_key,
        commence_time = EXCLUDED.commence_time,
        home_team = EXCLUDED.home_team,
        away_team = EXCLUDED.away_team,
        status = EXCLUDED.status,
        updated_at = now();
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION odds_norm.upsert_market(
  p_game_uid text, p_market_key text, p_book_key text,
  p_market_ref text, p_last_update timestamptz
) RETURNS void AS $$
BEGIN
  INSERT INTO odds_norm.markets(game_uid, market_key, book_key, market_ref, last_update)
  VALUES(p_game_uid, p_market_key, p_book_key, p_market_ref, p_last_update)
  ON CONFLICT (game_uid, market_key, book_key) DO UPDATE
    SET market_ref = EXCLUDED.market_ref,
        last_update = GREATEST(EXCLUDED.last_update, odds_norm.markets.last_update),
        updated_at = now();
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION odds_norm.insert_odds(
  p_game_uid text, p_market_key text, p_book_key text,
  p_side text, p_price integer, p_point numeric, p_last_update timestamptz
) RETURNS boolean AS $$
BEGIN
  INSERT INTO odds_norm.odds(game_uid, market_key, book_key, side, price, point, last_update)
  VALUES(p_game_uid, p_market_key, p_book_key, p_side, p_price, p_point, p_last_update)
  ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING;
  RETURN FOUND; -- true if inserted, false if duplicate
END; $$ LANGUAGE plpgsql;