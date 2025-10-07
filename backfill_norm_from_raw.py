import os, psycopg, textwrap

dsn = os.environ["postgresql://gsa_admin:kENWrILMAxj0AOyibrLzFnHXo1gozPKG@dpg-d3hi8n0gjchc73ah5eg0-a.oregon-postgres.render.com/gosignals_kg43"]

SQL_COLS = """
select column_name
from information_schema.columns
where table_schema = 'public' and table_name = 'odds_raw'
"""

def pick(cols, *candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    # 1) verify odds_raw exists
    cur.execute("select to_regclass('public.odds_raw')")
    if cur.fetchone()[0] is None:
        raise RuntimeError("Table public.odds_raw not found")

    # 2) discover columns
    cur.execute(SQL_COLS)
    cols = {r[0] for r in cur.fetchall()}

    game_id   = pick(cols, "game_id", "id", "event_id")
    sport_key = pick(cols, "sport_key", "league", "sport")
    commence  = pick(cols, "commence_time", "commence_time_utc", "start_time", "commence")
    home_team = pick(cols, "home_team", "home")
    away_team = pick(cols, "away_team", "away")
    book_key  = pick(cols, "book_key", "bookmaker_key", "sportsbook", "book")
    market    = pick(cols, "market_key", "market")
    last_upd  = pick(cols, "last_update", "last_update_utc", "updated_at")
    price     = pick(cols, "price", "american_odds", "odds_american", "odds")
    point     = pick(cols, "point", "line", "handicap", "total")
    side      = pick(cols, "side", "runner_side", "team_side")
    team_col  = pick(cols, "team", "participant", "runner", "selection")  # fallback if side missing
    observed  = pick(cols, "observed_at", "ingested_at", "created_at")

    required = [game_id, sport_key, commence, home_team, away_team, book_key, market, last_upd]
    if any(v is None for v in required):
        raise RuntimeError("odds_raw missing required columns; found: " + ", ".join(sorted(cols)))

    # 3) games backfill (upsert set-based)
    sql_games = f"""
    insert into odds_norm.games(game_uid, sport_key, commence_time, home_team, away_team, status)
    select distinct r.{game_id}, r.{sport_key}, r.{commence}, r.{home_team}, r.{away_team}, null
    from public.odds_raw r
    where r.{game_id} is not null
    on conflict (game_uid) do update set
      sport_key = excluded.sport_key,
      commence_time = excluded.commence_time,
      home_team = excluded.home_team,
      away_team = excluded.away_team,
      updated_at = now()
    returning 1;
    """
    cur.execute(sql_games)
    g = cur.rowcount if cur.rowcount != -1 else 0

    # 4) markets backfill (latest last_update per game/market/book)
    sql_markets = f"""
    with agg as (
      select r.{game_id} as game_uid,
             r.{market} as market_key,
             r.{book_key} as book_key,
             max(r.{last_upd}) as last_update
      from public.odds_raw r
      where r.{game_id} is not null and r.{market} is not null and r.{book_key} is not null and r.{last_upd} is not null
      group by 1,2,3
    )
    insert into odds_norm.markets(game_uid, market_key, book_key, market_ref, last_update)
    select a.game_uid, a.market_key, a.book_key, null, a.last_update
    from agg a
    on conflict (game_uid, market_key, book_key) do update
      set last_update = greatest(excluded.last_update, odds_norm.markets.last_update),
          updated_at = now()
    returning 1;
    """
    cur.execute(sql_markets)
    m = cur.rowcount if cur.rowcount != -1 else 0

    # 5) odds backfill (insert only, dedup via unique key)
    # Build side expression
    if side:
        side_expr = f"r.{side}"
    elif team_col:
        # derive home/away by matching team name
        side_expr = f"""case
            when r.{team_col} = r.{home_team} then 'home'
            when r.{team_col} = r.{away_team} then 'away'
            else null end"""
    else:
        side_expr = "null"

    obs_expr = f"r.{observed}" if observed else "now()"

    price_expr = f"r.{price}" if price else "null"
    point_expr = f"r.{point}" if point else "null"

    sql_odds = f"""
    insert into odds_norm.odds(game_uid, market_key, book_key, side, price, point, last_update, observed_at)
    select r.{game_id}, r.{market}, r.{book_key},
           {side_expr} as side,
           {price_expr} as price,
           {point_expr} as point,
           r.{last_upd} as last_update,
           {obs_expr} as observed_at
    from public.odds_raw r
    where r.{game_id} is not null and r.{market} is not null and r.{book_key} is not null and r.{last_upd} is not null
          and ({side_expr}) in ('home','away','draw')
    on conflict (game_uid, market_key, book_key, side, last_update) do nothing
    returning 1;
    """
    cur.execute(sql_odds)
    o = cur.rowcount if cur.rowcount != -1 else 0

print(f"BACKFILL OK; games={g}, markets={m}, odds_inserts={o}")