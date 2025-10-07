CREATE SCHEMA IF NOT EXISTS retention;

CREATE OR REPLACE FUNCTION retention.purge_portfolios_395d(
    p_batch    integer DEFAULT 50000,
    p_hard_cap bigint  DEFAULT 5000000,
    p_dry_run  boolean DEFAULT TRUE
)
RETURNS TABLE(affected bigint)
LANGUAGE plpgsql
AS $$
DECLARE
  cutoff timestamp := now() - interval '395 days';
  batch_count integer;
  total bigint := 0;
BEGIN
  IF p_dry_run THEN
    RETURN QUERY SELECT count(*)::bigint FROM portfolios WHERE created_at < cutoff;
    RETURN;
  END IF;

  PERFORM set_config('lock_timeout','3s',true);
  PERFORM set_config('statement_timeout','5min',true);

  LOOP
    WITH del AS (
      DELETE FROM portfolios
      WHERE ctid IN (
        SELECT ctid
        FROM portfolios
        WHERE created_at < cutoff
        ORDER BY created_at ASC
        LIMIT p_batch
      )
      RETURNING 1
    )
    SELECT count(*) INTO batch_count FROM del;

    total := total + COALESCE(batch_count,0);

    EXIT WHEN batch_count IS NULL OR batch_count = 0 OR total >= p_hard_cap;
  END LOOP;

  RETURN QUERY SELECT total;
END $$;