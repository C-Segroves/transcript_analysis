-- check_scoring.sql
-- Verify the scoring fleet is actually making progress.
-- Run with:  sudo -u postgres psql -d postgres -f sql/check_scoring.sql
-- (or paste individual queries). Queries 1-4 are fast; #5 is a slow full scan.

\echo '=== 1) total score rows (fast estimate, no scan) ==='
SELECT reltuples::bigint AS total_score_rows_est
FROM pg_class WHERE relname = 'vid_score_table';

\echo ''
\echo '=== 2) IS THE FLEET WRITING SCORES RIGHT NOW? ==='
\echo '    Run this twice ~60s apart. n_tup_ins climbing = scores being written;'
\echo '    the increase per minute is roughly the whole fleet''s scoring rate.'
SELECT n_live_tup,
       n_tup_ins  AS inserts_cumulative,
       n_tup_del  AS deletes_cumulative,
       last_autoanalyze
FROM pg_stat_user_tables WHERE relname = 'vid_score_table';

\echo ''
\echo '=== 3) connected clients and what they are doing ==='
SELECT state, count(*) AS backends
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY state
ORDER BY backends DESC;

\echo ''
\echo '=== 4) real-vs-empty score split (extrapolated from a 1% sample, fast) ==='
SELECT
  count(*) FILTER (WHERE cardinality(score) > 0) * 100 AS real_scores_est,
  count(*) FILTER (WHERE cardinality(score) = 0) * 100 AS empty_scores_est
FROM vid_score_table TABLESAMPLE SYSTEM (1);

\echo ''
\echo '=== 5) EXACT completed (non-empty) score count -- SLOW full scan of ~128M rows ==='
\echo '    Uncomment to run; takes minutes and adds load, so use sparingly.'
-- SELECT count(*) AS completed_scores FROM vid_score_table WHERE cardinality(score) > 0;
