\set aid random(1, :max_aid)

BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
END;