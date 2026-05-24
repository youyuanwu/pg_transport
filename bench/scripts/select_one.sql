-- Minimal-execution pgbench probe.
--
-- Pairs with `just pgbench … connect=1 script=bench/scripts/select_one.sql`
-- to isolate the connection-setup win from any query-execution cost.
-- A single `SELECT 1` returning one row of one int4 column — about as
-- cheap as PG can be on the server side while still doing a real
-- round-trip.
--
-- See docs/design/bench.md §2.7 for the workflow this enables.
SELECT 1;
