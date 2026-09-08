-- Requires a dedicated CN with [cn.frontend] processLimitationSize = 8388608.
-- It deliberately rejects a query; do not run against a shared service.
DROP DATABASE IF EXISTS hashjoin_area_regression;
CREATE DATABASE hashjoin_area_regression;
USE hashjoin_area_regression;
CREATE TABLE src(id BIGINT, payload TEXT);
CREATE TABLE rhs(id BIGINT, flag INT);
INSERT INTO rhs VALUES (-1,0);
INSERT INTO src SELECT result, REPEAT('a',64) FROM generate_series(1,8192) g;
SET SESSION optimizer_hints='execType=1,joinOrdering=1';
-- PHYPLAN executes the query: capture the same SQL before adding the large batch.
EXPLAIN PHYPLAN SELECT s.payload,r.flag FROM src s LEFT JOIN rhs r ON s.id=r.id AND r.flag=1;
INSERT INTO src SELECT result, REPEAT('b',2048) FROM generate_series(8193,16384) g;
SELECT s.payload,r.flag FROM src s LEFT JOIN rhs r ON s.id=r.id AND r.flag=1;
SELECT COUNT(*) FROM src;
-- @session:id=2&user=dump&password=111
SELECT COUNT(*) FROM hashjoin_area_regression.src;
-- @session
SET SESSION optimizer_hints='';
DROP DATABASE hashjoin_area_regression;
