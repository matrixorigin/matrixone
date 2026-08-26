-- SHOW CREATE TABLE round-trip for fulltext2 index options. The shared option renderer
-- (IndexParamsToStringList, also used by checkpoint restore) must emit every persisted
-- build/maintenance option so a rebuilt / restored / cloned index keeps them instead of
-- silently reverting to defaults. In particular the AUTO_UPDATE compaction cadence
-- (DAY/HOUR/SECOND) must round-trip — SECOND was previously dropped. The rendered DDL must
-- also re-parse (a table created from SHOW CREATE output produces the identical SHOW CREATE).
set experimental_fulltext2_index = 1;

drop database if exists ft2_showcreate;
create database ft2_showcreate;
use ft2_showcreate;

-- (1) parser + capacity options round-trip.
create table t1 (id bigint primary key, body text);
create fulltext2 index ft on t1(body) max_index_capacity 2;
show create table t1;

-- (2) AUTO_UPDATE cadence: SECOND must appear (regression guard for the dropped-SECOND bug).
create table t2 (id bigint primary key, body text);
create fulltext2 index ft on t2(body) auto_update = true second 5;
show create table t2;

-- (3) DAY/HOUR cadence also round-trips.
create table t3 (id bigint primary key, body text);
create fulltext2 index ft on t3(body) auto_update = true day 1 hour 2;
show create table t3;

-- (4) re-parse: a table created verbatim from a rendered SHOW CREATE clause is accepted and
-- renders identically (the rendered DDL is valid, re-executable SQL).
create table t4 (
  id bigint NOT NULL,
  body text DEFAULT NULL,
  PRIMARY KEY (id),
  FULLTEXT2 ft(body) WITH PARSER ngram auto_update = true second = 5
);
show create table t4;

drop database ft2_showcreate;
