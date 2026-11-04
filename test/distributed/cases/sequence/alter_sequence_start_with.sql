# issue #30679: ALTER SEQUENCE ... START WITH must persist a direction-aware
# restart that every later session observes; a start at or behind the stored
# position must never re-enter consumed values; the final definition is
# validated before persistence and catalog metadata stays replayable.
# v12-review F2: rel_createsql records the complete validated effective
# definition (type, increment, bounds, start, cycle) and replays byte-for-byte
# under a new name; v12-review F3 extended by v17 (E1): an uncalled sequence
# whose kept pending value falls outside the final bounds restarts at the
# declared start uniformly for CYCLE and NO CYCLE alike, while an uncalled
# start behind the pending value keeps the pending value (same direction-
# aware rule as a called sequence).
drop database if exists alter_seq_start_db;
create database alter_seq_start_db;
use alter_seq_start_db;

-- ascending restart: consumed 1,2,3; START WITH 500 restarts to exactly 500.
create sequence asw_asc as bigint increment by 1 minvalue 1 maxvalue 100000 start with 1 no cycle;
select nextval('asw_asc');
select nextval('asw_asc');
select nextval('asw_asc');
alter sequence asw_asc start with 500;
select * from asw_asc;

-- a fresh session observes the persisted restart, not the pre-alter position.
-- @session:id=1
use alter_seq_start_db;
select nextval('asw_asc');
select nextval('asw_asc');
-- @session

select * from asw_asc;

-- a start behind the stored position keeps the position; only the recorded
-- start boundary moves to the frontier, and allocation continues past it.
alter sequence asw_asc start with 300;
select * from asw_asc;
select nextval('asw_asc');
alter sequence asw_asc start with 502;
select * from asw_asc;
select nextval('asw_asc');
select nextval('asw_asc');
drop sequence asw_asc;

-- descending: consumed 100,99,98. START WITH 99 is behind the stored
-- position, so the next value is 97; START WITH 50 restarts to exactly 50.
create sequence asw_desc as bigint increment by -1 minvalue 1 maxvalue 100 start with 100 no cycle;
select nextval('asw_desc');
select nextval('asw_desc');
select nextval('asw_desc');
alter sequence asw_desc start with 99;
select nextval('asw_desc');
alter sequence asw_desc start with 50;
select * from asw_desc;
-- @session:id=1
use alter_seq_start_db;
select nextval('asw_desc');
-- @session
drop sequence asw_desc;

-- uncalled sequence: restart replaces the unissued start value.
create sequence asw_unc as bigint increment by 1 start with 10 no cycle;
alter sequence asw_unc start with 30;
select * from asw_unc;
select nextval('asw_unc');
drop sequence asw_unc;

-- combined ALTER: START plus MIN/MAX validated atomically; the catalog text
-- carries the complete effective definition (v12-review F2).
create sequence asw_replay as bigint increment by 1 minvalue 1 maxvalue 100 start with 1 no cycle;
select nextval('asw_replay');
alter sequence asw_replay minvalue 1 maxvalue 100000 start with 50000;
select * from asw_replay;
select relname, rel_createsql from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_replay';
select rel_createsql = 'create sequence if not exists `alter_seq_start_db`.`asw_replay` as bigint increment by 1 minvalue 1 maxvalue 100000 start with 50000 no cycle' from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_replay';
-- replay the catalog text itself under a new name (v12-review F2): no
-- hand-written replacement; the text the catalog stores must be sufficient.
set @ddl = (select replace(rel_createsql, 'asw_replay', 'asw_replayed') from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_replay');
prepare asw_replay_stmt from @ddl;
execute asw_replay_stmt;
deallocate prepare asw_replay_stmt;
-- every definition field of the replay matches the altered original's
-- definition fields (the runtime position/arm is not part of a CREATE).
select (select min_value from asw_replay) = (select min_value from asw_replayed);
select (select max_value from asw_replay) = (select max_value from asw_replayed);
select (select start_value from asw_replay) = (select start_value from asw_replayed);
select (select increment_value from asw_replay) = (select increment_value from asw_replayed);
select (select cycle from asw_replay) = (select cycle from asw_replayed);
select relname, rel_createsql from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_replayed';
select nextval('asw_replayed');
drop sequence asw_replay;
drop sequence asw_replayed;

-- F2 clause-loss regression: ALTER START WITH alone must not lose the
-- non-default increment, integer type, bounds or cycle of the definition.
create sequence asw_inc as int increment by 4 minvalue 1 maxvalue 1000 start with 8 cycle;
select nextval('asw_inc');
alter sequence asw_inc start with 55;
select * from asw_inc;
select rel_createsql from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_inc';
select rel_createsql = 'create sequence if not exists `alter_seq_start_db`.`asw_inc` as int increment by 4 minvalue 1 maxvalue 1000 start with 55 cycle' from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_inc';
set @ddl = (select replace(rel_createsql, 'asw_inc', 'asw_inc2') from mo_catalog.mo_tables where reldatabase = 'alter_seq_start_db' and relname = 'asw_inc');
prepare asw_inc_stmt from @ddl;
execute asw_inc_stmt;
deallocate prepare asw_inc_stmt;
select (select min_value from asw_inc) = (select min_value from asw_inc2);
select (select max_value from asw_inc) = (select max_value from asw_inc2);
select (select start_value from asw_inc) = (select start_value from asw_inc2);
select (select increment_value from asw_inc) = (select increment_value from asw_inc2);
select (select cycle from asw_inc) = (select cycle from asw_inc2);
select nextval('asw_inc2');
drop sequence asw_inc;
drop sequence asw_inc2;

-- F3 ascending NO CYCLE: the kept pending value lies outside the final
-- bounds, so the ALTER restarts at the declared start instead of emitting
-- an out-of-range value.
create sequence asw_oob as bigint increment by 1 minvalue 1 maxvalue 1000 start with 100 no cycle;
alter sequence asw_oob maxvalue 20 start with 10;
select * from asw_oob;
select nextval('asw_oob');
select * from asw_oob;
drop sequence asw_oob;

-- F3 descending NO CYCLE mirror: pending 900 above the shrunken max 850.
create sequence asw_oobd as bigint increment by -1 minvalue 1 maxvalue 1000 start with 900 no cycle;
alter sequence asw_oobd maxvalue 850 start with 700;
select * from asw_oobd;
select nextval('asw_oobd');
select * from asw_oobd;
drop sequence asw_oobd;

-- F3/#30359-review CYCLE out-of-bounds pending: the restart at the declared
-- start is uniform across CYCLE and NO CYCLE, so a never-issued emission
-- can never escape the tightened bounds (an uncalled pending 100 with the
-- new maxvalue 30 must restart at 10, never emit 100).
create sequence asw_oobc as bigint increment by 1 minvalue 1 maxvalue 1000 start with 100 cycle;
alter sequence asw_oobc maxvalue 30 start with 10;
select * from asw_oobc;
select nextval('asw_oobc');
select nextval('asw_oobc');
select * from asw_oobc;
drop sequence asw_oobc;

-- Descending CYCLE mirror: pending 900 falls below the raised minvalue 950;
-- the emission restarts at the declared start inside the new bounds.
create sequence asw_oobcd as bigint increment by -1 minvalue 1 maxvalue 1000 start with 900 cycle;
alter sequence asw_oobcd minvalue 950 start with 980;
select * from asw_oobcd;
select nextval('asw_oobcd');
select nextval('asw_oobcd');
select * from asw_oobcd;
drop sequence asw_oobcd;

-- boundary rejection is atomic: a restart above MAXVALUE fails and leaves
-- the sequence position and definition untouched.
create sequence asw_bound as bigint increment by 1 minvalue 1 maxvalue 100 start with 1 no cycle;
select nextval('asw_bound');
alter sequence asw_bound start with 200;
select * from asw_bound;
select nextval('asw_bound');
alter sequence asw_bound minvalue 1 maxvalue 100 start with 2;
select * from asw_bound;
select nextval('asw_bound');
drop sequence asw_bound;

-- F3 atomic rejection through the restart path itself: the declared start
-- must satisfy the final bounds; the sequence is untouched on failure.
create sequence asw_rej as bigint increment by 1 minvalue 1 maxvalue 1000 start with 100 no cycle;
-- @regex("STARTVALUE", true)
alter sequence asw_rej maxvalue 20 start with 50;
select * from asw_rej;
select nextval('asw_rej');
drop sequence asw_rej;

-- v17 (E1) ascending uncalled: the pending value is 10; an equal requested
-- start leaves the state untouched, and a behind start (5) KEEPS the
-- pending value while recording the declared start as the boundary — a
-- fresh session's next value stays 10, never 5.
create sequence asw_unb as bigint increment by 1 start with 10 no cycle;
alter sequence asw_unb start with 10;
select * from asw_unb;
alter sequence asw_unb start with 5;
select * from asw_unb;
-- @session:id=1
use alter_seq_start_db;
select nextval('asw_unb');
select nextval('asw_unb');
-- @session
select * from asw_unb;
drop sequence asw_unb;

-- v17 (E1) descending uncalled mirror: pending 10. START WITH 15 is behind
-- the direction of travel and keeps the pending value (fresh session 10);
-- START WITH 5 is strictly ahead and restarts the uncalled sequence to
-- exactly 5.
create sequence asw_undb as bigint increment by -1 minvalue 1 maxvalue 100 start with 10 no cycle;
alter sequence asw_undb start with 15;
select * from asw_undb;
-- @session:id=1
use alter_seq_start_db;
select nextval('asw_undb');
select nextval('asw_undb');
-- @session
drop sequence asw_undb;

create sequence asw_unda as bigint increment by -1 minvalue 1 maxvalue 100 start with 10 no cycle;
alter sequence asw_unda start with 5;
select * from asw_unda;
-- @session:id=1
use alter_seq_start_db;
select nextval('asw_unda');
select nextval('asw_unda');
-- @session
drop sequence asw_unda;

drop database alter_seq_start_db;
