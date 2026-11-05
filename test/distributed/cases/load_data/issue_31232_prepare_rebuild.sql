-- #31232 follow-up: a prepared inline LOAD must keep its SET NULLIF mapping
-- when ONLY_FULL_GROUP_BY changes between PREPARE and EXECUTE. The execution
-- wrapper then rebuilds the whole plan from the retained statement AST, and
-- that replan must normalize and serialize the same NULLIF rules the first
-- planning pass produced. Both EXECUTE calls must therefore store NULL for the
-- sentinel value 0; DEALLOCATE and the restored sql_mode leave no residue.
--
-- Row-variable group: a prepared inline LOAD that routes a raw field through
-- a row variable re-derives its NULLIF projection from the retained statement
-- on the same rebuild. The @v field feeds SET payload=NULLIF(@v,'sentinel'):
-- a nonmatching field value is stored as supplied and a matching literal maps
-- to NULL, after both the no-flip and the mode-flip EXECUTE. The target
-- default 'fallback' must never replace the assigned value.

SET @old_sql_mode = @@session.sql_mode;
drop table if exists t31232_prepare_rebuild;
create table t31232_prepare_rebuild(id int primary key, s varchar(20), flag int);

SET SESSION sql_mode = '';
PREPARE p31232_rebuild FROM "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\n2,beta,7\n' INTO TABLE t31232_prepare_rebuild FIELDS TERMINATED BY ',' SET flag=NULLIF(flag,0)";

-- EXECUTE with no mode change: the plan saved at PREPARE time is reused.
EXECUTE p31232_rebuild;
select id, s, ifnull(flag, 'N') as f_isnull from t31232_prepare_rebuild order by id;

-- The mode change forces the only-full-group-by rebuild path; the replanned
-- load must still map the sentinel 0 to NULL.
truncate table t31232_prepare_rebuild;
SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY';
EXECUTE p31232_rebuild;
select id, s, ifnull(flag, 'N') as f_isnull from t31232_prepare_rebuild order by id;

DEALLOCATE PREPARE p31232_rebuild;
drop table if exists t31232_prepare_rebuild;

drop table if exists t31232_prepare_rebuild_rowvar;
create table t31232_prepare_rebuild_rowvar(id int primary key, payload varchar(32) default 'fallback');

SET SESSION sql_mode = '';
PREPARE p31232_rowvar FROM "LOAD DATA INLINE FORMAT='csv', DATA='1,keep\n2,sentinel\n' INTO TABLE t31232_prepare_rebuild_rowvar FIELDS TERMINATED BY ',' (id, @v) SET payload=NULLIF(@v,'sentinel')";

-- EXECUTE with no mode change: the projection from PREPARE time is reused.
EXECUTE p31232_rowvar;
select id, payload, payload is null as is_sql_null from t31232_prepare_rebuild_rowvar order by id;

-- The mode change forces the rebuild; the replanned projection must still
-- assign the supplied value and map the matching literal to NULL, never the
-- target column default.
truncate table t31232_prepare_rebuild_rowvar;
SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY';
EXECUTE p31232_rowvar;
select id, payload, payload is null as is_sql_null from t31232_prepare_rebuild_rowvar order by id;

DEALLOCATE PREPARE p31232_rowvar;
SET SESSION sql_mode = @old_sql_mode;
drop table if exists t31232_prepare_rebuild_rowvar;
