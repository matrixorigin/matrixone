-- Regression for #30660: DELETE then INSERT reuses the deleted node's usearch slot
-- and rewrites it at the new vector's position. Rows that were reachable only through
-- the deleted node must still be returned. Points on a line link each node only to its
-- two line neighbours, so before the fix every id below the deleted one disappeared.
SET experimental_hnsw_index = 1;
SET hnsw_threads_build = 1;

drop database if exists hnsw_delete_insert;
create database hnsw_delete_insert;
use hnsw_delete_insert;

create table t(id bigint primary key, v vecf32(3));
insert into t select result, cast(concat('[', result, ',1,2]') as vecf32(3)) from generate_series(1, 1000) g;
create index hv using hnsw on t(v) op_type 'vector_l2_ops';
alter table t alter reindex hv hnsw FORCE_SYNC;

set @meta = (select index_table_name from mo_catalog.mo_indexes where name = 'hv' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_initial_sql = concat('select count(*) = 1 and sum(nrow) = 1000 as initial_ready from `', database(), '`.`', @meta, '`');
prepare wait_initial from @wait_initial_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_initial;
deallocate prepare wait_initial;

select count(*), count(distinct id) from (select id from t order by l2_distance(v, '[777.2,1,2]') limit 1000) q;

-- Delete id 100, then insert a row far from it.
set @capture_sql = concat('select checksum from `', database(), '`.`', @meta, '` limit 1 into @before');
prepare capture from @capture_sql;
execute capture;
deallocate prepare capture;

delete from t where id = 100;
insert into t values (2001, '[777.2,1,2]');

-- Open a transaction so the proxy pins this connection to its current CN: proxy
-- code (doc.go Connection balance rule 3; tunnel.go transfer gate on the tracked
-- inTxn flag) refuses tunnel migration while a transaction is open. The catch-up
-- waits and the exact assertions below therefore all observe the SAME serving
-- index cache; without the pin an inter-statement rebalance could hand the
-- assertions to a CN that has not loaded the generation yet.
begin;

-- A changed checksum proves the published model applied the DELETE and the INSERT.
set @wait_delta_sql = concat('select checksum <> @before and nrow = 1000 as delta_ready from `', database(), '`.`', @meta, '` limit 1');
prepare wait_delta from @wait_delta_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_delta;
deallocate prepare wait_delta;

-- The delta probe only proves the model was published to the hidden tables;
-- the CN serving this connection must also have loaded that generation before
-- the exact assertions below are meaningful under lag-serving index semantics.
set @wait_serve_sql = 'select (select id from t order by l2_distance(v, ''[777.2,1,2]'') limit 1) = 2001 as serve_ready';
prepare wait_serve from @wait_serve_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_serve;
deallocate prepare wait_serve;

select count(*), count(distinct id), sum(id < 100) as below_deleted from (select id from t order by l2_distance(v, '[1,1,2]') limit 1000) q;
select count(*), count(distinct id), sum(id < 100) as below_deleted from (select id from t order by l2_distance(v, '[777.2,1,2]') limit 1000) q;
select count(*), count(distinct id), sum(id < 100) as below_deleted from (select id from t order by l2_distance(v, '[1000,1,2]') limit 1000) q;
select id from t order by l2_distance(v, '[1,1,2]') limit 3;
select id from t order by l2_distance(v, '[777.2,1,2]') limit 1;
commit;

-- A second cut in the middle of the line: delete id 500, insert another far row.
set @capture_sql = concat('select checksum from `', database(), '`.`', @meta, '` limit 1 into @before');
prepare capture from @capture_sql;
execute capture;
deallocate prepare capture;

delete from t where id = 500;
insert into t values (2002, '[888.8,1,2]');

begin;

prepare wait_delta from @wait_delta_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_delta;
deallocate prepare wait_delta;

-- Same serving-view readiness for the second cut (2002/'[888.8,1,2]').
set @wait_serve_sql = 'select (select id from t order by l2_distance(v, ''[888.8,1,2]'') limit 1) = 2002 as serve_ready';
prepare wait_serve from @wait_serve_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_serve;
deallocate prepare wait_serve;

select count(*), count(distinct id), sum(id < 500) as below_deleted from (select id from t order by l2_distance(v, '[888.8,1,2]') limit 1000) q;
select id from t order by l2_distance(v, '[1,1,2]') limit 3;
commit;

drop database hnsw_delete_insert;
SET experimental_hnsw_index = 0;
SET hnsw_threads_build = 0;
