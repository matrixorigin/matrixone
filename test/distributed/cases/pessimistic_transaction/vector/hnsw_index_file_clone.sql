-- HNSW model files under CLONE, CDC sync, REINDEX, DROP and snapshot RESTORE: the copy carries
-- the models and their metadata rows, and their chunk rows cover the recorded sizes. The first
-- reader of the copy's models is its background CDC sync, which runs without a query process and
-- finds no cache entry under the copy's table, so it rebuilds them from the rows. A REINDEX or DROP
-- of the source afterwards does not touch the copy, and a restore from a snapshot taken before the
-- REINDEX brings back the models it replaced.
set experimental_hnsw_index = 1;
drop database if exists hnsw_index_file_clone;
create database hnsw_index_file_clone;
use hnsw_index_file_clone;

create table t (id bigint primary key, v vecf32(8));
-- random rows, and points far from them and from each other that each search targets
insert into t select result, concat('[', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ']')
from generate_series(1, 2000) g;
insert into t values (1000001, '[50,50,50,50,50,50,50,50]');
create index ix using hnsw on t(v) op_type 'vector_l2_ops';
select id from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;

-- a CN that did not run a sync sees its new models on its freshness sweep; shorten it so a
-- search served by another CN observes each sync within the waits below
set @freshness_override = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s');
select @freshness_override not like '%failed=[%' as all_cn_updated;

-- CLONE copies the models and their metadata rows
create table c clone t;
set @c_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @c_meta, '` where filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @c_meta,
    '`) = (select count(*) from `', database(), '`.`', @c_store, '`) as covered');
prepare c_covered from @c_covered_sql;
execute c_covered;
set @c_ts_sql = concat('select max(`timestamp`) into @c_ts from `', database(), '`.`', @c_meta, '`');
prepare c_ts from @c_ts_sql;
execute c_ts;
deallocate prepare c_ts;

-- nothing reads the copy's index before its CDC sync applies this row to its models
insert into c values (1000002, '[-50,-50,-50,-50,-50,-50,-50,-50]');
set @c_synced_sql = concat('select max(`timestamp`) > cast(@c_ts as bigint) as synced from `', database(), '`.`', @c_meta, '`');
prepare c_synced from @c_synced_sql;
-- @wait_expect(2, 120)
execute c_synced;
deallocate prepare c_synced;
execute c_covered;
-- @wait_expect(2, 60)
select id from c order by l2_distance(v, '[-50,-50,-50,-50,-50,-50,-50,-50]') limit 1;
select id from c order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;

-- a snapshot before REINDEX, then REINDEX the source: the copy is untouched
drop snapshot if exists hnsw_index_file_clone_sp;
create snapshot hnsw_index_file_clone_sp for account;
alter table t alter reindex ix hnsw force_sync;
select id from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
execute c_covered;
select id from c order by l2_distance(v, '[-50,-50,-50,-50,-50,-50,-50,-50]') limit 1;

-- dropping the source leaves the copy searchable
drop table t;
select id from c order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
deallocate prepare c_covered;

-- RESTORE from the snapshot before REINDEX brings back the source and the models REINDEX replaced
restore database sys.hnsw_index_file_clone {snapshot = 'hnsw_index_file_clone_sp'};
set @r_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @r_meta, '` where filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @r_meta,
    '`) = (select count(*) from `', database(), '`.`', @r_store, '`) as covered');
prepare r_covered from @r_covered_sql;
execute r_covered;
deallocate prepare r_covered;
select id from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
select id from c order by l2_distance(v, '[-50,-50,-50,-50,-50,-50,-50,-50]') limit 1;

drop snapshot hnsw_index_file_clone_sp;
set @freshness_restore = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');
select @freshness_restore not like '%failed=[%' as all_cn_restored;
drop database hnsw_index_file_clone;
