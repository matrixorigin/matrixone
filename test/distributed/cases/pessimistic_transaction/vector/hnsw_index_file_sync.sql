-- HNSW CDC sync: each sync rewrites the models it changes, under the same index_id with a new
-- checksum in their metadata rows. Their chunk rows cover the recorded sizes after every sync, a
-- search sees each change, and a named snapshot taken before a sync still reads the models it
-- replaced.
set experimental_hnsw_index = 1;
drop database if exists hnsw_index_file_sync;
create database hnsw_index_file_sync;
use hnsw_index_file_sync;

create table t (id bigint primary key, v vecf32(8));
insert into t select result, concat('[', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ',', rand(), ']')
from generate_series(1, 2000) g;
insert into t values (1000001, '[50,50,50,50,50,50,50,50]');
create index ix using hnsw on t(v) op_type 'vector_l2_ops';
set @t_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @t_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'hnsw_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @t_meta, '` where filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @t_meta,
    '`) = (select count(*) from `', database(), '`.`', @t_store, '`) as covered');
prepare covered from @covered_sql;
execute covered;
set @ts_sql = concat('select max(`timestamp`) into @ts from `', database(), '`.`', @t_meta, '`');
prepare ts from @ts_sql;
set @synced_sql = concat('select max(`timestamp`) > cast(@ts as bigint) as synced from `', database(), '`.`', @t_meta, '`');
select id from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;

-- a CN that did not run a sync sees its new models on its freshness sweep; shorten it so a
-- search served by another CN observes each sync within the waits below
set @freshness_override = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s');
select @freshness_override not like '%failed=[%' as all_cn_updated;

-- an insert: the sync rewrites the model, its rows cover the new size, and it is searched
execute ts;
prepare synced from @synced_sql;
insert into t values (1000002, '[-50,-50,-50,-50,-50,-50,-50,-50]');
-- @wait_expect(2, 120)
execute synced;
execute covered;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v, '[-50,-50,-50,-50,-50,-50,-50,-50]') limit 1;

-- a snapshot, then an update and a delete applied by the next sync
drop snapshot if exists hnsw_index_file_sync_sp;
create snapshot hnsw_index_file_sync_sp for account;
execute ts;
update t set v = '[50,-50,50,-50,50,-50,50,-50]' where id = 1000002;
delete from t where id = 1000001;
-- @wait_expect(2, 120)
execute synced;
execute covered;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v, '[50,-50,50,-50,50,-50,50,-50]') limit 1;
select count(*) from t where id = 1000001;

-- the named snapshot before that sync still reads the models it replaced
select id from t {snapshot = 'hnsw_index_file_sync_sp'} order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
select id from t {snapshot = 'hnsw_index_file_sync_sp'} order by l2_distance(v, '[-50,-50,-50,-50,-50,-50,-50,-50]') limit 1;
drop snapshot hnsw_index_file_sync_sp;

deallocate prepare covered;
deallocate prepare ts;
deallocate prepare synced;
set @freshness_restore = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');
select @freshness_restore not like '%failed=[%' as all_cn_restored;
drop database hnsw_index_file_sync;
