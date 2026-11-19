-- ivfpq model tars as index files under CLONE, REINDEX, DROP and snapshot RESTORE: the tar's
-- chunk rows cover the size its metadata row records, CLONE copies them and the clone is searched
-- from them under its own table, a REINDEX or DROP of the source leaves the clone alone, and a
-- restore from a snapshot taken before the REINDEX brings back the earlier model. GPU REQUIRED.
SET experimental_ivfpq_index = 1;
drop database if exists ivfpq_index_file;
create database ivfpq_index_file;
use ivfpq_index_file;

create table t(id bigint primary key, v vecf32(8) not null);
insert into t select result,
    concat('[', result, ',', result, ',', result, ',', result, ',', result, ',', result, ',', result, ',', result, ']')
from generate_series(1, 5000) g;
create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists = 16 m = 2 bits_per_code = 8;

-- the model tar is stored as chunk rows covering its recorded size
set @t_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @t_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @t_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @t_meta, '` where index_id not like ''cdc_tail%'' and filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @t_meta,
    '` where index_id not like ''cdc_tail%'') = (select count(*) from `', database(), '`.`', @t_store,
    '` where tag = 0) as covered');
prepare t_covered from @t_covered_sql;
execute t_covered;
select abs(id - 50) <= 5 as near from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;

-- CLONE copies the model rows; the clone is searched from them under its own table
create table c clone t;
set @c_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @c_meta, '` where index_id not like ''cdc_tail%'' and filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @c_meta,
    '` where index_id not like ''cdc_tail%'') = (select count(*) from `', database(), '`.`', @c_store,
    '` where tag = 0) as covered');
prepare c_covered from @c_covered_sql;
execute c_covered;
select abs(id - 4000) <= 5 as near from c order by l2_distance(v, '[4000,4000,4000,4000,4000,4000,4000,4000]') limit 1;

-- a snapshot, then REINDEX the source: the source has a new model, the clone is untouched
drop snapshot if exists ivfpq_index_file_sp;
create snapshot ivfpq_index_file_sp for account;
alter table t alter reindex ix ivfpq lists = 16 force_sync;
execute t_covered;
select abs(id - 50) <= 5 as near from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
execute c_covered;
select abs(id - 4000) <= 5 as near from c order by l2_distance(v, '[4000,4000,4000,4000,4000,4000,4000,4000]') limit 1;

-- dropping the source leaves the clone searchable
drop table t;
select abs(id - 2500) <= 5 as near from c order by l2_distance(v, '[2500,2500,2500,2500,2500,2500,2500,2500]') limit 1;
deallocate prepare t_covered;
deallocate prepare c_covered;

-- RESTORE from the snapshot before REINDEX brings back the earlier model
restore database sys.ivfpq_index_file {snapshot = 'ivfpq_index_file_sp'};
set @r_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'ivfpq_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @r_meta, '` where index_id not like ''cdc_tail%'' and filesize > 0) >= 1 as has_model, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @r_meta,
    '` where index_id not like ''cdc_tail%'') = (select count(*) from `', database(), '`.`', @r_store,
    '` where tag = 0) as covered');
prepare r_covered from @r_covered_sql;
execute r_covered;
deallocate prepare r_covered;
select abs(id - 50) <= 5 as near from t order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;

drop snapshot ivfpq_index_file_sp;
drop database ivfpq_index_file;
