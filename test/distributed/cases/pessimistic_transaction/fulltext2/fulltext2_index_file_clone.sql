-- fulltext2 index files under CLONE and snapshot RESTORE: the copy carries the base segment,
-- a tail frame larger than one chunk (read through the index-file cache) and a one-chunk tail
-- frame (read from its chunk row), each with its metadata row, and is searched from them. A
-- MERGE or DROP of the source afterwards does not touch the copy, and a restore from a
-- snapshot taken before MERGE brings back the index files MERGE replaced.
set experimental_fulltext2_index = 1;
drop database if exists ft2_index_file_clone;
create database ft2_index_file_clone;
use ft2_index_file_clone;

create table t (id bigint primary key, body text);
insert into t values (1,'hello world');
create fulltext2 index ft on t(body);
insert into t select result + 100, concat('bulk w', result, ' ', repeat('filler ', 10)) from generate_series(1, 3000) g;

set @t_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @t_frames_sql = concat('select count(*) as frames, max(filesize) > 65536 as large from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare t_frames from @t_frames_sql;
-- @wait_expect(2, 120)
execute t_frames;
insert into t values (2,'hello small');
set @t_small_sql = concat('select count(*) as frames, min(filesize) <= 65536 as small from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare t_small from @t_small_sql;
-- @wait_expect(2, 120)
execute t_small;
deallocate prepare t_frames;
deallocate prepare t_small;

-- CLONE copies the base, both tail frames and their metadata rows
create table c clone t;
set @c_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'c') limit 1);
set @c_files_sql = concat('select sum(index_id like ''cdc_tail:%'') as frames, sum(index_id not like ''cdc_tail:%'') as bases from `', database(), '`.`', @c_meta, '`');
prepare c_files from @c_files_sql;
execute c_files;
set @c_covered_sql = concat(
    'select (select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @c_meta,
    '` where index_id like ''cdc_tail:%'') = (select count(*) from `', database(), '`.`', @c_store,
    '` where index_id = ''cdc_tail'' and tag = 1) as covered');
prepare c_covered from @c_covered_sql;
execute c_covered;
select id from c where match(body) against('hello') order by id;
select count(*) from c where match(body) against('bulk');
select id from c where match(body) against('w2999') order by id;

-- a snapshot before MERGE, then MERGE the source: the clone is untouched
drop snapshot if exists ft2_index_file_clone_sp;
create snapshot ft2_index_file_clone_sp for account;
alter table t alter reindex ft fulltext2 merge;
set @t_tail_sql = concat('select count(*) as frames from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare t_tail from @t_tail_sql;
execute t_tail;
deallocate prepare t_tail;
select id from t where match(body) against('hello') order by id;
select count(*) from t where match(body) against('bulk');
execute c_files;
select id from c where match(body) against('hello') order by id;
select count(*) from c where match(body) against('bulk');

-- dropping the source leaves the clone searchable
drop table t;
select id from c where match(body) against('hello') order by id;
select count(*) from c where match(body) against('bulk');
deallocate prepare c_files;
deallocate prepare c_covered;

-- RESTORE from the snapshot before MERGE brings back the tail frames and their files
restore database sys.ft2_index_file_clone {snapshot = 'ft2_index_file_clone_sp'};
set @r_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @r_covered_sql = concat(
    'select (select count(*) from `', database(), '`.`', @r_meta, '` where index_id like ''cdc_tail:%'') as frames, ',
    '(select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @r_meta,
    '` where index_id like ''cdc_tail:%'') = (select count(*) from `', database(), '`.`', @r_store,
    '` where index_id = ''cdc_tail'' and tag = 1) as covered');
prepare r_covered from @r_covered_sql;
execute r_covered;
deallocate prepare r_covered;
select id from t where match(body) against('hello') order by id;
select count(*) from t where match(body) against('bulk');
select id from t where match(body) against('w2999') order by id;

drop snapshot ft2_index_file_clone_sp;
drop database ft2_index_file_clone;
