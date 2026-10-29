-- fulltext2 CDC tail frames: every frame has a metadata row covering its chunks and is
-- searched; a tail with frames that have no metadata row (written before every frame had one)
-- is still searched, from its chunk rows, and MERGE folds it into the base without losing rows.
set experimental_fulltext2_index = 1;
drop database if exists ft2_index_file_tail;
create database ft2_index_file_tail;
use ft2_index_file_tail;

create table t (id bigint primary key, body text);
insert into t values (1,'hello world');
create fulltext2 index ft on t(body);
set @t_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @t_store = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @frames_sql = concat('select count(*) as frames from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare frames from @frames_sql;
set @covered_sql = concat(
    'select (select coalesce(sum((filesize + 65535) div 65536), 0) from `', database(), '`.`', @t_meta,
    '` where index_id like ''cdc_tail:%'') = (select count(*) from `', database(), '`.`', @t_store,
    '` where index_id = ''cdc_tail'' and tag = 1) as covered');
prepare covered from @covered_sql;

-- a CDC frame gets a metadata row covering its chunks, and is searched
insert into t values (2,'hello matrix'),(3,'hello tail');
-- @wait_expect(2, 120)
execute frames;
execute covered;
-- @wait_expect(2, 60)
select id from t where match(body) against('hello') order by id;

-- a frame without a metadata row is still searched, beside a newer frame that has one
set @drop_frames_sql = concat('delete from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare drop_frames from @drop_frames_sql;
execute drop_frames;
deallocate prepare drop_frames;
execute covered;
insert into t values (4,'hello again');
-- @wait_expect(2, 120)
execute frames;
execute covered;
-- @wait_expect(2, 60)
select id from t where match(body) against('hello') order by id;

-- MERGE folds the whole tail, the frame without a row included, into the base
drop snapshot if exists ft2_index_file_tail_sp;
create snapshot ft2_index_file_tail_sp for account;
set @frames_before_sql = concat('select count(*) into @frames_before from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare frames_before from @frames_before_sql;
execute frames_before;
deallocate prepare frames_before;
delete from t where id = 1;
set @deleted_sql = concat('select count(*) > @frames_before as deleted from `', database(), '`.`', @t_meta, '` where index_id like ''cdc_tail:%''');
prepare deleted from @deleted_sql;
-- @wait_expect(2, 120)
execute deleted;
deallocate prepare deleted;
select id from t where match(body) against('hello') order by id;
alter table t alter reindex ft fulltext2 merge;
execute frames;
select id from t where match(body) against('hello') order by id;

-- a named snapshot taken before MERGE still reads the index files MERGE replaced
select id from t {snapshot = 'ft2_index_file_tail_sp'} where match(body) against('hello') order by id;
drop snapshot ft2_index_file_tail_sp;

deallocate prepare frames;
deallocate prepare covered;
drop database ft2_index_file_tail;
