-- Acquire local document contents with foreground administrator authority.
-- The indexed TEXT rows then use normal CDC, without background file grants.
set experimental_fulltext2_index = 1;
create stage ft2stage URL='file://$resources/fulltext/';
create table dl (id bigint primary key, fpath text, fulltext2 ftidx(fpath));
insert into dl values (0, load_text('stage://ft2stage/mo.pdf')), (1, load_text('stage://ft2stage/chinese.pdf'));
set @dl_ft2_index = (select index_table_name from mo_catalog.mo_indexes where name='ftidx' and algo_table_type='ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='dl') limit 1);
set @dl_wait_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as dl_ready from `', database(), '`.`', @dl_ft2_index, '` where index_id = ''cdc_tail'' and tag = 1');
prepare dl_wait from @dl_wait_sql;
-- @wait_expect(1, 120)
execute dl_wait;
deallocate prepare dl_wait;
select id from dl where match(fpath) against('matrixone');
select id from dl where match(fpath) against('慢慢地' in natural language mode);
select id from dl where match(fpath) against('ft2stage');
select id from dl where match(fpath) against('mo');
create table dl2 (id bigint primary key, fpath text);
insert into dl2 values (0, load_text('stage://ft2stage/file-sample_100kB.docx'));
create fulltext2 index ftidx on dl2(fpath);
select id from dl2 where match(fpath) against('Nulla facilisi' in natural language mode);
select id from dl2 where match(fpath) against('matrixone');
drop table dl;
drop table dl2;
drop stage ft2stage;
