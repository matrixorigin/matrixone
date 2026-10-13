-- fulltext2 DATALINK content indexing on the async CDC path. A DATALINK column must be
-- indexed by its resolved file CONTENT (PDF/DOCX text extracted via load_text/GetPlainText),
-- not by the URL string — the SAME as the synchronous build. fulltext2 is AlwaysAsync, so an
-- inline index builds an empty tag=0 base at CREATE and the datalink rows below flow through
-- ISCP CDC; the fulltext2 CDC writer resolves each datalink via SELECT load_text(...) run by
-- the internal SQL executor (which has a full proc: FileService + catalog for stage://).
-- durable cdc_tail polling waits for CDC to settle (the per-CN search cache is not
-- cross-invalidated, so a too-early MATCH can pin a stale snapshot), then MATCH against
-- the file CONTENT must find them. Regression guard for CDC-vs-build datalink parity.
set experimental_fulltext2_index = 1;

create stage ft2stage URL='file://$resources/fulltext/';

-- INSERT parity: an inline FULLTEXT2 index (empty base) + datalink rows via CDC. Default
-- (word) parser, mirroring the classic fulltext datalink test: mo.pdf contains 'matrixone',
-- chinese.pdf contains '慢慢地'.
create table dl (id bigint primary key, fpath datalink, FULLTEXT2 ftidx(fpath));
insert into dl values (0, 'stage://ft2stage/mo.pdf'), (1, 'stage://ft2stage/chinese.pdf');

-- dl2 is independent of dl. Start both initial CDC builds together; dl2 is not
-- searched until after its update, preserving the cache-safety contract below.
create table dl2 (id bigint primary key, fpath datalink, FULLTEXT2 ftidx(fpath));
insert into dl2 values (0, 'stage://ft2stage/mo.pdf');
set @dl_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dl') limit 1);
set @dl2_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dl2') limit 1);
set @wait_dl_initial_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @dl_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1) as dl_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @dl2_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1) as dl2_ready'
);
prepare wait_dl_initial from @wait_dl_initial_sql;
-- @wait_expect(2, 120)
execute wait_dl_initial;
deallocate prepare wait_dl_initial;

-- CDC must have indexed the resolved CONTENT, not the URL string.
select id from dl where match(fpath) against('matrixone');
select id from dl where match(fpath) against('慢慢地' in natural language mode);
-- If CDC had indexed the URL string instead, these URL tokens would match — they must NOT.
select id from dl where match(fpath) against('ft2stage');
select id from dl where match(fpath) against('mo');

-- UPDATE parity: a SEPARATE table so its first MATCH runs only AFTER the final mutation
-- has settled — fulltext2's per-CN search cache is not cross-invalidated by the CDC
-- consumer, so a pre-update MATCH could pin a stale snapshot on multi-CN (see
-- fulltext2_async). Updating the datalink must replace the old content terms with the
-- new file's terms.
set @capture_dl2_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @dl2_tail_before_update from `', database(), '`.`', @dl2_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_dl2_tail from @capture_dl2_tail_sql;
execute capture_dl2_tail;
deallocate prepare capture_dl2_tail;
update dl2 set fpath='stage://ft2stage/file-sample_100kB.docx' where id=0;
set @wait_dl2_update_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @dl2_tail_before_update,
    ' as dl2_update_ready from `', database(), '`.`', @dl2_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_dl2_update from @wait_dl2_update_sql;
-- @wait_expect(2, 120)
execute wait_dl2_update;
deallocate prepare wait_dl2_update;

-- id 0's content is now the docx ('Nulla facilisi'); the old mo.pdf 'matrixone' is gone.
select id from dl2 where match(fpath) against('Nulla facilisi' in natural language mode);
select id from dl2 where match(fpath) against('matrixone');

drop table dl;
drop table dl2;
drop stage ft2stage;
