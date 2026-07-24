-- fulltext2 DATALINK content indexing on the async CDC path. A DATALINK column must be
-- indexed by its resolved file CONTENT (PDF/DOCX text extracted via load_text/GetPlainText),
-- not by the URL string — the SAME as the synchronous build. fulltext2 is AlwaysAsync, so an
-- inline index builds an empty tag=0 base at CREATE and the datalink rows below flow through
-- ISCP CDC; the fulltext2 CDC writer resolves each datalink via SELECT load_text(...) run by
-- the internal SQL executor (which has a full proc: FileService + catalog for stage://).
-- sleep() waits for CDC to settle (the per-CN search cache is not cross-invalidated, so a
-- too-early MATCH can pin a stale snapshot), then MATCH against the file CONTENT must find
-- them. Regression guard for the CDC-vs-build datalink parity fix.
set experimental_fulltext2_index = 1;

create stage ft2stage URL='file://$resources/fulltext/';

-- INSERT parity: an inline FULLTEXT2 index (empty base) + datalink rows via CDC. Default
-- (word) parser, mirroring the classic fulltext datalink test: mo.pdf contains 'matrixone',
-- chinese.pdf contains '慢慢地'.
create table dl (id bigint primary key, fpath datalink, FULLTEXT2 ftidx(fpath));
insert into dl values (0, 'stage://ft2stage/mo.pdf'), (1, 'stage://ft2stage/chinese.pdf');
select sleep(30);

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
create table dl2 (id bigint primary key, fpath datalink, FULLTEXT2 ftidx(fpath));
insert into dl2 values (0, 'stage://ft2stage/mo.pdf');
select sleep(30);
update dl2 set fpath='stage://ft2stage/file-sample_100kB.docx' where id=0;
select sleep(30);

-- id 0's content is now the docx ('Nulla facilisi'); the old mo.pdf 'matrixone' is gone.
select id from dl2 where match(fpath) against('Nulla facilisi' in natural language mode);
select id from dl2 where match(fpath) against('matrixone');

drop table dl;
drop table dl2;
drop stage ft2stage;
