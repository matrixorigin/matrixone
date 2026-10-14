-- System-owned local Datalinks are admitted during a synchronous non-empty
-- index build. Async source-content resolution is covered with remote stages
-- by TestSQLFileAccessTenantStageCDC, including PDF/DOCX and UPDATE parity.
set experimental_fulltext2_index = 1;
create stage ft2stage URL='file://$resources/fulltext/';
create table dl (id bigint primary key, fpath datalink);
insert into dl values (0, 'stage://ft2stage/mo.pdf'), (1, 'stage://ft2stage/chinese.pdf');
create fulltext2 index ftidx on dl(fpath);
select id from dl where match(fpath) against('matrixone');
select id from dl where match(fpath) against('慢慢地' in natural language mode);
select id from dl where match(fpath) against('ft2stage');
select id from dl where match(fpath) against('mo');
create table dl2 (id bigint primary key, fpath datalink);
insert into dl2 values (0, 'stage://ft2stage/file-sample_100kB.docx');
create fulltext2 index ftidx on dl2(fpath);
select id from dl2 where match(fpath) against('Nulla facilisi' in natural language mode);
select id from dl2 where match(fpath) against('matrixone');
drop table dl;
drop table dl2;
drop stage ft2stage;
