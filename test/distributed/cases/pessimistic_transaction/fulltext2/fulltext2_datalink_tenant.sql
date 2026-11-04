-- Tenant stage metadata does not authorize access to a CN-local source.
-- Real remote tenant stage CDC/content-update coverage lives in
-- TestSQLFileAccessTenantStageCDC with an owned HTTP object fixture.
drop account if exists ft2tenant;
create account ft2tenant admin_name 'admin' identified by 'test123';
-- @session:id=2&user=ft2tenant:admin&password=test123
set experimental_fulltext2_index = 1;
create database ft2db;
use ft2db;
create stage ft2stage URL='file://$resources/fulltext/';
create table dl (id bigint primary key, fpath datalink);
insert into dl values (0, 'stage://ft2stage/mo.pdf'), (1, 'stage://ft2stage/chinese.pdf');
select load_text(fpath) from dl;
create fulltext2 index ftidx on dl(fpath);
select count(*) as source_rows from dl;
select count(*) as admitted_indexes from mo_catalog.mo_indexes where name='ftidx' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='dl');
drop table dl;
drop stage ft2stage;
drop database ft2db;
-- @session
drop account ft2tenant;
