-- fulltext2 DATALINK CDC resolution under a NON-SYSTEM tenant. A datalink stage:// is a
-- per-account catalog object (mo_stages is tenant-scoped). ISCP forces the CDC consumer
-- ctx tenant to System_Account, so the resolver must run under the SOURCE tenant that owns
-- the table (r.GetAccountID()) — not the ctx tenant — else the tenant's stage is looked up
-- in the system tenant's mo_stages and resolution fails (empty MATCH) or hits an unrelated
-- same-named system stage. This runs the whole datalink flow inside a fresh tenant so a
-- regression to GetAccountId(ctx) is caught: the MATCH below would go empty.
drop account if exists ft2tenant;
create account ft2tenant admin_name 'admin' identified by 'test123';

-- @session:id=2&user=ft2tenant:admin&password=test123
set experimental_fulltext2_index = 1;
create database ft2db;
use ft2db;

-- stage + fulltext2 datalink index owned by THIS tenant.
create stage ft2stage URL='file://$resources/fulltext/';
create table dl (id bigint primary key, fpath datalink, FULLTEXT2 ftidx(fpath));
insert into dl values (0, 'stage://ft2stage/mo.pdf'), (1, 'stage://ft2stage/chinese.pdf');
-- Poll the tenant-owned hidden tail from the tenant session. This proves that
-- its CDC writer resolved and durably indexed the rows before the first MATCH.
set @dl_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dl') limit 1);
set @wait_tenant_dl_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as tenant_dl_ready from `', database(), '`.`', @dl_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_tenant_dl from @wait_tenant_dl_sql;
-- @wait_expect(2, 120)
execute wait_tenant_dl;
deallocate prepare wait_tenant_dl;

-- CDC must have resolved the datalink CONTENT under this tenant's mo_stages: 'matrixone'
-- (mo.pdf), '慢慢地' (chinese.pdf). If the resolver ran under System_Account, the tenant's
-- ft2stage would not be found and these would be empty.
select id from dl where match(fpath) against('matrixone');
select id from dl where match(fpath) against('慢慢地' in natural language mode);

drop table dl;
drop stage ft2stage;
drop database ft2db;
-- @session

drop account ft2tenant;
