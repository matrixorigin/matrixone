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
select sleep(30);

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
