-- @suit
-- @case
-- @desc: Bounded typed JSON functional keys preserve canonical hidden-column lifecycle.
-- @label:bvt
drop database if exists functional_json_key_bvt;
create database functional_json_key_bvt;
use functional_json_key_bvt;
create table docs(id int primary key,doc json);
insert into docs values(1,'{"kind":"alpha"}'),(2,'{"kind":"beta"}'),(3,'{}'),(4,'{"kind":null}'),(5,'{"kind":"xxxxxxxxxxxxxxxxxxxxx"}'),(6,NULL);
create index idx_kind on docs ((cast(doc ->> '$.kind' as char(20))));
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
select id from docs ignore index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='null' order by id;
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='xxxxxxxxxxxxxxxxxxxx' order by id;
select id from docs where cast(doc ->> '$.kind' as char(20)) is null order by id;
update docs set doc='{"kind":"alpha"}' where id=2;
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
begin;
delete from docs where id=1;
rollback;
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
create table cloned like docs;
insert into cloned select * from docs;
select id from cloned force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
alter table docs add column extra int, algorithm=copy;
select id from docs force index(idx_kind) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='docs' and attr_has_generated=1;
drop index idx_kind on docs;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='docs' and attr_has_generated=1;
select id from docs where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
drop database functional_json_key_bvt;
