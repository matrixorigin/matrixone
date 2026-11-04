-- @suit
-- @case
-- @desc: Explicit bounded text casts of JSON extraction use existing functional indexes.
-- @label:bvt
drop database if exists functional_json_key_bvt;
create database functional_json_key_bvt;
use functional_json_key_bvt;
create table docs(id int primary key,doc json);
insert into docs values(1,'{"kind":"alpha"}'),(2,'{"kind":"beta"}'),(3,'{}'),(4,'{"kind":null}'),(5,'{"kind":"xxxxxxxxxxxxxxxxxxxxx"}'),(6,NULL);
create index idx_kind on docs ((cast(doc ->> '$.kind' as char(20))));
create index idx_max_path on docs ((cast(doc ->> '$[4294967295]' as char(20))));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='docs' and attr_has_generated=1;
drop index idx_max_path on docs;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='docs' and attr_has_generated=1;
create table empty_docs(id int primary key,doc json);
create index idx_json_value on empty_docs ((doc));
create index idx_json_extract on empty_docs ((json_extract(doc,'$.kind')));
create index idx_unbounded_text on empty_docs ((doc ->> '$.kind'));
create index idx_path_too_large on empty_docs ((cast(doc ->> '$[4294967296]' as char(20))));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='empty_docs' and attr_has_generated=1;
create index idx_path_integer_overflow on empty_docs ((cast(doc ->> '$[999999999999999999999999999999999999]' as char(20))));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='empty_docs' and attr_has_generated=1;
insert into empty_docs values(1,'{"kind":"alpha"}');
select id from empty_docs where doc ->> '$.kind'='alpha';
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='empty_docs' and attr_has_generated=1;
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
