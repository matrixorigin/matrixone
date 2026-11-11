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
-- #32437: numeric JSON functional indexes; admission, canonical spelling, replay.
create table nj32(id int primary key,doc json);
insert into nj32 values(1,'{"a":42}'),(2,'{"a":"123"}'),(3,'{"a":null}'),(4,'{}'),(5,NULL);
create index nj32_int on nj32 ((cast(json_extract(doc,'$.a') as int)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='nj32' and attr_has_generated=1;
select id from nj32 force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42 order by id;
select id from nj32 ignore index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42 order by id;
select id from nj32 force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=123 order by id;
select id from nj32 where cast(json_extract(doc,'$.a') as int) is null order by id;
insert into nj32 values(6,'{"a":2147483648}');
insert into nj32 values(7,'{"a":7}');
select id from nj32 force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42 order by id;
create table nj32_like like nj32;
insert into nj32_like select * from nj32;
select id from nj32_like force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42 order by id;
insert into nj32_like values(8,'{"a":2147483648}');
alter table nj32 add column extra int, algorithm=copy;
select id from nj32 force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42 order by id;
insert into nj32(id,doc) values(8,'{"a":2147483648}');
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='nj32' and attr_has_generated=1;
create table nj64(id int primary key,doc json);
insert into nj64 values(1,'{"a":42}'),(2,'{"a":2147483648}'),(3,'{"a":"123"}'),(4,'{}'),(5,NULL);
create index nj64_signed on nj64 ((cast(json_extract(doc,'$.a') as signed integer)));
show create table nj64;
select id from nj64 force index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=42 order by id;
select id from nj64 ignore index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=42 order by id;
select id from nj64 force index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=2147483648 order by id;
select id from nj64 ignore index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=2147483648 order by id;
select id from nj64 where cast(json_extract(doc,'$.a') as signed) is null order by id;
create table nj64_like like nj64;
insert into nj64_like select * from nj64;
select id from nj64_like force index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=2147483648 order by id;
alter table nj64 add column extra int, algorithm=copy;
select id from nj64 force index(nj64_signed) where cast(json_extract(doc,'$.a') as signed)=2147483648 order by id;
alter table nj64 rename column doc to renamed_doc;
show create table nj64;
select id from nj64 force index(nj64_signed) where cast(json_extract(renamed_doc,'$.a') as signed)=2147483648 order by id;
create table nj64_renamed_like like nj64;
insert into nj64_renamed_like select * from nj64;
select id from nj64_renamed_like force index(nj64_signed) where cast(json_extract(renamed_doc,'$.a') as signed)=2147483648 order by id;
create table nu64(id int primary key,doc json);
insert into nu64 values(1,'{"a":42}'),(2,'{"a":4294967296}'),(3,'{"a":"77"}'),(4,'{}'),(5,NULL);
create index nu64_uns on nu64 ((cast(json_extract(doc,'$.a') as unsigned integer)));
show create table nu64;
select id from nu64 force index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=42 order by id;
select id from nu64 ignore index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=42 order by id;
select id from nu64 force index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=4294967296 order by id;
select id from nu64 ignore index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=4294967296 order by id;
select id from nu64 where cast(json_extract(doc,'$.a') as unsigned) is null order by id;
-- #32437 review repair: ordinary decimal literals bind the numeric keys through
-- the value-exact integer-to-decimal promotion; non-integer literals match no
-- integer-valued key, and force and ignore agree.
select id from nj32 force index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42.0 order by id;
select id from nj32 ignore index(nj32_int) where cast(json_extract(doc,'$.a') as int)=42.0 order by id;
select id from nj32 where cast(json_extract(doc,'$.a') as int)=123.0 order by id;
select id from nu64 force index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=42.0 order by id;
select id from nu64 ignore index(nu64_uns) where cast(json_extract(doc,'$.a') as unsigned)=42.0 order by id;
select id from nu64 where cast(json_extract(doc,'$.a') as unsigned)=42.5 order by id;
-- #32437 challenge repair: the retained NYI integer cast targets keep their native
-- bind errors through the functional-index authoring path and add no hidden columns.
create index bad_ny_med on nj32 ((cast(json_extract(doc,'$.a') as mediumint)));
create index bad_ny_int3 on nj32 ((cast(json_extract(doc,'$.a') as int3)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_json_key_bvt' and att_relname='nj32' and attr_has_generated=1;
drop database functional_json_key_bvt;
