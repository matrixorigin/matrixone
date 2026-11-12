-- @suit
-- @case
-- @desc: Single-expression functional index lifecycle and metadata.
-- @label:bvt
drop database if exists functional_index_bvt;
create database functional_index_bvt;
use functional_index_bvt;
-- #29898: unsupported UNIQUE expressions leave schema/data intact and the session usable.
CREATE TABLE mo_index_probe (id INT PRIMARY KEY, email VARCHAR(254));
insert into mo_index_probe values (1,'alice@example.com'),(2,'bob@example.com');
show create table mo_index_probe;
select id,email from mo_index_probe order by id;
select index_name,column_name from information_schema.statistics where table_schema='functional_index_bvt' and table_name='mo_index_probe' order by index_name;
select count(*) as generated_columns from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='mo_index_probe' and attr_has_generated=1;
select count(*) as relations from mo_catalog.mo_tables where reldatabase='functional_index_bvt';
CREATE UNIQUE INDEX mo_unique_email_expression ON mo_index_probe ((LOWER(email)));
show create table mo_index_probe;
select id,email from mo_index_probe order by id;
select index_name,column_name from information_schema.statistics where table_schema='functional_index_bvt' and table_name='mo_index_probe' order by index_name;
select count(*) as generated_columns from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='mo_index_probe' and attr_has_generated=1;
select count(*) as relations from mo_catalog.mo_tables where reldatabase='functional_index_bvt';
select 1 as session_usable;
create unique index mo_unique_email on mo_index_probe(email);
insert into mo_index_probe values (3,'alice@example.com');
select id,email from mo_index_probe order by id;
drop table mo_index_probe;
create table t (id int primary key, name varchar(40), index il ((lower(name))));
insert into t values (1,'ABC'),(2,'def'),(3,NULL);
select count(*) as matched from t force index(il) where lower(name)='abc';
update t set name='aBc' where id=2;
select count(*) as matched from t force index(il) where lower(name)='abc';
replace into t values (2,'XYZ');
select count(*) as matched from t force index(il) where lower(name)='abc';
insert into t values (2,'ABC') on duplicate key update name=values(name);
select count(*) as matched from t force index(il) where lower(name)='abc';
create index ip on t ((id+1));
select id from t force index(ip) where id+1=3;
alter table t add column extra int first;
alter table t modify name varchar(60);
select count(*) as matched from t force index(il) where lower(name)='abc';
begin;
delete from t where id=1;
rollback;
select count(*) as matched from t force index(il) where lower(name)='abc';
create table cloned like t;
insert into cloned(id,name) select id,name from t;
select count(*) as matched from cloned force index(il) where lower(name)='abc';
select index_name,column_name is null as functional,expression is not null as has_expression from information_schema.statistics where table_schema='functional_index_bvt' and table_name='t' order by index_name;
drop index il on t;
drop index ip on t;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='t' and attr_has_generated=1;
select id,name from t order by id;
create table load_fi(id int primary key,name varchar(40),index il((lower(name))));
load data inline format='csv', data='1,ABC\n2,abc\n' into table load_fi fields terminated by ',' (id,name);
select count(*) as matched from load_fi force index(il) where lower(name)='abc';
create table multi_fi(id int primary key,tenant int,name varchar(40),index ix((lower(name)),(id+1)),index im(tenant,(lower(name))));
insert into multi_fi values(1,7,'ABC'),(2,7,'abc'),(3,7,NULL);
select count(*) as matched from multi_fi force index(ix) where lower(name)='abc' and id+1=2;
select count(*) as matched from multi_fi force index(im) where tenant=7 and lower(name)='abc';
drop index ix on multi_fi;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='multi_fi' and attr_has_generated=1;
drop index im on multi_fi;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='multi_fi' and attr_has_generated=1;
-- #32437: widened scalar-function admission, literal persistence, and rename replay.
create table fi_scalar(id int primary key, name varchar(40), cc char(10), pos int, posu int unsigned, extra varchar(30));
insert into fi_scalar values (1,'abcdef','xy',2,1,'X'),(2,'ghijkl',NULL,1,2,'YZ'),(3,NULL,'zz',3,3,'W');
create index is_sub on fi_scalar ((substring(name,1,4)));
create index is_mid on fi_scalar ((mid(name,2)));
create index is_left on fi_scalar ((left(name,3)));
create index is_pos on fi_scalar ((substring(name,pos,2)));
create index is_rev on fi_scalar ((reverse(name)));
create index is_md5 on fi_scalar ((md5(name)));
create index is_cc on fi_scalar ((md5(cc)));
create index is_crc on fi_scalar ((crc32(name)));
create index is_hex on fi_scalar ((hex(name)));
create index is_len on fi_scalar ((length(name)));
create index is_cat on fi_scalar ((concat(name,extra)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='fi_scalar' and attr_has_generated=1;
select id from fi_scalar force index(is_sub) where substring(name,1,4)='abcd' order by id;
select id from fi_scalar ignore index(is_sub) where substring(name,1,4)='abcd' order by id;
select id from fi_scalar force index(is_left) where left(name,3)='ghi' order by id;
select id from fi_scalar ignore index(is_left) where left(name,3)='ghi' order by id;
select id from fi_scalar force index(is_pos) where substring(name,pos,2)='hi' order by id;
select id from fi_scalar force index(is_mid) where mid(name,2)='bcdef' order by id;
select id from fi_scalar force index(is_rev) where reverse(name)='fedcba' order by id;
select id from fi_scalar force index(is_md5) where md5(name)=md5('abcdef') order by id;
select id from fi_scalar ignore index(is_md5) where md5(name)=md5('abcdef') order by id;
select id from fi_scalar force index(is_hex) where hex(name)=hex('ghijkl') order by id;
select id from fi_scalar force index(is_len) where length(name)=6 order by id;
select id from fi_scalar force index(is_crc) where crc32(name)=crc32('ghijkl') order by id;
select id from fi_scalar force index(is_cat) where concat(name,extra)='abcdefX' order by id;
select id from fi_scalar force index(is_cc) where md5(cc)=md5('xy') order by id;
select id from fi_scalar ignore index(is_cc) where md5(cc)=md5('xy') order by id;
update fi_scalar set name='abcdefg' where id=2;
select id from fi_scalar force index(is_left) where left(name,3)='abc' order by id;
begin;
delete from fi_scalar where id=1;
rollback;
select id from fi_scalar force index(is_left) where left(name,3)='abc' order by id;
create table fi_scalar_like like fi_scalar;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='fi_scalar_like' and attr_has_generated=1;
insert into fi_scalar_like select * from fi_scalar;
select id from fi_scalar_like force index(is_sub) where substring(name,1,4)='abcd' order by id;
select id from fi_scalar_like ignore index(is_sub) where substring(name,1,4)='abcd' order by id;
alter table fi_scalar add column more int, algorithm=copy;
select id from fi_scalar force index(is_sub) where substring(name,1,4)='abcd' order by id;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='fi_scalar' and attr_has_generated=1;
-- rejections leave the hidden-column count unchanged
create index bad_sha on fi_scalar ((sha1(name)));
create index bad_temporal on fi_scalar ((cast(extra as date)));
create index bad_upos on fi_scalar ((substring(name,posu,2)));
create index bad_concat_num on fi_scalar ((concat(name,id)));
create index bad_length_num on fi_scalar ((length(id)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='fi_scalar' and attr_has_generated=1;
-- PAD_CHAR_TO_FULL_LENGTH keeps CHAR-referencing keys residual-only: force and ignore agree.
set @fi_old_mode = @@sql_mode;
set @@sql_mode = 'PAD_CHAR_TO_FULL_LENGTH';
select id from fi_scalar force index(is_cc) where md5(cc)=md5('xy') order by id;
select id from fi_scalar ignore index(is_cc) where md5(cc)=md5('xy') order by id;
set @@sql_mode = @fi_old_mode;
-- backslash literal: width-preserving persisted spelling and replay losslessness
create table fi_bs(id int primary key, name varchar(40), index ib ((concat(name,'a\\b'))));
insert into fi_bs values (1,'x'),(2,'y'),(3,NULL);
select id from fi_bs force index(ib) where concat(name,'a\\b')='xa\\b' order by id;
select id from fi_bs ignore index(ib) where concat(name,'a\\b')='xa\\b' order by id;
show create table fi_bs;
create table fi_bs_like like fi_bs;
insert into fi_bs_like select * from fi_bs;
select id from fi_bs_like force index(ib) where concat(name,'a\\b')='xa\\b' order by id;
alter table fi_bs add column extra int, algorithm=copy;
select id from fi_bs force index(ib) where concat(name,'a\\b')='xa\\b' order by id;
show create table fi_bs;
-- rename then replay: RENAME COLUMN rewrites only the identifier
alter table fi_bs rename column name to renamed;
show create table fi_bs;
select id from fi_bs force index(ib) where concat(renamed,'a\\b')='xa\\b' order by id;
create table fi_bs_renamed_like like fi_bs;
insert into fi_bs_renamed_like select * from fi_bs;
select id from fi_bs_renamed_like force index(ib) where concat(renamed,'a\\b')='xa\\b' order by id;
alter table fi_bs add column extra2 int, algorithm=copy;
select id from fi_bs force index(ib) where concat(renamed,'a\\b')='xa\\b' order by id;
-- rename then replay: the renaming half of CHANGE COLUMN shares the producer
create table fi_bs2(id int primary key, name varchar(40), index ib2 ((concat(name,'a\\b'))));
insert into fi_bs2 values (1,'x');
alter table fi_bs2 change column name renamed varchar(40);
show create table fi_bs2;
select id from fi_bs2 force index(ib2) where concat(renamed,'a\\b')='xa\\b' order by id;
create table fi_bs2_like like fi_bs2;
insert into fi_bs2_like select * from fi_bs2;
select id from fi_bs2_like force index(ib2) where concat(renamed,'a\\b')='xa\\b' order by id;
alter table fi_bs2 add column extra int, algorithm=copy;
select id from fi_bs2 force index(ib2) where concat(renamed,'a\\b')='xa\\b' order by id;
-- #32437 challenge repair: bind-stage wrong-arity resolution failures report the
-- unsupported-expression family naming the raw text (not the raw registry error),
-- add no hidden columns, keep the MEDIUMINT NYI native, and a valid DDL recovers.
create table rejects_arity(id int primary key, v varchar(40));
create index bad_arity_sub on rejects_arity ((substring(v)));
create index bad_arity_left on rejects_arity ((left(v)));
create index bad_arity_concat on rejects_arity ((concat()));
create index bad_arity_med on rejects_arity ((cast(v as mediumint)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='rejects_arity' and attr_has_generated=1;
create index ok_arity on rejects_arity ((md5(v)));
-- #32437 challenge repair round 3: multi-argument diagnostics keep the authored
-- argument-list spelling (comma, no space) for walk-level domain rejections and
-- bind-stage no-match arity calls; metadata is still unchanged across failures
-- and a later valid index recovers.
create index bad_dom_concat on rejects_arity ((concat(v,1)));
create index bad_dom_replace on rejects_arity ((replace(v,'a','b')));
create index bad_arity_sub4 on rejects_arity ((substring(v,1,2,3)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='rejects_arity' and attr_has_generated=1;
create index ok_dom_recover on rejects_arity ((upper(v)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='rejects_arity' and attr_has_generated=1;
show create table rejects_arity;
drop table rejects_arity;
-- #32437 review repair: signed literal arguments admit the persisted unary-minus
-- spelling; negated columns stay rejected; ordinary numeric literals reach the
-- crc32 key through the value-exact integer-to-decimal promotion.
create index is_sneg on fi_scalar ((substring(name,-2,2)));
create index is_lneg on fi_scalar ((left(name,-1)));
create index bad_neg_col on fi_scalar ((substring(name,-pos,2)));
create index bad_neg_upos on fi_scalar ((substring(name,-posu,2)));
create index bad_neg_real on fi_scalar ((substring(name,-2.5,1)));
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='fi_scalar' and attr_has_generated=1;
select id from fi_scalar force index(is_sneg) where substring(name,-2,2)='ef' order by id;
select id from fi_scalar ignore index(is_sneg) where substring(name,-2,2)='ef' order by id;
select id from fi_scalar force index(is_sneg) where substring(name,-2,2)='fg' order by id;
select id from fi_scalar force index(is_lneg) where left(name,-1)='' order by id;
select id from fi_scalar ignore index(is_lneg) where left(name,-1)='' order by id;
select id from fi_scalar where substring(name,-2,2) is null order by id;
select id from fi_scalar force index(is_crc) where crc32(name)=824863398 order by id;
select id from fi_scalar ignore index(is_crc) where crc32(name)=824863398 order by id;
select id from fi_scalar where crc32(name)=1267612143 order by id;
drop database functional_index_bvt;
