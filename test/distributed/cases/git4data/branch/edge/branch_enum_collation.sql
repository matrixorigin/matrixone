-- DATA BRANCH must preserve stored ENUM ordinals and their collation.
drop database if exists br_enum_collation;
create database br_enum_collation;
use br_enum_collation;
set @saved_sql_mode=@@sql_mode;
set session sql_mode='STRICT_TRANS_TABLES';

create table base_t(
  id int primary key,
  b enum('A','a','','x') collate utf8mb4_bin,
  c enum('A','a','','x') collate utf8mb4_general_ci,
  u enum('A','a','','x') collate utf8mb4_unicode_ci
);
insert into base_t values (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,null,null,null);
insert ignore into base_t values (5,'bad','bad','bad');
data branch create table src_t from base_t;
data branch create table dst_t from base_t;
delete from src_t where id=2;
insert ignore into src_t values (6,'bad','bad','bad');
data branch diff src_t against dst_t output as out_t;
select id,b+0,c+0,u+0 from out_t order by id;
select column_name,character_set_name,collation_name from information_schema.columns
  where table_schema='br_enum_collation' and table_name='out_t' and column_name in ('b','c','u') order by column_name;
data branch merge src_t into dst_t;
select id,b+0,c+0,u+0 from dst_t order by id;

-- A portable SQL file cannot safely assign the stored error member in strict mode.
insert ignore into src_t values (7,'bad','bad','bad');
data branch diff src_t against dst_t output file '/tmp/';
select count(*) from dst_t where id=7;

-- Binary ENUM primary keys retain A and a as distinct keys through delete/insert.
create table pk_base(e enum('A','a','','x') collate utf8mb4_bin primary key,v int);
insert into pk_base values(1,10),(2,20),(3,30);
data branch create table pk_src from pk_base;
data branch create table pk_dst from pk_base;
delete from pk_src where e=2;
insert into pk_src values(4,40);
data branch diff pk_src against pk_dst output as pk_out;
select e+0,v from pk_out order by e+0;
data branch merge pk_src into pk_dst;
select e+0,v from pk_dst order by e+0;

-- A unique indexed ENUM uses the native UPDATE application path.
create table u_base(id int primary key,e enum('A','a','','x') collate utf8mb4_bin unique);
insert into u_base values(1,1),(2,3);
data branch create table u_src from u_base;
data branch create table u_dst from u_base;
update u_src set e=2 where id=1;
data branch merge u_src into u_dst;
select id,e+0 from u_dst order by id;

-- CTAS must carry the full ENUM domain through direct and view projections.
create table ctas_t as select id,b from base_t;
create view ctas_v as select id,b from base_t;
create table ctas_view_t as select id,b from ctas_v;
show create table ctas_t;
show create table ctas_view_t;
select id,b+0 from ctas_t order by id;
select id,b+0 from ctas_view_t order by id;

-- Error ordinal zero and the valid empty member are different primary keys.
create table zero_base(e enum('','A','a') collate utf8mb4_bin primary key,v int);
insert into zero_base values(1,10),(2,20),(3,30);
insert ignore into zero_base values('bad',0);
data branch create table zero_src from zero_base;
data branch create table zero_dst from zero_base;
delete from zero_src where e+0=0;
data branch merge zero_src into zero_dst;
select e+0,v from zero_dst order by e+0;

-- Text keys match ENUM labels; numeric keys are physical ordinals.
create table pick_base(e enum('2','x') collate utf8mb4_bin primary key,v int);
insert into pick_base values(1,10),(2,20);
data branch create table pick_src from pick_base;
data branch create table pick_text from pick_base;
data branch create table pick_num from pick_base;
data branch create table pick_query from pick_base;
update pick_src set v=11 where e+0=1;
update pick_src set v=22 where e+0=2;
data branch pick pick_src into pick_text keys('2');
data branch pick pick_src into pick_num keys(2);
create table pick_lookup(e enum('x','2') collate utf8mb4_bin);
insert into pick_lookup values(2);
data branch pick pick_src into pick_query keys(select e from pick_lookup);
select 'text',e+0,v from pick_text order by e+0;
select 'number',e+0,v from pick_num order by e+0;
select 'query',e+0,v from pick_query order by e+0;
data branch create table pick_prepare from pick_base;
prepare p from 'data branch pick pick_src into pick_prepare keys(?)';
set @pick_key='2';
execute p using @pick_key;
select e+0,v from pick_prepare order by e+0;
set @pick_key=2;
execute p using @pick_key;
select e+0,v from pick_prepare order by e+0;
deallocate prepare p;

-- A bare ENUM result can erase the distinction between 0 and the empty member.
create table pick_empty(e enum('') collate utf8mb4_bin primary key,v int);
insert into pick_empty values(1,10);
insert ignore into pick_empty values('bad',20);
data branch create table empty_src from pick_empty;
data branch create table empty_dst from pick_empty;
update empty_src set v=11 where e+0=1;
update empty_src set v=22 where e+0=0;
create table empty_lookup(e enum('') collate utf8mb4_bin);
insert ignore into empty_lookup values('bad');
data branch pick empty_src into empty_dst keys(select e from empty_lookup);
select e+0,v from empty_dst order by e+0;
data branch pick empty_src into empty_dst keys(select cast_value_to_index('a,b','a'));
select e+0,v from empty_dst order by e+0;
data branch pick empty_src into empty_dst keys(select cast(e as char) from empty_lookup);
select e+0,v from empty_dst order by e+0;
data branch pick empty_src into empty_dst keys(select cast(e as unsigned) from empty_lookup);
select e+0,v from empty_dst order by e+0;

-- Unicode zero-weight labels can also match the displayed error value.
create table uca_empty(e enum('́','x') collate utf8mb4_unicode_ci primary key,v int);
insert into uca_empty values(1,10),(2,30);
insert ignore into uca_empty values('bad',20);
data branch create table uca_empty_src from uca_empty;
data branch create table uca_empty_dst from uca_empty;
update uca_empty_src set v=11 where e+0=1;
update uca_empty_src set v=22 where e+0=0;
create table uca_empty_lookup(e enum('́','x') collate utf8mb4_unicode_ci);
insert ignore into uca_empty_lookup values('bad');
data branch pick uca_empty_src into uca_empty_dst keys(select e from uca_empty_lookup);
select e+0,v from uca_empty_dst order by e+0;
data branch pick uca_empty_src into uca_empty_dst keys(select cast(e as unsigned) from uca_empty_lookup);
select e+0,v from uca_empty_dst order by e+0;

set session sql_mode=@saved_sql_mode;
drop database br_enum_collation;
