-- DATA BRANCH preserves binary ENUM ordinals and rejects unreplayable SQL.
drop database if exists br_enum_collation;
create database br_enum_collation;
use br_enum_collation;
set @saved_sql_mode=@@sql_mode;
set session sql_mode='STRICT_TRANS_TABLES';

create table base_t(id int primary key,e enum('A','a','','x') collate utf8mb4_bin);
insert into base_t values(1,1),(2,2),(3,3),(4,null);
data branch create table src_t from base_t;
data branch create table dst_t from base_t;
delete from src_t where id=2;
insert ignore into src_t values(6,'bad');
data branch diff src_t against dst_t output as out_t;
select id,e+0 from out_t order by id;
show create table out_t;

-- Strict SQL cannot replay the stored error member as a numeric literal.
data branch diff src_t against dst_t output file '/tmp/';
data branch merge src_t into dst_t;
select id,e+0 from dst_t order by id;

-- After removing that source row, the deletion can be applied successfully.
delete from src_t where id=6;
data branch merge src_t into dst_t;
select id,e+0 from dst_t order by id;

set session sql_mode=@saved_sql_mode;
drop database br_enum_collation;
