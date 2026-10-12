-- provider 每代执行一次；只运行 ANN 或空/NULL 的原关系 fallback。
set @saved_ivf=@@experimental_ivf_index;
set experimental_ivf_index=1;
drop database if exists vector_scalar_query;
create database vector_scalar_query;
use vector_scalar_query;
set probe_limit=1;
create table items(id varchar(32) primary key,v vecf64(3) not null);
insert into items values ('a','[0,0,0]'),('b','[1,0,0]'),('c','[3,0,0]');
create index ix using ivfflat on items(v) lists=1 op_type 'vector_l2_ops';
create table query_vectors(id varchar(32) primary key,v vecf64(3));
insert into query_vectors values ('q','[1,0,0]'),('n',null);
-- @regex("Scalar Vector Query",true)
explain select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2;
-- @regex("ivf_search",true)
explain select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2;
select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2;
select id from items order by l2_distance(v,(select v from items ref where ref.id='b')) limit 2;
select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 1 offset 1;
select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2 by rank with option 'mode=pre';
select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2 by rank with option 'mode=force';
select * from (select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2) t limit 0;
select * from (select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2) t limit 5 offset 1;
select id from items where id='a' order by l2_distance(v,(select v from query_vectors where id='missing')) limit 2;
select count(*) from (select id from items order by l2_distance(v,(select v from query_vectors where id='missing')) limit 2) t;
select id from items where id='a' order by l2_distance(v,(select v from query_vectors where id='n')) limit 2;
select count(*) from (select id from items order by l2_distance(v,(select v from query_vectors where id='n')) limit 2) t;
select count(*) from (select id from items order by l2_distance(v,(select v from query_vectors where id='n')) limit 0) t;
-- 两个内联 selector 不得把外层 hash build 当成第四个懒分支。
with a as (select concat(id,'') as id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2),
b as (select concat(id,'') as id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2)
select a.id,b.id from a join b on a.id=b.id order by a.id;
with nearest as (select id from items order by l2_distance(v,(select v from query_vectors where id='q')) limit 2)
select a.id,b.id from nearest a join nearest b on a.id=b.id order by a.id;
prepare p from 'select id from items where id=''a'' order by l2_distance(v,(select v from query_vectors where id=?)) limit ?';
set @id='q';
set @k=0;
execute p using @id,@k;
set @k=2;
execute p using @id,@k;
set @id='n';
execute p using @id,@k;
set @id='missing';
execute p using @id,@k;
set @id='q';
execute p using @id,@k;
set @k=0;
execute p using @id,@k;
deallocate prepare p;
prepare nested from 'select * from (select id from items order by l2_distance(v,(select v from query_vectors where id=?)) limit 2) t limit ? offset 1';
set @k=0;
execute nested using @id,@k;
set @k=5;
execute nested using @id,@k;
update query_vectors set v='[3,0,0]' where id='q';
execute nested using @id,@k;
deallocate prepare nested;
-- 多行 provider 不作新改写，仍产生标量子查询基数错误。
select id from items order by l2_distance(v,(select v from items ref where ref.id in ('a','b'))) limit 2;
drop database vector_scalar_query;
set experimental_ivf_index=@saved_ivf;

set @saved_hnsw=@@experimental_hnsw_index;
set experimental_hnsw_index=1;
create database vector_scalar_query;
use vector_scalar_query;
create table items(id bigint primary key,v vecf32(3) not null);
insert into items values (1,'[0,0,0]'),(2,'[1,0,0]'),(3,'[3,0,0]');
create index ix using hnsw on items(v) op_type 'vector_l2_ops' M 4 EF_CONSTRUCTION 16 EF_SEARCH 16;
create table query_vectors(id bigint primary key,v vecf32(3));
insert into query_vectors values (1,'[1,0,0]'),(2,'[3,0,0]'),(3,null);
-- @regex("Scalar Vector Query",true)
explain select id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2;
select id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2;
select id from items where id=1 order by l2_distance(v,(select v from query_vectors where id=99)) limit 2;
select count(*) from (select id from items order by l2_distance(v,(select v from query_vectors where id=99)) limit 2) t;
select count(*) from (select id from items order by l2_distance(v,(select v from query_vectors where id=3)) limit 2) t;
select * from (select id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2) t limit 0;
select * from (select id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2) t limit 5 offset 1;
with a as (select concat(id,'') as id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2),
b as (select concat(id,'') as id from items order by l2_distance(v,(select v from query_vectors where id=1)) limit 2)
select a.id,b.id from a join b on a.id=b.id order by a.id;
prepare p from 'select id from items order by l2_distance(v,(select v from query_vectors where id=?)) limit ?';
set @id=1;
set @k=2;
execute p using @id,@k;
set @id=2;
execute p using @id,@k;
set @k=0;
execute p using @id,@k;
set @k=2;
update query_vectors set v='[0,0,0]' where id=2;
execute p using @id,@k;
deallocate prepare p;
prepare pn from 'select id from items where id=1 order by l2_distance(v,(select v from query_vectors where id=?)) limit ?';
set @id=3;
execute pn using @id,@k;
set @id=99;
execute pn using @id,@k;
set @id=1;
execute pn using @id,@k;
deallocate prepare pn;
drop database vector_scalar_query;
set experimental_hnsw_index=@saved_hnsw;
