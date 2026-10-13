-- Scalar query vectors are evaluated once before the selected search starts.
set experimental_ivf_index = 1;
drop database if exists vector_scalar_query;
create database vector_scalar_query;
use vector_scalar_query;
create table items(id varchar(32) primary key, v vecf64(3));
insert into items values ('a','[0,0,0]'), ('b','[1,0,0]'), ('c','[3,0,0]');
create index ix using ivfflat on items(v) lists=1 op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Scalar Vector Query", true)
explain select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2;
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2;
select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2;
select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 1 offset 1;
-- Outer pagination remains above the inner Top-K and its lazy selector.
select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2) t limit 0;
select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2) t limit 5 offset 1;
select count(*) from (select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id='absent')) limit 2) t limit 0) s;
prepare nested_scalar from 'select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id=?)) limit 2) t limit ? offset 1';
set @id='b';
set @k=0;
execute nested_scalar using @id, @k;
set @k=5;
execute nested_scalar using @id, @k;
deallocate prepare nested_scalar;
-- Computed outputs preserve both inline CTE selectors as direct JOIN inputs.
-- A JOIN's hash build is not a fourth lazy branch of either selector.
with a as (select concat(id,'') as id from items order by l2_distance(v,(select v from items ref where ref.id='b')) limit 2),
b as (select concat(id,'') as id from items order by l2_distance(v,(select v from items ref where ref.id='b')) limit 2)
select a.id,b.id from a join b on a.id=b.id order by a.id;
-- A reused/materialized CTE is a separate control boundary.
with nearest as (select id from items order by l2_distance(v, (select v from items ref where ref.id='b')) limit 2)
select a.id, b.id from nearest a join nearest b on a.id=b.id order by a.id;
with a as (select concat(id,'') as id from items where id='a' order by l2_distance(v,(select v from items ref where ref.id='absent')) limit 2),
b as (select concat(id,'') as id from items where id='a' order by l2_distance(v,(select v from items ref where ref.id='absent')) limit 2)
select a.id,b.id from a join b on a.id=b.id;
prepare scalar_self_join from 'with a as (select concat(id,'''') as id from items order by l2_distance(v,(select v from items ref where ref.id=?)) limit 2), b as (select concat(id,'''') as id from items order by l2_distance(v,(select v from items ref where ref.id=?)) limit 2) select a.id,b.id from a join b on a.id=b.id order by a.id';
set @id='b';
execute scalar_self_join using @id, @id;
set @id='c';
execute scalar_self_join using @id, @id;
deallocate prepare scalar_self_join;
-- An empty scalar returns NULL, not an empty outer relation.
-- @separator:table
-- @regex("Scalar Vector Query", true)
explain select id from items where id='a' order by l2_distance(v, (select v from items ref where ref.id='absent')) limit 2;
select id from items where id='a' order by l2_distance(v, (select v from items ref where ref.id='absent')) limit 2;
select count(*) from (select id from items order by l2_distance(v, (select v from items ref where ref.id='absent')) limit 2) s;
insert into items values ('null-vector', null);
select id from items where id='a' order by l2_distance(v, (select v from items ref where ref.id='null-vector')) limit 2;
select count(*) from (select id from items order by l2_distance(v, (select v from items ref where ref.id='null-vector')) limit 2) s;
select count(*) from (select id from items order by l2_distance(v, (select v from items ref where ref.id='null-vector')) limit 0) s;
prepare scalar_query from 'select id from items order by l2_distance(v, (select v from items ref where ref.id=?)) limit ?';
set @id='b';
set @k=2;
execute scalar_query using @id, @k;
set @id='c';
execute scalar_query using @id, @k;
update items set v='[5,0,0]' where id='b';
set @id='b';
execute scalar_query using @id, @k;
deallocate prepare scalar_query;
prepare scalar_null from 'select id from items where id=''a'' order by l2_distance(v, (select v from items ref where ref.id=?)) limit ?';
set @id='absent';
execute scalar_null using @id, @k;
set @id='null-vector';
execute scalar_null using @id, @k;
set @id='b';
execute scalar_null using @id, @k;
set @k=0;
execute scalar_null using @id, @k;
set @k=1;
execute scalar_null using @id, @k;
deallocate prepare scalar_null;
-- Without a uniqueness proof the ordinary scalar cardinality error is retained.
select id from items order by l2_distance(v, (select v from items ref where ref.id in ('a','b'))) limit 2;
drop database vector_scalar_query;
set experimental_ivf_index = 0;

set experimental_hnsw_index = 1;
create database vector_scalar_query;
use vector_scalar_query;
create table items(id bigint primary key, v vecf32(3));
insert into items values (1,'[0,0,0]'), (2,'[1,0,0]'), (3,'[3,0,0]');
create index ix using hnsw on items(v) op_type 'vector_l2_ops' M 4 EF_CONSTRUCTION 16 EF_SEARCH 16;
-- @separator:table
-- @regex("Scalar Vector Query", true)
explain select id from items order by l2_distance(v, (select v from items ref where ref.id=2)) limit 2;
select id from items order by l2_distance(v, (select v from items ref where ref.id=2)) limit 2;
select id from items where id=1 order by l2_distance(v, (select v from items ref where ref.id=99)) limit 2;
select count(*) from (select id from items order by l2_distance(v, (select v from items ref where ref.id=99)) limit 2) s;
-- Outer zero demand must not compile either result branch without its reader.
select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id=2)) limit 2) t limit 0;
select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id=2)) limit 2) t limit 5 offset 1;
prepare nested_hnsw from 'select * from (select id from items order by l2_distance(v, (select v from items ref where ref.id=?)) limit 2) t limit ? offset 1';
set @id=2;
set @k=0;
execute nested_hnsw using @id, @k;
set @k=5;
execute nested_hnsw using @id, @k;
deallocate prepare nested_hnsw;
-- CTE self-join consumers must not inherit the selector's lazy scheduler.
with a as (select concat(id,'') as id from items order by l2_distance(v,(select v from items ref where ref.id=2)) limit 2),
b as (select concat(id,'') as id from items order by l2_distance(v,(select v from items ref where ref.id=2)) limit 2)
select a.id,b.id from a join b on a.id=b.id order by a.id;
with nearest as (select id from items order by l2_distance(v, (select v from items ref where ref.id=2)) limit 2)
select a.id, b.id from nearest a join nearest b on a.id=b.id order by a.id;
with a as (select concat(id,'') as id from items where id=1 order by l2_distance(v,(select v from items ref where ref.id=99)) limit 2),
b as (select concat(id,'') as id from items where id=1 order by l2_distance(v,(select v from items ref where ref.id=99)) limit 2)
select a.id,b.id from a join b on a.id=b.id;
prepare hnsw_self_join from 'with a as (select concat(id,'''') as id from items order by l2_distance(v,(select v from items ref where ref.id=?)) limit 2), b as (select concat(id,'''') as id from items order by l2_distance(v,(select v from items ref where ref.id=?)) limit 2) select a.id,b.id from a join b on a.id=b.id order by a.id';
set @id=2;
execute hnsw_self_join using @id, @id;
set @id=3;
execute hnsw_self_join using @id, @id;
deallocate prepare hnsw_self_join;
-- HNSW output columns must not inherit the vector provider's input types at EXECUTE.
create table query_vectors(id bigint primary key, v vecf32(3));
insert into query_vectors values (1,'[1,0,0]'), (2,'[3,0,0]'), (3,null);
prepare scalar_hnsw from 'select id from items order by l2_distance(v, (select v from query_vectors where id=?)) limit ?';
set @id=1;
set @k=2;
execute scalar_hnsw using @id, @k;
set @id=2;
execute scalar_hnsw using @id, @k;
set @k=0;
execute scalar_hnsw using @id, @k;
set @k=2;
update query_vectors set v='[0,0,0]' where id=2;
execute scalar_hnsw using @id, @k;
deallocate prepare scalar_hnsw;
prepare scalar_hnsw_null from 'select id from items where id=1 order by l2_distance(v, (select v from query_vectors where id=?)) limit ?';
set @id=3;
execute scalar_hnsw_null using @id, @k;
set @id=99;
execute scalar_hnsw_null using @id, @k;
set @id=1;
execute scalar_hnsw_null using @id, @k;
deallocate prepare scalar_hnsw_null;
drop database vector_scalar_query;
set experimental_hnsw_index = 0;
