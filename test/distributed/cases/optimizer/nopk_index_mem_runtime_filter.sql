-- @suite
-- @case
-- @desc: No-PK index backfill preserves point results with committed memory rows
-- @label:bvt

drop database if exists nopk_index_mem_runtime_filter;
create database nopk_index_mem_runtime_filter;
use nopk_index_mem_runtime_filter;

create table t (id int, k int, payload varchar(16), key idx_k(k));
insert into t values (1,7,'a'),(2,7,'b'),(3,8,'c'),(4,null,'n'),(5,9,'x');

select id,k,payload from t force index(idx_k) where k in (7,8) order by id;
select id,k,payload from t ignore index(idx_k) where k in (7,8) order by id;

prepare idx_points from 'select id,k,payload from t force index(idx_k) where k in (?, ?) order by id';
set @k1='7';
set @k2='8';
execute idx_points using @k1,@k2;
set @k1='9';
execute idx_points using @k1,@k2;
deallocate prepare idx_points;

update t set payload='updated' where id=2;
delete from t where id=3;
select id,k,payload from t force index(idx_k) where k in (7,8) order by id;
select id,k,payload from t ignore index(idx_k) where k in (7,8) order by id;

select mo_ctl('dn', 'flush', 'nopk_index_mem_runtime_filter.t');
select id,k,payload from t force index(idx_k) where k in (7,8) order by id;
select id,k,payload from t ignore index(idx_k) where k in (7,8) order by id;

drop database nopk_index_mem_runtime_filter;
