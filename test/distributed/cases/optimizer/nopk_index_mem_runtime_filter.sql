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

-- 子查询的重复键不放大外表结果，NULL 不产生匹配。
create table members (k int);
insert into members values (7),(7),(8),(null);
select id,k,payload from t where k in (select k from members where k < 9) order by id;
select id,k,payload from t ignore index(idx_k) where k in (select k from members where k < 9) order by id;
select id,k,payload from t force index(idx_k) where k in (select k from members where k < 9) order by id;
select t.id,t.k,t.payload from t join (select distinct k from members where k < 9) m on t.k=m.k order by t.id;
select id,k,payload from t where k in (select k from members) order by id;
select id,k,payload from t where k in (select k from members where k > 9) order by id;

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

-- 同一半连接在持久化数据和残余谓词上保持结果等价。
select id,k,payload from t where k in (select k from members where k < 9) and payload = 'updated' order by id;
select id,k,payload from t ignore index(idx_k) where k in (select k from members where k < 9) and payload = 'updated' order by id;
select id,k,payload from t force index(idx_k) where k in (select k from members where k < 9) and payload = 'updated' order by id;

-- 降低容量，在小数据上覆盖会扩大残余谓词求值范围的成本拒绝。
truncate table t;
truncate table members;
insert into t values (1,1,'{"x":1}');
insert into t select result,10,'bad' from generate_series(2,10)g;
insert into members values (1),(10),(10);
select mo_ctl('dn', 'flush', 'nopk_index_mem_runtime_filter.t');
select mo_ctl('dn', 'flush', 'nopk_index_mem_runtime_filter.members');
-- 物理采样块数和字节数不是本用例契约，保留总体行数和完成状态检查。
-- @ignore:7,8
analyze table t(k) fullscan;
-- @ignore:7,8
analyze table members(k) fullscan;
set @saved_join_in_limit = @@session.runtime_filter_limit_in;
set runtime_filter_limit_in = 1;

-- 非匹配行的非法 JSON 不得因访问路径替换而被求值。
select count(*) from t where k in (select k from members where k < 2) and cast(payload as json) is not null;
select count(*) from t force index(idx_k) where k in (select k from members where k < 2) and cast(payload as json) is not null;
select count(*) from t where k in (select k from members where k < 2) and json_extract(payload, '$.x') is not null;
select count(*) from t force index(idx_k) where k in (select k from members where k < 2) and json_extract(payload, '$.x') is not null;
select count(*) from t where k in (select k from members where k < 2) and payload = '{"x":1}';

-- 匹配行上的真实转换错误仍必须报告。
update t set payload = 'bad' where id = 1;
select count(*) from t where k in (select k from members where k < 2) and cast(payload as json) is not null;
select count(*) from t where k in (select k from members where k < 2) and json_extract(payload, '$.x') is not null;
set runtime_filter_limit_in = cast(@saved_join_in_limit as signed);
select @@session.runtime_filter_limit_in = @saved_join_in_limit as restored_limit;
set @saved_join_in_limit = null;

drop database nopk_index_mem_runtime_filter;
