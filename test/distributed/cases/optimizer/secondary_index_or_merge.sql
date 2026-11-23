drop database if exists secondary_or_merge;
create database secondary_or_merge;
use secondary_or_merge;

-- 少量数据验证重叠、NULL 和基础行身份；规模收益独立验证。
create table f(id int primary key, k int, v int, payload varchar(20), key ik(k), key iv(v));
insert into f values
    (1, 3, 10, 'same'), (2, 3, 20, 'same'), (3, 7, 10, 'right'),
    (4, null, 10, 'null-left'), (5, 3, null, 'null-right'),
    (6, null, null, 'neither'), (7, 7, 30, 'neither'), (8, 8, 40, 'other');
analyze table f(k,v);

-- 覆盖路径必须使用两路索引，不能回扫基表。
-- @regex("Index Table Scan on f.ik",true)
-- @regex("Index Table Scan on f.iv",true)
-- @regex("Table Scan on secondary_or_merge.f",false)
explain select count(*) from f force index(ik,iv) where k=3 or v=10;
select count(*) from f force index(ik,iv) where k=3 or v=10;
select count(*) from f ignore index(ik,iv) where k=3 or v=10;
select id from f force index(ik,iv) where k=3 or v=10 order by id;
select id from f ignore index(ik,iv) where k=3 or v=10 order by id;

-- id+0 保持主键顺序，但避免既有排序索引规则提前选择单索引回表。
-- 不同主键的相同投影值不可去重；回表必须由两路索引的主键并集驱动。
-- @regex("Union",true)
-- @regex("Index Table Scan on f.ik",true)
-- @regex("Index Table Scan on f.iv",true)
-- @regex("Join Type: INDEX",true)
explain select payload from f force index(ik,iv) where k=3 or v=10 order by id+0;
select payload from f force index(ik,iv) where k=3 or v=10 order by id+0;
select payload from f ignore index(ik,iv) where k=3 or v=10 order by id+0;

-- 并集回表后仍需验证非索引列的残余条件。
-- @regex("Union",true)
-- @regex("Index Table Scan on f.ik",true)
-- @regex("Index Table Scan on f.iv",true)
-- @regex("Join Type: INDEX",true)
explain select id,payload from f force index(ik,iv) where (k=3 or v=10) and payload <> 'right' order by id+0;
select id,payload from f force index(ik,iv) where (k=3 or v=10) and payload <> 'right' order by id+0;
select id,payload from f ignore index(ik,iv) where (k=3 or v=10) and payload <> 'right' order by id+0;

-- 覆盖路径在主键并集去重后执行全局分页，不回扫基表。
-- @regex("Union",true)
-- @regex("Index Table Scan on f.ik",true)
-- @regex("Index Table Scan on f.iv",true)
-- @regex("Table Scan on secondary_or_merge.f",false)
explain select id from f force index(ik,iv) where k=3 or v=10 order by id+0 limit 2 offset 1;
select id from f force index(ik,iv) where k=3 or v=10 order by id+0 limit 2 offset 1;
select id from f ignore index(ik,iv) where k=3 or v=10 order by id+0 limit 2 offset 1;

-- IN、范围、空集合以及三个分支。
select id from f force index(ik,iv) where k in (3,8) or v=10 order by id;
select id from f ignore index(ik,iv) where k in (3,8) or v=10 order by id;
select id from f force index(ik,iv) where k between 3 and 7 or v=10 order by id;
select id from f ignore index(ik,iv) where k between 3 and 7 or v=10 order by id;
select id from f force index(ik,iv) where k<3 or v=10 order by id;
select id from f ignore index(ik,iv) where k<3 or v=10 order by id;
select count(*) from f force index(ik,iv) where k=999 or v=999;
select id from f force index(ik,iv) where k=3 or v=10 or v=40 order by id;

-- 已有单索引 OR/AND 与不支持分支不能发生结果回归。
select id from f where k=3 or k=7 order by id;
select id from f where k=3 and v=10 order by id;
select id from f where k=3 or payload='other' order by id;
select id from f ignore index(iv) where k=3 or v=10 order by id;
prepare p from 'select id from f where k=? or v=? order by id';
set @k=3;
set @v=10;
execute p using @k,@v;
set @k=null;
execute p using @k,@v;
deallocate prepare p;

-- 三个物理索引的分支在最终主键并集上去重。
create index ip on f(payload);
analyze table f(k,v,payload);
-- @regex("Union All",true)
-- @regex("Index Table Scan on f.ip",true)
explain select count(*) from f force index(ik,iv,ip) where k=3 or v=10 or payload='other';
select count(*) from f force index(ik,iv,ip) where k=3 or v=10 or payload='other';
select count(*) from f ignore index(ik,iv,ip) where k=3 or v=10 or payload='other';
select id from f force index(ik,iv) where (k=3 or v=10) and id<>3 order by id;
select id from f force index(ik,iv) where (k=3 or v=10) and (k=7 or v=20) order by id;
select id from f ignore index(ik,iv,ip) where (k=3 or v=10) and (k=7 or v=20) order by id;

-- 写入消费者也必须只选择一次重复命中的基础行。
begin;
update f force index(ik,iv) set payload=concat(payload,'!') where k=3 or v=10;
select id,payload from f order by id;
rollback;

-- 同一事务中读取索引和基表必须一致。
begin;
update f set k=3 where id=8;
delete from f where id=1;
select count(*) from f force index(ik,iv) where k=3 or v=10;
select count(*) from f ignore index(ik,iv) where k=3 or v=10;
rollback;
select count(*) from f force index(ik,iv) where k=3 or v=10;

-- 复合主键与隐藏主键按基础行身份合并。
create table composite_pk(tenant int, id int, k int, v int, primary key(tenant,id), key ik(k), key iv(v));
insert into composite_pk values (1,1,3,10),(2,1,3,10),(1,2,null,10),(2,2,7,null);
analyze table composite_pk(k,v);
select count(*) from composite_pk force index(ik,iv) where k=3 or v=10;
select tenant,id from composite_pk force index(ik,iv) where k=3 or v=10 order by tenant,id;
select tenant,id from composite_pk ignore index(ik,iv) where k=3 or v=10 order by tenant,id;
create table hidden_pk(k int, v int, key ik(k), key iv(v));
insert into hidden_pk values (3,10),(3,10),(null,10),(7,null);
analyze table hidden_pk(k,v);
select count(*) from hidden_pk force index(ik,iv) where k=3 or v=10;
select k,v from hidden_pk force index(ik,iv) where k=3 or v=10 order by k,v;
select k,v from hidden_pk ignore index(ik,iv) where k=3 or v=10 order by k,v;

drop database secondary_or_merge;
