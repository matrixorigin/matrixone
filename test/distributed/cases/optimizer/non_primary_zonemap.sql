-- 普通列的裁剪取决于各块值域，而不是主键标记或全局排序。
-- 两张表含相同的 k 多重集；显式 flush 用六行数据建立窄/宽对象值域。
drop database if exists non_primary_zonemap;
create database non_primary_zonemap;
use non_primary_zonemap;
set @saved_zonemap_hints=@@optimizer_hints;
create table clustered (id int primary key, k int);
create table interleaved (id int primary key, k int);

-- 高值块位于命中块之前，普通列不能使用排序键的提前停止条件。
insert into clustered values (1,200),(2,201);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.clustered');
insert into clustered values (3,0),(4,1);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.clustered');
insert into clustered values (5,100),(6,150);

-- 未持久化的命中行与持久化后的结果应一致，且包含闭区间两端。
select k from clustered where k between 100 and 150 order by k;
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.clustered');
select k from clustered where k between 100 and 150 order by k;
select rows_cnt, bit_cast(`min` as int) as lo, bit_cast(`max` as int) as hi from metadata_scan('non_primary_zonemap.clustered', 'k') g order by lo;
-- @regex("Table Scan on non_primary_zonemap[.]clustered[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=1\s+inputRows=2\b",true)
explain analyze select count(*) from clustered where k between 100 and 150;

-- 交换比较操作数不能改变准入，行结果仍由原比较表达式决定。
select k from clustered where 100 = k order by k;
-- @regex("Table Scan on non_primary_zonemap[.]clustered[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=1\s+inputRows=2\b",true)
explain analyze select count(*) from clustered where 100 = k;
select k from clustered where 100 < k order by k;
-- @regex("Table Scan on non_primary_zonemap[.]clustered[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=2\s+inputRows=4\b",true)
explain analyze select count(*) from clustered where 100 < k;
select k from clustered where 100 <= k order by k;
select k from clustered where 100 > k order by k;
select k from clustered where 100 >= k order by k;
select id from clustered where 3 < id order by id;

-- 相同数据打散后，每个对象的 min/max 均覆盖查询区间，应保留三个块。
insert into interleaved values (1,0),(2,100);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.interleaved');
insert into interleaved values (3,1),(4,200);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.interleaved');
insert into interleaved values (5,150),(6,201);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.interleaved');
select k from interleaved where k between 100 and 150 order by k;
-- @regex("Table Scan on non_primary_zonemap[.]interleaved[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=3\s+inputRows=6\b",true)
explain analyze select count(*) from interleaved where k between 100 and 150;

-- 主键对照与值域之外的空结果，不依赖 EXPLAIN 来判断 SQL 正确性。
select id from clustered where id between 5 and 6 order by id;
select id from interleaved where id between 5 and 6 order by id;
select count(*) from clustered where k between 300 and 400;
select count(*) from interleaved where k between 300 and 400;
-- @regex("Table Scan on non_primary_zonemap[.]clustered[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=0\s+inputRows=0\b",true)
explain analyze select count(*) from clustered where k between 300 and 400;

-- 参数边界在执行时物化；NULL 后复用不能遗留上次裁剪状态。
prepare zm_range from 'select k from clustered where k between ? and ? order by k';
set @zm_lower=100;
set @zm_upper=150;
execute zm_range using @zm_lower,@zm_upper;
set @zm_lower=null;
execute zm_range using @zm_lower,@zm_upper;
set @zm_lower=100;
execute zm_range using @zm_lower,@zm_upper;
deallocate prepare zm_range;

-- 常量左置的参数比较，重复执行不能原地反转函数或操作数。
prepare zm_commuted from 'select k from clustered where ? < k order by k';
execute zm_commuted using @zm_lower;
set @zm_lower=null;
execute zm_commuted using @zm_lower;
set @zm_lower=100;
execute zm_commuted using @zm_lower;
deallocate prepare zm_commuted;

-- 范围分区裁剪收到物化参数时，NULL 不能中断查询或污染后续执行。
create table range_bound (a int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue);
insert into range_bound values (1),(11),(21);
prepare zm_partition from 'select a from range_bound where a = ?';
set @zm_partition_value=11;
execute zm_partition using @zm_partition_value;
set @zm_partition_value=null;
execute zm_partition using @zm_partition_value;
set @zm_partition_value=11;
execute zm_partition using @zm_partition_value;
deallocate prepare zm_partition;

-- 同一持久化块混合真假值，BOOL 摘要不能把 true 行排除。
set optimizer_hints='';
create table bool_values (id int, enabled bool);
insert into bool_values values (1,true),(2,true),(3,true),(4,false),(5,false),(6,null);
select id from bool_values where enabled = true order by id;
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.bool_values');
select id from bool_values where enabled = true order by id;
select id from bool_values where enabled = false order by id;
select id from bool_values where enabled is null order by id;
select count(*) as left_count from bool_values where id in (1,2) and enabled = true;
select count(*) as right_count from bool_values where id in (2,3) and enabled = true;
-- 输入各两行，必须同时保留匹配、仅左侧和仅右侧三种结果。
select l.id as left_id, r.id as right_id
from (select id, enabled from bool_values where id in (1,2) and enabled = true) l
full outer join (select id, enabled from bool_values where id in (2,3) and enabled = true) r
on l.id = r.id and l.enabled = r.enabled
order by coalesce(l.id, r.id);

-- 保守 BOOL 上界不是精确最大值，聚合必须读取真实行。
create table bool_aggregate (b bool);
select min(b), max(b), max(if(b,1,0)) from bool_aggregate;
insert into bool_aggregate values (false),(false);
select min(b), max(b), max(if(b,1,0)) from bool_aggregate;
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.bool_aggregate');
select max(b) from bool_aggregate;
select count(1), max(b) from bool_aggregate;
select min(b), max(b) from bool_aggregate;
select min(b), max(b), max(if(b,1,0)) from bool_aggregate;
truncate table bool_aggregate;
insert into bool_aggregate values (true),(true);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.bool_aggregate');
select min(b), max(b) from bool_aggregate;
select min(b), max(b), max(if(b,1,0)) from bool_aggregate;
truncate table bool_aggregate;
insert into bool_aggregate values (false),(true),(null);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.bool_aggregate');
select min(b), max(b) from bool_aggregate;
select min(b), max(b), max(if(b,1,0)) from bool_aggregate;
truncate table bool_aggregate;
insert into bool_aggregate values (null),(null);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'non_primary_zonemap.bool_aggregate');
select min(b), max(b) from bool_aggregate;

-- 显式禁用仍须生效，但不能改变结果。
set optimizer_hints='blockFilter=2';
select id from bool_values where enabled = true order by id;
select count(*) as left_count from bool_values where id in (1,2) and enabled = true;
select count(*) as right_count from bool_values where id in (2,3) and enabled = true;
select l.id as left_id, r.id as right_id
from (select id, enabled from bool_values where id in (1,2) and enabled = true) l
full outer join (select id, enabled from bool_values where id in (2,3) and enabled = true) r
on l.id = r.id and l.enabled = r.enabled
order by coalesce(l.id, r.id);
prepare zm_partition from 'select a from range_bound where a = ?';
execute zm_partition using @zm_partition_value;
set @zm_partition_value=null;
execute zm_partition using @zm_partition_value;
set @zm_partition_value=11;
execute zm_partition using @zm_partition_value;
deallocate prepare zm_partition;
select k from clustered where 100 < k order by k;
select id from clustered where 3 < id order by id;
select k from clustered where k between 100 and 150 order by k;
-- @regex("Table Scan on non_primary_zonemap[.]clustered[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=3\s+inputRows=6\b",true)
explain analyze select count(*) from clustered where k between 100 and 150;
set optimizer_hints=@saved_zonemap_hints;
drop database non_primary_zonemap;
