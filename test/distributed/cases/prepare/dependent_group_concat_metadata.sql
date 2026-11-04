-- @suite
-- @setup
-- 独立元数据边界：小表与会话变量，不复用含截断 warning 的 GROUP_CONCAT fixture。
drop database if exists dependent_gc_metadata;
create database dependent_gc_metadata;
use dependent_gc_metadata;
create table t(k int);
insert into t values (1), (2);
set @saved_gc_metadata_limit = @@session.group_concat_max_len;
set session group_concat_max_len=64;
prepare ordinary_gc from 'select group_concat(k) as gc, k from t group by k order by k';
prepare dependent_gc from 'select group_concat(k) as gc, (select (select t.k)) as k from t group by k order by k';
execute ordinary_gc;
execute dependent_gc;
execute dependent_gc;
deallocate prepare ordinary_gc;
deallocate prepare dependent_gc;

-- 小结果不变；512/513 的元数据边界及执行后单调扩宽各自可观察。
set session group_concat_max_len=512;
prepare dependent_gc from 'select group_concat(k) as gc, (select (select t.k)) as k from t group by k order by k';
execute dependent_gc;
set session group_concat_max_len=513;
execute dependent_gc;
set session group_concat_max_len=64;
execute dependent_gc;
deallocate prepare dependent_gc;
set session group_concat_max_len=@saved_gc_metadata_limit;
drop database dependent_gc_metadata;
