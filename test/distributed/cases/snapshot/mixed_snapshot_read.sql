drop snapshot if exists mixed_read_s1;
drop snapshot if exists mixed_read_s2;
drop database if exists mixed_snapshot_read;
create database mixed_snapshot_read;
use mixed_snapshot_read;
create table t (x int);
create table u (x int);
create table m (s varchar(8));
create view v as select x from t;
insert into t values (1);
insert into u values (11), (12);
insert into m values ('old');
create snapshot mixed_read_s1 for account;
update t set x = 2;
update u set x = 22 where x = 11;
delete from u where x = 12;
alter table m modify s varchar(13);
update m set s = 'new';
create snapshot mixed_read_s2 for account;
update t set x = 3;

-- 独立历史读取、同一快照和当前读取对照。
select x from t {snapshot='mixed_read_s1'};
select x from t {snapshot='mixed_read_s2'};
select a.x, b.x from t {snapshot='mixed_read_s1'} a cross join t {snapshot='mixed_read_s1'} b;
select a.x, b.x from t a cross join t {snapshot='mixed_read_s1'} b;

-- 每个扫描保留自己的时间戳，交换顺序只交换结果列。
select a.x, b.x from t {snapshot='mixed_read_s1'} a cross join t {snapshot='mixed_read_s2'} b;
select a.x, b.x from t {snapshot='mixed_read_s2'} a cross join t {snapshot='mixed_read_s1'} b;
select a.x, b.x from t {snapshot='mixed_read_s1'} a cross join t {snapshot='mixed_read_s2'} b where a.x=1 and b.x=2;
select a.x, b.x from t {snapshot='mixed_read_s1'} a cross join u {snapshot='mixed_read_s2'} b;
select a.x, b.x from u {snapshot='mixed_read_s1'} a cross join u {snapshot='mixed_read_s2'} b order by a.x, b.x;
select 'a' as source, x from t {snapshot='mixed_read_s1'} union all select 'b', x from t {snapshot='mixed_read_s2'} order by source;
with a as (select x from t {snapshot='mixed_read_s1'}), b as (select x from t {snapshot='mixed_read_s2'}) select a.x, b.x from a cross join b;
select a.x, b.x from v {snapshot='mixed_read_s1'} a cross join v {snapshot='mixed_read_s2'} b;

-- 同一会话重复 prepared 执行，不能保留另一次执行的事务。
prepare mixed_read_stmt from "select a.x, b.x from t {snapshot='mixed_read_s1'} a cross join t {snapshot='mixed_read_s2'} b";
execute mixed_read_stmt;
select x from t;
execute mixed_read_stmt;
deallocate prepare mixed_read_stmt;

-- 历史元数据必须与各自快照的真实列类型一致。
select character_maximum_length from information_schema.columns {snapshot='mixed_read_s1'} where table_schema='mixed_snapshot_read' and table_name='m' and column_name='s';
select character_maximum_length from information_schema.columns {snapshot='mixed_read_s2'} where table_schema='mixed_snapshot_read' and table_name='m' and column_name='s';
select a.character_maximum_length, b.character_maximum_length from information_schema.columns {snapshot='mixed_read_s1'} a cross join information_schema.columns {snapshot='mixed_read_s2'} b where a.table_schema='mixed_snapshot_read' and a.table_name='m' and a.column_name='s' and b.table_schema='mixed_snapshot_read' and b.table_name='m' and b.column_name='s';
select a.character_maximum_length, b.character_maximum_length from information_schema.columns {snapshot='mixed_read_s2'} a cross join information_schema.columns {snapshot='mixed_read_s1'} b where a.table_schema='mixed_snapshot_read' and a.table_name='m' and a.column_name='s' and b.table_schema='mixed_snapshot_read' and b.table_name='m' and b.column_name='s';
select a.atttyp = b.atttyp as same_type from mo_catalog.mo_columns {snapshot='mixed_read_s1'} a cross join mo_catalog.mo_columns {snapshot='mixed_read_s2'} b where a.att_database='mixed_snapshot_read' and a.att_relname='m' and a.attname='s' and b.att_database='mixed_snapshot_read' and b.att_relname='m' and b.attname='s';
select a.s, b.s from m {snapshot='mixed_read_s1'} a cross join m {snapshot='mixed_read_s2'} b;

drop snapshot mixed_read_s2;
drop snapshot mixed_read_s1;
drop database mixed_snapshot_read;
