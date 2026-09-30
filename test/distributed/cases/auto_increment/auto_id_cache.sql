-- 表级缓存策略与持久化；每个场景仅使用少量行跨越分配边界。
drop database if exists ai_cache_bvt;
create database ai_cache_bvt;
use ai_cache_bvt;
set @saved_increment = @@session.auto_increment_increment;
set @saved_offset = @@session.auto_increment_offset;
set auto_increment_increment=1;
set auto_increment_offset=1;

-- 0 归一为默认；无自增列也可以显式指定 0。
create table ai_plain(id bigint) auto_id_cache=0;
show create table ai_plain;
create table ai_zero(id bigint auto_increment primary key) auto_id_cache=0;
show create table ai_zero;

-- 只读观察不预留号段，CACHE=1 保留显式/自动输入顺序。
create table ai_one(id bigint auto_increment primary key, v int) auto_id_cache=1;
select internal_auto_increment(database(),'ai_one');
select internal_auto_increment(database(),'ai_one');
insert into ai_one values (-1,10),(NULL,20),(100,30),(NULL,40);
select id,v from ai_one order by v;
select last_insert_id();
select internal_auto_increment(database(),'ai_one');
show create table ai_one;

-- 起点修改和 COPY/LIKE/CLONE/TRUNCATE 保留表级策略。
alter table ai_one auto_increment=200;
insert into ai_one(v) values(50);
select id from ai_one where v=50;
alter table ai_one add column extra int, algorithm=copy;
show create table ai_one;
create table ai_like like ai_one;
show create table ai_like;
insert into ai_like(v) values(1);
select id,v from ai_like;
create table ai_clone clone ai_one;
show create table ai_clone;
insert into ai_clone(v) values(60);
select id,v from ai_clone where v=60;
truncate table ai_one;
show create table ai_one;
insert into ai_one(v) values(1);
select id,v from ai_one;

-- 表级策略不是会话步长；两种参数分别作用于保留和区间内选值。
set auto_increment_increment=3;
set auto_increment_offset=2;
create table ai_series(id bigint auto_increment primary key) auto_increment=10 auto_id_cache=1;
insert into ai_series values(NULL),(NULL),(NULL);
select id from ai_series order by id;
select last_insert_id();
create table ai_span(id bigint auto_increment primary key) auto_id_cache=2;
insert into ai_span values(NULL),(NULL),(NULL);
select id from ai_span order by id;
show create table ai_span;
create table ai_max(id bigint auto_increment primary key) auto_id_cache=1000000;
show create table ai_max;
set auto_increment_increment=1;
set auto_increment_offset=1;

-- 临时 DDL 使用自己的事务，外层回滚不移除临时表或其策略。
begin;
create temporary table ai_temp(id bigint auto_increment primary key) auto_id_cache=1;
rollback;
show create table ai_temp;
insert into ai_temp values(NULL);
select id from ai_temp;
drop temporary table ai_temp;

-- 未提交 CREATE 的观察遵循事务所有权，提交/回滚均不因查询预留号码。
begin;
create table ai_observe(id bigint auto_increment primary key) auto_increment=10 auto_id_cache=1;
select auto_increment from information_schema.tables where table_schema=database() and table_name='ai_observe';
rollback;
select count(*) from information_schema.tables where table_schema=database() and table_name='ai_observe';
begin;
create table ai_observe(id bigint auto_increment primary key) auto_increment=10 auto_id_cache=1;
select auto_increment from information_schema.tables where table_schema=database() and table_name='ai_observe';
commit;
select auto_increment from information_schema.tables where table_schema=database() and table_name='ai_observe';
insert into ai_observe values(NULL);
select id from ai_observe;

-- 移除最后一个可见自增属性/列时归一为默认策略，数据保留。
create table ai_modify(id bigint auto_increment primary key, v int) auto_id_cache=1;
insert into ai_modify(v) values(7);
alter table ai_modify modify id bigint;
show create table ai_modify;
select id,v from ai_modify;
create table ai_drop(id bigint auto_increment primary key, v int) auto_id_cache=2;
insert into ai_drop(v) values(7);
alter table ai_drop drop column id;
show create table ai_drop;
select v from ai_drop;

-- 表级策略不能在分区/子分区中被静默忽略，包括默认值。
create table ai_bad_partition(id bigint auto_increment primary key) partition by range(id) (partition p0 values less than maxvalue auto_id_cache=0);
create table ai_bad_partition(id bigint auto_increment primary key) partition by range(id) subpartition by hash(id) (partition p0 values less than maxvalue (subpartition s0 auto_id_cache=1));
select count(*) from mo_catalog.mo_tables where reldatabase=database() and relname='ai_bad_partition';

-- 非法选项必须拒绝，不能静默截断或忽略。
create table ai_bad(id bigint auto_increment) auto_id_cache=1000001;
create table ai_bad(id bigint auto_increment) auto_id_cache=-1;
create table ai_bad(id bigint auto_increment) auto_id_cache=1.5;
create table ai_bad(id bigint auto_increment) auto_id_cache=1 auto_id_cache=2;
create table ai_bad(id bigint) auto_id_cache=1;
alter table ai_one auto_id_cache=2;
show create table ai_one;
set auto_increment_increment=@saved_increment;
set auto_increment_offset=@saved_offset;
drop database ai_cache_bvt;
