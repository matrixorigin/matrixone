drop database if exists force_index_order;
create database force_index_order;
use force_index_order;

create table t (id int primary key, a int, b int, payload int, key ix_ab(a,b));
insert into t values (1,1,1,30),(2,1,1,10),(3,1,2,20),(4,2,2,40);

-- An unrelated ORDER BY must not discard the forced, bounded access.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("prefix_in",true)
-- @regex("Sort",true)
explain select id,payload from t force index(ix_ab)
where a=1 and b in (1,2) order by payload,id;
select id,payload from t force index(ix_ab)
where a=1 and b in (1,2) order by payload,id;
select id,payload from t ignore index(ix_ab)
where a=1 and b in (1,2) order by payload,id;

-- A covering compatible order keeps the existing ordered reader LIMIT.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("Index Reader Param",true)
explain select id,a,b from t force index(ix_ab) order by a,b,id limit 2;
select id,a,b from t force index(ix_ab) order by a,b,id limit 2;
select id,a,b from t ignore index(ix_ab) order by a,b,id limit 2;
-- A mixed order must not push an incompatible reader LIMIT.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("Index Reader Param",false)
explain select id,a,b from t force index(ix_ab) order by a desc,b asc,id limit 2;
select id,a,b from t force index(ix_ab) order by a desc,b asc,id limit 2;
select id,a,b from t ignore index(ix_ab) order by a desc,b asc,id limit 2;

-- IGNORE takes precedence over FORCE, including for PRIMARY.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
explain select id,payload from t force index(primary,ix_ab) ignore index(primary)
where a=1 and b in (1,2) order by payload,id;

-- An equality-fixed leading part permits an order-scoped index hint.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("Sort",true)
explain select id,payload from t force index for order by(ix_ab)
where a=1 order by b desc,id desc limit 2;
select id,payload from t force index for order by(ix_ab)
where a=1 order by b desc,id desc limit 2;
select id,payload from t ignore index(ix_ab)
where a=1 order by b desc,id desc limit 2;

-- JOIN must not replace a covering access after publishing its columns.
create table u (id int primary key, v int);
insert into u values (1,10),(3,30),(4,40);
select t.id,t.a,t.b from t force index(ix_ab) join u on t.id=u.id
where t.a=1 and t.b in (1,2) order by t.b,t.id;
select t.id,t.a,t.b from t ignore index(ix_ab) join u on t.id=u.id
where t.a=1 and t.b in (1,2) order by t.b,t.id;

-- JOIN and ORDER scopes must not publish conflicting covering-column maps.
create table scope_t (id int primary key, a int, b int, key ix_ab(a,b), key ix_ba(b,a));
insert into scope_t values (1,1,1),(2,1,2),(3,2,3);
select s.id,s.a,s.b from scope_t s force index for join(ix_ab) force index for order by(ix_ab)
join u on s.id=u.id where s.a=1 order by s.b,s.id;
select s.id,s.a,s.b from scope_t s force index for join(ix_ba) force index for order by(ix_ab)
join u on s.id=u.id where s.a=1 order by s.b,s.id;
select s.id,s.a,s.b from u join scope_t s force index for join(ix_ba) force index for order by(ix_ab)
on s.id=u.id where s.a=1 order by s.b,s.id;
select s.id,s.a,s.b from scope_t s ignore index(ix_ab,ix_ba)
join u on s.id=u.id where s.a=1 order by s.b,s.id;

-- A JOIN can discard early index rows; its output LIMIT cannot be pushed below it.
create table limit_j (id int primary key, a int, key ix_limit_a(a));
create table limit_u (id int primary key);
insert into limit_j values (1,1),(2,2),(3,3),(4,4);
insert into limit_u values (3),(4);
select mo_ctl('dn','flush','force_index_order.limit_j');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_limit_a');
-- @separator:table
-- @regex("Index Table Scan.*ix_limit_a",true)
-- @regex("Index Reader Param",false)
explain select j.id,j.a from limit_j j force index for order by(ix_limit_a)
join limit_u u on j.id=u.id order by j.a limit 1;
select j.id,j.a from limit_j j force index for order by(ix_limit_a)
join limit_u u on j.id=u.id order by j.a limit 1;
select j.id,j.a from limit_j j ignore index(ix_limit_a)
join limit_u u on j.id=u.id order by j.a limit 1;

-- A full unique-index scan omits rows when any index part is NULL.
create table uq (id int primary key, a int, b int, unique key ux_ab(a,b));
insert into uq values (1,1,null),(2,1,2),(3,null,2),(4,2,2);
-- @separator:table
-- @regex("Index Table Scan.*ux_ab",false)
explain select id from uq force index for order by(ux_ab) where a=1 order by a;
select id from uq force index for order by(ux_ab) where a=1 order by a,id;
select id from uq ignore index(ux_ab) where a=1 order by a,id;
select a,b,count(*) from uq force index for group by(ux_ab) group by a,b order by a,b;

-- A JOIN on only the first part must not lose nullable UNIQUE suffixes.
-- @separator:table
-- @regex("Index Table Scan.*ux_ab",false)
explain select uq.id,uq.b from u join uq force index(ux_ab) on uq.a=u.id order by uq.id;
select uq.id,uq.b from u join uq force index(ux_ab) on uq.a=u.id order by uq.id;
select uq.id,uq.b from u join uq ignore index(ux_ab) on uq.a=u.id order by uq.id;

-- Persisted prefix-index keys are truncated, while JOIN inputs are full values.
-- The full JOIN condition remains authoritative after broad index backfill.
create table prefix_j (id int primary key, a varchar(20), b int, payload int, key ix_join_pref(a(4),b));
create table prefix_u (a varchar(20) primary key);
insert into prefix_j values (1,'abcdef',1,10),(2,'abcdxy',2,20),(3,'wxyz12',3,30);
insert into prefix_u values ('abcdef'),('abcdxy');
insert into prefix_j select result+100,concat('z',lpad(cast(result as varchar(20)),10,'0')),result,result from generate_series(1,10000) g;
select mo_ctl('dn','flush','force_index_order.prefix_j');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_join_pref');
-- @separator:table
-- @regex("Index Table Scan.*ix_join_pref",true)
-- @regex("Runtime Filter Probe: __mo_index_idx_col Match Prefix",false)
explain select j.id,j.a,j.payload from prefix_j j force index(ix_join_pref)
join prefix_u u on j.a=u.a order by j.payload,j.id;
select j.id,j.a,j.payload from prefix_j j force index(ix_join_pref)
join prefix_u u on j.a=u.a order by j.payload,j.id;
select j.id,j.a,j.payload from prefix_j j ignore index(ix_join_pref)
join prefix_u u on j.a=u.a order by j.payload,j.id;

-- A preceding full key part can still bound a JOIN when a later part is truncated.
create table suffix_j (id int primary key, a int, b varchar(20), payload int, key ix_suffix(a,b(4)));
create table suffix_u (a int, b varchar(20));
insert into suffix_j values (1,7,'abcdef',10),(2,7,'abcdxy',20),(3,8,'abcdef',30);
insert into suffix_u values (7,'abcdef');
-- @separator:table
-- @regex("Index Table Scan.*ix_suffix",true)
-- @regex("Runtime Filter Probe: __mo_index_idx_col Match Prefix",true)
explain select j.id,j.b from suffix_j j force index(ix_suffix)
join suffix_u u on j.a=u.a and j.b=u.b order by j.payload,j.id;
select j.id,j.b from suffix_j j force index(ix_suffix)
join suffix_u u on j.a=u.a and j.b=u.b order by j.payload,j.id;
select j.id,j.b from suffix_j j ignore index(ix_suffix)
join suffix_u u on j.a=u.a and j.b=u.b order by j.payload,j.id;

-- The same prepared statement must rebind values and NULL correctly.
prepare force_order_param from 'select id,payload from t force index(ix_ab) where a=? and b in (1,2) order by payload,id';
set @v=1;
execute force_order_param using @v;
set @v=2;
execute force_order_param using @v;
set @v=null;
execute force_order_param using @v;
set @v=1;
execute force_order_param using @v;
deallocate prepare force_order_param;


-- Dynamic TopValue can prune the base probe of an INDEX join; no static
-- reader limit may cross that join. A non-NULL first key permits safe pruning.
create table top_probe (id int primary key, a int, payload int not null, key ix_top(a));
insert into top_probe values (1,1,30),(2,1,10),(3,2,20),(4,2,40);
-- @separator:table
-- @regex("Index Table Scan.*ix_top",true)
-- @regex("MsgTopValue",true)
-- @regex("Index Reader Param",false)
explain select id,payload from top_probe force index(ix_top) order by payload,id limit 2;
select id,payload from top_probe force index(ix_top) order by payload,id limit 2;
select id,payload from top_probe ignore index(ix_top) order by payload,id limit 2;

-- A NULL can be the first SQL value even when the block's non-NULL zonemap
-- lies beyond the current TopValue. Both indexed and base scans must retain it.
create table nullable_top (id int primary key, a int not null, payload int, key ix_null_top(a));
set @@max_dop=1;
insert into nullable_top values (1,1,-10),(5,5,-20);
select mo_ctl('dn','flush','force_index_order.nullable_top');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_null_top');
insert into nullable_top values (2,2,100),(3,3,null);
select mo_ctl('dn','flush','force_index_order.nullable_top');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_null_top');
insert into nullable_top values (4,4,-10);
select mo_ctl('dn','flush','force_index_order.nullable_top');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_null_top');
-- @separator:table
-- @regex("Index Table Scan.*ix_null_top",true)
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_top force index(ix_null_top) order by payload limit 1;
select id,payload from nullable_top force index(ix_null_top) order by payload limit 1;
-- @separator:table
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_top ignore index(ix_null_top) order by payload limit 1;
select id,payload from nullable_top ignore index(ix_null_top) order by payload limit 1;
select id,payload from nullable_top where payload is null;

-- A bound may arrive from transaction-local rows before persisted NULL blocks.
begin;
insert into nullable_top values (6,6,-30);
select id,payload from nullable_top ignore index(ix_null_top) order by payload limit 1;
rollback;

-- With NULLS LAST, a NULL heap root must not become a numeric bound.
-- The later -10 block is still needed even though its maximum is below zero.
-- @separator:table
-- @regex("Index Table Scan.*ix_null_top",true)
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_top force index(ix_null_top) order by payload desc,id desc limit 2;
select id,payload from nullable_top force index(ix_null_top) order by payload desc,id desc limit 2;
select id,payload from nullable_top ignore index(ix_null_top) order by payload desc,id desc limit 2;

-- Equal first keys across blocks remain candidates for the second key.
-- @separator:table
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_top force index(ix_null_top) where id<>5 order by payload,id desc limit 2;
select id,payload from nullable_top force index(ix_null_top) where id<>5 order by payload,id desc limit 2;
select id,payload from nullable_top ignore index(ix_null_top) where id<>5 order by payload,id desc limit 2;

-- An old block may not contain a column added after it was flushed.
create table nullable_evo (id int primary key);
insert into nullable_evo values (1);
select mo_ctl('dn','flush','force_index_order.nullable_evo');
alter table nullable_evo add column payload int;
insert into nullable_evo values (2,-10),(3,100);
select mo_ctl('dn','flush','force_index_order.nullable_evo');
-- @separator:table
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_evo order by payload desc limit 3;
select id,payload from nullable_evo order by payload desc limit 3;
-- A missing column is NULL in the old block and must win ASC NULLS FIRST.
-- @separator:table
-- @regex("MsgTopValue",true)
explain select id,payload from nullable_evo order by payload limit 1;
select id,payload from nullable_evo order by payload limit 1;
set @@max_dop=0;

-- An integer range after an equality prefix can bound the physical index,
-- while the original SQL predicates remain authoritative.
-- @separator:table
-- @regex("Block Filter Cond:.*prefix_between",true)
explain select id,payload from t force index(ix_ab) where a=1 and b between 1 and 2 order by id;
select id,payload from t force index(ix_ab) where a=1 and b between 1 and 2 order by id;
select id,payload from t ignore index(ix_ab) where a=1 and b between 1 and 2 order by id;
select id,payload from t force index(ix_ab) where a=1 and b>1 and b<=2 order by id;
select id,payload from t ignore index(ix_ab) where a=1 and b>1 and b<=2 order by id;

prepare force_index_range from 'select id,payload from t force index(ix_ab) where a=? and b between ? and ? order by id';
set @a=1, @lo=1, @hi=2;
execute force_index_range using @a,@lo,@hi;
set @lo=null;
execute force_index_range using @a,@lo,@hi;
set @lo=2, @hi=1;
execute force_index_range using @a,@lo,@hi;
set @lo=1, @hi=2;
execute force_index_range using @a,@lo,@hi;
deallocate prepare force_index_range;

-- A coerced JOIN key retains plain FORCE with ORDER through the existing
-- broad index fallback. Out-of-range and NULL values keep SQL JOIN semantics.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
explain select t.id,t.a,t.b,t.payload from (values row(cast(1 as signed)),row(cast(2147483648 as signed)),row(cast(null as signed))) k(a)
join t force index(ix_ab) on t.a=k.a order by t.id;
select t.id,t.a,t.b,t.payload from (values row(cast(1 as signed)),row(cast(2147483648 as signed)),row(cast(null as signed))) k(a)
join t force index(ix_ab) on t.a=k.a order by t.id;
select t.id,t.a,t.b,t.payload from (values row(cast(1 as signed)),row(cast(2147483648 as signed)),row(cast(null as signed))) k(a)
join t ignore index(ix_ab) on t.a=k.a order by t.id;

-- The reported composite-primary-key and VARCHAR index case must retain its
-- bounded forced access after ORDER BY introduces a separate Sort.
create table issue32665 (pk1 int, pk2 int, a int, b varchar(20), payload int,
    primary key (pk1,pk2), key ix_issue32665_ab(a,b));
insert into issue32665 values
    (1,1,1,'alpha',10),(2,2,1,'alpha',20),(3,3,1,'beta',30),(4,4,2,'alpha',40);
select mo_ctl('dn','flush','force_index_order.issue32665');
select mo_ctl('dn','flush',cast(rel_id as varchar(32))) from mo_catalog.mo_tables
where relname in (select index_table_name from mo_catalog.mo_indexes where name='ix_issue32665_ab');
-- @separator:table
-- @regex("Index Table Scan.*ix_issue32665_ab",true)
-- @regex("prefix_in",true)
explain select pk1,pk2,payload from issue32665 force index(ix_issue32665_ab)
where a=1 and b in ('alpha','beta') order by payload;
select pk1,pk2,payload from issue32665 force index(ix_issue32665_ab)
where a=1 and b in ('alpha','beta') order by payload;
select pk1,pk2,payload from issue32665 ignore index(ix_issue32665_ab)
where a=1 and b in ('alpha','beta') order by payload;

drop database force_index_order;
