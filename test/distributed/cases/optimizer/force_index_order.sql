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

drop database force_index_order;
