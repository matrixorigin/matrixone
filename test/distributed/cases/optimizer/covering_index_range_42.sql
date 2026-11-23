drop database if exists covering_index_range_42;
create database covering_index_range_42;
use covering_index_range_42;

create table t (id int primary key, a int, b int, key ix_ab(a,b), key ix_b(b));
create table control (id int primary key, a int, b int);
insert into t values (1,1,5),(2,1,5),(3,1,6),(4,1,null),(5,null,5),(6,2,5),(7,1,0),(8,1,10);
insert into control select * from t;

-- A forced aggregate must apply the range to the serialized index key.
-- @separator:table
-- @regex("Index Table Scan.*ix_b",true)
-- @regex("prefix_between",true)
explain select count(distinct b) from t force index(ix_b) where b between 5 and 6;
select count(distinct b) from t force index(ix_b) where b between 5 and 6;
select count(distinct b) from control where b between 5 and 6;
select count(distinct b) from t force index(ix_b) where b>5;
select count(distinct b) from control where b>5;
select count(distinct b) from t force index(ix_b) where b<=5;
select count(distinct b) from control where b<=5;
-- Unsupported range flags retain the decoded predicate's semantics.
select count(distinct b) from t force index(ix_b) where in_range(b,5,6,4);
select count(distinct b) from control where in_range(b,5,6,4);
select count(distinct b) from t force index(ix_b) where in_range(b,5,6,0);
select count(distinct b) from control where in_range(b,5,6,0);
-- Two separate comparisons constrain both ends of the index range.
-- @separator:table
-- @regex("Index Table Scan.*ix_b",true)
-- @regex("prefix_in_range",true)
explain select count(distinct b) from t force index(ix_b) where b>=5 and b<7;
select count(distinct b) from t force index(ix_b) where b>=5 and b<7;
select count(distinct b) from control where b>=5 and b<7;

-- The suffix range narrows the equality prefix and keeps exact row checks.
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("prefix_in_range",true)
explain select sum(b) from t force index(ix_ab) where a=1 and b>5;
select id from t force index(ix_ab) where a=1 and b>5 and b<10 order by id;
select id from control where a=1 and b>5 and b<10 order by id;
select id from t force index(ix_ab) where a=1 and b>=5 and b<7 order by id;
select id from control where a=1 and b>=5 and b<7 order by id;
select id from t force index(ix_ab) where a=1 and b between 10 and 5 order by id;
select id from control where a=1 and b between 10 and 5 order by id;
select id from t force index(ix_b) where b<=5 order by id;
select id from control where b<=5 order by id;

-- A noncovered payload keeps index backfill and the combined range.
create table nc (id int primary key, a int, b int, payload int, key ix_ab(a,b));
create table nc_control (id int primary key, a int, b int, payload int);
insert into nc values (1,1,5,10),(2,1,6,20),(3,1,7,30),(4,2,5,40);
insert into nc_control select * from nc;
-- @separator:table
-- @regex("Index Table Scan.*ix_ab",true)
-- @regex("prefix_in_range",true)
explain select sum(payload) from nc force index(ix_ab) where a=1 and b>=5 and b<7;
select sum(payload) from nc force index(ix_ab) where a=1 and b>=5 and b<7;
select sum(payload) from nc_control where a=1 and b>=5 and b<7;

-- Nullable unique index entries cannot be assumed to contain all base rows.
create table u (id int primary key, a int, b int, c int, unique key ix_abc(a,b,c));
create table uc (id int primary key, a int, b int, c int);
insert into u values (1,1,5,null),(2,1,6,null),(3,1,7,1),(4,1,8,2);
insert into uc select * from u;
select id from u force index(ix_abc) where a=1 and b>4 order by id;
select id from uc where a=1 and b>4 order by id;

-- Escaped NUL peers must not satisfy the shorter equality prefix.
create table s (id int primary key, a varbinary(16), b int, key ix_ab(a,b));
create table sc (id int primary key, a varbinary(16), b int);
insert into s values (1,cast('a' as binary),91),(2,cast('a\0' as binary),92),(3,cast('a\0z' as binary),93),(4,cast('a' as binary),90);
insert into sc select * from s;
select id,hex(a),b from s force index(ix_ab) where a=cast('a' as binary) and b>=90 order by id;
select id,hex(a),b from sc where a=cast('a' as binary) and b>=90 order by id;

prepare p from 'select id from t force index(ix_ab) where a=? and b>? order by id';
set @a=1,@b=5;
execute p using @a,@b;
set @b=null;
execute p using @a,@b;
set @b=9;
execute p using @a,@b;
deallocate prepare p;
drop database covering_index_range_42;
