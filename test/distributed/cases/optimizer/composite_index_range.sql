-- A composite equality prefix plus a suffix range must preserve SQL results
-- while seeking only the matching serialized-key objects. The unindexed table
-- is the result oracle; flush boundaries give each key range a narrow zonemap.
drop database if exists composite_index_range;
create database composite_index_range;
use composite_index_range;
set @saved_max_dop=@@max_dop;
set max_dop=1;

create table t (k int primary key, a int, b int, key idx_ab(a,b), key idx_a(a));
set @tid=(select rel_id from mo_catalog.mo_tables where relname='t' and reldatabase=database() limit 1);
set @idx_ab=(select index_table_name from mo_catalog.mo_indexes where table_id=@tid and name='idx_ab' limit 1);
set @idx_a=(select index_table_name from mo_catalog.mo_indexes where table_id=@tid and name='idx_a' limit 1);

insert into t values (1,null,91),(2,9,null);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

insert into t values (3,8,99),(4,8,100);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

insert into t values (5,9,89),(6,9,90);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

insert into t values (7,9,91),(8,9,91);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

insert into t values (9,9,99),(10,9,100);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

insert into t values (11,10,91),(12,10,99);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.t'));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_ab));
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush',concat(database(),'.',@idx_a));

create table t_ctl (k int primary key, a int, b int);
insert into t_ctl select * from t;

select k,a,b from t force index(idx_ab) where a=9 and b>90 order by k;
select k,a,b from t_ctl where a=9 and b>90 order by k;
select k,a,b from t force index(idx_ab) where a=9 and b>=91 order by k;
select k,a,b from t_ctl where a=9 and b>=91 order by k;
select k,a,b from t force index(idx_ab) where a=9 and b<91 order by k;
select k,a,b from t_ctl where a=9 and b<91 order by k;
select k,a,b from t force index(idx_ab) where a=9 and b<=90 order by k;
select k,a,b from t_ctl where a=9 and b<=90 order by k;
select k,a,b from t force index(idx_ab) where a=9 and b between 90 and 99 order by k;
select k,a,b from t_ctl where a=9 and b between 90 and 99 order by k;
select k,a,b from t force index(idx_ab) where a=9 and b>90 and b<100 order by k;
select k,a,b from t_ctl where a=9 and b>90 and b<100 order by k;
select k,a,b from t force index(idx_ab) where 9=a and 90<b order by k;
select k,a,b from t_ctl where 9=a and 90<b order by k;

-- Empty, exact, reversed, and NULL bounds retain ordinary SQL semantics.
select k from t force index(idx_ab) where a=9 and b>100 order by k;
select k from t_ctl where a=9 and b>100 order by k;
select k from t force index(idx_ab) where a=9 and b=91 order by k;
select k from t_ctl where a=9 and b=91 order by k;
select k from t force index(idx_ab) where a=9 and b>100 and b<90 order by k;
select k from t_ctl where a=9 and b>100 and b<90 order by k;
select k from t force index(idx_ab) where a=null and b>90 order by k;
select k from t_ctl where a=null and b>90 order by k;
select k from t force index(idx_ab) where a=9 and b>null order by k;
select k from t_ctl where a=9 and b>null order by k;

select sum(k) from t force index(idx_ab) where a=9 and b>90;
select sum(k) from t_ctl where a=9 and b>90;
select sum(k) from t where a=9 and b>90;
-- @regex("Index Table Scan on t[.]idx_ab[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=2\s+inputRows=4\b",true)
explain analyze select sum(k) from t force index(idx_ab) where a=9 and b>90;
-- @regex("Index Table Scan on t[.]idx_ab[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=0\s+inputRows=0\b",true)
explain analyze select sum(k) from t force index(idx_ab) where a=9 and b>100;

-- The forced leading-column path uses the same serialized range machinery.
select count(distinct a) from t force index(idx_a) where a between 8 and 9;
select count(distinct a) from t_ctl where a between 8 and 9;
-- @regex("Index Table Scan on t[.]idx_a[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=5\s+inputRows=9\b",true)
explain analyze select count(distinct a) from t force index(idx_a) where a between 8 and 9;
select count(distinct a) from t force index(idx_a) where a is not null;
select count(distinct a) from t_ctl where a is not null;
-- @regex("Index Table Scan on t[.]idx_a[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=6\s+inputRows=12\b",true)
explain analyze select count(distinct a) from t force index(idx_a) where a is not null;

-- Exercise the original default (unhinted) range-join shape as a control.
select count(*) from t t1 join t t2 on t1.a=t2.a where t1.a<9;
select count(*) from t_ctl t1 join t_ctl t2 on t1.a=t2.a where t1.a<9;
-- @regex("Index Table Scan on t[.]idx_a[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputBlocks=2\s+inputRows=3\b",true)
explain analyze select count(*) from t t1 join t t2 on t1.a=t2.a where t1.a<9;

-- A unique index is an incomplete row source when a nullable key part is
-- unconstrained: its hidden table omits every key containing NULL.
create table uq3 (k int primary key, a int, b int, c int, unique key uq_abc(a,b,c));
create table uq3_ctl (k int primary key, a int, b int, c int);
insert into uq3 values (1,9,91,null),(2,9,92,null),(3,9,93,1),(4,8,95,null),(5,9,89,null),(6,9,99,2),(7,9,94,null);
insert into uq3_ctl values (1,9,91,null),(2,9,92,null),(3,9,93,1),(4,8,95,null),(5,9,89,null),(6,9,99,2),(7,9,94,null);
select b from uq3 where a=9 and b>90 and b<100 order by b;
select b from uq3_ctl where a=9 and b>90 and b<100 order by b;
select b from uq3 where a=9 and b>90 and b<100 and c is not null order by b;
select b from uq3_ctl where a=9 and b>90 and b<100 and c is not null order by b;
select b from uq3 where a=9 and b>90 order by b;
select b from uq3_ctl where a=9 and b>90 order by b;
select b from uq3 where a=9 and b>90 and c is not null order by b;
select b from uq3_ctl where a=9 and b>90 and c is not null order by b;

-- An escaped-NUL peer must not satisfy the equality prefix merely because its
-- serialized key lies inside the composite cursor endpoints.
create table bs (k int primary key, a varbinary(16), b int, key idx_ab(a,b));
create table bs_ctl (k int primary key, a varbinary(16), b int);
create table vs (k int primary key, a varchar(16), b int, key idx_ab(a,b));
create table vs_ctl (k int primary key, a varchar(16), b int);
insert into bs values (1,cast('a' as binary),91),(2,cast('a\0' as binary),92),(3,cast('a\0z' as binary),93),(4,cast('a' as binary),90),(5,cast('b' as binary),94),(6,null,95),(7,cast('a' as binary),null);
insert into bs_ctl select * from bs;
insert into vs select * from bs;
insert into vs_ctl select * from bs;

select hex(a),b from bs where a=cast('a' as binary) and b>90 order by b;
select hex(a),b from bs_ctl where a=cast('a' as binary) and b>90 order by b;
select hex(a),b from bs force index(idx_ab) where a=cast('a' as binary) and b>90 order by b;
select hex(a),b from bs where a=cast('a' as binary) and b>=90 order by b;
select hex(a),b from bs_ctl where a=cast('a' as binary) and b>=90 order by b;
select hex(a),b from bs force index(idx_ab) where a=cast('a' as binary) and b>=90 order by b;

select hex(a),b from vs where a='a' and b>90 order by b;
select hex(a),b from vs_ctl where a='a' and b>90 order by b;
select hex(a),b from vs force index(idx_ab) where a='a' and b>90 order by b;
select hex(a),b from vs where a='a' and b>=90 order by b;
select hex(a),b from vs_ctl where a='a' and b>=90 order by b;
select hex(a),b from vs force index(idx_ab) where a='a' and b>=90 order by b;

-- An unstable suffix retains equality-prefix evaluation when the prefix is empty.
create table filter_errors (k int primary key, a int, b varchar(32) collate utf8mb4_bin, key ix(a,b));
insert into filter_errors values (1,8,'x'),(2,9,'y');
select a,b from filter_errors where a=99 and b>regexp_replace('x','[','y');
-- A stable literal suffix retains the serialized range and ordinary SQL results.
select a,b from filter_errors where a=9 and b>'w' order by k;

set max_dop=@saved_max_dop;
drop database composite_index_range;
