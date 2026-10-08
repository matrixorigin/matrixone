-- Arithmetic must retain rounding and checked overflow before/after flush.
drop database if exists arithmetic_predicate_semantics;
create database arithmetic_predicate_semantics;
use arithmetic_predicate_semantics;
set @saved_transpose_hints=@@optimizer_hints;
create table fp(id int primary key, v double);
insert into fp values (1,-1e-17),(2,0),(3,1e-17),(4,null),(5,2);
select id,v+1e0=1e0 as p from fp order by id;
select id from fp where v+1e0=1e0 order by id;
select id from fp where v-1e0=-1e0 order by id;
select id from fp where 1e0-v=1e0 order by id;
select id from fp where 1e0=v+1e0 order by id;
select id from fp where v+1e0=1e0 or v is null order by id;
select id from fp where id>=2 and v+1e0=1e0 order by id;
prepare fp_predicate from 'select id from fp where v+cast(? as double)=cast(? as double) order by id';
set @one=1e0;
execute fp_predicate using @one,@one;
set @one=0;
execute fp_predicate using @one,@one;
set @one=null;
execute fp_predicate using @one,@one;
set @one=1e0;
execute fp_predicate using @one,@one;
deallocate prepare fp_predicate;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.fp');
set optimizer_hints='blockFilter=1';
select id from fp where v+1e0=1e0 order by id;
select id from fp where v-1e0=-1e0 order by id;
select id from fp where 1e0<=v order by id;
create table ov(v bigint);
insert into ov values (9223372036854775807);
select v from ov where v+1=9223372036854775807;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.ov');
select v from ov where v+1=9223372036854775807;
set optimizer_hints='blockFilter=2';
select v from ov where v+1=9223372036854775807;
select v,v+1 from ov;
truncate table ov;
insert into ov values (-9223372036854775808);
select v from ov where v-1=-9223372036854775808;
-- Column-only arithmetic has no constant-fold escape from metadata pruning.
create table persisted_overflow(v bigint primary key);
-- A matching PK row must not hide another row's arithmetic overflow.
insert into persisted_overflow values (3),(9223372036854775807);
select v from persisted_overflow where v+2=5;
-- @regex("data out of range: data type int64, ROUND",true)
select v from persisted_overflow where round(v,-1)=100;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.persisted_overflow');
set optimizer_hints='blockFilter=1';
select v from persisted_overflow where v+2=5;
-- @regex("data out of range: data type int64, ROUND",true)
select v from persisted_overflow where round(v,-1)=100;
set optimizer_hints='blockFilter=2';
-- @regex("data out of range: data type int64, ROUND",true)
select v from persisted_overflow where round(v,-1)=100;
set optimizer_hints='blockFilter=1';
select v from persisted_overflow where v+v=v;
select v from persisted_overflow where v*v=v;
select v from persisted_overflow where v-v=v;
select count(*) as healthy from persisted_overflow;
-- Fold support must not activate invalid quotient or decimal bounds.
-- @session:id=1{
use arithmetic_predicate_semantics;
set optimizer_hints='blockFilter=1';
create table reciprocal(v double);
insert into reciprocal values(-1),(.1),(1);
select v from reciprocal where 1/v>2;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.reciprocal');
select v from reciprocal where 1/v>2;
-- @regex("Analyze:",true)
explain (analyze true, check '["outputRows=1", "Block Filter Cond"]') select v from reciprocal where 1/v>2;
create table scaled_product(v decimal(16,8));
insert into scaled_product values(1);
select v from scaled_product where round(v*cast('0.00000001' as decimal(16,8)),8)=0.00000001;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.scaled_product');
select v from scaled_product where round(v*cast('0.00000001' as decimal(16,8)),8)=0.00000001;
-- @regex("Analyze:",true)
-- @regex("Block Filter Cond",false)
explain (analyze true, check '["outputRows=1"]') select v from scaled_product where round(v*cast('0.00000001' as decimal(16,8)),8)=0.00000001;
-- Independent column bounds contain an overflowing ROUND endpoint absent
-- from either actual row. Losing this proof must keep the matching row.
create table correlated(v bigint,w bigint);
insert into correlated values(9223372036854775707,0),(0,100);
select w from correlated where round(v+w,-1)=100;
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.correlated');
select w from correlated where round(v+w,-1)=100;
-- @regex("Analyze:",true)
-- @regex("Block Filter Cond",false)
explain (analyze true, check '["outputRows=1"]') select w from correlated where round(v+w,-1)=100;
set optimizer_hints='blockFilter=2';
select w from correlated where round(v+w,-1)=100;
-- @session}
set optimizer_hints=@saved_transpose_hints;
drop database arithmetic_predicate_semantics;
