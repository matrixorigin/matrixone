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
insert into persisted_overflow values (9223372036854775807);
-- @ignore:0
select mo_ctl('dn','flush','arithmetic_predicate_semantics.persisted_overflow');
set optimizer_hints='blockFilter=1';
select v from persisted_overflow where v+v=v;
select v from persisted_overflow where v*v=v;
select v from persisted_overflow where v-v=v;
select count(*) as healthy from persisted_overflow;
set optimizer_hints=@saved_transpose_hints;
drop database arithmetic_predicate_semantics;
