-- test pushed down fake_pk_col `in` filter won't panic when decoding bloom filter, see #15118
drop database if exists testdb;
create database testdb;
use testdb;
create table hhh (a int, b int, index(`b`));
insert into hhh select *,* from generate_series(1, 81920)g;
select a from hhh where b = 999;
drop table hhh;

select enable_fault_injection();

create table t2(a int primary key);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'testdb.t2');
insert into t2 select * from generate_series(1,81920)g;
select a from t2 where a in (1,2) or a in (10000,10001) or a in (20000,20001) or a in (40000,40001);
drop table t2;
drop database testdb;
select disable_fault_injection();