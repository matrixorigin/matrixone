drop database if exists test;
create database test;
use test;

create table t(a int, b int, c varchar);
begin;
insert into t select *,*,"abcdefghijklmnopqrst" from generate_series(1, 8192000)g;
delete from t where a mod 999 = 0 limit 1000;
select count(*) + 1000 from t;
rollback;

select enable_fault_injection();
select add_fault_point('fj/log/workspace',':::','echo',40,'test.t');
begin;
insert into t select *,*,"abcdefghijklmnopqrst" from generate_series(1, 8192000)g;
delete from t where a mod 999 = 0 limit 1000;
select count(*) + 1000 from t;
rollback;
select disable_fault_injection();

drop table if exists t;
drop database if exists test;