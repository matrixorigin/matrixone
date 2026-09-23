-- The corresponding CN drain timing is exercised by the multi-CN integration
-- test. This BVT verifies only the committed and rolled-back SQL outcomes.
drop database if exists cn_safe_drain_dml;
create database cn_safe_drain_dml;
use cn_safe_drain_dml;

create table t (id int primary key, value int);
insert into t values (1, 10), (2, 20);

begin;
insert into t values (3, 30);
update t set value = 11 where id = 1;
delete from t where id = 2;
commit;
select id, value from t order by id;

begin;
insert into t values (4, 40);
update t set value = 99 where id = 1;
delete from t where id = 3;
rollback;
select id, value from t order by id;

drop database cn_safe_drain_dml;
