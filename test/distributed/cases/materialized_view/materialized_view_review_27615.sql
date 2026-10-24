drop database if exists mv_review_27615;
drop database if exists mv_review_27615_other;
create database mv_review_27615;
create database mv_review_27615_other;
use mv_review_27615;

-- A repeated unqualified source must retain its CREATE-time database in
-- every refresh-SQL table reference, even with a same-name source elsewhere.
create table src(id int primary key);
create table mv_review_27615_other.src(id int primary key);
insert into src values (1);
insert into mv_review_27615_other.src values (1);
create materialized view self_join refresh complete on demand as
select a.id from src a join src b on a.id=b.id
join mv_review_27615_other.src c on a.id=c.id;
refresh materialized view self_join;
select count(*)=1 and min(id)=1 as refresh_bound_to_defining_database from self_join;

-- Partial restore recreates the materialized view before a VIEW using it.
create view dependent as select id from self_join;
drop snapshot if exists mv_review_27615_s;
create snapshot mv_review_27615_s for account;
drop view dependent;
drop materialized view self_join;
restore database mv_review_27615 {snapshot='mv_review_27615_s'};
-- Restore recreates an ON DEMAND definition; refresh publishes its data.
refresh materialized view self_join;
select count(*)=1 and min(id)=1 as restored_materialized_dependency from dependent;
drop view dependent;
drop materialized view self_join;
restore table mv_review_27615.self_join {snapshot='mv_review_27615_s'};
refresh materialized view self_join;
select count(*)=1 and min(id)=1 as restored_materialized_table from self_join;
drop snapshot mv_review_27615_s;

-- Both AVG state variants must store SUM(5 * 2) and SUM(DISTINCT 8, 9).
create table decimal_source(id int primary key, g int, v decimal(1,0));
insert into decimal_source values (1,1,2),(2,1,2),(3,1,2),(4,1,2),(5,1,2),(6,2,8),(7,2,9);
create materialized view decimal_avg refresh fast on change as
select g, avg(v) a, avg(distinct v) ad from decimal_source group by g;
-- @wait_expect(2, 30)
select sum(a=2)=1 and sum(ad=8.5)=1 as avg_state_has_sum_capacity from decimal_avg;
drop materialized view decimal_avg;

-- SQL groups and DISTINCT compare floating +0 and -0 as equal. FAST must
-- refuse byte-serialized floating keys until its state uses SQL equality.
create table floating_source(id int primary key, g int, k double);
insert into floating_source values (1, 1, cast('-0' as double));
create materialized view bad_group refresh fast on change as
select k, count(*) c from floating_source group by k;
create materialized view bad_distinct refresh fast on change as
select g, count(distinct k) c from floating_source group by g;
create materialized view full_control refresh complete on demand as
select k, count(distinct k) c from floating_source group by k;
refresh materialized view full_control;
insert into floating_source values (2, 1, cast('0' as double));
refresh materialized view full_control;
select count(*)=1 and sum(c)=1 as signed_zero_uses_sql_equality from full_control;

drop materialized view full_control;
drop materialized view self_join;
drop database mv_review_27615_other;
drop database mv_review_27615;
