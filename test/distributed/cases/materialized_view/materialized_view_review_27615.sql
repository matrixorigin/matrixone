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
