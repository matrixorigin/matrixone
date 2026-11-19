-- issue #32125: native BIT input to the five value windows (LAG/LEAD/
-- FIRST_VALUE/LAST_VALUE/NTH_VALUE) derives the unsigned integer result
-- domain (MySQL), not the BIT domain.
drop database if exists issue_32125;
create database issue_32125;
use issue_32125;
create table t(id int primary key, b bit(64));
insert into t values(1,258),(2,255),(3,0),(4,null),(5,18446744073709551615);
select id, length(a), length(z), length(l), length(r), length(n) from (
  select id,
    first_value(b) over (order by id) a,
    last_value(b) over (order by id rows between unbounded preceding and unbounded following) z,
    lag(b) over (order by id) l,
    lead(b) over (order by id) r,
    nth_value(b, 2) over (order by id rows between unbounded preceding and unbounded following) n
  from t) s order by id;
select id, a, z, l, r, n from (
  select id,
    first_value(b) over (order by id) a,
    last_value(b) over (order by id rows between unbounded preceding and unbounded following) z,
    lag(b) over (order by id) l,
    lead(b) over (order by id) r,
    nth_value(b, 2) over (order by id rows between unbounded preceding and unbounded following) n
  from t) s order by id;
create table c_wi as select id, first_value(b) over (order by id) b from t;
select column_name, data_type, column_type from information_schema.columns where table_schema = 'issue_32125' and table_name = 'c_wi' and column_name = 'b';
select id, length(b), b+0 from c_wi order by id;
create view v_wi as select id, first_value(b) over (order by id) b from t;
select id, length(b), b+0 from v_wi order by id;
select id, length(a), length(z), length(l), length(r), length(n) from (
  select id,
    first_value(cast(b as unsigned)) over (order by id) a,
    last_value(cast(b as unsigned)) over (order by id rows between unbounded preceding and unbounded following) z,
    lag(cast(b as unsigned)) over (order by id) l,
    lead(cast(b as unsigned)) over (order by id) r,
    nth_value(cast(b as unsigned), 2) over (order by id rows between unbounded preceding and unbounded following) n
  from t) s order by id;
select id, lag(b, 1, 255) over (order by id), lead(b, 1, 0) over (order by id), lag(b, 1, b) over (order by id) from t order by id;
create table ci(id int primary key, i bigint, v varchar(10));
insert into ci values (1, 258, 'aa'), (2, 255, 'bb'), (3, 0, 'cc');
select id, first_value(i) over (order by id) fi, lag(v) over (order by id) lv from ci order by id;
drop database issue_32125;
