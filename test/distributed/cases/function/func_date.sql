-- @suite
-- @setup
drop table if exists t1;
create table t1 (id int,d date, dt datetime,c char(10),vc varchar(20));
insert into t1 values (1,"2021-01-13", "2021-01-13 13:00:00", "2021-12-15", "2021-12-16");
insert into t1 values (1,"2021-01-31", "2021-01-31 13:00:00", "2021-12-15", "2021-12-16");
insert into t1 values (2,"2022-02-15", "2022-02-15 18:54:29", "2021-02-15", "2021-02-15");
insert into t1 values (2,"2022-02-28", "2022-02-28 18:54:29", "2021-02-15", "2021-02-15");
insert into t1 values (3,"2000-02-29", "2000-02-29 18:54:29", "2021-02-15", "2021-02-15");
insert into t1 values (4,"2023-03-17", "2021-02-17 23:54:59", "2021-03-17", "2021-03-17");
insert into t1 values (5,"1985-04-18", "1985-04-18 00:00:01", "1985-04-18", "1985-04-18");
insert into t1 values (6,"1987-05-20", "1987-05-20 22:59:59", "1987-05-20", "1987-05-20");
insert into t1 values (7,"1989-06-22", "1989-06-22 15:00:30", "1989-06-22", "1989-06-22");
insert into t1 values (8,"1993-07-25", "1987-07-25 03:04:59", "1993-07-25", "1993-07-25");
insert into t1 values (9,"1995-08-27", "1987-08-27 04:32:33", "1995-08-27", "1995-08-27");
insert into t1 values (10,"1999-09-30", "1999-09-30 10:11:12", "1999-09-30", "1999-09-30");
insert into t1 values (11,"2005-10-30", "2005-10-30 18:18:59", "2005-10-30", "2005-10-30");
insert into t1 values (12,"2008-11-30", "2008-11-30 22:59:59", "2008-11-30", "2008-11-30");
insert into t1 values (13,"2013-12-01", "2013-12-01 22:59:59", "2013-12-01", "2013-12-01");
insert into t1 values (14,null, null, null, null);

-- @case
-- @desc:test for month func
-- @label:bvt
select month(d),month(dt) from t1;
select month(c),month(vc) from t1;

-- @case
-- @desc:test for weekday func
-- @label:bvt
select weekday(d),weekday(dt) from t1;


select weekday(c),weekday(vc) from t1;

-- select week(d),week(dt) from t1;
-- select day(d),day(dt) from t1;
-- select dayofmonth(d),dayofmonth(dt) from t1;
-- select date(c),date(vc) from t1;
-- select dayofyear(d),dayofyear(dt) from t1;
-- select hour(d),hour(dt) from t1;
-- select minute(d),minute(dt) from t1;
-- select second(d),second(dt) from t1;

-- @case
-- @desc: test for month with func max,min,etc
-- @label:bvt
select max(month(d)),max(month(dt)) from t1;
select min(month(d)),min(month(d)) from t1;
select avg(month(d)),avg(month(d)) from t1;

select sum(month(d)),sum(month(d)) from t1;

-- @case
-- @desc: test for weekday with func max,min,etc
-- @label:bvt
select max(weekday(d)),max(weekday(dt)) from t1;
select min(weekday(d)),min(weekday(d)) from t1;
select avg(weekday(d)),avg(weekday(d)) from t1;
select sum(weekday(d)),sum(weekday(d)) from t1;

-- @case
-- @desc: test for month with distinct
-- @label:bvt
select distinct(month(d)) from t1;

-- @case
-- @desc: test for month with operators
-- @label:bvt
select id,c,vc from t1 where month(d) > 2;
select id,c,vc from t1 where month(d) < 3;
select id,c,vc from t1 where month(d) <> 4;

drop table t1;
