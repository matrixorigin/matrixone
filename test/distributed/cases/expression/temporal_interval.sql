-- @suite

-- @case
-- @desc:test for temporal interval unit with data_add
-- @label:bvt
select date_add("1997-12-31 23:59:59.000002",INTERVAL "10000 99:99:99.999999" DAY_MICROSECOND);
select date_add("1997-12-31 23:59:59.000002",INTERVAL "10000:99:99.999999" HOUR_MICROSECOND);
select date_add("1997-12-31 23:59:59.000002",INTERVAL "10000:99.999999" MINUTE_MICROSECOND);
select date_add("1997-12-31 23:59:59.000002",INTERVAL "10000.999999" SECOND_MICROSECOND);
select date_add("1997-12-31 23:59:59.000002",INTERVAL "999999" MICROSECOND);
select date_add("1997-12-31 23:59:59",INTERVAL 1 SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL 1 MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL 1 HOUR);
select date_add("1997-12-31 23:59:59",INTERVAL 1 DAY);
select date_add("1997-12-31 23:59:59",INTERVAL 0 DAY);
select date_add("1997-12-31 23:59:59",INTERVAL 1 MONTH);
select date_add("1997-12-31 23:59:59",INTERVAL 1 QUARTER);
select date_add("1997-12-31 23:59:59",INTERVAL 1 YEAR);
select date_add("1997-12-31 23:59:59",INTERVAL "1:1" MINUTE_SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL "1:1" HOUR_MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL "1:1" DAY_HOUR);
select date_add("1997-12-31 23:59:59",INTERVAL "1 1" YEAR_MONTH);
select date_add("1997-12-31 23:59:59",INTERVAL "1:1:1" HOUR_SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL "1 1:1" DAY_MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL "1 1:1:1" DAY_SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL 100000 SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL -100000 MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL 100000 HOUR);
select date_add("1997-12-31 23:59:59",INTERVAL -100000 DAY);
select date_add("1997-12-31 23:59:59",INTERVAL 100000 MONTH);
select date_add("1997-12-31 23:59:59",INTERVAL 100000 QUARTER);
select date_add("1997-12-31 23:59:59",INTERVAL -100000 YEAR);
select date_add("1997-12-31 23:59:59",INTERVAL "10000:1" MINUTE_SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL "-10000:1" HOUR_MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL "10000:1" DAY_HOUR);
select date_add("1997-12-31 23:59:59",INTERVAL "-100 1" YEAR_MONTH);
select date_add("1997-12-31 23:59:59",INTERVAL "10000:99:99" HOUR_SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL " -10000 99:99" DAY_MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL "10000 99:99:99" DAY_SECOND);
select date_add("1997-12-31",INTERVAL 1 SECOND);
select date_add("1997-12-31",INTERVAL 1 DAY);
select date_add(NULL,INTERVAL 100000 SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL NULL SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL NULL MINUTE_SECOND);
select date_add("9999-12-31 23:59:59",INTERVAL 1 SECOND);
select date_sub("0000-00-00 00:00:00",INTERVAL 1 SECOND);
select date_add('1998-01-30',Interval 1 month);
select date_add('1998-01-30',Interval '2:1' year_month);
select date_add('1996-02-29',Interval '1' year);
select date_add('1996-02-29',Interval q year);
select date_add("1997-12-31 23:59:59",INTERVAL 1.5 SECOND);
select date_add("1997-12-31 23:59:59",INTERVAL 1.5 MINUTE);
select date_add("1997-12-31 23:59:59",INTERVAL 1.5 HOUR);
select date_add("1997-12-31 23:59:59",INTERVAL 1.5 DAY);
select date_add("1997-12-31 23:59:59",INTERVAL 1.5 ABC);

-- @case
-- @desc:test for temporal interval unit with data_sub
-- @label:bvt

select date_sub("1998-01-01 00:00:00",INTERVAL 1 SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 DAY);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 MONTH);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 QUARTER);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 YEAR);
select date_sub("1998-01-01 00:00:00",INTERVAL 100000 SECOND);
select date_sub("1998-01-01 00:00:009",INTERVAL -100000 MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL 100000 HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL 0 HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL -100000 DAY);
select date_sub("1998-01-01 00:00:00",INTERVAL 100000 MONTH);
select date_sub("1998-01-01 00:00:00",INTERVAL 100000 QUARTER);
select date_sub("1998-01-01 00:00:00",INTERVAL -100000 YEAR);
select date_sub("1998-01-01 00:00:00",INTERVAL "1:1" MINUTE_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL "1:1" HOUR_MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL "1:1" DAY_HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL "1 1" YEAR_MONTH);
select date_sub("1998-01-01 00:00:00",INTERVAL "1:1:1" HOUR_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL "1 1:1" DAY_MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL "1 1:1:1" DAY_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL "10000:1" MINUTE_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL "-10000:1" HOUR_MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL "10000:1" DAY_HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL "-100 1" YEAR_MONTH);
select date_sub("1998-01-01 00:00:00",INTERVAL "10000:99:99" HOUR_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL " -10000 99:99" DAY_MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL "10000 99:99:99" DAY_SECOND);
select date_sub("1998-01-01 00:00:00.000001",INTERVAL "1 1:1:1.000002" DAY_MICROSECOND);
select date_sub("1998-01-01 00:00:00.000001",INTERVAL "1:1:1.000002" HOUR_MICROSECOND);
select date_sub("1998-01-01 00:00:00.000001",INTERVAL "1:1.000002" MINUTE_MICROSECOND);
select date_sub("1998-01-01 00:00:00.000001",INTERVAL "1.000002" SECOND_MICROSECOND);
select date_sub("1998-01-01 00:00:00.000001",INTERVAL "000002" MICROSECOND);
select date_sub("1998-01-01",INTERVAL 1 SECOND);
select date_sub("1998-01-01",INTERVAL 1 DAY);
select date_sub(NULL,INTERVAL 100000 SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL NULL SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL NULL MINUTE_SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 SECOND);
select date_sub("0000-00-00 00:00:00",INTERVAL 1 SECOND);
select date_add('1998-01-30',Interval 1 month);
select date_sub('1998-02-01',Interval '2:1' year_month);
select date_sub('1996-02-29',Interval '1' year);
select date_add('1996-02-29',Interval a year);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 SECOND);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 MINUTE);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 HOUR);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 DAY);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 MONTH);
select date_sub("1998-01-01 00:00:00",INTERVAL 1.5 QUARTER);
select date_sub("1998-01-01 00:00:00",INTERVAL 1 ABC);

select date_sub(NULL,INTERVAL 100000 SECOND);
select date_sub("1998-01-02",INTERVAL 31 DAY);

-- @case
-- @desc:test for temporal interval unit with timestamp_sub
-- @label:bvt
-- select TIMESTAMPADD(MINUTE,8820,'2012-08-24 09:00:00');

-- @case
-- @desc:test for temporal interval unit with expression+-
-- @label:bvt
select "1997-12-31 23:59:59" + INTERVAL 1 SECOND;
select INTERVAL 1 DAY + "1997-12-31";
select "1998-01-01 00:00:00" - INTERVAL 1 SECOND;
SELECT "1900-01-01 00:00:00" + INTERVAL 2147483648 SECOND;
SELECT "1900-01-01 00:00:00" + INTERVAL "1:2147483647" MINUTE_SECOND;
SELECT "1900-01-01 00:00:00" + INTERVAL "100000000:214748364700" MINUTE_SECOND;

SELECT "1900-01-01 00:00:00" + INTERVAL 1<<37 SECOND;
SELECT "1900-01-01 00:00:00" + INTERVAL 1<<31 MINUTE;
SELECT "1900-01-01 00:00:00" + INTERVAL 1<<20 HOUR;
SELECT "1900-01-01 00:00:00" + INTERVAL 1<<38 SECOND;
SELECT "1900-01-01 00:00:00" + INTERVAL 1<<33 MINUTE;
SELECT "1900-01-01 00:00:00" + INTERVAL 1<<30 HOUR;

SELECT "1900-01-01 00:00:00" + INTERVAL "1000000000:214748364700" MINUTE_SECOND;

SELECT "1997-12-31 23:59:59" + INTERVAL 1 SECOND;

-- @case
-- @desc:test for temporal interval with date,datetime,char,varchar column
-- @label:bvt

create table t1(i int,a date,b date,c datetime,d char(20),e varchar(50));
insert into t1 values(1,"1997-12-31","1997-12-31","1997-12-31 23:59:59.000002","1997-12-31 23:59:59","1997-12-31 23:59:59.000002");
insert into t1 values(2,"1998-01-01","1998-01-01","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t1 values(3,NULL,NULL,NULL,NULL,NULL);
select date_add(a,INTERVAL 1 SECOND), date_add(b,INTERVAL 1 MINUTE), date_add(c,INTERVAL 1 HOUR), date_add(d,INTERVAL 1 MONTH), date_add(e,INTERVAL 1 QUARTER) from t1;
select date_add(a,INTERVAL 1 YEAR), date_add(b,"1:1" MINUTE_SECOND), date_add(c,INTERVAL "1:1" HOUR_MINUTE), date_add(d,INTERVAL "1:1" DAY_HOUR), date_add(e,INTERVAL "1 1" YEAR_MONTH) from t1;
select date_add(a,"1:1:1" HOUR_SECOND), date_add(b,"1:1:1" HOUR_SECOND), date_add(c,INTERVAL "1 1:1" DAY_MINUTE), date_add(d,INTERVAL "1 1:1:1" DAY_SECOND), date_add(e,INTERVAL "1 1" YEAR_MONTH) from t1;
select date_sub(a,INTERVAL 1 SECOND), date_sub(b,INTERVAL 1 MINUTE), date_sub(c,INTERVAL 1 HOUR), date_sub(d,INTERVAL 1 MONTH), date_sub(e,INTERVAL 1 QUARTER) from t1;
select date_sub(a,INTERVAL 1 YEAR), date_sub(b,INTERVAL "1:1" MINUTE_SECOND), date_sub(c,INTERVAL "1:1" HOUR_MINUTE), date_sub(d,INTERVAL "1:1" DAY_HOUR), date_sub(e,INTERVAL "1 1" YEAR_MONTH) from t1;
select date_sub(a,INTERVAL "1:1:1" HOUR_SECOND), date_sub(b,INTERVAL "1:1:1" HOUR_SECOND), date_sub(c,INTERVAL "1 1:1" DAY_MINUTE), date_sub(d,INTERVAL "1 1:1:1" DAY_SECOND), date_sub(e,INTERVAL "1 1" YEAR_MONTH) from t1;
select a + INTERVAL 1 SECOND,b + INTERVAL 1 MINUTE,c + INTERVAL 1 HOUR from t1;
select a - INTERVAL 1 SECOND,b - INTERVAL 1 MINUTE,c - INTERVAL 1 HOUR from t1;
select i + INTERVAL 1 SECOND from t1;

-- @case
-- @desc:test for temporal interval with month,weekday,week,date,dayofyear,hour,minute,second,cast
-- @label:bvt
select month(date_sub("1998-01-01 00:00:00",INTERVAL 1 SECOND));
select weekday(date_sub("1998-01-01 00:00:00",INTERVAL 1 MINUTE));
select date(date_sub("1998-01-01 00:00:00",INTERVAL 1 DAY));
select dayofyear(date_sub("1998-01-01 00:00:00",INTERVAL 1 MONTH));

select month(date_add("1997-12-31 23:59:59",INTERVAL "1:1" MINUTE_SECOND));

select weekday(date_add("1997-12-31 23:59:59",INTERVAL "1:1" HOUR_MINUTE));
select date(date_add("1997-12-31 23:59:59",INTERVAL "1 1" YEAR_MONTH));
select dayofyear(date_add("1997-12-31 23:59:59",INTERVAL "1:1:1" HOUR_SECOND));

select date("1997-12-31 23:59:59" + INTERVAL 1 SECOND) + INTERVAL "1:1:1" HOUR_SECOND;


SELECT CAST(CAST('2006-08-10 10:11:12' AS DATETIME) AS DECIMAL(20,6));
SELECT CAST(CAST('2006-08-10 10:11:12' AS DATETIME) + INTERVAL 14 MICROSECOND AS DECIMAL(20,6));

-- @case
-- @desc:test for temporal interval with insert,update
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1(i int,a datetime,b datetime,c datetime,d char(200),e varchar(50));
create table t2(i int,a datetime,b datetime,c datetime,d char(200),e varchar(50));
insert into t1 select 1,"1997-12-30" + INTERVAL 1 SECOND ,"1997-12-31 23:59:59" + INTERVAL 1 MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL 1 HOUR,"1997-12-31 23:59:59" + INTERVAL 1 DAY,"1997-12-31 23:59:59.000002" + INTERVAL 1 MONTH;
insert into t1 select 2,"1997-12-30" + INTERVAL 1 YEAR,"1997-12-31 23:59:59" + INTERVAL "1:1" MINUTE_SECOND,"1997-12-31 23:59:59.000002" + INTERVAL "1:1" DAY_HOUR,"1997-12-31 23:59:59" + INTERVAL "1 1" YEAR_MONTH,"1997-12-31 23:59:59.000002" + INTERVAL "1:1:1" HOUR_SECOND;
insert into t1 select 3,"1997-12-30" + INTERVAL "1:1:1" HOUR_SECOND,"1997-12-31 23:59:59" + INTERVAL "1 1:1" DAY_MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL "1 1:1:1" DAY_SECOND,"1997-12-31 23:59:59","1997-12-31 23:59:59.000002";
insert into t1 select 4,"1997-12-30" + INTERVAL 1 SECOND ,"1997-12-31 23:59:59" + INTERVAL 1 MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL 1 HOUR,"1997-12-31 23:59:59" + INTERVAL 1 DAY,"1997-12-31 23:59:59.000002" + INTERVAL 1 MONTH;
insert into t1 select 5,"1997-12-30" + INTERVAL 1 YEAR,"1997-12-31 23:59:59" + INTERVAL "1:1" MINUTE_SECOND,"1997-12-31 23:59:59.000002" + INTERVAL "1:1" DAY_HOUR,"1997-12-31 23:59:59" + INTERVAL "1 1" YEAR_MONTH,"1997-12-31 23:59:59.000002" + INTERVAL "1:1:1" HOUR_SECOND;
insert into t1 select 6,"1997-12-30" + INTERVAL "1:1:1" HOUR_SECOND,"1997-12-31 23:59:59" + INTERVAL "1 1:1" DAY_MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL "1 1:1:1" DAY_SECOND,"1997-12-31 23:59:59","1997-12-31 23:59:59.000002";

insert into t1 values(7,"1998-01-01","1998-01-01","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t1 values(8,"1998-01-01","1998-01-01","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t1 values(9,"1998-01-01","1998-01-01","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t1 select 10,"2010-11-12" + interval 14 microsecond,"1998-01-01 00:00:00","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002";
insert into t1 values(11,NULL,NULL,NULL,NULL,NULL);

insert into t2 select 1,"1997-12-30" + INTERVAL 1 SECOND ,"1997-12-31 23:59:59" + INTERVAL 1 MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL 1 HOUR,"1997-12-31 23:59:59" + INTERVAL 1 DAY,"1997-12-31 23:59:59.000002" + INTERVAL 1 MONTH;
insert into t2 select 2,"1997-12-30" + INTERVAL 1 YEAR,"1997-12-31 23:59:59" + INTERVAL "1:1" MINUTE_SECOND,"1997-12-31 23:59:59.000002" + INTERVAL "1:1" DAY_HOUR,"1997-12-31 23:59:59" + INTERVAL "1 1" YEAR_MONTH,"1997-12-31 23:59:59.000002" + INTERVAL "1:1:1" HOUR_SECOND;
insert into t2 select 3,"1997-12-30" + INTERVAL "1:1:1" HOUR_SECOND,"1997-12-31 23:59:59" + INTERVAL "1 1:1" DAY_MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL "1 1:1:1" DAY_SECOND,"1997-12-31 23:59:59","1997-12-31 23:59:59.000002";
insert into t2 select 4,"1997-12-30" + INTERVAL 1 SECOND ,"1997-12-31 23:59:59" + INTERVAL 1 MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL 1 HOUR,"1997-12-31 23:59:59" + INTERVAL 1 DAY,"1997-12-31 23:59:59.000002" + INTERVAL 1 MONTH;
insert into t2 select 5,"1997-12-30" + INTERVAL 1 YEAR,"1997-12-31 23:59:59" + INTERVAL "1:1" MINUTE_SECOND,"1997-12-31 23:59:59.000002" + INTERVAL "1:1" DAY_HOUR,"1997-12-31 23:59:59" + INTERVAL "1 1" YEAR_MONTH,"1997-12-31 23:59:59.000002" + INTERVAL "1:1:1" HOUR_SECOND;
insert into t2 select 6,"1997-12-30" + INTERVAL "1:1:1" HOUR_SECOND,"1997-12-31 23:59:59" + INTERVAL "1 1:1" DAY_MINUTE,"1997-12-31 23:59:59.000002" + INTERVAL "1 1:1:1" DAY_SECOND,"1997-12-31 23:59:59","1997-12-31 23:59:59.000002";

insert into t2 values(7,"1998-01-01","1998-01-01 00:00:00","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t2 values(8,"1998-01-01","1998-01-01 00:00:00","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t2 values(9,"1998-01-01","1998-01-01 00:00:00","1998-01-01 00:00:00.000001","1998-01-01 00:00:00","1997-12-31 23:59:59.000002");
insert into t2 values(10,NULL,NULL,NULL,NULL,NULL);

-- @bvt:issue#3254
select * from t1 where a = "1997-12-29" + INTERVAL 1 DAY;
select * from t1 where a > "1997-12-29" + INTERVAL 1 DAY;
select * from t1 where (a + INTERVAL 1 DAY) > "1997-12-31";
select * from t1 where (a + INTERVAL 1 DAY) <> ("1997-12-30" + INTERVAL 1 DAY);
-- @bvt:issue

select date_add(b,INTERVAL 1 DAY),date_add(c,INTERVAL 1 SECOND) from t1;

-- @bvt:issue#3254
select distinct(a) from t1 where c > "1998-01-01 00:59:59";
-- @bvt:issue

select count(a),c + INTERVAL 1 DAY as c1 from t1 group by (c + INTERVAL 1 DAY) having c1 > "1998-01-01 00:59:59";

-- @bvt:issue#3254
select i,c + INTERVAL 1 MINUTE from t1 where a - INTERVAL 1 SECOND  > "1997-01-01 00:00:00.000001" order by c + INTERVAL 1 MINUTE DESC;
select i,c + INTERVAL 1 MINUTE from t1 where a - INTERVAL 1 SECOND  > "1997-01-01 00:00:00.000001" order by c + INTERVAL 1 MINUTE ASC;
-- @bvt:issue

select t1.i,t2.i,t1.c + INTERVAL 1 MINUTE,t2.b + INTERVAL 1 YEAR from t1 join t2 where (t1.a + INTERVAL 1 DAY) = (t2.c -INTERVAL 1 DAY );


select '2007-01-01' + interval i day from t2;
select b + interval i day from t2;

update t1 set c = c + INTERVAL 1 DAY where i > 6;
-- @bvt:issue#10895
select * from t1 where i > 6;
-- @bvt:issue
drop table if exists t1;
drop table if exists t2;

-- @case
-- @desc:test for temporal interval with between and
-- @label:bvt
drop table if exists t1;
CREATE TABLE t1 ( datum DATE );

INSERT INTO t1 VALUES ( "2000-1-1" );
INSERT INTO t1 VALUES ( "2000-1-2" );
INSERT INTO t1 VALUES ( "2000-1-3" );
INSERT INTO t1 VALUES ( "2000-1-4" );
INSERT INTO t1 VALUES ( "2000-1-5" );
SELECT * FROM t1 WHERE datum BETWEEN cast("2000-1-2" as date) AND cast("2000-1-4" as date);
SELECT * FROM t1 WHERE datum BETWEEN cast("2000-1-2" as date) AND datum - INTERVAL 100 DAY;


-- @case
-- @desc:test for temporal interval with cast
-- @label:bvt
SELECT CAST('2006-09-26' AS DATE) + INTERVAL 1 DAY;
SELECT CAST('2006-09-26' AS DATE) + INTERVAL 1 MONTH;
SELECT CAST('2006-09-26' AS DATE) + INTERVAL 1 YEAR;
SELECT CAST('2006-09-26' AS DATE) + INTERVAL 1 WEEK;

-- @case
-- @desc:test for temporal interval with invalid date turned to NULL from date_sub/date_add
-- @label:bvt
drop table if exists t1;
create table t1 (d date);
insert into t1 (d) select date_sub('2000-01-01', INTERVAL 2001 YEAR);
insert into t1 (d) select date_add('2000-01-01',interval 8000 year);
insert into t1 select date_add(NULL, INTERVAL 1 DAY);
insert into t1 select date_add('2000-01-04', INTERVAL NULL DAY);
insert into t1 (d) select date_sub('2000-01-01', INTERVAL 2001 YEAR);
insert into t1 (d) select date_add('2000-01-01',interval 8000 year);
insert into t1 select date_add(NULL, INTERVAL 1 DAY);
insert into t1 select date_add('2000-01-04', INTERVAL NULL DAY);
select * from t1;
drop table t1;


set @tt=now();
-- @ignore:0
select @tt;
-- @ignore:0
select date_add(@tt, Interval 30 SECOND);
-- @ignore:0
select date_sub(@tt, Interval 30 SECOND);
