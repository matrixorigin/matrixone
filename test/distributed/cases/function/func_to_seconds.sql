select TO_SECONDS(null);
SELECT TO_SECONDS('19950501');
SELECT TO_SECONDS('2007-10-07');
SELECT TO_SECONDS('2008-10-07');
SELECT TO_SECONDS('0001-01-01');

drop table if exists t_dates;
create table t_dates(
a_date date,
b_datetime datetime,
c_timestamp timestamp
);

insert into t_dates values('1999-04-05', '1999-04-05 11:01:02', '1999-04-05 11:01:02');
insert into t_dates values('2004-04-03', '2004-04-03 13:11:10', '2004-04-03 13:11:10');
insert into t_dates values('2012-04-05', '2012-04-05 11:01:02', '2012-04-05 11:01:02.123456');
insert into t_dates values('1997-04-03', '1997-04-03 13:11:10', '1997-04-03 13:11:10.123456');
insert into t_dates values('2022-04-05', '2022-04-05 11:01:02.123456', '2022-04-05 11:01:02.123456');
insert into t_dates values('1999-09-05', '2004-09-03 13:11:10.123456', '2004-09-03 13:11:10.123456');
insert into t_dates values(null, null, null);
insert into t_dates values('', '', '');

select a_date, TO_SECONDS(a_date) from t_dates order by a_date;
select * from t_dates where TO_SECONDS(a_date) > 730214;

select b_datetime, TO_SECONDS(b_datetime) from t_dates order by b_datetime;
select * from t_dates where TO_SECONDS(b_datetime) > 730214;

drop table t_dates;