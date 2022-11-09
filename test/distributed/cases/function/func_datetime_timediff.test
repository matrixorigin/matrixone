#time
select timediff(cast('22:22:22' as time), cast('11:11:11' as time));
select timediff(cast('22:22:22' as time), cast('-11:11:11' as time));
select timediff(cast('-22:22:22' as time), cast('11:11:11' as time));
select timediff(cast('-22:22:22' as time), cast('-11:11:11' as time));
select timediff(cast('11:11:11' as time) , cast('22:22:22' as time));
select timediff(cast('11:11:11' as time) , cast('-22:22:22' as time));
select timediff(cast('-11:11:11' as time) , cast('22:22:22' as time));
select timediff(cast('-11:11:11' as time) , cast('-22:22:22' as time));
select timediff(cast('-838:59:59' as time) , cast('838:59:59' as time));
select timediff(cast('838:59:59' as time) , cast('-838:59:59' as time));
select timediff(cast('838:59:59' as time) , cast('838:59:59' as time));

#invalid time
select timediff(cast('22:22:22' as time), null);
select timediff(null, cast('11:11:11' as time));
select timediff(null, null);

#datetime
select timediff(CAST('2017-08-08 22:22:22' as datetime), CAST('2000-01-02 11:00:00' as datetime));
select timediff(CAST('2012-12-12 22:22:22' as datetime), CAST('2012-12-12 11:11:11' as datetime));
select timediff(CAST('2012-12-12 22:22:22' as datetime), CAST('2012-12-12 22:22:22' as datetime));
select timediff(CAST('2012-12-12 22:22:22' as datetime), CAST('2000-12-12 11:11:11' as datetime));
select timediff(CAST('2000-12-12 11:11:11' as datetime), CAST('2012-12-12 22:22:22' as datetime));
select timediff(CAST('2012-12-12 22:22:22' as datetime), CAST('2012-10-10 11:11:11' as datetime));
select timediff(CAST('2012-10-10 11:11:11' as datetime), CAST('2012-12-12 22:22:22' as datetime));
select timediff(CAST('2012-12-12 22:22:22' as datetime), CAST('2012-12-10 11:11:11' as datetime));
select timediff(CAST('2012-12-10 11:11:11' as datetime), CAST('2012-12-12 22:22:22' as datetime));
select timediff(CAST('2012-12-10 11:11:11' as datetime), CAST('2012-12-10 11:11:11' as datetime));

#invalid datetime
SELECT TIMEDIFF(CAST('2012-12-12 11:11:11' AS DATETIME), NULL);
SELECT TIMEDIFF(NULL, CAST('2012-12-12 11:11:11' AS DATETIME));
SELECT TIMEDIFF(NULL, NULL);

#different input types
select timediff(CAST('2017-08-08 22:22:22' as datetime), cast('11:11:11' as time));
select timediff(cast('11:11:11' as time), cast('2017-08-08 22:22:22' as datetime));
select timediff(CAST('2017-08-08 22:22:22' as datetime), 1);

drop table if exists t1;
create table t1(a INT,  b time);
insert into t1 values(1, '22:22:22');
insert into t1 values(2, '11:11:11');
insert into t1 values(3, '-22:22:22');
insert into t1 values(4, '-11:11:11');
insert into t1 values(5, '838:59:59');
insert into t1 values(6, '-838:59:59');
insert into t1 values(7, '00:00:00');
select a, timediff(cast('22:22:22' as time), b) from t1;
select a, timediff(b, cast('22:22:22' as time)) from t1;
drop table t1;

drop table if exists t2;
create table t2(a INT,  b datetime);
insert into t2 values(1, '2012-12-12 23:22:22');
insert into t2 values(2, '2012-12-10 11:11:11');
insert into t2 values(3, '2012-12-14 11:11:11');
insert into t2 values(4, '2012-12-20 11:11:11');
insert into t2 values(2, '2012-10-10 11:11:11');
select a, timediff(cast('2012-12-12 22:22:21' as datetime), b) from t2;
select a, timediff(b, cast('2012-12-12 22:22:21' as datetime)) from t2;
drop table t2;

select timediff('20',NULL);
select timediff(NULL,'24:59:09');
select timediff('20','24:59:09');
select timediff('12:00','24:59:09');
select timediff('-838:59:59','-1122');
select timediff('12:00:00','24:59:09');

drop table if exists  time_01;
create table time_01(t1 time,t2 time,t3 time);
insert into time_01 values("-838:59:59.0000","838:59:59.00","22:00:00");
insert into time_01 values("0:00:00.0000","0","0:00");
insert into time_01 values(null,NULL,null);
insert into time_01 values("23","1122","-1122");
insert into time_01 values("101412","4","-101219");
insert into time_01 values("24:59:09.932823","24:02:00.93282332424","24:20:34.00000000");
insert into time_01 values("2022-09-08 12:00:01","019","23403");
select timediff(t1,t2) from time_01;
drop table time_01;