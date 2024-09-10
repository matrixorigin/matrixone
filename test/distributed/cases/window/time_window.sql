drop database if exists time_window;
create database time_window;
use time_window;

-- abnormal test: the time column is not primary key column
drop table if exists time_window01;
create table time_window01 (ts timestamp, col2 int);
insert into time_window01 values ('2021-01-12 00:00:00.000', 12);
insert into time_window01 values ('2020-01-12 12:00:12.000', 24);
insert into time_window01 values ('2021-01-12 00:00:00.000', 34);
insert into time_window01 values ('2020-01-12 12:00:12.000', 20);
select * from time_window01;
select _wstart, _wend, max(col2), min(col2) from time_window01 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
drop table time_window01;

drop table if exists time_window02;
create table time_window02 (ts timestamp primary key , col2 bool);
insert into time_window02 values ('2023-10-26 10:00:00.000', false);
insert into time_window02 values ('2023-10-26 10:10:00.000', true);
insert into time_window02 values ('2023-10-26 10:20:00.000', null);
insert into time_window02 values ('2023-10-26 10:30:00.000', true);
select * from time_window02;
select _wstart, _wend, max(col2), min(col2) from time_window02 where ts > '2020-01-11 12:00:12.000' and ts < '2024-01-13 00:00:00.000' interval(ts, 10, second) fill(prev);
drop table time_window02;

-- abnormal test: the primary key column of time window is not timestamp
drop table if exists time_window03;
create table time_window03 (ts datetime primary key , col2 int);
insert into time_window03 values ('2021-01-12 00:00:00', 12);
insert into time_window03 values ('2020-01-12 12:00:12', 24);
insert into time_window03 values ('2021-01-14 00:00:00', 34);
insert into time_window03 values ('2020-01-16 12:00:12', 20);
select * from time_window03;
select _wstart, _wend, max(col2), min(col2) from time_window03 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
drop table time_window03;

-- abnormal test: the aggr column is char/varchar/text
drop table if exists time_window04;
create table time_window04 (ts timestamp primary key, col2 char, col3 varchar(10), col4 text);
insert into time_window04 values ('2023-10-26 10:00:00.000', 'a', 'b', 'djiweijwfcjwefwq');
insert into time_window04 values ('2020-01-12 12:00:12.000', '1', '2', 'efwq3232e数据库系统');
insert into time_window04 values ('2021-01-12 00:00:00.000', '是', 'srewrew', null);
insert into time_window04 values ('2023-10-26 10:30:00.000', 'w', null, null);
select * from time_window04;
select _wstart, _wend, max(col2), min(col3), max() from time_window04 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
drop table time_window04;

-- abnormal test: the time column is date
drop table if exists time_window05;
create table time_window05 (ts datetime primary key , col2 decimal);
insert into time_window05 values ('2022-10-10', 4324.43423);
insert into time_window05 values ('2022-10-12', -4324.43423);
select * from time_window05;
select _wstart, _wend, max(col2) from time_window05 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
drop table time_window05;

-- abnormal test：column ts is the type timestamp,but it is not primary key
drop table if exists time_window06;
create table time_window06 (ts timestamp, col2 int);
insert into time_window06 values ('2020-01-01 10:00:00.000', 212332);
insert into time_window06 values ('2020-01-02 12:00:00.000', -3890232);
insert into time_window06 values ('2020-01-04 09:00:00.000', null);
select * from time_window06;
select _wstart, _wend, max(col2) from time_window05 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
drop table time_window06;

-- abnormal test：interval is less than 1s
drop table if exists time_window07;
create table time_window07 (ts timestamp primary key, col2 smallint unsigned);
insert into time_window07 values ('2020-01-01 10:00:00.000', 127);
insert into time_window07 values ('2020-01-02 12:00:00.000', 0);
select _wstart, _wend, max(col2) from time_window07 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 0.1, second) fill(next);
drop table time_window07;

-- abnormal test：sliding window is larger than time window
drop table if exists time_window08;
create table time_window08 (ts timestamp primary key, col2 smallint unsigned);
insert into time_window08 values ('2020-01-01 10:00:00.000', 127);
insert into time_window08 values ('2020-01-02 12:00:00.000', 0);
insert into time_window08 values ('2020-01-03 12:00:00.000', 22);
select _wstart, _wend, max(col2) from time_window08 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) sliding(2,day);
drop table time_window08;

-- the aggr column is int，with agg function max,min,avg,sum,count, interval day hour
drop table if exists int01;
create table int01 (ts timestamp primary key , col2 tinyint unsigned, col3 smallint, col4 bigint unsigned);
insert into int01 values ('2020-01-01 10:00:00.000', 127, null, 32151654354);
insert into int01 values ('2020-01-02 12:00:00.000', 0, null, 9223372036854775807);
insert into int01 values ('2020-01-03 13:00:00.000', 64, null, null);
insert into int01 values ('2020-01-04 09:00:00.000', 100, -1921, 32173892173092);
insert into int01 values ('2020-01-05 09:00:00.000', 200, 0, 37219739821);
insert into int01 values ('2020-01-06 09:00:00.000', 200, 0, 294095);
select * from int01;
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(prev);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(next);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(linear);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(none);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(VALUE,200);
select _wstart, _wend, max(col2), min(col3), avg(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 24, hour);
select _wstart, _wend, sum(col2), sum(col3), sum(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 13, hour);
select _wstart, _wend, sum(col2), sum(col3), sum(col4) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 2, day);
select _wstart, _wend, count(col2) from int01 where ts > '2020-01-01 00:00:00.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 1, day) fill(none);
drop table int01;

-- the aggr column is decimal,float,double with agg function max,min,avg,sum,count,interval hour
drop table if exists int02;
create table int02 (ts timestamp primary key, col2 float, col3 double, col4 decimal(30,10));
insert into int02 values ('2023-10-27 00:00:00.000', 321421.42314, 38021.324143214, 321798372091.324213421421342);
insert into int02 values ('2023-10-27 01:01:00.000', -2379823.0, 214214.32143214, -321798372091.324213421421342);
insert into int02 values ('2023-10-27 01:59:12.123', 321421.42314, null, 321798372091.324213421421342);
insert into int02 values ('2023-10-27 02:10:12.123', 321421.42314, -38021.111, null);
insert into int02 values ('2023-10-27 02:39:12.123', 0, 38021.324143214, 321798372091.324213421421342);
insert into int02 values ('2023-10-27 03:02:12.123', -321.42314832, 43824.43543, null);
insert into int02 values ('2023-10-27 12:10:12.110', 0, 0, 0);
insert into int02 values ('2023-10-27 12:33:12.110', 321421.42314, 2222, 3211.324213487734324535432523421421342);
insert into int02 values ('2023-10-27 13:33:12.110', 8430923, 332, 0);
insert into int02 values ('2023-10-27 13:33:15.110', 2141243.423141234213421421, 38021.324143214, 321798372091.324213421421342);
insert into int02 values ('2023-10-27 23:33:15.110', 321421.42314, 38021.324143214, 321798372091.324213421421342);
insert into int02 values ('2023-10-27 22:33:15.110', 321421.42314, null, null);
select * from int02;
select _wstart, _wend, max(col2), min(col3), min(col4) from int02 where ts >= '2023-10-27 00:00:00.000' and ts < '2023-10-27 23:00:00.000' interval(ts, 1, hour) fill(none);
select _wstart, _wend, max(col2), min(col3), min(col4) from int02 where ts > '2023-10-27 00:00:00.000' and ts < '2023-10-27 23:00:00.000' interval(ts, 10, minute) fill(next);
select _wstart, _wend, sum(col2), sum(col3), sum(col4) from int02 where ts > '2023-10-27 00:00:00.000' and ts < '2023-10-27 23:00:00.000' interval(ts, 60, minute) fill(next);
select _wstart, _wend, avg(col2), avg(col3), avg(col4) from int02 where ts > '2023-10-27 00:00:00.000' and ts < '2023-10-27 23:00:00.000' interval(ts, 20, minute);
select _wstart, _wend, count(col2), count(col3), count(col4) from int02 where ts > '2023-10-27 00:00:00.000' and ts < '2023-10-27 23:00:00.000' interval(ts, 30, minute) fill(prev);
drop table int02;

-- interval and sliding window with fill
drop table if exists sliding_window01;
create table sliding_window01 (ts timestamp(3) primary key , col2 double);
insert into sliding_window01 values ('2023-08-01 00:00:00', 25.0);
insert into sliding_window01 values ('2023-08-01 00:05:00', 26.0);
insert into sliding_window01 values ('2023-08-01 00:15:00', 28.0);
insert into sliding_window01 values ('2023-08-01 00:20:00', 30.0);
insert into sliding_window01 values ('2023-08-01 00:25:00', 27.0);
insert into sliding_window01 values ('2023-08-01 00:30:00', null);
insert into sliding_window01 values ('2023-08-01 00:35:00', null);
insert into sliding_window01 values ('2023-08-01 00:40:00', 28);
insert into sliding_window01 values ('2023-08-01 00:45:00', 38);
insert into sliding_window01 values ('2023-08-01 00:50:00', 31);
insert into sliding_window01 values ('2023-07-31 23:55:00', 22);
select * from sliding_window01;
select _wstart, _wend, max(col2), min(col2) from sliding_window01 where ts > "2023-08-01 00:00:00.000" and ts < "2023-08-01 00:50:00.000" interval(ts, 10, minute) sliding(5, minute);
select _wstart, _wend, max(col2), min(col2) from sliding_window01 where ts > "2023-08-01 00:00:00.000" and ts < "2023-08-01 00:50:00.000" interval(ts, 10, minute) sliding(5, minute) fill(null);
select _wstart, _wend, count(col2), avg(col2) from sliding_window01 where ts > "2023-08-01 00:00:00.000" and ts < "2023-08-01 00:50:00.000" interval(ts, 10, minute) sliding(10, minute) fill(next);
select _wstart, _wend, count(col2), avg(col2) from sliding_window01 where ts > "2023-08-01 00:00:00.000" and ts < "2023-08-01 00:50:00.000" interval(ts, 10, minute) sliding(6, minute) fill(value,1000);
select _wstart, _wend, sum(col2) from sliding_window01 where ts > "2023-08-01 00:00:00.000" and ts < "2023-08-01 00:50:00.000" interval(ts, 1, hour) sliding(10, minute) fill(none);
drop table sliding_window01;

drop table if exists sliding_window02;
create table sliding_window02 (ts timestamp primary key , col2 double);
insert into sliding_window02 values ('2023-08-01 00:01:01.000',37281932.32143214);
insert into sliding_window02 values ('2023-08-01 00:01:02.000',-328934.324);
insert into sliding_window02 values ('2023-08-01 00:01:03.000',-23.23232);
insert into sliding_window02 values ('2023-08-01 00:01:04.000',null);
select * from sliding_window02;
select _wstart, _wend, max(col2), min(col2), sum(col2) from sliding_window02 where ts > "2023-08-01 00:01:00.000" and ts < "2023-08-01 00:01:10.000" interval(ts, 1, second) fill(none);
select _wstart, _wend, max(col2), min(col2), sum(col2) from sliding_window02 where ts >= "2023-08-01 00:01:00.000" and ts <= "2023-08-01 00:01:10.000" interval(ts, 2, second);
select _wstart, _wend, max(col2), min(col2), sum(col2) from sliding_window02 where ts >= "2023-08-01 00:01:00.000" and ts <= "2023-08-01 00:01:10.000" interval(ts, 2, second) fill(prev);
select _wstart, _wend, count(col2), avg(col2) from sliding_window02 where ts > "2023-08-01 00:01:00.000" and ts < "2023-08-01 00:01:10.000" interval(ts, 2, second) fill(none);
select _wstart, _wend, count(col2), avg(col2) from sliding_window02 where ts > "2023-08-01 00:01:00.000" and ts < "2023-08-01 00:01:10.000" interval(ts, 2, second) fill(linear);
select * from sliding_window02;
drop table sliding_window02;

-- time window in prepare
drop table if exists sliding_window03;
create table sliding_window03 (ts timestamp primary key , col2 float);
insert into sliding_window03 values ('2023-08-01 00:01:01.000', 32412.3421);
insert into sliding_window03 values ('2023-08-01 00:01:03.000', -23.23232);
insert into sliding_window03 values ('2023-08-01 00:01:04.000', -3289.328939201);
select * from sliding_window03;
prepare s1 from 'select _wstart, _wend, max(col2), min(col2), sum(col2), count(col2), avg(col2) from sliding_window03 where ts > "2023-08-01 00:01:00.000" and ts < "2023-08-01 00:01:10.000" interval(ts, 1, second) fill(none);';
execute s1;
prepare s2 from 'select _wstart, _wend, max(col2), min(col2), sum(col2), count(col2), avg(col2) from sliding_window03 where ts > "2023-08-01 00:01:00.000" and ts < "2023-08-01 00:01:10.000" interval(ts, 1, second) sliding(2, second) fill(none);';
execute s2;
drop table sliding_window03;

-- sliding window：interval equal to sliding window
drop table if exists sliding_window04;
create table sliding_window04 (ts timestamp primary key , col2 double);
insert into sliding_window04 values ('2023-08-01 00:01:01.000', 121432421.32142314);
insert into sliding_window04 values ('2023-08-01 00:01:03.000', null);
insert into sliding_window04 values ('2023-08-01 00:01:04.000', 32151323.32151251252512);
insert into sliding_window04 values ('2023-08-01 00:01:05.000', -38298432.32143214231);
insert into sliding_window04 values ('2023-08-01 00:01:06.000', 0);
insert into sliding_window04 values ('2023-08-01 00:01:07.111', 0);
insert into sliding_window04 values ('2023-08-01 00:01:08.123', 38298392.32142142);
select * from sliding_window04;
select _wstart, _wend, count(col2), avg(col2) from sliding_window04 where ts >= "2023-08-01 00:01:00.000" and ts <= "2023-08-01 00:01:10.000" interval(ts, 1, second) sliding(1, second) fill(prev);
select _wstart, _wend, count(col2), avg(col2) from sliding_window04 where ts >= "2023-08-01 00:01:00.000" and ts <= "2023-08-01 00:01:10.000" interval(ts, 2, second) sliding(2, second) fill(next);
drop table sliding_window04;

-- sliding window: fill(null),fill(value,val),fill(LINEAR)
drop table if exists sliding_window05;
create table sliding_window05 (ts timestamp primary key , col2 int);
insert into sliding_window05 values ('2018-01-13 00:01:01.000', 100);
insert into sliding_window05 values ('2018-10-13 00:01:02.000', 37213);
insert into sliding_window05 values ('2019-01-29 00:10:01.000', -2146261);
insert into sliding_window05 values ('2019-12-13 00:11:57.000', 0);
insert into sliding_window05 values ('2019-12-13 00:12:58.000', 21132);
insert into sliding_window05 values ('2019-02-13 00:13:51.000', null);
insert into sliding_window05 values ('2019-12-13 00:13:59.000', null);
insert into sliding_window05 values ('2020-12-13 00:21:59.000', null);
insert into sliding_window05 values ('2021-12-13 12:12:59.000', null);
insert into sliding_window05 values ('2021-12-14 00:11:59.000', -328193471);
insert into sliding_window05 values ('2022-07-17 00:04:12.000', -3281891);
select * from sliding_window05;
select _wstart, _wend, sum(col2) from sliding_window05 where ts >= '1997-01-13 00:00:0.000' and ts <= '2022-07-18 00:04:12.123' interval(ts, 365, day) sliding(200, day) fill(VALUE, 100);
select _wstart, _wend, avg(col2) from sliding_window05 where ts >= '1997-01-13 00:00:0.000' and ts <= '2022-07-18 00:04:12.123' interval(ts, 100, day) sliding(100, day) fill(linear);
select _wstart, _wend, sum(col2) from sliding_window05 where ts >= '1998-01-29 00:10:01.000' and ts <= '2022-07-17 00:04:12.000' interval(ts, 200, day) sliding(100, day) fill(linear);
drop table sliding_window05;

-- temporary table
-- @bvt:issue#7889
drop table if exists temporary01;
create temporary table temporary01 (ts timestamp primary key, col2 bigint);
insert into temporary01 values ('2022-07-17 00:04:12.000', -2147483647);
insert into temporary01 values ('2022-08-17 12:23:12.000', null);
insert into temporary01 values ('2022-02-15 00:23:12.000', 100);
insert into temporary01 values ('2022-09-17 00:23:12.000', 324421432);
insert into temporary01 values ('2022-08-27 00:11:12.000', -32434);
insert into temporary01 values ('2023-01-01 12:12:12.000', -232);
insert into temporary01 values ('2024-03-04 13:14:56.000', -3892323);
insert into temporary01 values ('2024-12-12 01:34:46.000', 0);
insert into temporary01 values ('2020-10-10 09:09:09.000', null);
select * from temporary01;
select _wstart, _wend, sum(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 365, day) sliding(200, day);
select _wstart, _wend, avg(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 365, day) sliding(4800, hour);
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(20, day) limit 10;
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(28800, minute) limit 10;
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(20, day) fill(none);
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(28800, minute) fill(prev);
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(20, day) fill(next);
select _wstart, _wend, max(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 100, day) sliding(48800, minute) fill (VALUE,10000);
-- @bvt:issue
-- @bvt:issue#12469
select _wstart, _wend, min(col2) from temporary01 where ts >= '2020-10-10 09:09:09' and ts <= '2024-12-12 01:34:46' interval(ts, 365, day) sliding(200, day) fill (linear);
-- @bvt:issue
-- @bvt:issue#7889
drop table temporary01;
-- @bvt:issue

drop table if exists temporary02;
create table temporary02 (ts timestamp primary key , col2 double, col3 double, col4 float);
insert into temporary02 values ('2023-11-01 00:00:01.000', 100.23242134, 73823.90902, 28392432);
insert into temporary02 values ('2023-11-01 00:00:03.000', null, null, null);
insert into temporary02 values ('2023-11-01 00:00:04.000', -3921.32421, -3290.32, null);
insert into temporary02 values ('2023-11-01 00:00:05.000', -4324, 432424.4324234, 0);
insert into temporary02 values ('2023-11-01 00:00:06.000', -38249382324324.990, 4032.432, 90909);
insert into temporary02 values ('2023-11-01 00:00:07.000', 212112.3233, 9302, -392032);
insert into temporary02 values ('2023-11-01 00:00:08.000', 0, 0, 0);
insert into temporary02 values ('2023-11-01 00:00:10.000', 0, null, null);
insert into temporary02 values ('2023-11-01 00:00:11.000', 9999.999, 382.343, -32932);
insert into temporary02 values ('2023-11-01 00:00:12.000', null, null, -23.4324324);
insert into temporary02 values ('2023-11-01 00:01:13.000', -0.1, 328903.32334, 909);
insert into temporary02 values ('2023-11-01 00:13:14.000', null, 392432.4324, 8932);
select * from temporary02;
select _wstart, _wend, max(col2), min(col3) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 3, second);
select _wstart, _wend, avg(col2), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 2, second);
select _wstart, _wend, avg(col2), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 2, second) fill(none);
select _wstart, _wend, avg(col2), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 2, second) fill(prev);
select _wstart, _wend, avg(col2), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 2, second) fill(linear);
select _wstart, _wend, sum(col3), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 1, minute);
select _wstart, _wend, sum(col3), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 60, second);
select _wstart, _wend, sum(col3), count(col4) from temporary02 where ts >= '2023-11-01 00:00:01.000' and ts <= '2023-11-01 00:13:14.000' interval(ts, 5, second) sliding(4, second) fill(value, 2190);
drop table temporary02;

-- external table
drop table if exists external01;
create external table external01(ts timestamp primary key,col1 tinyint default null,col2 smallint default null,col3 int default null,col4 bigint default null,col5 tinyint unsigned default null,col6 smallint unsigned default null,col7 int unsigned default null,col8 bigint unsigned default null,col9 float default null,col10 double default null)infile{"filepath"='$resources/external_table_file/time_window.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select _wstart, _wend, max(col2), min(col3), avg(col4) from external01 where ts >= '2014-01-01 00:01:17' and ts <= '2014-01-01 00:14:58' interval(ts, 3, minute);
select _wstart, _wend, count(col2), sum(col6), avg(col7) from external01 where ts >= '2014-01-01 00:01:17' and ts <= '2014-01-01 00:14:58' interval(ts, 10, minute) sliding(5,minute);
select _wstart, _wend, count(col2), sum(col10) from external01 where ts >= '2014-01-01 00:01:17' and ts <= '2014-01-01 00:14:58' interval(ts, 15, minute) sliding(10,minute);
select _wstart, _wend, count(col2), sum(col10) from external01 where ts >= '2014-01-01 00:01:17' and ts <= '2014-01-01 00:14:58' interval(ts, 15, minute) sliding(10,minute) fill(value,1000);
drop table external01;

drop table if exists tt1;
create table tt1(ts timestamp primary key, a int);
select max(a) as enable, min(a) as collation from tt1 interval(ts, 1, minute);

drop database time_window;