drop table if exists t1;
create table t1(
cdate varchar(20),
ctime varchar(20),
cdatetime varchar(20)
);
insert into t1 values('04/31/2004','09:30:17', '2022-05-27 11:30:00');
insert into t1 values('05/31/2012','11:30:17', '2012-05-26 12:30:00');
insert into t1 values('04/23/2009','01:30:17', '2002-07-26 02:30:01');
insert into t1 values('01/31/2004','12:30:17', '2001-03-26 08:10:01');
insert into t1 values('07/03/2018','05:30:17', '2011-08-26 07:15:01');
insert into t1 values('08/25/2014','09:30:17', '2011-11-26 06:15:01');
insert into t1 values('06/30/2022','04:30:17', '2011-12-26 06:15:01');
SELECT cdate,STR_TO_DATE(cdate,'%m/%d/%Y') from t1;
SELECT ctime,STR_TO_DATE(ctime,'%h:%i:%s')  from t1;
SELECT cdatetime,STR_TO_DATE(cdatetime,'%Y-%m-%d %H:%i:%s') from t1;

SELECT cdate,TO_DATE(cdate,'%m/%d/%Y') from t1;
SELECT ctime,TO_DATE(ctime,'%h:%i:%s')  from t1;
SELECT cdatetime,TO_DATE(cdatetime,'%Y-%m-%d %H:%i:%s') from t1;
drop table t1;

drop table if exists t2;
create table t2(
cdate varchar(20),
ctime varchar(20),
cdatetime varchar(30)
);
insert into t2 values('May 1, 2013','11:13:56','8:10:2.123456 13-01-02');
insert into t2 values('Feb 28, 2022','12:33:51','12:19:2.123456 06-01-02');
insert into t2 values('Jul 20, 2022','03:23:36','15:21:2.123456 22-01-02');
insert into t2 values('Aug 1, 2013','01:43:46','11:11:2.123456 25-01-02');
insert into t2 values('Nov 28, 2022','10:53:41','19:31:2.123456 11-01-02');
insert into t2 values('Dec 20, 2022','09:23:46','1:41:2.123456 02-01-02');
SELECT cdate,STR_TO_DATE(cdate,'%b %d,%Y') from t2;
SELECT ctime,STR_TO_DATE(ctime,'%r')  from t2;
SELECT cdatetime,STR_TO_DATE(cdatetime,'%H:%i:%S.%f %y-%m-%d') from t2;
SELECT cdate,TO_DATE(cdate,'%b %d,%Y') from t2;
SELECT ctime,TO_DATE(ctime,'%r')  from t2;
SELECT cdatetime,TO_DATE(cdatetime,'%H:%i:%S.%f %y-%m-%d') from t2;

drop table t2;

SELECT STR_TO_DATE('04/31/2004', '%m/%d/%Y');
SELECT str_to_date('May 1, 2013','%M %d,%Y');
SELECT str_to_date('February 28, 2022','%M %d,%Y');
select str_to_date('8:10:2.123456 13-01-02','%H:%i:%S.%f %y-%m-%d');
select str_to_date('11:13:56', '%r');
SELECT DATE_ADD(str_to_date('9999-12-30 23:59:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT str_to_date('01,5,2013','%d,%m,%Y');
SELECT str_to_date('May 1, 2013','%M %d,%Y');
SELECT str_to_date('a09:30:17','a%h:%i:%s');
SELECT str_to_date('a09:30:17','%h:%i:%s');
SELECT str_to_date('09:30:17a','%h:%i:%s');
SELECT str_to_date('abc','abc');
SELECT str_to_date('9','%m');
SELECT str_to_date('9','%s');
SELECT str_to_date('00/00/0000', '%m/%d/%Y');
SELECT str_to_date('04/31/2004', '%m/%d/%Y');
SELECT str_to_date('00/00/0000', '%m/%d/%Y');
SELECT str_to_date('200442 Monday', '%X%V %W');
select str_to_date(concat_ws(' ','15-01-2001'), ' 2:59:58.999');
select concat_ws('',str_to_date('8:11:2.123456 03-01-02','%H:%i:%S.%f %y-%m-%d'));
select str_to_date('04 /30/2004', '%m /%d/%Y');
select str_to_date('04/30 /2004', '%m /%d /%Y');
select str_to_date('04/30/2004 ', '%m/%d/%Y ');
SELECT DATE_SUB(str_to_date('9999-12-31 00:01:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT DATE_ADD(str_to_date('9999-12-30 23:59:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT str_to_date('09:22', '%H:%i');
SELECT str_to_date('09:22:23.33', '%H:%i:%s.%f');
SELECT str_to_date('09:22', '%H:%i');

SELECT TO_DATE('04/31/2004', '%m/%d/%Y');
SELECT to_date('May 1, 2013','%M %d,%Y');
SELECT to_date('February 28, 2022','%M %d,%Y');
select to_date('8:10:2.123456 13-01-02','%H:%i:%S.%f %y-%m-%d');
select to_date('11:13:56', '%r');
SELECT DATE_ADD(to_date('9999-12-30 23:59:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT to_date('01,5,2013','%d,%m,%Y');
SELECT to_date('May 1, 2013','%M %d,%Y');
SELECT to_date('a09:30:17','a%h:%i:%s');
SELECT to_date('a09:30:17','%h:%i:%s');
SELECT to_date('09:30:17a','%h:%i:%s');
SELECT to_date('abc','abc');
SELECT to_date('9','%m');
SELECT to_date('9','%s');
SELECT to_date('00/00/0000', '%m/%d/%Y');
SELECT to_date('04/31/2004', '%m/%d/%Y');
SELECT to_date('00/00/0000', '%m/%d/%Y');
SELECT to_date('200442 Monday', '%X%V %W');
select to_date(concat_ws(' ','15-01-2001'), ' 2:59:58.999');
select concat_ws('',to_date('8:11:2.123456 03-01-02','%H:%i:%S.%f %y-%m-%d'));
select to_date('04 /30/2004', '%m /%d/%Y');
select to_date('04/30 /2004', '%m /%d /%Y');
select to_date('04/30/2004 ', '%m/%d/%Y ');
SELECT DATE_SUB(to_date('9999-12-31 00:01:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT DATE_ADD(to_date('9999-12-30 23:59:00','%Y-%m-%d %H:%i:%s'), INTERVAL 1 MINUTE);
SELECT to_date('09:22', '%H:%i');
SELECT to_date('09:22:23.33', '%H:%i:%s.%f');
SELECT to_date('09:22', '%H:%i');