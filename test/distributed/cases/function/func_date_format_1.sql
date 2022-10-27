
-- test date format function
drop table if exists t1;
create table t1 (d datetime);

insert into t1 values ('2004-07-14 08:19:30.333333'),('2005-08-15 22:50:59.999999');
select date_format(d,'%a') from t1;
select date_format(d,'%b') from t1;
select date_format(d,'%c') from t1;
select date_format(d,'%D') from t1;
select date_format(d,'%d') from t1;
select date_format(d,'%e') from t1;
select date_format(d,'%f') from t1;
select date_format(d,'%H') from t1;
select date_format(d,'%h') from t1;
select date_format(d,'%I') from t1;
select date_format(d,'%i') from t1;
select date_format(d,'%j') from t1;
select date_format(d,'%k') from t1;
select date_format(d,'%l') from t1;
select date_format(d,'%M') from t1;
select date_format(d,'%m') from t1;
select date_format(d,'%p') from t1;
select date_format(d,'%r') from t1;
select date_format(d,'%S') from t1;
select date_format(d,'%s') from t1;
select date_format(d,'%T') from t1;
select date_format(d,'%U') from t1;
select date_format(d,'%u') from t1;
select date_format(d,'%V') from t1;
select date_format(d,'%v') from t1;
select date_format(d,'%W') from t1;
select date_format(d,'%w') from t1;
select date_format(d,'%X') from t1;
select date_format(d,'%x') from t1;
select date_format(d,'%Y') from t1;
select date_format(d,'%y') from t1;

drop table t1;

-- test select
drop table if exists t2;
create table t2 (f1 datetime);
insert into t2 (f1) values ('2005-01-01');
insert into t2 (f1) values ('2005-02-01');
select date_format(f1, "%m") as d1, date_format(f1, "%M") as d2 from t2 order by date_format(f1, "%M");
drop table t2;


-- test insert
drop table if exists t3;
create table t3 (d date);
insert into t3 values (date_format('20221214', '%Y-%m-%d'));
drop table t3;


-- test update,delete
drop table if exists t4;
create table t4 (a int, d date);
insert into t4 values (1, '2002-02-20');
update t4 set d=date_format('20221214', '%Y-%m-%d') where a=1;
delete from t4 where d=date_format('20221214', '%Y-%m-%d');
select * from t4;
drop table t4;


-- test operator
drop table if exists t5;
create table t5 (a int, b date);
insert into t5 values
(1,'2000-02-05'),(2,'2000-10-08'),(3,'2005-01-03'),(4,'2007-09-01'),(5,'2022-01-01');

select * from t5 where b=date_format('20000205', '%Y-%m-%d');
select * from t5 where b!=date_format('20000205', '%Y-%m-%d');
select * from t5 where b<>date_format('20000205', '%Y-%m-%d');
select * from t5 where b>date_format('20000205', '%Y-%m-%d');
select * from t5 where b<date_format('20000205', '%Y-%m-%d');
select * from t5 where b<=date_format('20000205', '%Y-%m-%d');
select * from t5 where b between date_format('20000205', '%Y-%m-%d') and date_format('20220101', '%Y-%m-%d');
select * from t5 where b not between date_format('20000205', '%Y-%m-%d') and date_format('20220101', '%Y-%m-%d');

drop table t5;


SELECT DATE_FORMAT("2009-01-01",'%W %d %M %Y') as valid_date;

-- @bvt:issue#4764
SELECT DATE_FORMAT('0000-01-01','%W %d %M %Y') as valid_date;
SELECT DATE_FORMAT('0000-02-28','%W %d %M %Y') as valid_date;
-- @bvt:issue

SELECT DATE_FORMAT('9999-02-28','%W %d %M %Y') as valid_date;

select date_format('1997-01-02 03:04:05', '%M %W %D %Y %y %m %d %h %i %s %w');
select date_format('1997-01-02', concat('%M %W %D','%Y %y %m %d %h %i %s %w'));

select date_format('1998-12-31','%x-%v'),date_format('1999-01-01','%x-%v');
select date_format('1999-12-31','%x-%v'),date_format('2000-01-01','%x-%v');

select date_format(concat('19980131',131415),'%H|%I|%k|%l|%i|%p|%r|%S|%T| %M|%W|%D|%Y|%y|%a|%b|%j|%m|%d|%h|%s|%w');

-- echo error
select date_format(19980021000000,'%H|%I|%k|%l|%i|%p|%r|%S|%T| %M|%W|%D|%Y|%y|%a|%b|%j|%m|%d|%h|%s|%w');


select date_format('19980021000000','%H|%I|%k|%l|%i|%p|%r|%S|%T| %M|%W|%D|%Y|%y|%a|%b|%j|%m|%d|%h|%s|%w');

