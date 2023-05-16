-- @suite
-- @case
-- @desc:test for builtin function mo_log_date()
-- @label:bvt

-- date part like %Y/%m/%d
select mo_log_date('2021/01/01');
select mo_log_date('1997/01/13');
select mo_log_date('1969/01/01');
select mo_log_date('1988/1/3');
select mo_log_date('1970/01/3');
select mo_log_date('1970/9/29');
select mo_log_date('20/12/12');
select mo_log_date('9/10/13');
select mo_log_date('00/12/13');
select mo_log_date('1/02/3');
select mo_log_date(null);

-- abnormal test: the year/month/day exceeds the normal boundary value
select mo_log_date('1000/12/89');
select mo_log_date('2023/13/19');
select mo_log_date('12345/19/01');

-- abnormal test: incorrect date format
select mo_log_date('2020-01-01');
select mo_log_date('1997-01-13');
select mo_log_date('1000/02/(*');
select mo_log_date('1997-&*-10');
select mo_log_date('&&()-01-13');
select mo_log_date('2022-12-{}');

-- select between and
select mo_log_date('2021/01/01') between '2021-01-01' and '2021-01-02' as val;
select mo_log_date('1998/01/01') between '2021-01-01' and '2021-01-02' as val;
select mo_log_date('1999/01/13') between '1970-01-01' and '2020-12-13' as val;
select mo_log_date('2023/1/13') between '2023-01-13' and '2023-12-13' as val;
select mo_log_date('2023/11/9') between '2023-1-9' and '2025-12-19' as val;

-- > and <
select mo_log_date('2023/01/03') > '2023-01-01' as val;
select mo_log_date('1997/12/12') < '2023-12-12' as val;
select mo_log_date('1995/12/30') > '1996-12-30' as val;
select mo_log_date('2001/05/06') < '1997-07-03' as val;
select mo_log_date('2020/01/06') > '2020-01-05' and mo_log_date('2020/01/06') < '2021-01-05';

-- abnormal test
select mo_log_date('2021-01-13') between '2020-01-13' and '2022-11-12';
select mo_log_date('1997-01-13') between '1970-01-01' and '2023-04-24';

-- mixed string
select mo_log_date('数据库Database/table/2022/03/04/^&(*)(');
select mo_log_date('hienrvkew/&*?/1970/01/01/uf893I');
select mo_log_date('gyekucwheu3p2u4oi3k2if3u9i0ikvoi43kvk43io5juti4o3m48651/2023/2/28');
select mo_log_date('yufiehw4&*(*)()_/1997/12/9/78weufiwhruewoijvriejbvrewkbe');
select mo_log_date('dhiuwnjdksnfv/2023/2025/2029/04/05/hiuewhrijvewoke&**');
select mo_log_date('dhiuwnjdksnfv/2023/04/05/09/hiuewhrijvewoke&**');
select mo_log_date('abc/def/github/com');
select mo_log_date('cuankjvar3242/ewhiuhj32i4jl3f/com/72989432');

select mo_log_date('数据库Database/table/2022/chuiwne/03/04/6^&(*)(');
select mo_log_date('hvjwejkv/1970/3823/djkvmfeve/12/24/vckjwrvew');

-- char and varchar in the table
drop table if exists date01;
create table date01(col1 char(10),col2 varchar(100));
insert into date01 values('1990/1/02','数据库Database/table/2022/03/04/^&(*)(');
insert into date01 values('2020/11/6','gyekucwheu3p2u4oi3k2if3u9i0ikvoi43kvk43io5juti4o3m48651/2023/2/28');
insert into date01 values(NULL,'yufiehw4&*(*)()_/1997/12/9/78weufiwhruewoijvriejbvrewkbe');
insert into date01 values('2023/04/24','abc/def/github/com');
select * from date01;

select mo_log_date(col1) from date01;
select mo_log_date(col2) from date01;

select date(mo_log_date(col1)) as date1,date(mo_log_date(col2)) as date2 from date01;
select date_add(mo_log_date(col1),interval 45 day) as date1 from date01;
select date_add(mo_log_date(col2), interval 30 day) as date2 from date01;
select date_format(mo_log_date(col1), '%W %M %Y') from date01;
select date_format(mo_log_date(col2), '%D %y %a %d %m %b %j') from date01;
select date_sub(mo_log_date(col1), interval 2 year) as date1, date_sub(mo_log_date(col2), interval 2 month) as date2 from date01;
select datediff(mo_log_date(col1),mo_log_date(col2)) from date01;
select day(mo_log_date(col1)),day(mo_log_date(col2)) from date01;
select dayofyear(mo_log_date(col1)),dayofyear(mo_log_date(col2)) from date01;
select extract(year from mo_log_date(col1)) as year1 from date01;
select extract(year from mo_log_date(col2)) as year2 from date01;
select extract(month from mo_log_date(col1)) as month from date01;
select date_format(mo_log_date(col1), '%X %V') from date01;
select date_format(mo_log_date(col2), '%X %V') from date01;
select month(mo_log_date(col1)),month(mo_log_date(col2)) from date01;
select weekday(mo_log_date(col1)),weekday(mo_log_date(col2)) from date01;
select year(mo_log_date(col1)),year(mo_log_date(col2)) from date01;
drop table date01;

-- tinytext/mediumtext/longtext in the table
drop table if exists text01;
create table text01(col1 tinytext not null, col2 mediumtext default null, col3 longtext);
insert into text01 values('1878/02/23','euiwq32/2019/8d29f/03/04','tdyuh3n1iuhfiu4h2fiu432gig432jfcurenvu4hiu32hmvcke4mvn439809328093284092432/1990/38209483298432042/03/04/4328932094820941032fkiwjvklwegvre');
insert into text01 values('1989/1/25',null,'789798888888888444451<>>?<></2020/12/13/14/89954454]\[\[]0');
insert into text01 values('23/01/15','deuwiqjiq/20/12/12','897e7fwqefi38928749208492*(&^(&**)(*/199/11/25/ewpqorpewoeq');
select * from text01;

select mo_log_date(col1) from text01;
select mo_log_date(col2) from text01;
select mo_log_date(col3) from text01;

-- nested with date function
select date(mo_log_date(col1)) as date1,date(mo_log_date(col2)) as date2, date(mo_log_date(col3)) as date3 from text01;
select date_add(mo_log_date(col1),interval 45 day) as date1 from text01;
select date_add(mo_log_date(col2), interval 30 day) as date2 from text01;
select date_add(mo_log_date(col3), interval 30 day) as date3 from text01;
select date_format(mo_log_date(col1), '%W %M %Y') from text01;
select date_format(mo_log_date(col2), '%D %y %a %d %m %b %j') from text01;
select date_format(mo_log_date(col3), '%D %y %a %d %m %b %j') from text01;
select date_sub(mo_log_date(col1), interval 2 year) as date1, date_sub(mo_log_date(col2), interval 2 month) as date2,date_sub(mo_log_date(col3), interval 2 day) as date3 from text01;
select datediff(mo_log_date(col1),mo_log_date(col2)) from text01;
select day(mo_log_date(col1)),day(mo_log_date(col2)),day(mo_log_date(col3)) from text01;
select dayofyear(mo_log_date(col1)),dayofyear(mo_log_date(col2)),dayofyear(mo_log_date(col3)) from text01;
select extract(year from mo_log_date(col1)) as year1 from text01;
select extract(year from mo_log_date(col2)) as year2 from text01;
select extract(month from mo_log_date(col1)) as month from text01;
select extract(year from mo_log_date(col2)) as year from text01;
select date_format(mo_log_date(col1), '%X %V') from text01;
select date_format(mo_log_date(col2), '%X %V') from text01;
select date_format(mo_log_date(col3), '%X %V') from text01;
select month(mo_log_date(col1)),month(mo_log_date(col2)),month(mo_log_date(col3)) from text01;
select weekday(mo_log_date(col1)),weekday(mo_log_date(col2)),weekday(mo_log_date(col3)) from text01;
select year(mo_log_date(col1)),year(mo_log_date(col2)),year(mo_log_date(col3)) from text01;
drop table text01;

-- nested with date funciton
select date(mo_log_date('hienrvkew/&*?/1970/01/01/uf893I'));
select date_add(mo_log_date('数据库/2021/01/01/guanli系统'),interval 45 day);
select date_format(mo_log_date('dhiuwnjdksnfv/2023/2025/2029/04/05/hiuewhrijvewoke&**'), '%W %M %Y');
select date_format(mo_log_date('数据库Database/table/2022/03/04/^&(*)('), '%D %y %a %d %m %b %j');
select date_sub(mo_log_date('2021/01/01'), interval 2 year);
select datediff(mo_log_date('2023/04/25'),mo_log_date('2024/04/25'));
select day(mo_log_date('新版本下个月发布/2045/1998/12/29/78/&**('));
select dayofyear(mo_log_date('1996/12/29'));
select extract(year from mo_log_date('2029/01/14')) as year;
select extract(month from mo_log_date('2029/01/14')) as month;
select date_format(mo_log_date('2029/01/14'), '%X %V');
select month(mo_log_date('1990/1/25'));
select weekday(mo_log_date('1258/2026/4/27/88ijmm'));
select year(mo_log_date('1212/01/03'));

-- date function nested with the columns of the table
drop table if exists date02;
create table date02(col1 text);
insert into date02 values('sfiuenhfwu8793u2r43r/2020/02/02');
select mo_log_date(col1) from date02;
drop table date02;
