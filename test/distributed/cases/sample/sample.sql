-- The sample column is equal or larger than the original column
drop table if exists sample01;
create table sample01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into sample01 values (1, null, 'database');
insert into sample01 values (2, 38291.32132, 'database');
insert into sample01 values (3, null, 'database management system');
insert into sample01 values (4, 10, null);
insert into sample01 values (1, -321.321, null);
insert into sample01 values (2, -1, null);
select * from sample01;
select sample (col3, 3 rows) from sample01;
select sample (col3, 4 rows) as newCol3 from sample01;
select sample (col3, 3 rows) as newCol3 from sample01 where col3 is not null;
select sample (col3, 3 rows) from sample01 where col3 is null;
select sample (col2, 2 rows) from sample01 where col3 is null group by col1;
select col1, sample(col2, 20 rows) from sample01 group by col1 order by col1;

select sample (col1 * 3, 10 rows) as newCol1 from sample01 where col2 is not null;
select col1,sample (col2 * 3, 10 rows) as newCol from sample01 group by col1;

-- nested with functions
select sample (reverse(col3), 4 rows) as newcol3 from sample01;
select col1, sample (startswith(col3, 'database'), 3 rows) from sample01;
select col1, sample (endswith(col3, 'system'), 3 rows) from sample01;
drop table sample01;

-- sample column is time type
drop table if exists sample02;
create table sample02 (col1 int, col2 datetime);
insert into sample02 values (1, '2020-10-13 10:10:10');
insert into sample02 values (2, null);
insert into sample02 values (1, '2021-10-10 00:00:00');
insert into sample02 values (2, '2023-01-01 12:12:12');
insert into sample02 values (2, null);
insert into sample02 values (3, null);
insert into sample02 values (4, '2023-11-27 01:02:03');
select * from sample02;
select sample (col2, 4 rows) from sample02 order by col2 desc;
select sample (col2, 5 rows) from sample02 group by col1 order by col2;
select sample (col2, 100 percent) from sample02 group by col1 order by col2;
select sample (col2, 0 percent) from sample02 group by col1 order by col2 desc;
select col1, sample (col2, 5 rows) as newColumn from sample02 group by col1 order by col2;
drop table sample02;

-- sample multiple columns
drop table if exists sample03;
create table sample03 (col1 int, col2 float, col3 decimal, col4 enum('1','2','3','4'));
insert into sample03 values (1, 12.21, 32324.32131, 1);
insert into sample03 values (2, null, null, 2);
insert into sample03 values (2, -12.1, 34738, null);
insert into sample03 values (1, 90.2314, null, 4);
insert into sample03 values (1, 43425.4325, -7483.432, 2);
select * from sample03;
select sample(col1, col2, col3, col4, 2 rows) from sample03 where col2 != null and col4 is not null;
-- sample multi columns cannot have alias
select sample(col1, col2, col3, col4, 2 rows) from sample03 where col4 is not null;
select sample(col1, col2, col3, 4 rows) from sample03 where col4 is not null;
select sample(col1 + 100, col2 + 100, col3 + 100, 4 rows) from sample03 where col4 is not null;
select sample(col1 * 2, col2 * 3, col3, 100 rows) from sample03 where col1 = 2;
select sample(col1 * 2, col2 * 3, col3, 100 rows) from sample03 where col1 = 2 limit 1;
drop table sample03;

-- with prepare
drop table is exists sample04;
create table sample04 (col1 int, col2 binary);
insert into sample04 values (1, 'a');
insert into sample04 values (2, 'b');
insert into sample04 values (3, 'c');
insert into sample04 values (1, null);
insert into sample04 values (2, null);
insert into sample04 values (2, 'c');
prepare s1 from 'select col1, sample(col2, 4 rows) from sample04 group by col1';
execute s1;
prepare s2 from 'select col1, sample(col2, 100 percent) from sample04 group by col1 order by col1 desc';
execute s2;
drop table sample04;

-- sample from temporary table
drop table if exists sample05;
create temporary table sample05 (col1 int, col2 binary);
insert into sample05 values (1, 'a');
insert into sample05 values (2, 'b');
insert into sample05 values (3, 'c');
insert into sample05 values (1, null);
insert into sample05 values (2, null);
insert into sample05 values (2, 'c');
insert into sample05 (col1, col2) values (2, true);
select sample (col1, col2, 5 rows) from sample05;
select sample (col1, col2, 100 percent) from sample05;
select sample (col1, col2, 0 percent) from sample05;
drop table sample05;