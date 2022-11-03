
-- Test not support default value and DDL index on

-- @bvt:issue#4538
drop table if exists t1;
create table t1 (
a text not null,
b text default '111'
);
drop table t1;
-- @bvt:issue

drop table if exists t2;
create table t2(
a int,
b text primary key
);
drop table t2;

-- test different type cast text type

drop table if exists t3;
create table t3 (
col1 bigint,
col2 bigint unsigned
);
insert into t3 values (-9223372036854775808,18446744073709551615);

select cast(col1 as text), cast(col2 as text) from t3;
drop table t3;

drop table if exists t4;
create table t4 (
col1 float,
col2 double
);
insert into t4 values (1.23456789333, 8.12334456564564564995645674564567457);

select cast(col1 as text), cast(col2 as text) from t4;
drop table t4;

drop table if exists t5;
create table t5 (
col1 char(25),
col2 varchar(25)
);

insert into t5 values ('this is a char type', 'this is a varchar type');
select cast(col1 as text), cast(col2 as text) from t5;
select cast(col1 as text) as col1, cast(col2 as text) as col2 from t5;
drop table t5;

drop table if exists t6;
create table t6(
col1 date,
col2 datetime,
col3 timestamp
);
insert into t6 values ('2022-02-01', '2022-02-01:23:59 59.999999', '2037-09-08 23:59:59.999999');

select cast(col1 as text), cast(col2 as text), cast(col3 as text) from t6;
drop table t6;

drop table if exists t7;
create table t7(
col1 decimal(5,2),
col2 decimal(22,20),
col3 bool
);

insert into t7 values (1.12345678,12.112233445566778899009867, TRUE);
insert into t7 values (1.12345678,12.112233445566778899009867, FALSE);
select cast(col1 as text), cast(col2 as text), cast(col3 as text) from t7;
drop table t7;

-- test text type cast other type

drop table if exists t8;
create table t8(
col1 text,
col2 text,
col3 text,
col4 text,
col5 text,
col6 text,
col7 text,
col8 text,
col9 text,
col10 text,
col11 text,
col12 text,
col13 text
);
insert into t8(col1) values ('-128');
insert into t8(col1) values ('127');
-- test implicit conversion
insert into t8(col1) values (-128);
insert into t8(col1) values (127);
select cast(col1 as tinyint) from t8;

insert into t8(col1) values ('-129');
select cast(col1 as tinyint) from t8;
delete from t8 where col1='-129';

insert into t8(col1) values ('128');
select cast(col1 as tinyint) from t8;
delete from t8 where col1 is not NULL;

--------------------------------------------
insert into t8(col2) values ('-32768');
insert into t8(col2) values ('32767');
-- test implicit conversion
insert into t8(col2) values (-32768);
insert into t8(col2) values (32767);
select cast(col2 as smallint) from t8;

insert into t8(col2) values ('-32769');
select cast(col2 as smallint) from t8;
delete from t8 where col2 is not NULL;

insert into t8(col2) values ('32768');
select cast(col2 as smallint) from t8;
delete from t8 where col2 is not NULL;

------------------------------------------
insert into t8(col3) values ('-2147483648');
insert into t8(col3) values ('2147483647');
-- test implicit conversion
insert into t8(col3) values (-2147483648);
insert into t8(col3) values (2147483647);
select cast(col3 as int) from t8;

insert into t8(col3) values ('-2147483649');
select cast(col3 as int) from t8;
delete from t8 where col3 is not NULL;

insert into t8(col3) values ('2147483648');
select cast(col3 as int) from t8;
delete from t8 where col3 is not NULL;

-------------------------------------------
insert into t8(col4) values ('-9223372036854775808');
insert into t8(col4) values ('9223372036854775807');
-- test implicit conversion
insert into t8(col4) values (-9223372036854775808);
insert into t8(col4) values (9223372036854775807);
select cast(col4 as bigint) from t8;

insert into t8(col4) values ('-9223372036854775809');
select cast(col4 as bigint) from t8;
delete from t8 where col4 is not NULL;

insert into t8(col4) values ('9223372036854775808');
select cast(col4 as bigint) from t8;
delete from t8 where col4 is not NULL;

-------------------------------------------
insert into t8(col5) values ('0');
insert into t8(col5) values ('255');
-- test implicit conversion
insert into t8(col5) values (0);
insert into t8(col5) values (255);
select cast(col5 as tinyint unsigned) from t8;

insert into t8(col5) values ('-1');
select cast(col5 as tinyint unsigned) from t8;
delete from t8 where col5 is not NULL;

insert into t8(col5) values ('256');
select cast(col5 as tinyint unsigned) from t8;
delete from t8 where col5 is not NULL;

-------------------------------------------
insert into t8(col6) values ('0');
insert into t8(col6) values ('65535');
-- test implicit conversion
insert into t8(col6) values (0);
insert into t8(col6) values (65535);
select cast(col6 as smallint unsigned) from t8;

insert into t8(col6) values ('-1');
select cast(col6 as smallint unsigned) from t8;
delete from t8 where col6 is not NULL;

insert into t8(col6) values ('65636');
select cast(col6 as smallint unsigned) from t8;
delete from t8 where col6 is not NULL;

-------------------------------------------
insert into t8(col7) values ('0');
insert into t8(col7) values ('4294967295');
-- test implicit conversion
insert into t8(col7) values (0);
insert into t8(col7) values (4294967295);
select cast(col7 as int unsigned) from t8;

insert into t8(col7) values ('-1');
select cast(col7 as int unsigned) from t8;
delete from t8 where col7 is not NULL;

insert into t8(col7) values ('4294967296');
select cast(col7 as int unsigned) from t8;
delete from t8 where col7 is not NULL;

-------------------------------------------
insert into t8(col8) values ('0');
insert into t8(col8) values ('18446744073709551615');
-- test implicit conversion
insert into t8(col8) values (18446744073709551615);
select cast(col8 as bigint unsigned) from t8;

insert into t8(col8) values ('-1');
select cast(col8 as bigint unsigned) from t8;
delete from t8 where col8 is not NULL;

insert into t8(col8) values ('18446744073709551616');
select cast(col8 as bigint unsigned) from t8;
delete from t8 where col8 is not NULL;

-------------------------------------------
insert into t8(col9) values ('1.234567890434546457475756856');
insert into t8(col9) values ('111.222333344445556667777888899999000008998');
-- test implicit conversion
insert into t8(col9) values (1.234567890434546457475756856);
insert into t8(col9) values (111.222333344445556667777888899999000008998);

select cast(col9 as float) from t8;
select cast(col9 as double) from t8;
delete from t8 where col9 is not NULL;

insert into  t8(col9) values ('1000000000000000000000000000000000000000');
insert into  t8(col9) values (1000000000000000000000000000000000000000);
select cast(col9 as float) from t8;
select cast(col9 as double) from t8;
delete from t8 where col9 is not NULL;

-------------------------------------------

-- @bvt:issue#4634
insert into t8(col10) values ('this is a char type');
insert into t8(col10) values ('this is a varchar type');

select cast(col10 as char) from t8;
select cast(col10 as varchar) from t8;

select cast(col10 as char(1)) from t8;
select cast(col10 as varchar(1)) from t8;

delete from t8 where col10 is not NULL;
-- @bvt:issue

-------------------------------------------
insert into t8(col11) values ('2020-01-01');
insert into t8(col11) values ('2020-01-01 13:10:10');

-- @bvt:issue#4655
insert into t8(col11) values (2020-01-01);
insert into t8(col11) values (2020-01-01 13:10:10);
-- @bvt:issue

select cast(col11 as date) from t8;
delete from t8 where col11 is not NULL;


insert into t8(col11) values ('2020-01-01');
insert into t8(col11) values ('2020-01-01 13:10:10');

select cast(col11 as datetime) from t8;
delete from t8 where col11 is not NULL;

insert into t8(col11) values ('2020-01-01');
insert into t8(col11) values ('2020-01-01 13:10:10');
insert into t8(col11) values ('2020-01-01 13:10:59.999999');

-- @bvt:issue#4655
insert into t8(col11) values (2020-01-01 13:10:59.999999);
-- @bvt:issue

select cast(col11 as timestamp) from t8;
delete from t8 where col11 is not NULL;

-------------------------------------------
insert into t8(col12) values ('1');
insert into t8(col12) values ('0');
insert into t8(col12) values (1);
insert into t8(col12) values (0);

select cast(col12 as bool) from t8;
delete from t8 where col12 is not NULL;
insert into t8(col12) values ('true');
insert into t8(col12) values ('trUe');
insert into t8(col12) values ('falSe');
select cast(col12 as bool) from t8;
insert into t8(col12) values ('hello');
select cast(col12 as bool) from t8;
delete from t8 where col12 is not NULL;

insert into t8(col12) values ('2');
select cast(col12 as bool) from t8;
delete from t8 where col12 is not NULL;

-------------------------------------------

insert into t8(col13) values ('12345.123456789');
insert into t8(col13) values (12345.123456789);
select cast(col13 as decimal(5,3)) from t8;
insert into t8(col13) values ('1.234567');
insert into t8(col13) values (1.234567);

select cast(col13 as decimal(5,3)) from t8;
select cast(col13 as decimal(20,15)) from t8;

drop table if exists t8;


-- test text support function
drop table if exists t9;
create table t9 (
a text,
b text
);

insert into t9 values('aaa','bbb');
insert into t9 values('aaa1','bbb1');
insert into t9 values('_aaa2','_bbb2');
insert into t9 values(',aaa3',',bbb3');

select CONCAT_WS(' ',a, b) from t9;
select CONCAT_WS(',',a, b) from t9;
select CONCAT_WS('_',a, b) from t9;

select find_in_set('aaa', a) from t9;
select find_in_set('bbb', b) from t9;

delete from t9 where a is not NULL;

insert into t9 values ('', 'abcd');
insert into t1 values ('1111', '');
select empty(a),empty(b) from t9;

insert into t9 values ('a', 'b');
insert into t9 values ('aa', 'bb');

select a, length(a), b, length(b) from t9;
delete from t9 where a is not NULL;

insert into t9 values ('  matrix',' matrix_origin');
select ltrim(a), ltrim(b) from t9;

insert into t9 values ('matrix  ','matrix_one ');
select rtrim(a),rtrim(b) from t9;
delete from t9 where a is not NULL;

insert into t9 values ('a','1');
insert into t9 values ('ab','12');
insert into t9 values ('abc','123');
insert into t9 values ('abcd','1234');
select LPAD(a, 10, ',') from t9;
select LPAD(b, 8, '!') from t9;

select RPAD(a, 10, ',') from t9;
select RPAD(b, 8, '!') from t9;
delete from t9 where a is not NULL;

insert into t9 values ('Ananya Majumdar', 'IX'),('Anushka Samanta', 'X'),('Sharma', 'XI');
select startswith(a,'An') from t9;
select endswith(b,'X') from t9;
delete from t9 where a is not NULL;

insert into t9 values('  sdfad  ','2022-02-02 22:22:22');
insert into t9 values('sdfad  ','2022-02-02 22:22:22');
insert into t9 values('adsf  sdfad','2022-02-02 22:22:22');
insert into t9 values('    sdfad','2022-02-02 22:22:22');
-- @separator:table
select reverse(a),reverse(b) from t9;

delete from t9 where a is not NULL;

insert into t9 values ('123456789', 'abcdefgh');
insert into t9 values ('987654321', 'aabbccddee');

select substr(a, 2, 5) from t9;
select substr(b, 2, 5) from t9;

drop table t9;


drop table if exists t10;
create table t10 (
a int,
b varchar(255),
c text
);

insert into t10 values (1, 1, 1);
insert into t10 values (1, 2, 2);
insert into t10 values (1, 1, 2);
insert into t10 values (100, 100, 100);
insert into t10(a) values (10);
insert into t10 values (10, '', '');
insert into t10 values (10, 'aabsdfsb', 'aabdsfsfsdb');
insert into t10 values (10, 'aa,bb', 'aa,bb');
insert into t10 values (10, 'aa%bb', 'aa%bb');

-- echo error
select * from t10 where c=1;
update t10 set c=10000 where a=10;
update t10 set c=true where a=10;
update t10 set c='true' where a=10;
update t10 set c='false' where a=10;

select * from t10 where c='1';
select * from t10 where c!='1';
select * from t10 where c>'1';
select * from t10 where c>='1';
select * from t10 where c<'2';
select * from t10 where c=<'2';
select * from t10 where c is NULL;
select * from t10 where c='';
select * from t10 where c like '%a%';
select * from t10 where c like '%a\%%';
select * from t10 where c like '%,bb';
select * from t10 where c like 'aa,%';

select c,count(c) from t10 group by c;
select * from t10 order by c;

drop table t10;

drop table if exists t11;
create table t11 (a text);
insert into t11 values ('111'),('222'),('333');

drop table if exists t12;
create table t12 (b text);
insert into t12 values ('aaa'),('bbb'),('ccc');

select * from t11 join t12 on t11.a!=t12.b;
select * from t11 left join t12 on t11.a!=t12.b;
select * from t11 right join t12 on t11.a!=t12.b;

select * from t11 union select * from t12 order by a;
select * from t11 union all select * from t12 order by a;

