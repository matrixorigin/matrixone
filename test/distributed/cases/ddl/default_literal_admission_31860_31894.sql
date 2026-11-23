-- Literal DEFAULT admission and error semantics (#31860, #31894).
-- A numeric literal DEFAULT whose converted value exceeds the CHAR/VARCHAR
-- width, and a literal DEFAULT that cannot convert to the column type, must
-- both report MySQL's ER_INVALID_DEFAULT with the real column name, for
-- CREATE / ADD / MODIFY / CHANGE / ALTER COLUMN SET DEFAULT, in strict and
-- empty sql_mode. Fitting defaults keep storing the full converted value, and
-- the DML / CAST / expression-default neighbours keep their own behaviors.
-- Every rejected statement is followed by a probe on the same connection.

drop database if exists default_literal_admission_31860_31894;
create database default_literal_admission_31860_31894;
use default_literal_admission_31860_31894;

set @saved_literal_default_mode = @@sql_mode;

-- #31860 CREATE TABLE rejects wide numeric defaults like over-long strings.
create table t1(v varchar(3) default 12345);
select 1 as recovered_strict_create_int;
create table t1(v varchar(3) default 1.25);
select 2 as recovered_strict_create_decimal;
create table t1(v char(3) default 12345);
select 3 as recovered_strict_create_char;

-- #31860 ALTER TABLE ADD COLUMN rejects the same overflow.
create table a(id int primary key, v varchar(5));
alter table a add column n varchar(2) default 100;
select 4 as recovered_strict_add;

-- #31860 MODIFY and CHANGE reject the overflow with the authored column name.
create table m(id int primary key, v varchar(5) default 12);
alter table m modify column v varchar(3) default 12345;
select 5 as recovered_strict_modify;
alter table m change column v w varchar(3) default 1234;
select 6 as recovered_strict_change;

-- #31860 ALTER COLUMN SET DEFAULT rejects the overflow.
alter table a alter column v set default 123456;
select 7 as recovered_strict_setdefault;

-- #31894 bad-value and out-of-range literals report ER_INVALID_DEFAULT with
-- the actual column name; the ADD COLUMN shape names the new column c2.
create table i1(id int primary key, c int default 1);
alter table i1 alter column c set default 'abc';
alter table i1 alter column c set default '12x';
alter table i1 alter column c set default 99999999999;
alter table i1 modify column c int default 'abc';
alter table i1 add column c2 int default 'abc';
select 8 as recovered_strict_badvalue_add;
create table i2(id int, c int default 'abc');
create table i3(id int, c tinyint default 300);
create table i4(id int, c date default 'abc');
select 9 as recovered_strict_badvalue_create;

-- The same admissions hold under sql_mode=''.
set @@sql_mode='';
create table e1(v varchar(3) default 12345);
select 10 as recovered_empty_create;
create table e1(id int primary key, c int default 1);
alter table e1 alter column c set default 'abc';
alter table e1 add column c2 int default 'abc';
select 11 as recovered_empty_badvalue;
alter table e1 modify column c int default 'abc';
select 12 as recovered_empty_modify;
set @@sql_mode=@saved_literal_default_mode;

-- Fitting numeric defaults store the whole converted value and serve INSERT.
create table f1(v varchar(5) default 12345);
show create table f1;
insert into f1 values ();
select * from f1;
create table f2(v varchar(4) default 1.25);
insert into f2 values ();
select * from f2;
create table f3(id int primary key, m mediumint default 1);
insert into f3 values (1, default);
select id, m from f3 order by id;

-- Neighbouring owners are unchanged.
create table s1(v varchar(3) default 'abcd');
select 13 as recovered_overlong_string;
create table s2(v varchar(3) default (12345));
insert into s2 values ();
select * from s2;
create table g1(id int primary key, m mediumint(9) default 8388608);
create table g2(id int primary key, d decimal(5,2) unsigned default -1.25);
create table n1(id int primary key, d datetime default now());
insert into n1 (id) values (1);
select id from n1 order by id;
create table d1(v varchar(3));
insert into d1(v) values (12345);
select 14 as recovered_dml_truncation;
select cast(12345 as char(3));


-- Review 5766780637: a leading-zero bare signed decimal keeps the numeric
-- domain admission and round-trips through the same default value, and the
-- maximum CHAR width rejects its one-character overflow while the exact fit
-- stays admitted; every rejected statement is followed by a same-connection
-- probe and the existing definitions keep their data.
create table lz1(id int primary key, v varchar(5) default -000001.25);
insert into lz1 (id) values (1);
select v from lz1 order by id;
create table lz1m(id int primary key, v varchar(5));
alter table lz1m alter column v set default -000001.25;
insert into lz1m (id) values (1);
select v from lz1m order by id;
create table c255(id int primary key, v char(255) default 1.2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222);
insert into c255 (id) values (1);
select length(v) from c255;
create table c255x(id int primary key, v char(255) default 1.22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222);
select 21 as recovered_after_char_create_overflow;
alter table c255 add column w char(255) default 1.22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222;
select 22 as recovered_after_alter_add_overflow;
alter table c255 alter column v set default 1.22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222;
select 23 as recovered_after_alter_default_overflow;
create table abase(id int primary key);
alter table abase add column v char(255) default 1.2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222;
alter table abase modify column v char(255) default 1.22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222;
select 24 as recovered_after_modify_overflow;
select v from lz1 order by id;

-- Review 5771414743: a bare unary plus literal authored as new DDL is a bare
-- literal like the existing unary minus form - out-of-range values remap to
-- ER_INVALID_DEFAULT with the authored column name, fitting values keep
-- storing the full converted value and serve INSERT, and every rejected
-- statement is followed by a same-connection probe.
create table up1(id int primary key, c tinyint default +300);
select 31 as recovered_after_unary_plus_tinyint_overflow;
create table up2(id int primary key, c int default +99999999999);
select 32 as recovered_after_unary_plus_int_overflow;
create table up3(id int primary key, c tinyint default +100);
insert into up3 (id) values (1);
select c from up3 order by id;
create table up4(id int primary key, v varchar(5) default +1.25);
insert into up4 (id) values (1);
select v from up4 order by id;
-- The persisted origin keeps the authored unary plus: SHOW CREATE reparses
-- the stored origin string, and the ALTER round-trip must reproduce it.
show create table up4;
alter table up4 modify column v varchar(5) default +1.25;
show create table up4;
create table up5(id int primary key, v varchar(5) default +123456);
select 33 as recovered_after_unary_plus_width_overflow;

-- Same-connection recovery is proven; clean the catalog and restore sql_mode.
drop database default_literal_admission_31860_31894;
