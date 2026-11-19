-- #31908 CREATE/ALTER sequence parameters must fit the declared integer
-- domain; rejections report the value the user wrote.
drop database if exists issue31908;
create database issue31908;
use issue31908;

create sequence s1 as smallint start with 70000;
select nextval('s1');
create sequence s2 as smallint unsigned start with -1;
select nextval('s2');
create sequence s3 as int minvalue -4294967296 maxvalue 10;
select nextval('s3');
create sequence s4 as smallint maxvalue 99999;
create sequence s5 start with 9223372036854775808;

create sequence b1 as smallint minvalue -32768 maxvalue 32767 start with 32767;
select nextval('b1');
create sequence b2 as smallint unsigned minvalue 0 maxvalue 65535 start with 65535;
select nextval('b2');
create sequence b3 as bigint minvalue -9223372036854775808 maxvalue 9223372036854775807 start with -9223372036854775808;
select nextval('b3');
create sequence b4 as bigint unsigned minvalue 0 maxvalue 18446744073709551615 start with 18446744073709551615;
select nextval('b4');
create sequence b5 as int minvalue -2147483648 maxvalue 2147483647 start with 2147483647;
select nextval('b5');
create sequence b6 as int unsigned minvalue 0 maxvalue 4294967295 start with 4294967295;
select nextval('b6');
drop sequence b1;
drop sequence b2;
drop sequence b3;
drop sequence b4;
drop sequence b5;
drop sequence b6;

-- a rejected ALTER keeps the stored definition and the issued position
create sequence s7 as smallint start with 10;
select nextval('s7');
alter sequence s7 start with 70000;
select currval('s7');
select nextval('s7');
show sequences;

-- INCREMENT keeps its documented int64 storage width
create sequence s8 as int increment by 4294967297;
alter sequence s7 increment by 65537;

drop database if exists issue31908;
