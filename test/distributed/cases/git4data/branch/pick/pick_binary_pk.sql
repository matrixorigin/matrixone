drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK with BINARY/VARBINARY primary keys: literal, hex and prepared keys.
-- Every check projects HEX(id), OCTET_LENGTH(id) and full row values; each
-- case ends with an unselected-row and source-integrity check.
-- ================================================================

-- case 1: BINARY(4) primary key. The string literal 'a' and the raw hex
-- forms x'61'/0x61 address the same stored zero-padded 0x61 key, while
-- '0x61' is its own four-ASCII-byte key. Row val=13 stays unselected.
create table t0 (id binary(4), val int, primary key(id));
insert into t0 values ('aa', 1), ('ab', 2);
data branch create table t1s from t0;
data branch create table t1n from t0;
data branch create table t1x from t0;
data branch create table t1p from t0;
data branch create table t2 from t0;

insert into t2 values (x'61', 11), ('0x61', 12), ('cd', 13);
data branch pick t2 into t1s keys('a');
select hex(id), octet_length(id), val from t1s order by val;
data branch pick t2 into t1n keys(0x61);
select hex(id), octet_length(id), val from t1n order by val;
data branch pick t2 into t1x keys(x'61');
select hex(id), octet_length(id), val from t1x order by val;

-- SQL PREPARE: bound bytes participate verbatim, so a bound '0x61' is its
-- own four-ASCII-byte key and never a second hexadecimal decoding.
prepare ps from 'data branch pick t2 into t1p keys(?)';
set @k = 'a';
execute ps using @k;
select hex(id), octet_length(id), val from t1p order by val;
set @k = '0x61';
execute ps using @k;
select hex(id), octet_length(id), val from t1p order by val;
deallocate prepare ps;

-- unselected row val=13 remains in the source and out of every target
select hex(id), octet_length(id), val from t2 order by val;
drop table t0;
drop table t1s;
drop table t1n;
drop table t1x;
drop table t1p;
drop table t2;

-- case 2: VARBINARY(8) primary key keeps raw byte lengths: the trailing-NUL
-- base key 'a\0' (6100) stays distinct from the short 'a' (61), and the hex
-- spelling x'626100' addresses the trailing-NUL key exactly.
create table v0 (id varbinary(8), val int, primary key(id));
insert into v0 values ('za', 1), ('a\0', 2);
data branch create table v1 from v0;
data branch create table v2 from v0;
insert into v2 values ('a', 11), ('0x61', 12), ('ba\0', 13);

data branch pick v2 into v1 keys(x'61');
select hex(id), octet_length(id), val from v1 order by val;
data branch pick v2 into v1 keys('0x61');
select hex(id), octet_length(id), val from v1 order by val;
data branch pick v2 into v1 keys(x'626100');
select hex(id), octet_length(id), val from v1 order by val;

-- every source row is unchanged by PICK
select hex(id), octet_length(id), val from v2 order by val;
drop table v0;
drop table v1;
drop table v2;

-- case 3: composite primary key with a BINARY(3) component; both PK columns
-- and both spellings of the same component key select each row.
create table c0 (a int, b binary(3), val int, primary key(a, b));
insert into c0 values (1, 'a', 1), (1, 'b', 2);
data branch create table c1 from c0;
data branch create table c2 from c0;
insert into c2 values (2, x'63', 3), (3, 'dd', 4);

data branch pick c2 into c1 keys((2, 'c'), (3, x'6464'));
select a, hex(b), octet_length(b), val from c1 order by val;
select a, hex(b), val from c2 order by val;
drop table c0;
drop table c1;
drop table c2;

drop database test;
