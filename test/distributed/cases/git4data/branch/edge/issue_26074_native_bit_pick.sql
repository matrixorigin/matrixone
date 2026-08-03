-- Regression for issue #26074.
-- DATA BRANCH PICK accepts BOOL plus decimal and native BIT literal keys.
drop database if exists bvt_issue_26074;
create database bvt_issue_26074;
use bvt_issue_26074;

-- BOOL literals update only the selected rows.
create table bool_base(k bool primary key, payload varchar(32), n int);
insert into bool_base values(false, 'base-false', 0), (true, 'base-true', 1);
data branch create table bool_src from bool_base;
data branch create table bool_dst from bool_base;
update bool_src set payload = concat(payload, '-src'), n = n + 10;
data branch pick bool_src into bool_dst keys(false);
select k, payload, n from bool_dst order by k;
data branch pick bool_src into bool_dst keys(true);
select k, payload, n from bool_dst order by k;

-- Numeric BOOL keys cover deletes and conflict handling.
data branch create table bool_conf_src from bool_base;
data branch create table bool_conf_dst from bool_base;
delete from bool_conf_src where payload = 'base-false';
update bool_conf_src set payload = 'src-true' where payload = 'base-true';
data branch pick bool_conf_src into bool_conf_dst keys(0);
select k, payload from bool_conf_dst order by k;
update bool_conf_dst set payload = 'dst-true' where payload = 'base-true';
-- @regex("conflict: bool_conf_src INSERT and bool_conf_dst INSERT on pk\(true\) with different values",true)
data branch pick bool_conf_src into bool_conf_dst keys(1) when conflict fail;
select k, payload from bool_conf_dst order by k;
data branch pick bool_conf_src into bool_conf_dst keys(1) when conflict accept;
select k, payload from bool_conf_dst order by k;

-- BIT(1) accepts its complete native-literal key range.
create table bit1_base(k bit(1) primary key, payload varchar(32));
insert into bit1_base values(b'0', 'zero'), (b'1', 'one');
data branch create table bit1_src from bit1_base;
data branch create table bit1_dst from bit1_base;
update bit1_src set payload = concat(payload, '-src');
data branch pick bit1_src into bit1_dst keys(b'0', B'1');
select hex(k), payload from bit1_dst order by k;

-- The exact decimal BIT path from the issue remains equivalent to a typed subquery.
create table bit8_decimal_base(k bit(8) primary key, payload varchar(32));
insert into bit8_decimal_base values(b'00000001', 'one'), (b'00000010', 'two');
data branch create table bit8_decimal_src from bit8_decimal_base;
data branch create table bit8_decimal_dst from bit8_decimal_base;
update bit8_decimal_src set payload = concat(payload, '-src');
data branch pick bit8_decimal_src into bit8_decimal_dst keys(1);
select hex(k), payload from bit8_decimal_dst order by k;
data branch pick bit8_decimal_src into bit8_decimal_dst
    keys(select k from bit8_decimal_src where hex(k) = '2');
select hex(k), payload from bit8_decimal_dst order by k;

-- Native BIT(8) literals cover lower/upper boundaries, update, delete, and no-op.
create table bit8_base(k bit(8) primary key, payload varchar(32));
insert into bit8_base values
    (b'00000000', 'zero'),
    (b'00000001', 'one'),
    (b'01111111', 'one-two-seven'),
    (b'11111111', 'two-five-five');
data branch create table bit8_src from bit8_base;
data branch create table bit8_dst from bit8_base;
update bit8_src set payload = 'zero-src' where hex(k) = '0';
update bit8_src set payload = 'one-two-seven-src' where hex(k) = '7F';
update bit8_src set payload = 'two-five-five-src' where hex(k) = 'FF';
delete from bit8_src where hex(k) = '1';
data branch pick bit8_src into bit8_dst
    keys(b'00000000', b'00000001', b'00000011', B'01111111', b'11111111');
select hex(k), payload from bit8_dst order by k;

-- BOOL and native BIT literals work together in composite primary keys.
create table mixed_base(
    flag bool,
    k bit(8),
    payload varchar(32),
    primary key(flag, k)
);
insert into mixed_base values
    (false, b'00000001', 'false-one'),
    (false, b'00000010', 'false-two'),
    (true, b'00000001', 'true-one'),
    (true, b'11111111', 'true-max');
data branch create table mixed_src from mixed_base;
data branch create table mixed_dst from mixed_base;
update mixed_src set payload = concat(payload, '-src');
delete from mixed_src where flag = false and hex(k) = '2';
data branch pick mixed_src into mixed_dst
    keys((false, b'00000001'), (false, B'00000010'), (true, b'11111111'));
select flag, hex(k), payload from mixed_dst order by flag, k;

-- BIT(64) accepts native literals across the signed boundary and at uint64 max.
create table bit64_base(k bit(64) primary key, payload varchar(32));
insert into bit64_base values
    (0, 'zero'),
    (9223372036854775808, 'high-bit'),
    (18446744073709551615, 'max');
data branch create table bit64_src from bit64_base;
data branch create table bit64_dst from bit64_base;
update bit64_src set payload = concat(payload, '-src');
data branch pick bit64_src into bit64_dst keys(
    b'0000000000000000000000000000000000000000000000000000000000000000',
    b'1000000000000000000000000000000000000000000000000000000000000000',
    b'1111111111111111111111111111111111111111111111111111111111111111'
);
select hex(k), payload from bit64_dst order by k;

-- Decimal and native BIT(8) overflow fail before any selected row is applied.
data branch create table bit8_bad_src from bit8_base;
data branch create table bit8_bad_dst from bit8_base;
update bit8_bad_src set payload = 'changed' where hex(k) in ('1', 'FF');
-- @regex("data type bit\(8\), value '256'",true)
data branch pick bit8_bad_src into bit8_bad_dst keys(255, 256);
select hex(k), payload from bit8_bad_dst order by k;
-- @regex("data type bit\(8\), value '-1'",true)
data branch pick bit8_bad_src into bit8_bad_dst keys(-1);
select hex(k), payload from bit8_bad_dst order by k;
-- @regex("data type bit\(8\), value '256'",true)
data branch pick bit8_bad_src into bit8_bad_dst keys(b'11111111', b'100000000');
select hex(k), payload from bit8_bad_dst order by k;

drop database bvt_issue_26074;
