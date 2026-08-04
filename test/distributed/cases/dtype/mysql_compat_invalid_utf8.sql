-- @suite

-- @case
-- @desc: MySQL strict mode rejects invalid UTF-8 at binary-to-text assignment boundaries; MATRIXONE_NATIVE preserves raw-byte writes
-- @label:bvt

drop database if exists mysql_compat_invalid_utf8;
create database mysql_compat_invalid_utf8;
use mysql_compat_invalid_utf8;

set @old_sql_mode = @@session.sql_mode;
-- ERROR 1366 is MySQL's strict-mode behavior. Keep this explicit instead of
-- relying on the server default sql_mode.
set session sql_mode = 'STRICT_TRANS_TABLES';

create table t_invalid_utf8 (
    id int primary key,
    c char(10) character set utf8mb4,
    v varchar(10) character set utf8mb4,
    t text character set utf8mb4,
    b binary(2),
    vb varbinary(10),
    bl blob
);

-- Every UTF-8 text target rejects the malformed sequence C3 28 in
-- MySQL-compatible mode. Each failed statement must leave no row behind.
insert into t_invalid_utf8 (id, c) values (1, unhex('C328'));
insert into t_invalid_utf8 (id, v) values (2, unhex('C328'));
insert into t_invalid_utf8 (id, t) values (3, unhex('C328'));
select count(*) as rejected_text_rows from t_invalid_utf8;

-- Binary targets remain raw-byte containers in MySQL-compatible mode.
insert into t_invalid_utf8 (id, c, v, t, b, vb, bl)
    values (10, 'ok', 'ok', 'ok', unhex('C328'), unhex('C328'), unhex('C328'));
select id, hex(b) as b_hex, hex(vb) as vb_hex, hex(bl) as bl_hex
    from t_invalid_utf8 where id = 10;

-- UPDATE uses the same assignment boundary and rejects malformed bytes.
insert into t_invalid_utf8 (id, c, v, t) values (11, 'old', 'old', 'old');
update t_invalid_utf8 set c = unhex('C328') where id = 11;
update t_invalid_utf8 set v = unhex('C328') where id = 11;
update t_invalid_utf8 set t = unhex('C328') where id = 11;
select id, c, v, t from t_invalid_utf8 where id = 11;

-- Native mode deliberately preserves MatrixOne's historical raw-byte behavior
-- for both INSERT and UPDATE text assignments.
set session sql_mode = 'MATRIXONE_NATIVE';
insert into t_invalid_utf8 (id, c, v, t, b, vb, bl)
    values (20, unhex('C328'), unhex('C328'), unhex('C328'),
            unhex('C328'), unhex('C328'), unhex('C328'));
select id, hex(c) as c_hex, hex(v) as v_hex, hex(t) as t_hex,
       hex(b) as b_hex, hex(vb) as vb_hex, hex(bl) as bl_hex
    from t_invalid_utf8 where id = 20;

update t_invalid_utf8
    set c = unhex('C328'), v = unhex('C328'), t = unhex('C328')
    where id = 11;
select id, hex(c) as c_hex, hex(v) as v_hex, hex(t) as t_hex
    from t_invalid_utf8 where id = 11;

-- Compatible-mode writes must revalidate values created in MATRIXONE_NATIVE,
-- including same-type INSERT SELECT and UPDATE assignments that need no
-- ordinary conversion cast.
set session sql_mode = 'STRICT_TRANS_TABLES';
create table t_invalid_utf8_copy (
    id int primary key,
    c char(10) character set utf8mb4,
    v varchar(10) character set utf8mb4,
    t text character set utf8mb4
);
insert into t_invalid_utf8_copy (id, c, v, t)
    select id, c, v, t from t_invalid_utf8 where id = 20;
select count(*) as strict_copy_rows from t_invalid_utf8_copy;
insert into t_invalid_utf8_copy values (21, 'old', 'old', 'old');
update t_invalid_utf8_copy
    set c = (select c from t_invalid_utf8 where id = 20),
        v = (select v from t_invalid_utf8 where id = 20),
        t = (select t from t_invalid_utf8 where id = 20)
    where id = 21;
select id, c, v, t from t_invalid_utf8_copy where id = 21;

-- Keep every strict same-type and cross-type assignment independent: a
-- rejected CHAR write must not conceal a VARCHAR/TEXT bypass in the same DML.
insert into t_invalid_utf8_copy (id, c)
    select id + 100, c from t_invalid_utf8 where id = 20;
insert into t_invalid_utf8_copy (id, v)
    select id + 101, v from t_invalid_utf8 where id = 20;
insert into t_invalid_utf8_copy (id, t)
    select id + 102, t from t_invalid_utf8 where id = 20;
create table t_invalid_utf8_cross (
    id int primary key,
    c char(10) character set utf8mb4,
    v varchar(10) character set utf8mb4,
    t text character set utf8mb4
);
insert into t_invalid_utf8_cross (id, v)
    select id + 110, c from t_invalid_utf8 where id = 20;
insert into t_invalid_utf8_cross (id, t)
    select id + 111, v from t_invalid_utf8 where id = 20;
insert into t_invalid_utf8_cross (id, c)
    select id + 112, t from t_invalid_utf8 where id = 20;
select count(*) as strict_cross_copy_rows from t_invalid_utf8_cross;
drop table t_invalid_utf8_cross;

-- MySQL non-strict and IGNORE writes retain only the valid prefix before an
-- invalid sequence. C328 has no valid prefix, so it is stored as ''.
set session sql_mode = '';
insert into t_invalid_utf8_copy (id, c) values (30, unhex('41C32842'));
insert into t_invalid_utf8_copy (id, v) values (31, unhex('C328'));
insert into t_invalid_utf8_copy (id, t) values (32, unhex('41C32842'));
set session sql_mode = 'STRICT_TRANS_TABLES';
insert ignore into t_invalid_utf8_copy (id, c) values (40, unhex('41C32842'));
insert ignore into t_invalid_utf8_copy (id, v) values (41, unhex('C328'));
insert ignore into t_invalid_utf8_copy (id, t) values (42, unhex('41C32842'));

-- UPDATE follows the same mode matrix. Seed valid data, then exercise each
-- text destination separately for ordinary non-strict and strict IGNORE DML.
insert into t_invalid_utf8_copy values
    (50, 'old', 'old', 'old'),
    (51, 'old', 'old', 'old'),
    (52, 'old', 'old', 'old'),
    (60, 'old', 'old', 'old'),
    (61, 'old', 'old', 'old'),
    (62, 'old', 'old', 'old');
set session sql_mode = '';
update t_invalid_utf8_copy set c = unhex('41C32842') where id = 50;
update t_invalid_utf8_copy set v = unhex('C328') where id = 51;
update t_invalid_utf8_copy set t = unhex('41C32842') where id = 52;
set session sql_mode = 'STRICT_TRANS_TABLES';
update ignore t_invalid_utf8_copy set c = unhex('41C32842') where id = 60;
update ignore t_invalid_utf8_copy set v = unhex('C328') where id = 61;
update ignore t_invalid_utf8_copy set t = unhex('41C32842') where id = 62;
select id, hex(c) as c_hex, hex(v) as v_hex, hex(t) as t_hex
    from t_invalid_utf8_copy
    where id in (30, 31, 32, 40, 41, 42, 50, 51, 52, 60, 61, 62)
    order by id;
drop table t_invalid_utf8_copy;

drop table t_invalid_utf8;
set session sql_mode = @old_sql_mode;
drop database mysql_compat_invalid_utf8;
