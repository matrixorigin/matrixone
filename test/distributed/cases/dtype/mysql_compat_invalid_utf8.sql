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

drop table t_invalid_utf8;
set session sql_mode = @old_sql_mode;
drop database mysql_compat_invalid_utf8;
