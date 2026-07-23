-- string width enforcement on INSERT/UPDATE assignment paths, aligned with MySQL sql_mode semantics
drop database if exists insert_str_width;
create database insert_str_width;
use insert_str_width;
set @old_sql_mode = @@sql_mode;

-- ============================================================
-- (1) sql_mode gating on INSERT VALUES
-- ============================================================
create table t (c varchar(3));

-- strict mode: over-length non-space value is rejected (1406)
set session sql_mode = 'STRICT_TRANS_TABLES';
insert into t values ('abcd');

-- strict mode: over-length consisting only of trailing spaces is accepted (truncated)
insert into t values ('abc   ');
select c, char_length(c) from t;

-- non-strict mode: over-length value is truncated
set session sql_mode = '';
insert into t values ('wxyz');
select c, char_length(c) from t order by c;

-- ============================================================
-- (2) INSERT IGNORE downgrades over-length error to truncation (even under strict)
-- ============================================================
set session sql_mode = 'STRICT_TRANS_TABLES';
truncate table t;
insert ignore into t values ('abcd');
select c, char_length(c) from t;

-- non-literal VALUES expressions follow the same INSERT IGNORE downgrade
truncate table t;
insert ignore into t values (repeat('x', 4));
select c, char_length(c) from t;

-- prepared literal VALUES expressions resolve sql_mode at execution time
truncate table t;
set session sql_mode = 'STRICT_TRANS_TABLES';
prepare stmt_width_lenient from 'insert into t values (''prep'')';
set session sql_mode = '';
execute stmt_width_lenient;
select c, char_length(c) from t;
deallocate prepare stmt_width_lenient;

truncate table t;
set session sql_mode = '';
prepare stmt_width_strict from 'insert into t values (''fail'')';
set session sql_mode = 'STRICT_TRANS_TABLES';
execute stmt_width_strict;
select count(*) from t;
deallocate prepare stmt_width_strict;

-- ============================================================
-- (3) INSERT ... SELECT honors sql_mode
-- ============================================================
create table src (c varchar(10));
insert into src values ('1234567');
truncate table t;

set session sql_mode = 'STRICT_TRANS_TABLES';
insert into t select c from src;

set session sql_mode = '';
insert into t select c from src;
select c, char_length(c) from t;

-- ============================================================
-- (4) UPDATE honors sql_mode
-- ============================================================
truncate table t;
set session sql_mode = '';
insert into t values ('ab');
set session sql_mode = 'STRICT_TRANS_TABLES';
update t set c = 'abcd';
set session sql_mode = '';
update t set c = 'abcd';
select c, char_length(c) from t;

-- ============================================================
-- (5) ON DUPLICATE KEY UPDATE honors sql_mode
-- ============================================================
create table p (id int primary key, c varchar(3));
insert into p values (1, 'ab');

set session sql_mode = 'STRICT_TRANS_TABLES';
insert into p values (1, 'xx') on duplicate key update c = 'abcd';

set session sql_mode = '';
insert into p values (1, 'xx') on duplicate key update c = 'abcd';
select id, c, char_length(c) from p;

-- ============================================================
-- (6) INSERT IGNORE truncates over-long PK value, then ignores duplicate
-- ============================================================
create table pkv (c varchar(3) primary key);
set session sql_mode = 'STRICT_TRANS_TABLES';
insert into pkv values ('abc');
insert ignore into pkv values ('abcd');
select c, char_length(c) from pkv;
drop table pkv;

-- ============================================================
-- (7) DDL column DEFAULT length check is NOT relaxed by sql_mode (MySQL: DDL-layer)
-- ============================================================
set session sql_mode = '';
create table d (c varchar(3) default 'abcd');

set session sql_mode = 'STRICT_TRANS_TABLES';
create table d (c varchar(3) default 'abcd');

-- ============================================================
-- (8) stored generated CHAR/VARCHAR column follows DML sql_mode / INSERT IGNORE
-- ============================================================
create table g (id int primary key, t text, v varchar(1) generated always as (coalesce(t, '')) stored);

-- non-strict: over-length generated value is truncated
set session sql_mode = '';
insert into g values (1, 'ab');
select id, v, char_length(v) from g;

-- strict: over-length generated value is rejected (1406)
set session sql_mode = 'STRICT_TRANS_TABLES';
insert into g values (2, 'cd');

-- INSERT IGNORE: truncated even under strict
insert ignore into g values (2, 'cd');
select id, v, char_length(v) from g order by id;

-- UPDATE recomputes the generated value under sql_mode (non-strict truncates)
set session sql_mode = '';
update g set t = 'xy' where id = 1;
select id, v, char_length(v) from g where id = 1;
drop table g;

-- ============================================================
-- (9) JSON source honors the trailing-space exemption on assignment (cast_assign)
-- ============================================================
create table jsrc (jc json);
create table vdst (vc varchar(3));

-- trailing-space-only overflow is exempt under strict: truncated to 'abc'
insert into jsrc values ('"abc   "');
set session sql_mode = 'STRICT_TRANS_TABLES';
insert into vdst select jc from jsrc;
select vc, char_length(vc) from vdst;

-- real overflow is rejected under strict (1406)
truncate table jsrc;
insert into jsrc values ('"abcd"');
insert into vdst select jc from jsrc;
select count(*) from vdst;
drop table jsrc;
drop table vdst;

-- ============================================================
-- (10) TRADITIONAL is a strict combination mode
-- ============================================================
create table traditional_dst (c varchar(3));
set session sql_mode = 'TRADITIONAL';
insert into traditional_dst values ('abcd');
select count(*) from traditional_dst;
drop table traditional_dst;

-- ============================================================
-- (11) GEOMETRY-to-VARCHAR honors assignment width semantics
-- ============================================================
create table geometry_src (g geometry);
create table geometry_dst (c varchar(3));
insert into geometry_src values (st_geomfromtext('POINT(1 2)'));

set session sql_mode = 'STRICT_TRANS_TABLES';
insert into geometry_dst select g from geometry_src;
insert into geometry_dst values (st_geomfromtext('POINT(1 2)'));
select count(*) from geometry_dst;

set session sql_mode = '';
insert into geometry_dst select g from geometry_src;
insert into geometry_dst values (st_geomfromtext('POINT(1 2)'));
select c, char_length(c) from geometry_dst order by c;

select cast(st_geomfromtext('POINT(1 2)') as char(3));
drop table geometry_src;
drop table geometry_dst;

drop database insert_str_width;
set session sql_mode = @old_sql_mode;
select @@sql_mode = @old_sql_mode;
