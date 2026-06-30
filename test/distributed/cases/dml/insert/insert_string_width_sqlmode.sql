-- string width enforcement on INSERT/UPDATE assignment paths, aligned with MySQL sql_mode semantics
drop database if exists insert_str_width;
create database insert_str_width;
use insert_str_width;

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
-- (6) DDL column DEFAULT length check is NOT relaxed by sql_mode (MySQL: DDL-layer)
-- ============================================================
set session sql_mode = '';
create table d (c varchar(3) default 'abcd');

set session sql_mode = 'STRICT_TRANS_TABLES';
create table d (c varchar(3) default 'abcd');

set session sql_mode = 'STRICT_TRANS_TABLES';
drop database insert_str_width;
