-- SHOW CREATE TABLE must print string defaults so that replaying it keeps the same default (#31066).
drop database if exists show_create_default_quote;
create database show_create_default_quote;
use show_create_default_quote;
create table t1 (id int, a varchar(10) default 'it''s', b varchar(10) default 'x`y', c varchar(10) default "q""r", d varchar(10) default '');
show create table t1;
insert into t1(id) values (1);
select id, a, b, c, d from t1;
-- replay the SHOW CREATE TABLE output
create table t2 (
  `id` int DEFAULT NULL,
  `a` varchar(10) DEFAULT 'it''s',
  `b` varchar(10) DEFAULT 'x`y',
  `c` varchar(10) DEFAULT 'q"r',
  `d` varchar(10) DEFAULT ''
);
show create table t2;
insert into t2(id) values (2);
select id, a, b, c, d from t2;
drop database show_create_default_quote;
