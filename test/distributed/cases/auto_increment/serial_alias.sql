-- #31748: SERIAL is the MySQL alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
-- The alias must reuse the canonical allocation and unique enforcement, show the
-- identical catalog definition, and share the ALTER ADD column grammar.
drop database if exists serial_alias_bvt;
create database serial_alias_bvt;
use serial_alias_bvt;

-- alias spelling vs canonical spelling must produce identical definitions
create table t_alias (id serial, v int);
create table t_canon (id bigint unsigned not null auto_increment unique, v int);
show create table t_alias;
show create table t_canon;

-- allocation follows the canonical auto_increment semantics for both
-- spellings: two omitted-id rows (one with NULL payload) plus two explicit
-- (id, v) rows with id = NULL — four rows total per spelling
insert into t_alias (v) values (10), (NULL);
insert into t_alias (id, v) values (NULL, 20), (NULL, 30);
insert into t_canon (v) values (10), (NULL);
insert into t_canon (id, v) values (NULL, 20), (NULL, 30);
select id, v from t_alias order by id;
select id, v from t_canon order by id;

-- unique enforcement: duplicate explicit id is rejected; the same connection
-- continues and the next insert allocates
-- @ignore:0
insert into t_alias (id, v) values (1, 1);
insert into t_alias (v) values (40);
select id, v from t_alias order by id;
select count(*) from t_alias;
select count(*) from t_canon;

-- unsigned boundary: explicit negative id is rejected exactly as on the
-- canonical unsigned column, allocation continues afterwards
-- @ignore:0
insert into t_alias (id, v) values (-1, -1);
select max(id) from t_alias;
insert into t_alias (v) values (50);
select max(id) from t_alias;

-- ALTER ADD accepts SERIAL through the same column-definition grammar
create table t_alt (v int);
alter table t_alt add column id serial;
show create table t_alt;
insert into t_alt (v) values (1), (2);
select id, v from t_alt order by id;

drop database serial_alias_bvt;
