-- issue #25119: CREATE TABLE IF NOT EXISTS <t> LIKE <s> must be a no-op when
-- <t> already exists, instead of raising ERROR 1050. The IF NOT EXISTS clause
-- was dropped by the parser for the LIKE form.

drop database if exists t25119;
create database t25119;
use t25119;

create table foo (id int, name varchar(20));
insert into foo values (1,'a'),(2,'b');

-- first call creates the shadow table
create table if not exists foo_shadow like foo;
show create table foo_shadow;

-- repeated calls are silent no-ops (no error, table unchanged)
create table if not exists foo_shadow like foo;
create table if not exists foo_shadow like foo;
show tables;

-- the shadow table is an independent empty copy of the source schema
desc foo_shadow;
select count(*) from foo_shadow;

-- without IF NOT EXISTS, recreating an existing table still errors
create table foo_shadow like foo;

-- IF NOT EXISTS LIKE on a non-existing target still creates it
create table if not exists foo_shadow2 like foo;
show tables;

-- TEMPORARY + IF NOT EXISTS LIKE is also idempotent
create temporary table if not exists foo_tmp like foo;
create temporary table if not exists foo_tmp like foo;
select count(*) from foo_tmp;

drop table if exists foo_tmp;
drop table foo_shadow2;
drop table foo_shadow;
drop table foo;
drop database t25119;
