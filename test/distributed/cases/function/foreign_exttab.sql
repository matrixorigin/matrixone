-- ENGINE = SQL foreign external table over a loopback connection to the MO
-- server itself. Covers: __mo_query = / IN derivation, local predicates,
-- __mo_query projection, the 'query' option default, session-var config
-- fallback, prepare with __mo_query = ?, INSERT ... SELECT ETL, SHOW CREATE
-- redaction, hidden-column invisibility, and the error cases.
drop database if exists foreign_exttab;
create database foreign_exttab;
use foreign_exttab;
create table src(id int, name varchar(50), amount decimal(12,2), created datetime);
insert into src values
 (1,'alice',100.50,'2026-01-01 10:00:00'),
 (2,'bob',NULL,'2026-01-02 11:00:00'),
 (3,'c,d',75.25,'2026-01-03 12:00:00');

create external table orders (
  id bigint,
  name varchar(64),
  amount decimal(12,2),
  created datetime
) engine = sql with ('config' = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_exttab"}');

-- basic scan: types, NULL, embedded comma round-trip.
select id, name, amount from orders
where __mo_query = 'select id, name, amount, created from src order by id';

-- select * hides __mo_query.
select * from orders where __mo_query = 'select id, name, amount, created from src where id = 2';

-- desc hides __mo_query.
desc orders;

-- MO evaluates ordinary predicates locally on the returned rows.
select id, name from orders
where __mo_query = 'select id, name, amount, created from src'
  and amount > 80 order by id;

-- IN of two queries; each row tagged with the query that produced it.
select id, name, __mo_query from orders
where __mo_query in (
  'select id, name, amount, created from src where id = 1',
  'select id, name, amount, created from src where id = 3')
order by id;

-- OR of two equalities.
select id from orders
where __mo_query = 'select id, name, amount, created from src where id = 1'
   or __mo_query = 'select id, name, amount, created from src where id = 2'
order by id;

-- a non-generating query-level conjunct (LIKE) prunes the derived list.
select id, __mo_query from orders
where __mo_query in (
  'select id, name, amount, created from src where id = 1',
  'select id, name, amount, created from src where id = 3')
and __mo_query like '%id = 1%';

-- the 'query' table option is the default when no predicate derives a text.
create external table with_default (
  id bigint, name varchar(64), amount decimal(12,2), created datetime
) engine = sql with (
  'config' = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_exttab"}',
  'query'  = 'select id, name, amount, created from src order by id');
select id, name from with_default;

-- config falls back to @sql_tvf_config when the table has none.
create external table sess_cfg (id bigint, name varchar(64), amount decimal(12,2), created datetime) engine = sql;
select count(*) from sess_cfg where __mo_query = 'select id, name, amount, created from src';
set @sql_tvf_config = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_exttab"}';
select count(*) as c from sess_cfg where __mo_query = 'select id, name, amount, created from src';

-- ETL: INSERT ... SELECT FROM the foreign table.
create table etl_target(id bigint, name varchar(64));
insert into etl_target select id, name from with_default;
select count(*) as loaded from etl_target;

-- prepare with __mo_query = ? (derived at EXECUTE time).
prepare s1 from 'select id, name from orders where __mo_query = ?';
set @q = 'select id, name, amount, created from src where id = 2';
execute s1 using @q;
deallocate prepare s1;

-- SHOW CREATE redacts an inline config; the 'query' option is emitted.
-- @separator:table
show create table with_default;

-- error: no __mo_query predicate and no 'query' option.
select * from orders;

-- error: bad foreign SQL text surfaces the source error.
select * from orders where __mo_query = 'select nope from no_such';

-- error: source returns a different column count than declared.
select * from orders where __mo_query = 'select id, name from src';

-- error: writes are rejected.
insert into orders values (9,'x',1.0,'2026-01-01 00:00:00');

-- error: unknown option at CREATE time.
create external table badopt (id int) engine = sql with ('recheck'='true');

-- error: bad driver in an inline config is rejected at CREATE time.
create external table badcfg (id int) engine = sql with ('config'='{"driver":"nope","dsn":"x"}');

drop database foreign_exttab;
