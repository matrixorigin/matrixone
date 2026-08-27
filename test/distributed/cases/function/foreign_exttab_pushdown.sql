-- Predicate pushdown for ENGINE = SQL foreign external tables ('recheck' =
-- 'false'): the query text is wrapped as a derived table carrying the
-- deparsable conjuncts, so the source narrows the result instead of MO.
-- Covers: identical answers with and without pushdown, conjuncts the deparser
-- cannot express, query texts with a trailing ';' or line comment, IN-derived
-- query lists, SHOW CREATE round-trip, and the option's validation.
drop database if exists foreign_pushdown;
create database foreign_pushdown;
use foreign_pushdown;
create table src(id int, name varchar(50), amount decimal(12,2), created datetime);
insert into src values
 (1,'alice',100.50,'2026-01-01 10:00:00'),
 (2,'bob',NULL,'2026-01-02 11:00:00'),
 (3,'carol',75.25,'2026-01-03 12:00:00'),
 (4,'dave',20.00,'2026-01-04 13:00:00');

-- the same source through a table that pushes and one that does not
create external table plain (id int, name varchar(50), amount decimal(12,2), created datetime)
 engine = sql with ('config' = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_pushdown"}');
create external table pushed (id int, name varchar(50), amount decimal(12,2), created datetime)
 engine = sql with ('config' = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_pushdown"}', 'recheck' = 'false');

-- pushdown must not change a single answer
select id, name from plain where __mo_query = 'select id, name, amount, created from src order by id' and id > 2;
select id, name from pushed where __mo_query = 'select id, name, amount, created from src order by id' and id > 2;

-- comparisons, IS NULL, LIKE, BETWEEN, IN: all deparsable
select id from pushed where __mo_query = 'select id, name, amount, created from src' and amount is null;
select id from pushed where __mo_query = 'select id, name, amount, created from src' and name like 'c%';
select id from pushed where __mo_query = 'select id, name, amount, created from src' and id between 2 and 3 order by id;
select id from pushed where __mo_query = 'select id, name, amount, created from src' and id in (1,4) order by id;
select id from pushed where __mo_query = 'select id, name, amount, created from src' and (id = 1 or name = 'dave') order by id;

-- a conjunct the deparser cannot express stays local and still filters
select id from pushed where __mo_query = 'select id, name, amount, created from src' and abs(id - 3) < 1;
-- ... alongside one that is pushed
select id from pushed where __mo_query = 'select id, name, amount, created from src' and id > 1 and abs(id - 3) < 2 order by id;

-- a query text that is a valid statement but not a valid derived table body
-- verbatim: the wrapper trims the terminator and closes the comment
select id from pushed where __mo_query = 'select id, name, amount, created from src ;' and id > 3;
select id from pushed where __mo_query = 'select id, name, amount, created from src -- every row' and id > 3;

-- an IN list derives several queries; each one is wrapped
select id, name from pushed
where __mo_query in ('select id, name, amount, created from src where id < 3',
                     'select id, name, amount, created from src where id > 3')
  and id <> 1 order by id;

-- the 'query' table option is wrapped the same way as a predicate
create external table pushed_default (id int, name varchar(50), amount decimal(12,2), created datetime)
 engine = sql with ('config' = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/foreign_pushdown"}',
                    'query' = 'select id, name, amount, created from src',
                    'recheck' = 'false');
select id from pushed_default where id > 3;

-- SHOW CREATE renders the opt-in, and only the opt-in
show create table pushed;
show create table plain;

-- the wrapper filters on the DECLARED column names, so a query text that
-- projects different names works verbatim and fails once wrapped
select id from plain where __mo_query = 'select id as ident, name as nm, amount as amt, created as c from src' and id > 3;
select id from pushed where __mo_query = 'select id as ident, name as nm, amount as amt, created as c from src' and id > 3;

-- option validation
create external table bad_esql (a int) engine = esql
 with ('config' = '{"addresses":["http://127.0.0.1:9200"]}', 'recheck' = 'false');
create external table bad_value (a int) engine = sql
 with ('config' = '{"driver":"mysql","dsn":"d"}', 'recheck' = 'sometimes');

drop database foreign_pushdown;
