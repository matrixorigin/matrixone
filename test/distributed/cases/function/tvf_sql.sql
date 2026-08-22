-- sql_tvf: query the MatrixOne server itself through a loopback foreign SQL
-- connection. Exercises schema typing, NULL handling, column pruning, predicate
-- pushdown on top of the foreign result, multi-instance joins, the schema-less
-- JSON-array mode, the default @sql_tvf_config connection, and connect/disconnect.
drop database if exists sql_tvf_test;
create database sql_tvf_test;
use sql_tvf_test;
create table t(id int, name varchar(50), score int);
insert into t values (1,'alice',90),(2,'bob',NULL),(3,'carol',75),(4,'d,e',60),(5,'q"x',NULL);

-- connect back to this server over loopback; the handle is deterministic per config.
set @h = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/sql_tvf_test"}');

-- schema mode: typed columns, NULLs, and embedded comma/quote round-trip.
select * from sql_tvf('select id,name,score from t order by id', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) x;

-- NULL materializes as a real SQL NULL (expect 2).
select count(*) as null_scores from sql_tvf('select id,name,score from t', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) x where x.score is null;

-- column projection is mapped by name, so pruning never misaligns fields.
select x.id, x.score from sql_tvf('select id,name,score from t order by id', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) x where x.score is not null order by x.id;

-- MO applies predicates on top of the foreign result (expect id>2 => 3).
select count(*) as c from sql_tvf('select id,name,score from t', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) x where x.id > 2;

-- two TVF instances joined in one query.
select a.id, b.name from
  sql_tvf('select id,name,score from t', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) a
  join sql_tvf('select id,name,score from t', '{"cols":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"score","type":"int64"}]}', @h) b
  on a.id = b.id where a.id in (1,4) order by a.id;

-- schema-less mode: a single JSON-array column per row.
select * from sql_tvf('select id,name from t order by id', NULL, @h) x;

-- default connection resolved from @sql_tvf_config.
set @sql_tvf_config = '{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/sql_tvf_test"}';
select count(*) as c from sql_tvf('select * from t') x;

-- an unsupported driver errors clearly.
select sql_tvf_connect('{"driver":"nope","dsn":"x"}');

-- disconnect returns true; a second disconnect of the same handle returns false.
select sql_tvf_disconnect(@h) as disconnected;
select sql_tvf_disconnect(@h) as disconnected_again;
-- an unknown handle errors clearly (literal handle keeps the message deterministic).
select count(*) from sql_tvf('select 1', '{"cols":[{"name":"x","type":"int64"}]}', 'sql:deadbeefdeadbeef') x;

drop database sql_tvf_test;
