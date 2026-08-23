-- The per-session foreign-connection cache is bounded: the 17th DISTINCT
-- config is rejected with an actionable error, and disconnecting frees a
-- slot. Runs in its own file so the session starts with an empty cache.
drop database if exists conn_limit;
create database conn_limit;
use conn_limit;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=1s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=2s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=3s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=4s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=5s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=6s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=7s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=8s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=9s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=10s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=11s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=12s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=13s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=14s"}') is not null;
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=15s"}') is not null;
-- whitespace variants canonicalize to the SAME handle: no new slot consumed.
select sql_tvf_connect('{ "driver": "mysql",  "dsn": "dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=15s" }') is not null;
set @h16 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=16s"}');
-- the 17th distinct config is rejected.
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=17s"}');
-- disconnecting frees a slot.
select sql_tvf_disconnect(@h16);
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=17s"}') is not null;
drop database conn_limit;
