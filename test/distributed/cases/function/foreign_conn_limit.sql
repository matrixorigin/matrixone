-- The per-session foreign-connection cache is bounded: the 17th DISTINCT
-- config is rejected with an actionable error, and disconnecting frees a
-- slot. NOTE: the BVT runner shares one session across case files, so this
-- file (a) assumes it is the alphabetically-first case that opens foreign
-- connections and (b) disconnects every handle it opened before finishing.
drop database if exists conn_limit;
create database conn_limit;
use conn_limit;
set @h1 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=1s"}');
select @h1 is not null;
set @h2 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=2s"}');
select @h2 is not null;
set @h3 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=3s"}');
select @h3 is not null;
set @h4 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=4s"}');
select @h4 is not null;
set @h5 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=5s"}');
select @h5 is not null;
set @h6 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=6s"}');
select @h6 is not null;
set @h7 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=7s"}');
select @h7 is not null;
set @h8 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=8s"}');
select @h8 is not null;
set @h9 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=9s"}');
select @h9 is not null;
set @h10 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=10s"}');
select @h10 is not null;
set @h11 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=11s"}');
select @h11 is not null;
set @h12 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=12s"}');
select @h12 is not null;
set @h13 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=13s"}');
select @h13 is not null;
set @h14 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=14s"}');
select @h14 is not null;
set @h15 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=15s"}');
select @h15 is not null;
-- whitespace variants canonicalize to the SAME handle: no new slot consumed.
select sql_tvf_connect('{ "driver": "mysql",  "dsn": "dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=15s" }') = @h15;
set @h16 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=16s"}');
-- the 17th distinct config is rejected.
select sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=17s"}');
-- disconnecting frees a slot.
select sql_tvf_disconnect(@h16);
set @h17 = sql_tvf_connect('{"driver":"mysql","dsn":"dump:111@tcp(127.0.0.1:6001)/conn_limit?timeout=17s"}');
select @h17 is not null;
-- leave the shared session clean: disconnect every remaining handle.
select sql_tvf_disconnect(@h1);
select sql_tvf_disconnect(@h2);
select sql_tvf_disconnect(@h3);
select sql_tvf_disconnect(@h4);
select sql_tvf_disconnect(@h5);
select sql_tvf_disconnect(@h6);
select sql_tvf_disconnect(@h7);
select sql_tvf_disconnect(@h8);
select sql_tvf_disconnect(@h9);
select sql_tvf_disconnect(@h10);
select sql_tvf_disconnect(@h11);
select sql_tvf_disconnect(@h12);
select sql_tvf_disconnect(@h13);
select sql_tvf_disconnect(@h14);
select sql_tvf_disconnect(@h15);
select sql_tvf_disconnect(@h17);
drop database conn_limit;
