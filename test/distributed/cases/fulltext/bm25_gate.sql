-- CREATE INDEX ... USING bm25 is gated behind the experimental_bm25_index session
-- variable (off by default). Without it the DDL is rejected; enabling it allows the
-- index, and querying then works.
drop database if exists bm25_gate;
create database bm25_gate;
use bm25_gate;
create table t (id bigint primary key, body text);
insert into t values (1,'apple banana'),(2,'apple');
-- explicitly turn the flag OFF (mo-tester reuses the session across files, so an
-- earlier bm25 case's `set experimental_bm25_index=1` can leak in).
set experimental_bm25_index = 0;
-- flag off -> rejected
create index ft using bm25 on t(body) with parser gojieba;
-- enable and retry -> succeeds
set experimental_bm25_index = 1;
create index ft using bm25 on t(body) with parser gojieba;
select id from t where bm25(body) against('apple');
drop database bm25_gate;
