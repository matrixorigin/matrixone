-- fulltext2_threads_build sizes the parallel CREATE build (parallel tokenization and
-- background segment finishing). The built segments are identical at any value, so the
-- same table built with 1 thread and with 8 threads must answer every query identically.
-- A small max_index_capacity seals many segments so the pipeline hands off and writes
-- several of them; 2000 rows span several tokenization chunks.
set experimental_fulltext2_index = 1;
drop database if exists ft2_threads_build;
create database ft2_threads_build;
use ft2_threads_build;

-- default is 0 (resolves to the effective GOMAXPROCS), settable in [0,1024].
select @@fulltext2_threads_build as default_val;
set fulltext2_threads_build = 1024;
select @@fulltext2_threads_build as after_set;

create table src(id bigint primary key, body text, category int);
insert into src select result,
  concat('needle', repeat(' alpha', result % 5), repeat(' beta', result % 7), ' gamma', result % 13, ' delta', result % 97),
  result % 4
from generate_series(1, 2000) g;

create table t1(id bigint primary key, body text, category int);
insert into t1 select * from src;
create table t8(id bigint primary key, body text, category int);
insert into t8 select * from src;

set fulltext2_threads_build = 1;
create fulltext2 index ft1 on t1(body) include(category) max_index_capacity 50;
set fulltext2_threads_build = 8;
create fulltext2 index ft8 on t8(body) include(category) max_index_capacity 50;

select count(*) from t1 where match(body) against('alpha');
select count(*) from t8 where match(body) against('alpha');
select id, match(body) against('beta gamma7') as sc from t1 where match(body) against('beta gamma7') order by sc desc, id limit 10;
select id, match(body) against('beta gamma7') as sc from t8 where match(body) against('beta gamma7') order by sc desc, id limit 10;
-- Every matched (id, score) agrees between the serial and the pipelined build.
select count(*) from t1 where match(body) against('alpha gamma7');
select count(*) from t8 where match(body) against('alpha gamma7');
select count(*) as only_serial from (
  select id, match(body) against('alpha gamma7') as sc from t1 where match(body) against('alpha gamma7')
  except
  select id, match(body) against('alpha gamma7') as sc from t8 where match(body) against('alpha gamma7')) d;
select count(*) as only_pipelined from (
  select id, match(body) against('alpha gamma7') as sc from t8 where match(body) against('alpha gamma7')
  except
  select id, match(body) against('alpha gamma7') as sc from t1 where match(body) against('alpha gamma7')) d;
-- INCLUDE values are carried by the pipelined build.
select count(*) from t1 where match(body) against('needle') and category = 2;
select count(*) from t8 where match(body) against('needle') and category = 2;

-- token_filter = 'stop' (gojieba, bag-of-words) through the same pipeline.
create table s1(id bigint primary key, body text);
insert into s1 select id, concat('the ', body, ' of the') from src;
create table s8(id bigint primary key, body text);
insert into s8 select * from s1;
set fulltext2_threads_build = 1;
create fulltext2 index fs1 on s1(body) with parser gojieba position_free = true token_filter = 'stop' max_index_capacity 50;
set fulltext2_threads_build = 8;
create fulltext2 index fs8 on s8(body) with parser gojieba position_free = true token_filter = 'stop' max_index_capacity 50;
select count(*) from s1 where match(body) against('the alpha beta' in bm25 mode);
select count(*) from s8 where match(body) against('the alpha beta' in bm25 mode);
select count(*) as only_serial from (
  select id, match(body) against('the alpha beta' in bm25 mode) as sc from s1 where match(body) against('the alpha beta' in bm25 mode)
  except
  select id, match(body) against('the alpha beta' in bm25 mode) as sc from s8 where match(body) against('the alpha beta' in bm25 mode)) d;
select count(*) as only_pipelined from (
  select id, match(body) against('the alpha beta' in bm25 mode) as sc from s8 where match(body) against('the alpha beta' in bm25 mode)
  except
  select id, match(body) against('the alpha beta' in bm25 mode) as sc from s1 where match(body) against('the alpha beta' in bm25 mode)) d;
select count(*) from s8 where match(body) against('alpha' in bm25 mode);

set fulltext2_threads_build = 0;
drop database ft2_threads_build;
