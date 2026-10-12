-- @suite
-- @case
-- @desc: JSON_TABLE core consumers, policy admission, join predicates and lifecycle
drop database if exists json_table_core;
drop stage if exists json_table_core_stage;
create database json_table_core;
use json_table_core;
set @json_table_saved_mode=@@sql_mode;
set sql_mode='';
-- Constant text, JSON, ordinality, EXISTS and empty-result metadata.
select jt.n from json_table('[1,2]', '$[*]' columns(n int path '$')) jt order by jt.n;
select jt.o,jt.n,jt.present from json_table(cast('[{"n":7},{}]' as json), '$[*]' columns(o for ordinality,n int path '$.n' default '9' on empty,present int exists path '$.n')) jt order by jt.o;
select jt.n from json_table('[]', '$[*]' columns(n int path '$')) jt;
select jt.n from json_table(null, '$[*]' columns(n int path '$')) jt;
-- Correlated CROSS, ON, USING and NATURAL must not silently become Cartesian.
create table jt_sources(id int,doc text);
insert into jt_sources values(1,'[1,2]'),(2,'[2,3]'),(3,'[]'),(4,null);
select t.id,jt.n from jt_sources t,json_table(t.doc, '$[*]' columns(n int path '$')) jt order by t.id,jt.n;
select t.id,jt.n from jt_sources t join json_table(t.doc, '$[*]' columns(n int path '$')) jt on t.id=jt.n order by t.id,jt.n;
select count(*) as rejected_rows from jt_sources t join json_table(t.doc, '$[*]' columns(n int path '$')) jt on jt.n<0;
select id from jt_sources t join json_table(t.doc, '$[*]' columns(id int path '$')) jt using(id) order by id;
select id from jt_sources t natural join json_table(t.doc, '$[*]' columns(id int path '$')) jt order by id;
-- G3 LEFT/reversed syntax/view compatibility are explicitly rejected.
select t.id,jt.n from jt_sources t left join json_table(t.doc, '$[*]' columns(n int path '$')) jt on true;
select jt.n from json_table('[]', '$[*]' columns(n int path '$' null on error null on empty)) jt;
create view jt_unsupported_view as select * from json_table('[1]', '$[*]' columns(n int path '$')) jt;
-- Default admission cannot depend on whether a source row will be emitted.
select jt.n from json_table('[{}]', '$[*]' columns(n int path '$.n' default '7' on empty)) jt;
select jt.n from json_table('["bad"]', '$[*]' columns(n int path '$' default '8' on error)) jt;
select jt.n from json_table('["bad"]', '$[*]' columns(n int path '$' null on error)) jt;
select jt.n from json_table('[]', '$[*]' columns(n int path '$' default '"bad"' on empty)) jt;
select jt.n from json_table('[]', '$[*]' columns(n tinyint path '$' default '300' on error)) jt;
select jt.n from json_table('[]', '$[*]' columns(n int path '$' default '{}' on error)) jt;
select jt.n from json_table('[]', '$[*]' columns(n int path '$' default 'invalid' on empty)) jt;
select jt.n from json_table('invalid', '$[*]' columns(n int path '$')) jt;
select jt.n from json_table('["bad"]', '$[*]' columns(n int path '$' error on error)) jt;
select jt.n from json_table('[4]', '$[*]' columns(n int path '$')) jt;
-- Nested siblings add rows; defaults must not fill unmatched nested sources.
select jt.a,jt.b from json_table('{"a":[],"b":[10,20]}', '$' columns(nested path '$.a[*]' columns(a int path '$' default '7' on empty),nested path '$.b[*]' columns(b int path '$'))) jt order by jt.b;
select jt.a,jt.b from json_table('{"a":[],"b":[]}', '$' columns(nested path '$.a[*]' columns(a int path '$' default '7' on empty),nested path '$.b[*]' columns(b int path '$'))) jt;
-- Each prepared EXECUTE starts a fresh document/diagnostic generation.
prepare jt_stmt from 'select jt.n from json_table(?, ''$[*]'' columns(n int path ''$'')) jt order by jt.n';
set @jt_doc='[5,6]';
execute jt_stmt using @jt_doc;
set @jt_doc='[7]';
execute jt_stmt using @jt_doc;
deallocate prepare jt_stmt;
-- Successful early LIMIT retains exactly one local column truncation warning.
select jt.v from json_table('[1.25,2.75]', '$[*]' columns(v decimal(3,1) path '$')) jt limit 1;
show count(*) warnings;
show warnings;
select jt.v from json_table('[1.25,2.75]', '$[*]' columns(v decimal(3,1) path '$')) jt;
show count(*) warnings;
-- Scalar edges without a verified MySQL policy fail closed, not NULL ON ERROR.
select jt.v from json_table('["ab"]', '$[*]' columns(v varchar(1) path '$' null on error)) jt;
select jt.v from json_table('[1.25]', '$[*]' columns(v int path '$' null on error)) jt;
select jt.v from json_table('[]', '$[*]' columns(v int path '$' default '1.25' on empty)) jt;
-- One committed warning across output batches and no retained presentation list.
set @jt_saved_max_error_count=@@max_error_count;
set max_error_count=0;
select count(*) as rows_seen,min(jt.v) as minimum,max(jt.v) as maximum from json_table(concat('[',repeat('1.25,',8192),'1.25]'), '$[*]' columns(v decimal(3,1) path '$')) jt;
show count(*) warnings;
show warnings;
set max_error_count=@jt_saved_max_error_count;
-- A failed attempt discards warnings even after the first successful output batch.
select sum(jt.v) from json_table(concat('[',repeat('1.25,',8192),'{}]'), '$[*]' columns(v decimal(3,1) path '$' error on error)) jt;
show count(*) warnings;
show warnings;
-- Same column name in two distinct function occurrences has two local identities.
select a.v,b.v from json_table('[1.25]', '$[*]' columns(v decimal(3,1) path '$')) a,json_table('[2.75]', '$[*]' columns(v decimal(3,1) path '$')) b;
show count(*) warnings;
-- Correct string literal bytes under both modes, without CAST in PATH/DEFAULT.
select hex(jt.v) as encoded from json_table('{}', '$' columns(v varchar(20) path '$.missing' default '"a\\\\b''c"' on empty)) jt;
select jt.n from json_table(json_object('a\\b''c',7), '$."a\\\\b''c"' columns(n int path '$')) jt;
set sql_mode='NO_BACKSLASH_ESCAPES,ANSI_QUOTES';
select hex(jt.v) as encoded from json_table('{}', '$' columns(v varchar(20) path '$.missing' default '"a\\b''c"' on empty)) jt;
select jt.n from json_table(json_object('a\b''c',7), '$."a\\b''c"' columns(n int path '$')) jt;
set sql_mode='';
-- Direct and correlated stage-DATALINK, plus reader failure and next-query health.
create stage json_table_core_stage url='file://$resources/json_table/';
select jt.n from json_table(cast('stage://json_table_core_stage/input.json' as datalink), '$[*]' columns(n int path '$')) jt order by jt.n;
create table jt_links(id int,doc datalink);
insert into jt_links values(1,cast('stage://json_table_core_stage/input.json' as datalink));
select t.id,jt.n from jt_links t,json_table(t.doc, '$[*]' columns(n int path '$')) jt order by t.id,jt.n;
select jt.n from json_table(cast('stage://json_table_core_stage/missing.json' as datalink), '$[*]' columns(n int path '$' null on error)) jt;
select jt.n from json_table('[8]', '$[*]' columns(n int path '$')) jt;
set sql_mode=@json_table_saved_mode;
drop stage json_table_core_stage;
drop database json_table_core;
