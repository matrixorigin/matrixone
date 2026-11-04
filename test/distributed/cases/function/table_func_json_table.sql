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
-- 字符集声明必须保留到输出类型；未声明的 VARCHAR 保持原有行为。
select jt.channel from json_table('["alpha","beta"]', '$[*]' columns(channel varchar(150) path '$')) jt order by jt.channel;
select jt.channel,charset(jt.channel),collation(jt.channel) from json_table('["alpha","beta"]', '$[*]' columns(channel varchar(150) character set utf8mb4 path '$')) jt order by jt.channel;
-- 宽度按字符计算，中文与四字节 Unicode 不应丢失或截断。
select jt.o,jt.v,hex(jt.v),char_length(jt.v) from json_table('["中","😀"]', '$[*]' columns(o for ordinality,v varchar(1) character set utf8mb4 path '$')) jt order by jt.o;
select jt.v from json_table('[{}]', '$[*]' columns(v varchar(4) character set utf8mb4 path '$.v' default '"中😀"' on empty)) jt;
-- 显式列声明独立于连接的字符集和排序规则，执行后恢复会话状态。
set @jt_saved_character_set_connection=@@character_set_connection;
set @jt_saved_collation_connection=@@collation_connection;
set character_set_connection='utf8mb3';
select jt.v,hex(jt.v),charset(jt.v) from json_table('["\\u4e2d","\\ud83d\\ude00"]', '$[*]' columns(v varchar(1) character set utf8mb4 path '$')) jt order by hex(jt.v);
set collation_connection='utf8mb4_bin';
select jt.v,charset(jt.v),collation(jt.v) from json_table('["alpha"]', '$[*]' columns(v varchar(20) character set utf8mb4 path '$')) jt;
set character_set_connection=@jt_saved_character_set_connection;
set collation_connection=@jt_saved_collation_connection;
-- 不支持或空的字符集即使面对空源也必须拒绝。
select jt.v from json_table('[]', '$[*]' columns(v varchar(20) character set latin1 path '$')) jt;
select jt.v from json_table('[]', '$[*]' columns(v varchar(20) character set '' path '$')) jt;
-- LangGraph 的关联 JSON_KEYS 查询必须保留每个 checkpoint 的 key 集。
create table jt_checkpoints(id int,checkpoint json);
insert into jt_checkpoints values(1,'{"channel_versions":{"alpha":1,"beta":2}}'),(2,'{"channel_versions":{"中":3,"😀":4}}');
select c.id,channels.channel from jt_checkpoints c,json_table(json_keys(c.checkpoint,'$.channel_versions'), '$[*]' columns(channel varchar(150) character set utf8mb4 path '$')) as channels order by c.id,hex(channels.channel);
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
-- 带字符集声明的预处理语句重复执行仍保留字符值与类型元数据。
prepare jt_charset_stmt from 'select jt.v,charset(jt.v) from json_table(?, ''$[*]'' columns(v varchar(4) character set utf8mb4 path ''$'')) jt';
set @jt_doc='["中"]';
execute jt_charset_stmt using @jt_doc;
set @jt_doc='["😀"]';
execute jt_charset_stmt using @jt_doc;
deallocate prepare jt_charset_stmt;
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
