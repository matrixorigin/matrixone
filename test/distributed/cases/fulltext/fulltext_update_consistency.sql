set experimental_fulltext_index = 1;

drop database if exists fulltext_update_consistency;
create database fulltext_update_consistency;
use fulltext_update_consistency;

-- These three CDC consumers are independent. Start all of them before the
-- synchronous coverage below, then use one barrier that checks every exact
-- initial identity. Later PK mutations still have their own generation waits.
create table ft_async_pk(
    id bigint primary key,
    body text,
    fulltext ft_body(body) async
);
insert into ft_async_pk values (10, 'async identity token');

create table ft_async_composite(
    tenant varchar(20),
    id bigint,
    body text,
    primary key(tenant, id),
    fulltext ft_body(body) async
);
insert into ft_async_composite values ('a', 1, 'composite identity token');

create table ft_null_async(
    id int primary key,
    left_text text,
    right_text text,
    fulltext ft_body(left_text, right_text) async
);
insert into ft_null_async(id, right_text) values (1, 'asyncrighttoken');

create table ft_sync(id int primary key, k int unique, body text, fulltext ft_body(body));
insert into ft_sync values (1, 1, 'sync old token'), (2, 2, 'sync peer token');
select id from ft_sync where match(body) against('old') order by id;
update ft_sync set body = 'sync new token' where id = 1;
select id from ft_sync where match(body) against('old') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- Rolling back the statement cancels both the base mutation and all hidden
-- table maintenance steps.
begin;
update ft_sync set body = 'sync rollback token' where id = 1;
rollback;
select id from ft_sync where match(body) against('rollback') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- The synchronous-index PK rejection happens before locks or mutations and
-- must leave both the base identity and hidden payload unchanged.
update ft_sync set id = 3, body = 'sync failed token' where id = 1;
-- TEXT/VARCHAR JDBC width differs between direct CN and proxy; values remain exact.
-- @metacmp(false)
select id, k, body from ft_sync order by id;
select id from ft_sync where match(body) against('failed') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- A single retry must observe all three independent initial indexes. Casting
-- only gives the heterogeneous identities one comparable output column.
-- @metacmp(false)
-- @wait_expect(2, 30)
select case_name, identity from (select 'composite' as case_name, concat(tenant, ':', id) as identity from ft_async_composite where match(body) against('composite') union all select 'null-column', cast(id as char) from ft_null_async where match(left_text, right_text) against('asyncrighttoken') union all select 'pk', cast(id as char) from ft_async_pk where match(body) against('identity')) readiness order by case_name;

-- Async maintenance is CDC-only. A committed PK update must replace the CDC
-- identity rather than retain an entry keyed by the old PK.
update ft_async_pk set id = 20 where id = 10;
-- @wait_expect(2, 30)
select id from ft_async_pk where match(body) against('identity') order by id;

-- Rolled-back PK changes never become visible to the CDC consumer.
begin;
update ft_async_pk set id = 30 where id = 20;
rollback;
-- @wait_expect(2, 30)
select id from ft_async_pk where match(body) against('identity') order by id;

-- Composite source identities exercise CDC tombstone and insert encoding.
update ft_async_composite set tenant = 'b', id = 2 where tenant = 'a' and id = 1;
-- @metacmp(false)
-- @wait_expect(2, 30)
select tenant, id from ft_async_composite where match(body) against('composite') order by tenant, id;

-- A NULL in one indexed content column skips only that column. Cover index
-- creation for every parser accepted by fulltext_index_tokenize.
create table ft_null_default(id int primary key, left_text text, right_text text);
insert into ft_null_default values
    (1, null, 'defaultrighttoken'),
    (2, 'defaultlefttoken', null),
    (3, null, null);
create fulltext index ft_body on ft_null_default(left_text, right_text);
select id from ft_null_default where match(left_text, right_text) against('defaultrighttoken') order by id;
select id from ft_null_default where match(left_text, right_text) against('defaultlefttoken') order by id;

create table ft_null_ngram(id int primary key, left_text text, right_text text);
insert into ft_null_ngram values (1, null, '風月無情'), (2, '神雕俠侶', null), (3, null, null);
create fulltext index ft_body on ft_null_ngram(left_text, right_text) with parser ngram;
select id from ft_null_ngram where match(left_text, right_text) against('風月無情') order by id;
select id from ft_null_ngram where match(left_text, right_text) against('神雕俠侶') order by id;

create table ft_null_gojieba(id int primary key, left_text text, right_text text);
insert into ft_null_gojieba values (1, null, '清华大学'), (2, '北京', null), (3, null, null);
create fulltext index ft_body on ft_null_gojieba(left_text, right_text) with parser gojieba;
select id from ft_null_gojieba where match(left_text, right_text) against('清华大学') order by id;
select id from ft_null_gojieba where match(left_text, right_text) against('北京') order by id;

create table ft_null_json(id int primary key, left_doc json, right_doc json);
insert into ft_null_json values
    (1, null, '{"k":"jsonrighttoken"}'),
    (2, '{"k":"jsonlefttoken"}', null),
    (3, null, null);
create fulltext index ft_body on ft_null_json(left_doc, right_doc) with parser json;
select id from ft_null_json where match(left_doc, right_doc) against('jsonrighttoken') order by id;
select id from ft_null_json where match(left_doc, right_doc) against('jsonlefttoken') order by id;

create table ft_null_json_value(id int primary key, left_doc json, right_doc json);
insert into ft_null_json_value values
    (1, null, '{"k":"jsonvaluerighttoken"}'),
    (2, '{"k":"jsonvaluelefttoken"}', null),
    (3, null, null);
create fulltext index ft_body on ft_null_json_value(left_doc, right_doc) with parser json_value;
select id from ft_null_json_value where match(left_doc, right_doc) against('jsonvaluerighttoken') order by id;
select id from ft_null_json_value where match(left_doc, right_doc) against('jsonvaluelefttoken') order by id;

-- Synchronous DML maintenance must add, replace, remove, and revive documents
-- when indexed columns move between NULL and non-NULL.
create table ft_null_dml(
    id int primary key,
    left_text text,
    right_text text,
    fulltext ft_body(left_text, right_text)
);
insert into ft_null_dml(id, right_text) values (1, 'insertrighttoken');
select id from ft_null_dml where match(left_text, right_text) against('insertrighttoken') order by id;
update ft_null_dml set left_text = 'updatelefttoken', right_text = null where id = 1;
select id from ft_null_dml where match(left_text, right_text) against('insertrighttoken') order by id;
select id from ft_null_dml where match(left_text, right_text) against('updatelefttoken') order by id;
update ft_null_dml set left_text = null, right_text = null where id = 1;
select id from ft_null_dml where match(left_text, right_text) against('updatelefttoken') order by id;
update ft_null_dml set right_text = 'reviverighttoken' where id = 1;
select id from ft_null_dml where match(left_text, right_text) against('reviverighttoken') order by id;
delete from ft_null_dml where id = 1;
select id from ft_null_dml where match(left_text, right_text) against('reviverighttoken') order by id;

drop database fulltext_update_consistency;
