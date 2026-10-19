-- @suite
-- @setup
-- correlation 目录必须覆盖同步索引维护的最终 DML 图。
drop database if exists dependent_fulltext_dml;
create database dependent_fulltext_dml;
use dependent_fulltext_dml;
create table keys_src(id int primary key);
insert into keys_src values (1), (2);
create table docs(id int primary key, body text, fulltext ft_body(body));

-- 普通同步索引写入/删除控制，与 dependent 路径共用维护 helper。
insert into docs values (3, 'control token');
select id from docs where match(body) against('control') order by id;
delete from docs where id=3;
select count(*) from docs;

-- INSERT 的主表写入与同步索引写入均发生且只发生一次。
insert into docs select (select (select k.id)), 'alpha token' from keys_src k;
select id, body from docs order by id;
select id from docs where match(body) against('alpha') order by id;

-- UPDATE 读取捕获的旧行身份；旧词项必须被删除，新词项可检索。
update docs d set body=concat('beta token ', cast((select (select d.id)) as char));
select id, body from docs order by id;
select id from docs where match(body) against('alpha') order by id;
select id from docs where match(body) against('beta') order by id;

-- REPLACE 必须删除旧索引文档并建立新文档，不能重复执行 source。
replace into docs select (select (select k.id)), 'gamma token' from keys_src k;
select id, body from docs order by id;
select id from docs where match(body) against('beta') order by id;
select id from docs where match(body) against('gamma') order by id;

-- DELETE 的主输出 sink 身份不能被后续维护步骤顶替。
delete from docs where id=(select (select docs.id)) and id=1;
select id, body from docs order by id;
select id from docs where match(body) against('gamma') order by id;
delete from docs where id=(select (select docs.id));
select count(*) from docs;
select id from docs where match(body) against('gamma') order by id;
drop database dependent_fulltext_dml;
