-- MySQL-style non-unique single-expression functional-index regression suite.
drop database if exists functional_index;
create database functional_index;
use functional_index;

create table fi_inline (
    id int primary key,
    doc json,
    name varchar(32),
    qty int,
    key idx_sku ((json_unquote(json_extract(doc, '$.sku'))))
);
insert into fi_inline (id, doc, name, qty) values
    (1, '{"sku":"alpha"}', 'Alice', 2),
    (2, '{"sku":"beta"}', 'Bob', 4),
    (3, '{"sku":null}', 'Carol', 6),
    (4, '{"other":"missing"}', 'Dora', 8);
select id, json_unquote(json_extract(doc, '$.sku')) as sku from fi_inline order by id;
-- @regex("KEY `idx_sku`.*json_unquote", true)
-- @regex("__mo_fi_", false)
show create table fi_inline;
-- @regex("idx_sku.*NULL.*json_unquote", true)
show index from fi_inline;
-- @regex("Index Table Scan.*idx_sku", true)
explain select id from fi_inline where json_unquote(json_extract(doc, '$.sku')) = 'alpha';
select id from fi_inline where json_unquote(json_extract(doc, '$.sku')) = 'alpha';

create index idx_name_lower on fi_inline ((lower(name)));
alter table fi_inline add index idx_qty_plus ((qty + 1));
insert into fi_inline (id, doc, name, qty) values (5, '{"sku":"gamma"}', 'Eve', 10);
update fi_inline set doc = '{"sku":"delta"}', qty = 11 where id = 1;
delete from fi_inline where id = 2;
replace into fi_inline (id, doc, name, qty) values (3, '{"sku":"epsilon"}', 'Carol', 12);
insert into fi_inline (id, doc, name, qty) values (1, '{"sku":"zeta"}', 'Alice', 13) on duplicate key update doc = values(doc), qty = values(qty);
select id, json_unquote(json_extract(doc, '$.sku')) as sku, qty from fi_inline order by id;

drop index idx_name_lower on fi_inline;
alter table fi_inline drop index idx_qty_plus;
drop index idx_sku on fi_inline;
-- @regex("__mo_fi_", false)
show create table fi_inline;
select count(*) from mo_catalog.mo_columns where attrelname = 'fi_inline' and attdatabase = 'functional_index' and attname like '__mo_fi_%';
select count(*) from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 'fi_inline' and reldatabase = 'functional_index') and column_name like '__mo_fi_%';

-- Stable rejection matrix.
-- @regex("functional", true)
create table fi_unique (id int primary key, a int, unique key uq ((a + 1)));
-- @regex("functional", true)
create table fi_composite (id int primary key, a int, b int, key bad ((a + 1), b));
-- @regex("unsupported type", true)
create table fi_json (id int primary key, doc json, key bad ((json_extract(doc, '$.sku'))));
-- @regex("functional", true)
create temporary table fi_temp (id int, a int, key bad ((a + 1)));

drop database functional_index;
