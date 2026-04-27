# Issue #23006, #23007: MySQL JSON -> and ->> operators
# -> equivalent to JSON_EXTRACT; ->> equivalent to JSON_UNQUOTE(JSON_EXTRACT(...))
# MySQL requires the left of -> to be a column (table or subquery), not a bare expression.

# --- Literal object: use subquery so -> applies to column j ---
SELECT j -> '$.key' AS result1 FROM (SELECT JSON_OBJECT('key', 'value') AS j) t;
SELECT j -> '$.b' AS result2 FROM (SELECT JSON_OBJECT('a', 1, 'b', 2) AS j) t;
SELECT j ->> '$.key' AS result3 FROM (SELECT JSON_OBJECT('key', 'value') AS j) t;
SELECT j ->> '$.b' AS result4 FROM (SELECT JSON_OBJECT('a', 1, 'b', 2) AS j) t;

# --- Nested object path: subquery ---
SELECT j -> '$.c.d' AS nested FROM (SELECT JSON_OBJECT('a', 1, 'b', 2, 'c', JSON_OBJECT('d', 4)) AS j) t;
SELECT j ->> '$.c.d' AS nested_unquote FROM (SELECT JSON_OBJECT('a', 1, 'b', 2, 'c', JSON_OBJECT('d', 4)) AS j) t;

# --- Array index path: subquery for JSON_ARRAY; table for string literal JSON ---
SELECT a -> '$[1]' AS elem1 FROM (SELECT JSON_ARRAY(10, 20, 30, 40) AS a) t;
SELECT a ->> '$[1]' AS elem1_unquote FROM (SELECT JSON_ARRAY(10, 20, 30, 40) AS a) t;

CREATE TABLE t_arr (id INT, a JSON);
INSERT INTO t_arr VALUES (1, '[3,10,5,17,44]');
INSERT INTO t_arr VALUES (2, '[3,10,5,17,[22,44,66]]');
INSERT INTO t_arr VALUES (3, '[1,2,3]');
SELECT a -> '$[4]' AS elem4 FROM t_arr WHERE id = 1;
SELECT a -> '$[4]' AS elem4_nested FROM t_arr WHERE id = 2;
SELECT a -> '$[4][1]' AS nested_idx FROM t_arr WHERE id = 2;
SELECT a -> '$[4][1]' AS missing_nested FROM t_arr WHERE id = 1;

# --- Path not found returns NULL ---
SELECT j -> '$.b' AS missing_key FROM (SELECT JSON_OBJECT('a', 1) AS j) t;
SELECT a -> '$[10]' AS out_of_range FROM t_arr WHERE id = 3;

DROP TABLE t_arr;

# --- Table with JSON column: SELECT / WHERE / ORDER BY (like MySQL jemp) ---
CREATE TABLE jemp (c JSON, g INT);
INSERT INTO jemp VALUES ('{"id": "2", "name": "Wilma"}', 2);
INSERT INTO jemp VALUES ('{"id": "3", "name": "Barney"}', 3);
INSERT INTO jemp VALUES ('{"id": "4", "name": "Betty"}', 4);

SELECT c -> '$.id' AS id_json, c ->> '$.name' AS name_text FROM jemp ORDER BY g;
SELECT c, c -> '$.id', g FROM jemp WHERE (c -> '$.id') > 1 ORDER BY (c -> '$.name');
SELECT c ->> '$.name' AS name FROM jemp WHERE g > 2 ORDER BY c ->> '$.name';

DROP TABLE jemp;

# --- Table with JSON array (MySQL tj10 style) ---
CREATE TABLE tj10 (a JSON, b INT);
INSERT INTO tj10 VALUES ('[3,10,5,17,44]', 33);
INSERT INTO tj10 VALUES ('[3,10,5,17,[22,44,66]]', 0);

SELECT a -> '$[4]' AS arr_elem4 FROM tj10 ORDER BY b;
SELECT a -> '$[4][1]' AS arr_nested FROM tj10 ORDER BY b;
SELECT * FROM tj10 WHERE (a -> '$[0]') = 3 ORDER BY b;
SELECT a ->> '$[4][1]' AS unquote_nested FROM tj10 ORDER BY b;

DROP TABLE tj10;

# --- Array with mixed types: -> keeps JSON, ->> unquotes ---
CREATE TABLE tj_mixed (a JSON, b INT);
INSERT INTO tj_mixed VALUES ('[3,10,5,"x",44]', 33);
INSERT INTO tj_mixed VALUES ('[3,10,5,17,[22,"y",66]]', 0);

SELECT a -> '$[3]' AS with_quotes, a -> '$[4][1]' AS nested FROM tj_mixed ORDER BY b;
SELECT a ->> '$[3]' AS no_quotes, a ->> '$[4][1]' AS nested_unquote FROM tj_mixed ORDER BY b;

DROP TABLE tj_mixed;
-- ============================================================
-- PART 2: #23006 - JSON -> operator
-- ============================================================

drop table if exists jt;
create table jt(id int primary key, doc json);

-- 2.1 Basic usage - string value
insert into jt values (1, '{"name": "alice", "age": 30}');
select doc -> '$.name' from jt where id = 1;
select json_extract(doc, '$.name') from jt where id = 1;

-- 2.2 Numeric value
select doc -> '$.age' from jt where id = 1;

-- 2.3 Nested object
truncate table jt;
insert into jt values (1, '{"user": {"name": "bob", "addr": {"city": "beijing", "zip": "100000"}}}');
select doc -> '$.user.name' from jt where id = 1;
select doc -> '$.user.addr.city' from jt where id = 1;
select doc -> '$.user.addr' from jt where id = 1;

-- 2.4 Array access
truncate table jt;
insert into jt values (1, '{"arr": [10, 20, 30], "nested": [[1,2],[3,4]]}');
select doc -> '$.arr' from jt where id = 1;
select doc -> '$.arr[0]' from jt where id = 1;
select doc -> '$.arr[2]' from jt where id = 1;
select doc -> '$.nested[0][1]' from jt where id = 1;

-- 2.5 Non-existent path returns NULL
select doc -> '$.nonexistent' from jt where id = 1;
select doc -> '$.arr[99]' from jt where id = 1;

-- 2.6 JSON value type coverage
truncate table jt;
insert into jt values (1, '{"str": "hello", "num": 42, "flt": 3.14, "bool_t": true, "bool_f": false, "nil": null}');
select doc -> '$.str' from jt where id = 1;
select doc -> '$.num' from jt where id = 1;
select doc -> '$.flt' from jt where id = 1;
select doc -> '$.bool_t' from jt where id = 1;
select doc -> '$.bool_f' from jt where id = 1;
select doc -> '$.nil' from jt where id = 1;

-- 2.7 Usage in WHERE clause
truncate table jt;
insert into jt values (1, '{"status": "active", "score": 90}');
insert into jt values (2, '{"status": "inactive", "score": 60}');
insert into jt values (3, '{"status": "active", "score": 85}');
select id from jt where doc -> '$.score' > 80 order by id;

-- 2.8 Combined with other JSON functions
truncate table jt;
insert into jt values (1, '{"tags": ["go", "rust", "python"]}');

-- 2.9 Edge case - empty JSON object/array
truncate table jt;
insert into jt values (1, '{}');
insert into jt values (2, '[]');
select doc -> '$.anything' from jt where id = 1;
select doc -> '$[0]' from jt where id = 2;

-- 2.10 Deeply nested
truncate table jt;
insert into jt values (1, '{"a":{"b":{"c":{"d":{"e":"deep"}}}}}');
select doc -> '$.a.b.c.d.e' from jt where id = 1;

-- 2.11 Special character keys
truncate table jt;
insert into jt values (1, '{"key with space": 1, "key-dash": 2}');
select doc -> '$."key with space"' from jt where id = 1;
select doc -> '$."key-dash"' from jt where id = 1;

-- 2.12 Large JSON
truncate table jt;
insert into jt values (1, concat('{"big": "', repeat('x', 10000), '"}'));

-- 2.13 NULL doc
truncate table jt;
insert into jt values (1, null);
select doc -> '$.key' from jt where id = 1;

drop table jt;

-- ============================================================
-- PART 3: #23007 - JSON ->> operator
-- ============================================================

drop table if exists jt2;
create table jt2(id int primary key, doc json);

-- 3.1 Basic usage - compare -> and ->>
insert into jt2 values (1, '{"name": "alice", "age": 30}');
-- -> returns quoted: "alice"
select doc -> '$.name' from jt2 where id = 1;
-- ->> returns unquoted: alice
select doc ->> '$.name' from jt2 where id = 1;
select json_unquote(json_extract(doc, '$.name')) from jt2 where id = 1;

-- 3.2 Numeric value
select doc ->> '$.age' from jt2 where id = 1;

-- 3.3 Nested object
truncate table jt2;
insert into jt2 values (1, '{"user": {"name": "bob", "addr": {"city": "shanghai"}}}');
select doc ->> '$.user.name' from jt2 where id = 1;
select doc ->> '$.user.addr.city' from jt2 where id = 1;
select doc ->> '$.user.addr' from jt2 where id = 1;

-- 3.4 Array access
truncate table jt2;
insert into jt2 values (1, '{"arr": ["hello", "world"], "nums": [1, 2, 3]}');
select doc ->> '$.arr[0]' from jt2 where id = 1;
select doc ->> '$.arr[1]' from jt2 where id = 1;
select doc ->> '$.nums[0]' from jt2 where id = 1;

-- 3.5 Non-existent path returns NULL
select doc ->> '$.nonexistent' from jt2 where id = 1;

-- 3.6 JSON value type coverage
truncate table jt2;
insert into jt2 values (1, '{"str": "hello", "num": 42, "flt": 3.14, "bool_t": true, "bool_f": false, "nil": null}');
select doc ->> '$.str' from jt2 where id = 1;
select doc ->> '$.num' from jt2 where id = 1;
select doc ->> '$.flt' from jt2 where id = 1;
select doc ->> '$.bool_t' from jt2 where id = 1;
select doc ->> '$.bool_f' from jt2 where id = 1;
select doc ->> '$.nil' from jt2 where id = 1;

-- 3.7 Usage of ->> in WHERE clause
truncate table jt2;
insert into jt2 values (1, '{"status": "active", "score": 90}');
insert into jt2 values (2, '{"status": "inactive", "score": 60}');
insert into jt2 values (3, '{"status": "active", "score": 85}');
select id from jt2 where doc ->> '$.status' = 'active' order by id;

-- 3.8 Usage in ORDER BY
select id, doc ->> '$.score' as score from jt2 order by doc ->> '$.score' desc;

-- 3.9 Escape character handling
truncate table jt2;
insert into jt2 values (1, '{"msg": "hello\\nworld", "path": "c:\\\\temp"}');
select doc ->> '$.msg' from jt2 where id = 1;

-- 3.10 Unicode characters
truncate table jt2;
insert into jt2 values (1, '{"cn": "中文测试", "emoji": "🎉"}');
select doc ->> '$.cn' from jt2 where id = 1;
select doc ->> '$.emoji' from jt2 where id = 1;

-- 3.11 Edge case - empty string value
truncate table jt2;
insert into jt2 values (1, '{"empty": ""}');
select doc ->> '$.empty' from jt2 where id = 1;

-- 3.12 Deeply nested
truncate table jt2;
insert into jt2 values (1, '{"a":{"b":{"c":{"d":{"e":"deep_value"}}}}}');
select doc ->> '$.a.b.c.d.e' from jt2 where id = 1;

-- 3.13 NULL doc
truncate table jt2;
insert into jt2 values (1, null);
select doc ->> '$.key' from jt2 where id = 1;

drop table jt2;

-- ============================================================
-- PART 4: Combined tests
-- ============================================================

-- 4.1 JSON operators + various column constraints
drop table if exists json_constrained;
create table json_constrained(
    id int primary key auto_increment,
    doc json not null,
    label varchar(50) default 'none',
    created_at timestamp default current_timestamp
);
insert into json_constrained(doc, label) values ('{"k": "v1"}', 'first');
insert into json_constrained(doc, label) values ('{"k": "v2"}', 'second');
insert into json_constrained(doc) values ('{"k": "v3"}');
select id, doc -> '$.k' as jval from json_constrained order by id;
select id, doc ->> '$.k' as jval, label from json_constrained order by id;
drop table json_constrained;

-- 4.2 JSON operators + temporary table
create temporary table tmp_json(id int, doc json);
insert into tmp_json values (1, '{"temp": true}');
select doc -> '$.temp' from tmp_json;
select doc ->> '$.temp' from tmp_json;
drop table tmp_json;

-- 4.3 JSON operators + JOIN
drop table if exists orders;
drop table if exists customers;
create table customers(id int primary key, info json);
create table orders(id int primary key, cust_id int, detail json);
insert into customers values (1, '{"name": "alice", "vip": true}');
insert into customers values (2, '{"name": "bob", "vip": false}');
insert into orders values (1, 1, '{"product": "laptop", "price": 999}');
insert into orders values (2, 1, '{"product": "mouse", "price": 29}');
insert into orders values (3, 2, '{"product": "keyboard", "price": 79}');
select c.info ->> '$.name' as customer,
       o.detail ->> '$.product' as product,
       o.detail ->> '$.price' as price
from customers c
join orders o on c.id = o.cust_id
order by c.id, o.id;
drop table if exists orders;
drop table if exists customers;

-- 4.4 JSON operators + subquery
drop table if exists sub_json;
create table sub_json(id int primary key, doc json);
insert into sub_json values (1, '{"score": 90}');
insert into sub_json values (2, '{"score": 60}');
insert into sub_json values (3, '{"score": 85}');
select id from sub_json where cast(doc ->> '$.score' as decimal) > (select avg(cast(doc ->> '$.score' as decimal)) from sub_json) order by id;
drop table sub_json;

-- 4.5 JSON operators + GROUP BY / HAVING
drop table if exists grp_json;
create table grp_json(id int primary key, doc json);
insert into grp_json values (1, '{"dept": "eng", "salary": 100}');
insert into grp_json values (2, '{"dept": "eng", "salary": 120}');
insert into grp_json values (3, '{"dept": "sales", "salary": 80}');
insert into grp_json values (4, '{"dept": "sales", "salary": 90}');
select doc ->> '$.dept' as dept, count(*) as cnt, sum(cast(doc ->> '$.salary' as decimal)) as total
from grp_json
group by doc ->> '$.dept'
order by dept;
drop table grp_json;

-- 4.6 Multi-table drop + JSON table combination
drop table if exists jc1;
drop table if exists jc2;
drop table if exists jc3;
create table jc1(id int primary key, data json);
create table jc2(id int primary key, info json);
create table jc3(id int primary key, meta json);
insert into jc1 values (1, '{"type": "A", "val": 100}');
insert into jc2 values (1, '{"type": "B", "val": 200}');
insert into jc3 values (1, '{"type": "C", "val": 300}');
select data -> '$.type' from jc1;
select info ->> '$.type' from jc2;
select meta ->> '$.val' from jc3;
drop table jc1, jc2, jc3;
drop table if exists jc1;
drop table if exists jc2;
drop table if exists jc3;

-- ============================================================
-- Clean up test database
-- ============================================================

-- ============================================================
-- The following cases have MO vs MySQL result differences, wrapped with tags
-- ============================================================

-- JSON -> metadata difference when used in WHERE clause
drop table if exists jt_diff;
create table jt_diff(id int primary key, doc json);
insert into jt_diff values (1, '{"status": "active", "score": 90}');
insert into jt_diff values (2, '{"status": "inactive", "score": 60}');
insert into jt_diff values (3, '{"status": "active", "score": 85}');
-- comparing -> result with JSON-quoted string should return empty set (MySQL compatible)
select id from jt_diff where doc -> '$.status' = '"active"' order by id;
-- comparing -> result with unquoted string should return matching rows (MySQL compatible)
select id from jt_diff where doc -> '$.status' = 'active' order by id;
drop table if exists jt_diff;