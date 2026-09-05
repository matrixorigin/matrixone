create database if not exists test;
use test;

drop table if exists t_json;
create table t_json (c1 int, c2 json);

insert into t_json values (1, '{"panel_no": 1, "area": "A1"}');
insert into t_json values (2, '{"panel_no": 2, "area": "A2"}');
insert into t_json values (3, '{"panel_no": "", "area": "A3"}');
insert into t_json values (4, '{"area": "A2"}');
insert into t_json values (5, '{"area": "A3"}');
insert into t_json values (6, '{"area": ""}');
insert into t_json values (7, '{}');
insert into t_json values (8, '{"panel_no": 2}');
insert into t_json values (9, '{"panel_no": 3}');
insert into t_json values (10, '{"panel_no": ""}');
insert into t_json values (11, NULL);

select * from t_json order by c1 asc;

--t1
select * from t_json order by json_extract(c2, '$.area') asc;
--t2
select * from t_json order by json_extract(c2, '$.area') desc;
--t3
select * from t_json order by json_extract(c2, '$.panel_no') asc;
--t4
select * from t_json order by json_extract(c2, '$.panel_no') desc;
--t5
select * from t_json order by json_extract(c2, '$.panel_no') , json_extract(c2, '$.area') asc;
--t6
 select * from t_json order by json_extract(c2, '$.panel_no') , json_extract(c2, '$.area') desc;


CREATE TABLE data_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  json_data JSON NOT NULL
);

INSERT INTO data_table (json_data)
VALUES
  ('{"value": 100}'),
  ('{"value": -200}'),
  ('{"value": 150.5}'),
  ('{"value": 0}'),
  ('{"value": -99.9}'),
  ('{"value": 50}'),
  ('{"value": 300}');

INSERT INTO data_table (json_data)
VALUES
  ('{"value": 1000000000000}'),
  ('{"value": -2000000000000}'),
  ('{"value": 1500000000000.5}'),
  ('{"value": 0}'),
  ('{"value": -999999999999.9}'),
  ('{"value": 500000000000}'),
  ('{"value": 3000000000000}');

select JSON_EXTRACT(json_data, '$.value') from data_table;

SELECT id, json_data
FROM data_table
ORDER BY JSON_EXTRACT(json_data, '$.value') ASC;

SELECT id, json_data
FROM data_table
ORDER BY JSON_EXTRACT(json_data, '$.value') DESC;

INSERT INTO data_table (json_data)
VALUES
  ('{"value": "1000000000000"}'),
  ('{"value": "-2000000000000"}'),
  ('{"value": "1500000000000.5"}'),
  ('{"value": "0"}'),
  ('{"value": "-999999999999.9"}'),
  ('{"value": "500000000000"}'),
  ('{"value": "3000000000000"}'),
  ('{"value":"A2"}'),
  ('{"value":"A1"}'),
  ('{"value":"A3"}'),
  ('{"value":""}'),
  ('{"value":null}');

select JSON_EXTRACT(json_data, '$.value') from data_table;

SELECT id, json_data
FROM data_table
ORDER BY JSON_EXTRACT(json_data, '$.value') ASC;

SELECT id, json_data
FROM data_table
ORDER BY JSON_EXTRACT(json_data, '$.value') DESC;

-- MySQL JSON scalar precedence: null, number, string, object, array,
-- false, true, date, time, datetime, bit, blob.
drop table if exists json_order_values;
create table json_order_values(rank_no int, label varchar(16), j json);
insert into json_order_values values
  (0, 'sql-null', null),
  (1, 'json-null', convert('null', json)),
  (2, 'number', convert('0', json)),
  (3, 'string', convert('"x"', json)),
  (4, 'object', convert('{"a":1}', json)),
  (5, 'array', convert('[1]', json)),
  (6, 'false', convert(false, json)),
  (7, 'true', convert(true, json)),
  (8, 'date', json_extract(json_array(cast('2024-01-02' as date)), '$[0]')),
  (9, 'time', json_extract(json_array(cast('10:00:00.1' as time(1))), '$[0]')),
  (10, 'datetime', json_extract(json_array(cast('2024-01-02 03:04:05.120000' as datetime(6))), '$[0]')),
  (11, 'bit', json_extract(json_array(cast(b'1' as bit(1))), '$[0]')),
  (12, 'blob', json_extract(json_array(cast(x'01' as blob)), '$[0]'));
select rank_no, label, json_type(j) from json_order_values order by rank_no;
select sum(case when (a.j < b.j) = (a.rank_no < b.rank_no) then 0 else 1 end) as lt_mismatches, sum(case when (a.j = b.j) = (a.rank_no = b.rank_no) then 0 else 1 end) as eq_mismatches, sum(case when (a.j > b.j) = (a.rank_no > b.rank_no) then 0 else 1 end) as gt_mismatches from json_order_values a cross join json_order_values b where a.rank_no > 0 and b.rank_no > 0;
select rank_no, label from json_order_values order by j asc, rank_no asc;
select rank_no, label from json_order_values order by j desc, rank_no desc;
drop table json_order_values;

drop database if exists test;
