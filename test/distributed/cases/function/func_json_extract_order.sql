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

drop database if exists test;
