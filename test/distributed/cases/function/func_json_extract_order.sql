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

drop database if exists test;
