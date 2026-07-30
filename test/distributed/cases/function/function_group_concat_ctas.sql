-- CTAS must preserve GROUP_CONCAT ORDER BY and separator syntax during reparse.
drop database if exists ctas_group_concat_null;
create database ctas_group_concat_null;
use ctas_group_concat_null;
create table t_src (g int, v varchar(10));
insert into t_src values (1, 'aa'), (1, null), (1, 'bbb'), (2, null);
select g, group_concat(v order by v) as gc from t_src group by g order by g;
create table t_dst as
select g, group_concat(v order by v) as gc from t_src group by g;
select g, gc from t_dst order by g;
drop database ctas_group_concat_null;
