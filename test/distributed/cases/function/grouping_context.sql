-- @suite
-- @case
drop database if exists grouping_context;
create database grouping_context;
use grouping_context;

create table t(g int, h int);
insert into t values (1, 10), (1, 20), (2, 10);

select grouping(g) from t order by g, h;
select g, grouping(g), count(*) from t group by g order by g;
select g, count(*) from t group by g having grouping(g) = 0 order by g;
select g from t order by grouping(g), g;

select g, count(*), grouping(g)
from t
group by g with rollup
order by grouping(g), g;

select count(*) from t;

drop database grouping_context;
