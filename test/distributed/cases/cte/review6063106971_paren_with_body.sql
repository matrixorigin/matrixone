-- review 6063106971 / issue 32606: sole parenthesized WITH body
with cte as (select 1 as a) (select * from cte);
with cte as (select 1 as a) select * from cte;
create table pw606(x int);
insert into pw606 values (7);
with q as (select x from pw606) (select * from q) order by x limit 1;
with cte as (select 1 as a) (select * from cte order by a) order by a;
(select x from pw606 limit 1) limit 1;
drop table pw606;
