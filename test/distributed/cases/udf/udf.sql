create function sql_sum (a int, b int) returns int language sql as '$1 + $2';
select sql_sum(1, 1) as ret;
drop function sql_sum(int, int);