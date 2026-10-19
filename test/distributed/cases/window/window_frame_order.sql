-- @suite

-- @case
-- @desc: #30308 distinguish inverted window frame ordering from illegal bound values
-- @label:bvt

select sum(x) over (order by x rows between 1 following and 1 preceding) from (select 1 x) t;
select sum(x) over (order by x range between current row and unbounded preceding) from (select 1 x) t;
select sum(x) over (order by x rows between unbounded following and current row) from (select 1 x) t;
select sum(x) over (order by x rows between unbounded preceding and unbounded preceding) from (select 1 x) t;
-- A valid ordering domain isolates the negative-bound diagnostic.
select sum(x) over (order by cast('2026-01-01' as datetime) range between interval -1 day preceding and current row) from (select 1 x) t;
-- Numeric ordering rejects interval bounds independently of their sign.
select sum(x) over (order by x range between interval 1 day preceding and current row) from (select 1 x) t;
select sum(x) over (order by x range between interval -1 day preceding and current row) from (select 1 x) t;
select sum(x) over (order by x rows between 1 preceding and current row) as s from (select 1 x) t;
select sum(x) over (order by x range between current row and 1 following) as s from (select 1 x) t;
