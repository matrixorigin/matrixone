-- issue #28871: a grouped MEDIAN partial must remain grouped when an outer
-- aggregate consumes the parallel subquery result.
drop table if exists issue_28871_median_nested_parallel;

create table issue_28871_median_nested_parallel as
select
    mod(r.result, 20000) as g,
    case mod(floor(r.result / 20000), 4)
        when 0 then -3
        when 1 then -1
        when 2 then 1
        else 3
    end as x
from generate_series(400000) r;

set @issue_28871_old_max_dop = @@max_dop;
set @@max_dop = 2;

-- Keep every outer aggregate: each one consumes the multi-row grouped MEDIAN
-- result and therefore guards against silently testing only a scalar path.
select min(m), max(m), sum(m), avg(m), count(m)
from (
    select g, median(x) as m
    from issue_28871_median_nested_parallel
    where g < 23
    group by g
) q;

set @@max_dop = @issue_28871_old_max_dop;
drop table issue_28871_median_nested_parallel;
