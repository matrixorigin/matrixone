-- The global result window must not remove rows needed by recursive feedback.
with recursive c(n) as (select 1 union all select n + 1 from c where n < 10 limit 3 offset 1) select n from c order by n;

-- Consumer filters and aggregates observe the window, not the feedback prefix.
with recursive c(n) as (select 1 union all select n + 1 from c where n < 10 limit 3 offset 1) select n from c where n > 3;
with recursive c(n) as (select 1 union all select n + 1 from c where n < 10 limit 3 offset 1) select count(*), sum(n) from c;

-- The window can start within a seed batch and end in a later recursive round.
with recursive c(n) as (select n from (select 1 as n union all select 1) seed union all select n + 2 from c where n < 4 limit 3 offset 1) select n from c order by n;

-- A positive limit terminates a recursive member without a predicate.
with recursive c(n) as (select 1 union all select n + 1 from c limit 3) select n from c order by n;
with recursive c(n) as (select 1 union all select n + 1 from c limit 2 offset 3) select n from c order by n;

-- Zero limits preserve the feedback cleanup protocol, including DISTINCT.
with recursive c(n) as (select 1 union all select n + 1 from c where n < 3 limit 0) select n from c;
with recursive c(n) as (select 1 union all select n + 1 from c limit 0 offset 2) select n from c;
with recursive c(n) as (select 1 union distinct select n + 1 from c limit 0) select n from c;

-- Input exhaustion and the largest legal limit do not wrap the prefix bound.
with recursive c(n) as (select 1 union all select n + 1 from c where n < 3 limit 2 offset 3) select n from c;
with recursive c(n) as (select 1 union all select n + 1 from c where n < 3 limit 18446744073709551615 offset 1) select n from c order by n;

-- DISTINCT applies before the global window, not independently in each round.
with recursive c(n) as (select 1 union distinct select n + 1 from c where n < 5 limit 2 offset 2) select n from c order by n;

-- Each prepared execution owns fresh window and recursive state.
prepare cte_window from 'with recursive c(n) as (select 1 union all select n + 1 from c where n < 5 limit ? offset ?) select n from c order by n';
set @cte_limit = 2;
set @cte_offset = 1;
execute cte_window using @cte_limit, @cte_offset;
set @cte_limit = 0;
execute cte_window using @cte_limit, @cte_offset;
set @cte_limit = 1;
set @cte_offset = 3;
execute cte_window using @cte_limit, @cte_offset;
deallocate prepare cte_window;
set @cte_limit = null;
set @cte_offset = null;
