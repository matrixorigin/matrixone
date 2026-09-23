set @old_cte_max_recursion_depth = @@session.cte_max_recursion_depth;

-- The configured depth counts recursive result levels, not the anchor or the
-- empty iteration needed to detect convergence.
set session cte_max_recursion_depth = 2;
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 3
)
select n from r order by n;

-- One productive level is allowed when the limit is one.
set session cte_max_recursion_depth = 1;
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 2
)
select n from r order by n;

-- Two recursive levels exceed a limit of one.
set session cte_max_recursion_depth = 1;
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 3
)
select n from r order by n;

-- Zero depth allows the anchor when the recursive member is empty.
set session cte_max_recursion_depth = 0;
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 1
)
select n from r order by n;

-- Zero depth still rejects a recursive level that produces a row.
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 2
)
select n from r order by n;

-- A failed over-limit CTE must not leave partial INSERT rows behind.
drop table if exists cte_29138_insert;
create table cte_29138_insert(n int);
set session cte_max_recursion_depth = 1;
insert into cte_29138_insert
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 3
)
select n from r;
select count(*) from cte_29138_insert;
drop table cte_29138_insert;

set session cte_max_recursion_depth = @old_cte_max_recursion_depth;
