set @old_cte_max_recursion_depth = @@session.cte_max_recursion_depth;

-- Compatibility contract: unlike MySQL 8.0.45, MatrixOne counts only
-- productive recursive frontiers, excluding empty convergence attempts.
set session cte_max_recursion_depth = 2;
-- MySQL 8.0.45 errors after the third (empty) recursive attempt; MatrixOne
-- permits two productive recursive levels and returns 1, 2, 3.
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

-- MySQL 8.0.45 errors on the empty recursive attempt at depth zero; MatrixOne
-- excludes that convergence probe and returns the anchor.
set session cte_max_recursion_depth = 0;
with recursive r(n) as (
    select 1
    union all
    select n + 1 from r where n < 1
)
select n from r order by n;

-- Duplicate-only UNION DISTINCT rounds do not add a new frontier in MatrixOne.
-- MySQL 8.0.45 counts the recursive attempt and errors at depth zero.
set session cte_max_recursion_depth = 0;
with recursive r(n) as (
    select 1
    union distinct
    select n from r
)
select count(*) from r;

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
