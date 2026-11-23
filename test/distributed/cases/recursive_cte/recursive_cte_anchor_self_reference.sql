-- #32795: a recursive CTE whose initial statement references its own
-- declaration has no non-recursive seed member. The planner must reject the
-- statement with a typed 1064-family parse error instead of rebinding the
-- anchor until the server dies of a stack overflow. MySQL also rejects a
-- self-referencing initial member; MO reports this boundary with its own
-- diagnostic. The error/liveness pattern mirrors
-- recursive_cte_subquery_scope.sql: each rejected statement is followed by a
-- statement on the same connection proving the server stays usable.
with recursive qn as (select 1 from qn union all select 1 from qn) select * from qn;

select 'after anchor reentry error' as service_status;

with recursive r(n) as (select 1 union all select n + 1 from r where n < 3) select * from r order by n;
