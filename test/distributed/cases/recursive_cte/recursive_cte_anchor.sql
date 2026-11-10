-- #23079: MySQL requires parentheses around a UNION operand with local
-- ORDER BY or LIMIT. These clauses select seeds before recursive expansion.
-- https://dev.mysql.com/doc/refman/8.0/en/set-operations.html
-- Recursive self-references in subqueries are a separate restriction covered
-- by recursive_cte_subquery_scope.sql; anchor parentheses do not remove it.
drop database if exists recursive_cte_anchor_data;
create database recursive_cte_anchor_data;
use recursive_cte_anchor_data;
create table nodes(id int primary key, parent_id int, priority int);
insert into nodes values (1, null, 20), (2, null, 10), (11, 1, 0), (12, 2, 0);

-- ORDER BY + LIMIT chooses root 2 despite its larger id. Its child survives
-- the anchor limit; only the outer ORDER BY determines final output order.
with recursive c(n, depth) as (
    (select id, 0 from nodes where parent_id is null order by priority, id limit 1)
    union all
    select nodes.id, c.depth + 1 from nodes join c on nodes.parent_id = c.n
    where c.depth < 1
)
select n, depth from c order by n desc;

-- OFFSET belongs to the seed relation too: select the other root and child.
with recursive c(n, depth) as (
    (select id, 0 from nodes where parent_id is null order by priority, id limit 1 offset 1)
    union all
    select nodes.id, c.depth + 1 from nodes join c on nodes.parent_id = c.n
    where c.depth < 1
)
select n, depth from c order by n desc;

-- ORDER BY without LIMIT retains both roots. Do not rely on anchor ordering
-- surviving UNION ALL: explicitly order the complete recursive result.
with recursive c(n, depth) as (
    (select id, 0 from nodes where parent_id is null order by priority, id)
    union all
    select nodes.id, c.depth + 1 from nodes join c on nodes.parent_id = c.n
    where c.depth < 1
)
select n, depth from c order by n;

-- LIMIT alone selects one unspecified root. Either root has exactly one child,
-- so cardinality by depth is deterministic without assuming a winning id.
with recursive c(n, depth) as (
    (select id, 0 from nodes where parent_id is null limit 1)
    union all
    select nodes.id, c.depth + 1 from nodes join c on nodes.parent_id = c.n
    where c.depth < 1
)
select depth, count(*) as row_count from c group by depth order by depth;

-- Zero seeds produce neither anchor rows nor descendants.
with recursive c(n, depth) as (
    (select id, 0 from nodes where parent_id is null order by priority, id limit 0)
    union all
    select nodes.id, c.depth + 1 from nodes join c on nodes.parent_id = c.n
    where c.depth < 1
)
select count(*) as row_count from c;

drop database recursive_cte_anchor_data;
