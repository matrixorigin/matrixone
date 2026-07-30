drop database if exists recursive_cte_string_width;
create database recursive_cte_string_width;
use recursive_cte_string_width;

-- An inferred one-character anchor must reject the growing recursive value.
with recursive r(n, s) as (
    select 1, 'a'
    union all
    select n + 1, concat(s, 'b') from r where n < 4
)
select * from r order by n;

-- An explicitly widened anchor preserves the recursive value.
with recursive r(n, s) as (
    select 1, cast('a' as char(100))
    union all
    select n + 1, concat(s, 'b') from r where n < 4
)
select n, s from r order by n;

drop database recursive_cte_string_width;
