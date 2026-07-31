drop database if exists recursive_cte_string_width;
create database recursive_cte_string_width;
use recursive_cte_string_width;
set @old_sql_mode = @@sql_mode;

-- Strict mode rejects a recursive value wider than the inferred anchor.
set session sql_mode = 'STRICT_TRANS_TABLES';
with recursive r(n, s) as (
    select 1, 'a'
    union all
    select n + 1, concat(s, 'b') from r where n < 4
)
select * from r order by n;

-- Non-strict mode follows MySQL and truncates every recursive value to the
-- one-character anchor width.
set session sql_mode = '';
with recursive r(n, s) as (
    select 1, 'a'
    union all
    select n + 1, concat(s, 'b') from r where n < 4
)
select * from r order by n;

-- The assignment cast resolves sql_mode on each prepared execution rather
-- than retaining the mode active when the recursive CTE was prepared.
prepare recursive_cte_string_width_stmt from 'with recursive r(n, s) as (select 1, ''a'' union all select n + 1, concat(s, ''b'') from r where n < 4) select * from r order by n';
set session sql_mode = 'STRICT_TRANS_TABLES';
execute recursive_cte_string_width_stmt;
set session sql_mode = '';
execute recursive_cte_string_width_stmt;
deallocate prepare recursive_cte_string_width_stmt;

-- An explicitly widened anchor preserves the recursive value.
with recursive r(n, s) as (
    select 1, cast('a' as char(100))
    union all
    select n + 1, concat(s, 'b') from r where n < 4
)
select n, s from r order by n;

set session sql_mode = @old_sql_mode;
drop database recursive_cte_string_width;
