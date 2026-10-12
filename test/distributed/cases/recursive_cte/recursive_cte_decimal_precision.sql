drop database if exists recursive_cte_decimal_precision;
create database recursive_cte_decimal_precision;
use recursive_cte_decimal_precision;
set @old_sql_mode = @@sql_mode;
set session sql_mode = 'STRICT_TRANS_TABLES';

-- DECIMAL64 recursive values are constrained by the anchor precision.
with recursive r(n) as (
    select cast(999.99 as decimal(5, 2))
    union all
    select n + 0.01 from r where n < 1000
)
select n from r order by n;

with recursive r(n) as (
    select cast(-999.99 as decimal(5, 2))
    union all
    select n - 0.01 from r where n > -1000
)
select n from r order by n;

-- A failed recursive INSERT is atomic.
create table decimal64_target(n decimal(5, 2));
insert into decimal64_target
with recursive r(n) as (
    select cast(999.99 as decimal(5, 2))
    union all
    select n + 0.01 from r where n < 1000
)
select n from r;
select count(*) from decimal64_target;

-- A failed CTAS must not leave a table containing an out-of-range value.
create table decimal64_derived as
with recursive r(n) as (
    select cast(999.99 as decimal(5, 2))
    union all
    select n + 0.01 from r where n < 1000
)
select n from r;
select count(*) from mo_catalog.mo_tables
where reldatabase = 'recursive_cte_decimal_precision' and relname = 'decimal64_derived';

-- DECIMAL128 follows the same precision contract.
with recursive r(n) as (
    select cast(99999999999999999.99 as decimal(19, 2))
    union all
    select n + 0.01 from r where n < 100000000000000000
)
select n from r order by n;

-- Increasing scale can reduce the available integer digits even when total
-- precision grows. The recursive assignment must validate the complete target.
with recursive r(n) as (
    select cast(0.0000 as decimal(5, 4))
    union all
    select cast(99.99 as decimal(4, 2)) from r where n = 0
)
select n from r order by n;

create table scale_growth_target(n decimal(5, 4));
insert into scale_growth_target
with recursive r(n) as (
    select cast(0.0000 as decimal(5, 4))
    union all
    select cast(99.99 as decimal(4, 2)) from r where n = 0
)
select n from r;
select count(*) from scale_growth_target;

-- Legal boundaries remain available, including through scale growth and
-- prepared execution.
with recursive r(n) as (
    select cast(0.0000 as decimal(5, 4))
    union all
    select cast(9.99 as decimal(3, 2)) from r where n = 0
)
select n from r order by n;

with recursive r(n) as (
    select cast(999.98 as decimal(5, 2))
    union all
    select n + 0.01 from r where n < 999.99
)
select n from r order by n;

prepare recursive_decimal_stmt from 'with recursive r(n) as (select cast(999.99 as decimal(5, 2)) union all select n + 0.01 from r where n < 1000) select n from r';
execute recursive_decimal_stmt;
select 1;
deallocate prepare recursive_decimal_stmt;

set session sql_mode = @old_sql_mode;
drop database recursive_cte_decimal_precision;
