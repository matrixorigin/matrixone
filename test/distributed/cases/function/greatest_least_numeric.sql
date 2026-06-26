-- issue #25145: GREATEST / LEAST should implicitly promote mixed numeric
-- argument types instead of rejecting them.

-- constant literals: bigint + decimal / bigint + double
select greatest(1, 2.0);
select least(1, 2.0);
select greatest(cast(1 as bigint), cast(2.0 as double));
select least(cast(1 as bigint), cast(2.0 as double));

-- explicit casts covering the documented promotion matrix
select greatest(cast(1 as bigint), cast(2.0 as double)) as g, least(cast(1 as bigint), cast(2.0 as double)) as l;
select greatest(cast(5 as bigint), cast(2.5 as decimal(10,2)));
select least(cast(5 as bigint), cast(2.5 as decimal(10,2)));

-- same-type still works (regression guard)
select greatest(cast(1 as bigint), cast(2 as bigint));
select greatest(cast(1.0 as double), cast(2.0 as double));

-- mixed integer widths
select greatest(cast(1 as tinyint), cast(2000 as int));
select least(cast(1 as tinyint), cast(2000 as int));

-- NULL handling is preserved under promotion
select greatest(1, null, 2.0);
select least(1, null, 2.0);

-- table-driven: aggregate-vs-aggregate comparisons from the issue
drop table if exists toll_transactions;
create table toll_transactions(id int, gate_processing_sec double, lanes int);
insert into toll_transactions values
  (1, 3.5, 2), (2, 7.0, 2), (3, 1.0, 4), (4, 12.5, 3);

-- greatest(count(*), avg(x)) : bigint vs double
select greatest(count(*), avg(gate_processing_sec)) from toll_transactions;
select least(count(*), avg(gate_processing_sec)) from toll_transactions;

-- clamp pattern: greatest(0, expr) with a decimal-bearing expression
select greatest(0, count(*) - lanes * (3600.0 / avg(gate_processing_sec))) as est
from toll_transactions group by lanes order by lanes;

-- per-row mixed column types
drop table if exists mixed_num;
create table mixed_num(a bigint, b double, c decimal(20,4));
insert into mixed_num values (10, 2.5, 7.25), (3, 9.0, 4.5), (-5, -1.0, 100.0);
select a, b, c, greatest(a, b) as gab, least(a, b) as lab from mixed_num order by a;
select a, c, greatest(a, c) as gac, least(a, c) as lac from mixed_num order by a;
select greatest(a, b, c) as g3, least(a, b, c) as l3 from mixed_num order by a;

drop table if exists toll_transactions;
drop table if exists mixed_num;
