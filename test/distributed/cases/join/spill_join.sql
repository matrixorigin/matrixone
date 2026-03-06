-- Test spill join functionality with various join types
-- This test verifies that joins work correctly when memory spills to disk

-- Setup: Create test tables
drop table if exists spill_t1;
drop table if exists spill_t2;

create table spill_t1 (
    id int primary key,
    name varchar(100),
    value int
);

create table spill_t2 (
    id int primary key,
    t1_id int,
    category varchar(50)
);

-- Insert test data - increased to trigger spill with 128MB threshold
insert into spill_t1 select result, concat('name_', result), result * 10 from generate_series(1, 100000) g;
insert into spill_t2 select result, (result % 100000) + 1, concat('cat_', result % 5) from generate_series(1, 100000) g;

-- Test 1: Inner join with spill enabled (low threshold to force spilling)
set @@join_spill_mem = 10000;
select count(*) from spill_t1 inner join spill_t2 on spill_t1.id = spill_t2.t1_id;

-- Test 2: Left join with spill
select count(*) from spill_t1 left join spill_t2 on spill_t1.id = spill_t2.t1_id;

-- Test 3: Join with aggregation and spill
select spill_t2.category, count(*) as cnt
from spill_t1
inner join spill_t2 on spill_t1.id = spill_t2.t1_id
group by spill_t2.category
order by spill_t2.category;

-- Test 4: Join with where clause and spill
select count(*)
from spill_t1
inner join spill_t2 on spill_t1.id = spill_t2.t1_id
where spill_t1.value > 1000 and spill_t1.value < 5000;

-- Test 5: Join with order by and limit
select spill_t1.id, spill_t1.name, spill_t2.category
from spill_t1
inner join spill_t2 on spill_t1.id = spill_t2.t1_id
where spill_t1.id <= 5
order by spill_t1.id;

-- Test 6: Disable spill and verify join still works
set @@join_spill_mem = 0;
select count(*) from spill_t1 inner join spill_t2 on spill_t1.id = spill_t2.t1_id;

-- Test 7: Very low threshold to force aggressive spilling
set @@join_spill_mem = 10000;
select count(*)
from spill_t1
inner join spill_t2 on spill_t1.id = spill_t2.t1_id
where spill_t1.id between 100 and 200;

-- Test 8: Join with subquery and spill
select count(*)
from spill_t1
inner join (
    select t1_id, count(*) as cnt
    from spill_t2
    group by t1_id
) sub on spill_t1.id = sub.t1_id
where sub.cnt > 0;

-- Cleanup
drop table spill_t1;
drop table spill_t2;
set @@join_spill_mem = 0;
