-- Test spill join edge cases and stress scenarios
-- This test covers corner cases and boundary conditions for spill join

-- Setup
drop table if exists spill_edge_t1;
drop table if exists spill_edge_t2;

-- Test 1: Empty table join with spill enabled
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
set @@join_spill_mem = 10000;
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 2: Single row join with spill
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 values (1, 100);
insert into spill_edge_t2 values (1, 200);
select * from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 3: No matching rows with spill
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 values (1, 100), (2, 200);
insert into spill_edge_t2 values (3, 300), (4, 400);
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 4: All rows match with spill
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 select result, result * 10 from generate_series(1, 10000) g;
insert into spill_edge_t2 select result, result * 20 from generate_series(1, 10000) g;
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 5: Duplicate keys with spill
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 values (1, 10), (1, 20), (2, 30);
insert into spill_edge_t2 values (1, 100), (1, 200), (1, 300);
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 6: NULL values with spill
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 values (1, 10), (null, 20), (3, 30);
insert into spill_edge_t2 values (1, 100), (null, 200), (3, 300);
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 7: Large result set with very low spill threshold
create table spill_edge_t1 (id int, val int);
create table spill_edge_t2 (id int, val int);
insert into spill_edge_t1 select result, result from generate_series(1, 50000) g;
insert into spill_edge_t2 select result, result from generate_series(1, 50000) g;
select count(*) from spill_edge_t1 inner join spill_edge_t2 on spill_edge_t1.id = spill_edge_t2.id;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Test 8: Multiple join conditions with spill
create table spill_edge_t1 (id1 int, id2 int, val int);
create table spill_edge_t2 (id1 int, id2 int, val int);
insert into spill_edge_t1 select result, result % 10, result from generate_series(1, 10000) g;
insert into spill_edge_t2 select result, result % 10, result from generate_series(1, 10000) g;
select count(*) from spill_edge_t1 inner join spill_edge_t2
on spill_edge_t1.id1 = spill_edge_t2.id1 and spill_edge_t1.id2 = spill_edge_t2.id2;
drop table spill_edge_t1;
drop table spill_edge_t2;

-- Cleanup
set @@join_spill_mem = 0;
