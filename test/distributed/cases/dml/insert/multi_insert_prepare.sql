-- Multi-table INSERT: SQL PREPARE/EXECUTE binds markers in WHEN, VALUES and source.
drop database if exists multi_insert_prepare_db;
create database multi_insert_prepare_db;

create table multi_insert_prepare_db.src (id int primary key, kind varchar(16), amount int);
create table multi_insert_prepare_db.hit (id int primary key, amount int);
create table multi_insert_prepare_db.miss (id int primary key, amount int);
insert into multi_insert_prepare_db.src values (1, 'hot', 10), (2, 'cold', 20), (3, 'hot', 30);

prepare multi_insert_route from '
insert first
  when kind = ? then into multi_insert_prepare_db.hit (id, amount) values (id + ?, amount + ?)
  else into multi_insert_prepare_db.miss (id, amount) values (id + ?, amount + ?)
select id, kind, amount from multi_insert_prepare_db.src where id >= ?';

-- One prepared handle routes a subset to both targets. This binds all marker
-- positions, including the conditional predicate, the two VALUES lists and
-- the source WHERE clause.
set @kind = 'hot', @hit_id_offset = 10, @hit_amount_offset = 100,
    @miss_id_offset = 1000, @miss_amount_offset = 2000, @min_id = 2;
execute multi_insert_route using @kind, @hit_id_offset, @hit_amount_offset,
    @miss_id_offset, @miss_amount_offset, @min_id;
select * from multi_insert_prepare_db.hit order by id;
select * from multi_insert_prepare_db.miss order by id;

-- Reuse the same handle with a NULL WHEN argument. SQL three-valued logic
-- makes the WHEN not true, so every selected source row must reach ELSE.
delete from multi_insert_prepare_db.hit;
delete from multi_insert_prepare_db.miss;
set @kind = null, @hit_id_offset = 100, @hit_amount_offset = 1000,
    @miss_id_offset = 2000, @miss_amount_offset = 3000, @min_id = 1;
execute multi_insert_route using @kind, @hit_id_offset, @hit_amount_offset,
    @miss_id_offset, @miss_amount_offset, @min_id;
select * from multi_insert_prepare_db.hit order by id;
select * from multi_insert_prepare_db.miss order by id;

deallocate prepare multi_insert_route;
drop database multi_insert_prepare_db;
