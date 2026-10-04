-- @case
-- @desc: correlated LIMIT is evaluated once per correlation key, issue #26816
-- @label:bvt
drop database if exists correlated_limit;
create database correlated_limit;
use correlated_limit;

create table parent_t (
    id int primary key,
    corr_key int,
    top_amount int
);
create table child_t (
    id int primary key,
    parent_id int,
    amount int
);
insert into parent_t values
    (1, 1, null),
    (2, 2, null),
    (3, 3, null),
    (4, 4, null),
    (5, null, null);
insert into child_t values
    (1, 1, 10),
    (2, 1, 20),
    (3, 2, 30),
    (4, 2, 40),
    (5, 2, 40),
    (6, 3, null),
    (7, 3, 50),
    (8, 3, 60),
    (9, null, 70);

-- Ordered scalar pagination must select independently within every key.
select p.id,
       (select c.amount
          from child_t c
         where c.parent_id = p.corr_key
         order by c.amount desc, c.id desc
         limit 1) as top_amount,
       (select c.id
          from child_t c
         where c.parent_id = p.corr_key
         order by c.amount desc, c.id desc
         limit 1) as top_child_id
  from parent_t p
 order by p.id;

-- Plain LIMIT still has to retain one row for every non-empty key.
select p.id,
       (select 1
          from child_t c
         where c.parent_id = p.corr_key
         limit 1) as has_child
  from parent_t p
 order by p.id;

-- OFFSET and LIMIT define a per-key row-number interval.
select p.id,
       (select c.id
          from child_t c
         where c.parent_id = p.corr_key
         order by c.amount desc, c.id desc
         limit 1 offset 1) as second_child_id
  from parent_t p
 order by p.id;

-- NULL-safe equality includes the NULL correlation partition.
select p.id,
       (select c.amount
          from child_t c
         where c.parent_id <=> p.corr_key
         order by c.amount desc, c.id desc
         limit 1) as top_amount
  from parent_t p
 order by p.id;

select p.id
  from parent_t p
 where exists (
       select 1 from child_t c where c.parent_id = p.corr_key limit 1)
 order by p.id;

select p.id
  from parent_t p
 where not exists (
       select 1 from child_t c where c.parent_id = p.corr_key limit 1)
 order by p.id;

-- LIMIT 0 is empty for every key and does not need a partitioned rewrite.
select p.id,
       coalesce((select c.amount
                   from child_t c
                  where c.parent_id = p.corr_key
                  limit 0), -1) as limited_zero
  from parent_t p
 order by p.id;

-- DML must consume the same per-key scalar result.
update parent_t p
   set p.top_amount = (select c.amount
                         from child_t c
                        where c.parent_id = p.corr_key
                        order by c.amount desc, c.id desc
                        limit 1)
 where p.id <= 4;
select id, top_amount from parent_t where id <= 4 order by id;

start transaction;
update parent_t p
   set p.top_amount = (select c.amount
                         from child_t c
                        where c.parent_id = p.corr_key
                        order by c.amount desc, c.id desc
                        limit 1 offset 1)
 where p.id <= 4;
rollback;
select id, top_amount from parent_t where id <= 4 order by id;

create table delete_exists_t (id int primary key);
insert into delete_exists_t values (1), (2), (3), (4);
delete from delete_exists_t d
 where exists (
       select 1 from child_t c where c.parent_id = d.id limit 1);
select id from delete_exists_t order by id;

create table delete_not_exists_t (id int primary key);
insert into delete_not_exists_t values (1), (2), (3), (4);
delete from delete_not_exists_t d
 where not exists (
       select 1 from child_t c where c.parent_id = d.id limit 1);
select id from delete_not_exists_t order by id;

-- Aggregate and explicit-window equivalents are result controls.
select p.id,
       (select max(c.amount) from child_t c where c.parent_id = p.corr_key) as max_amount
  from parent_t p
 where p.id <= 4
 order by p.id;

select p.id, r.amount as window_amount
  from parent_t p
  left join (
       select parent_id, amount,
              row_number() over (partition by parent_id order by amount desc, id desc) as rn
         from child_t) r
    on r.parent_id = p.corr_key and r.rn = 1
 where p.id <= 4
 order by p.id;

drop database correlated_limit;
