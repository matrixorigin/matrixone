-- Issue #32440: a bounded correlated scalar aggregate must use the correct
-- child columns after predicate pushdown and column pruning.
drop database if exists test_corr_scalar_range;
create database test_corr_scalar_range;
use test_corr_scalar_range;

create table parent (id int, ceiling bigint);
create table child (unused int, parent_id int, event_idx bigint);
insert into parent values (1, 49);
insert into child values (0, 1, 1);

-- Keep the original one-row witness before adding any other data.
select (
    select max(c.event_idx)
    from child as c
    where c.parent_id = p.id
      and c.event_idx > -1
      and c.event_idx <= p.ceiling
)
from parent as p;

-- Nearest controls: remove the unused column, one bound, or the aggregate.
create table child_narrow (parent_id int, event_idx bigint);
insert into child_narrow values (1, 1);
select (select max(c.event_idx) from child_narrow c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling) from parent p;
select (select max(c.event_idx) from child c where c.parent_id = p.id and c.event_idx <= p.ceiling) from parent p;
select (select c.event_idx from child c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling order by c.event_idx desc limit 1) from parent p;

-- Exercise both bounds, duplicates, missing children, and NULL operands.
delete from parent;
delete from child;
insert into parent values (1, 2), (2, 0), (3, 9), (4, 9), (5, null), (6, 9);
insert into child values (0, 1, 2), (0, 1, 2), (0, 1, 3), (0, 2, 0), (0, 3, -1), (0, 5, 1), (0, 6, null);
select p.id, (select max(c.event_idx) from child c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling) as max_event from parent p order by p.id;

-- Moving the referenced columns must preserve the same results.
create table child_reordered (event_idx bigint, unused int, parent_id int);
insert into child_reordered select event_idx, unused, parent_id from child;
select p.id, (select max(c.event_idx) from child_reordered c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling) as max_event from parent p order by p.id;

-- An empty outer input returns no rows, unlike a parent with no match.
select p.id, (select max(c.event_idx) from child c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling) as max_event from parent p where p.id = 99;

-- Reuse one prepared plan across different parents and after a NULL result.
prepare bounded_max from 'select p.id, (select max(c.event_idx) from child c where c.parent_id = p.id and c.event_idx > -1 and c.event_idx <= p.ceiling) as max_event from parent p where p.id = ?';
set @parent_id = 1;
execute bounded_max using @parent_id;
set @parent_id = 2;
execute bounded_max using @parent_id;
set @parent_id = 3;
execute bounded_max using @parent_id;
set @parent_id = 1;
execute bounded_max using @parent_id;
deallocate prepare bounded_max;

drop database test_corr_scalar_range;
