-- @suit
-- @case
-- @desc:information_schema.CHECK_CONSTRAINTS exposes decoded CHECK metadata
-- @label:bvt

drop database if exists check_constraints_metadata;
create database check_constraints_metadata;
use check_constraints_metadata;

create table no_check_values (
    id int primary key
);

create table check_values (
    id int primary key,
    amount int constraint chk_amount_nonnegative check (amount >= 0),
    constraint chk_amount_positive check (amount > 0),
    constraint chk_amount_limit check (amount < 100)
);

use information_schema;
select constraint_catalog, constraint_schema, constraint_name, check_clause
from check_constraints
where constraint_schema = 'check_constraints_metadata'
order by constraint_name;

-- LIMIT must be honored by the metadata table function before the catalog
-- stream is exhausted.
select count(*)
from (
    select constraint_name
    from check_constraints
    where constraint_schema = 'check_constraints_metadata'
    limit 1
) limited_checks;

use check_constraints_metadata;
create table partitioned_values (
    id int,
    constraint chk_partition_positive check (id >= 0)
) partition by range columns (id) (
    partition p0 values less than (10),
    partition p1 values less than (20)
);

use information_schema;
select count(*)
from check_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_partition_positive';
select count(*)
from table_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_partition_positive';

use check_constraints_metadata;
create temporary table check_values_tmp (
    id int primary key,
    amount int,
    constraint chk_tmp_positive check (amount > 0)
);

use information_schema;
select count(*)
from check_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_tmp_positive';

use check_constraints_metadata;
drop temporary table check_values_tmp;
drop database check_constraints_metadata;

-- @case
-- @desc:CHECK constraint names stay unique per schema so metadata joins attribute expressions to the owning table (#30343)
-- @label:bvt

drop database if exists check_names_30343;
create database check_names_30343;
use check_names_30343;

-- unnamed CHECKs are named after their owning table, MySQL-style
create table plain1 (x int, check (x > 0));
create table plain2 (y int, check (y < 10));
show create table plain1;
show create table plain2;

-- CREATE TABLE LIKE regenerates every CHECK name
create table src (id int primary key, x int, constraint ck_positive check (x > 0));
create table dst like src;
show create table dst;

-- an explicit name already used by a sibling table is rejected
create table dup1 (z int, constraint ck_positive check (z > 0));

-- metadata joins attribute each CHECK expression to its owning table
select constraint_name, check_clause
from information_schema.check_constraints
where constraint_schema = 'check_names_30343'
order by constraint_name;
select tc.table_name, cc.check_clause
from information_schema.table_constraints tc
join information_schema.check_constraints cc
  on tc.constraint_schema = cc.constraint_schema
 and tc.constraint_name = cc.constraint_name
where tc.constraint_schema = 'check_names_30343'
  and tc.constraint_type = 'CHECK'
order by tc.table_name;

-- enforcement still rejects violating rows under the regenerated name
insert into dst values (2, -1);
select count(*) from dst;

-- the per-table ordinal counts every CHECK in definition order
create table multi_chk (a int, check (a > 0), constraint named_chk check (a < 10), check (a > -5));
show create table multi_chk;

drop database check_names_30343;
-- @case
-- @desc:RENAME TABLE carries generated CHECK names so the old table name can be recreated; TRUNCATE keeps them (#30343)
-- @label:bvt

drop database if exists check_rename_30343;
create database check_rename_30343;
use check_rename_30343;

-- renaming moves the generated name with its table
create table t (a int, check (a > 0));
rename table t to u;
show create table u;

-- the released old name can own a fresh generated CHECK again
create table t (a int, check (a < 100));
show create table t;

insert into u values (-1);
insert into t values (200);
select count(*) from u;
select count(*) from t;

-- TRUNCATE recreates the table internally but keeps its CHECK identity
truncate table u;
show create table u;
insert into u values (-1);
select count(*) from u;

select constraint_name, check_clause
from information_schema.check_constraints
where constraint_schema = 'check_rename_30343'
order by constraint_name;

-- a rename producing duplicate CHECK names within the table is rejected
create table conflict_src (a int, check (a > 0), constraint u2_chk_1 check (a < 100));
rename table conflict_src to u2;
show create table conflict_src;

-- a rename producing duplicate CHECK names across sibling tables is rejected
create table owner (a int, constraint u3_chk_1 check (a < 0));
create table mover (a int, check (a > 0));
rename table mover to u3;
show create table mover;

-- a combined table+column rename carries the final CHECK names
create table combo (a int, check (a > 0));
alter table combo rename to combo2, rename column a to b;
show create table combo2;
insert into combo2 values (-1);
create table combo (a int, check (a > 0));
show create table combo;

drop database check_rename_30343;
