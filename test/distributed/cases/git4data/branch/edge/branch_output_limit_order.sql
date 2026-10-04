-- DATA BRANCH DIFF OUTPUT LIMIT must select the prefix of the final PK order.

drop database if exists branch_output_limit_order;
create database branch_output_limit_order;
use branch_output_limit_order;

create table base(id int primary key, v varchar(20));
insert into base values (50, 'root');
data branch create table child from base;

-- The target-side high key is emitted before the base-side low key in the
-- regression scenario. LIMIT must still retain the low key.
insert into base values (1, 'base-low');
insert into child values (100, 'child-high');
data branch diff child against base;
data branch diff child against base output limit 1;

-- A projected result must use the hidden PK value for limit selection.
data branch diff child against base columns (v) output limit 1;

-- Selecting more than one row must retain the complete sorted prefix.
insert into base values (2, 'base-mid');
insert into child values (99, 'child-mid');
data branch diff child against base output limit 2;

-- Composite keys are compared lexicographically across all PK columns.
create table composite_base(grp int, id int, v varchar(20), primary key(grp, id));
insert into composite_base values (0, 0, 'root');
data branch create table composite_child from composite_base;
insert into composite_base values (1, 1, 'base-low');
insert into composite_child values (1, 2, 'child-high');
data branch diff composite_child against composite_base output limit 1;

drop database branch_output_limit_order;
