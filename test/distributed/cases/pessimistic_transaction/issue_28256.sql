-- TRUNCATE TABLE follows MySQL's implicit-commit-before-and-after rule.
-- The assertions deliberately check both a successful DDL and failures that
-- occur during planning, because the preceding user transaction must not
-- share the rollback fate of the TRUNCATE attempt.
drop database if exists issue_28256_truncate;
create database issue_28256_truncate;
use issue_28256_truncate;

create table truncate_target (
    id int auto_increment primary key,
    value int
);
create table transaction_marker (
    id int primary key
);
insert into truncate_target(value) values (10), (20);

-- A successful TRUNCATE commits the marker before it clears the target.  The
-- following ROLLBACK cannot restore the target or remove the marker.
start transaction;
insert into transaction_marker values (1);
truncate table truncate_target;
rollback;
select 'success' as scenario,
       (select count(*) from truncate_target) as target_rows,
       (select count(*) from transaction_marker) as marker_rows;

insert into truncate_target(value) values (30);
select 'auto_increment_reset' as scenario, id from truncate_target;

-- A foreign-key rejection still commits work that preceded the TRUNCATE.
create table truncate_parent (id int primary key);
create table truncate_child (
    id int primary key,
    parent_id int,
    foreign key(parent_id) references truncate_parent(id)
);
insert into truncate_parent values (1);
insert into truncate_child values (1, 1);
start transaction;
insert into transaction_marker values (2);
truncate table truncate_parent;
rollback;
select 'foreign_key_rejection' as scenario,
       (select count(*) from truncate_parent) as parent_rows,
       (select count(*) from truncate_child) as child_rows,
       (select count(*) from transaction_marker where id = 2) as marker_rows;

-- A missing target is a compile/planning failure after the implicit precommit.
start transaction;
insert into transaction_marker values (3);
truncate table missing_truncate_target;
rollback;
select 'missing_target' as scenario,
       (select count(*) from transaction_marker where id = 3) as marker_rows;

-- AUTOCOMMIT=0 changes the session setting, not the TRUNCATE boundary.
set autocommit = 0;
insert into transaction_marker values (4);
truncate table truncate_target;
insert into truncate_target(value) values (40);
rollback;
select 'autocommit_off' as scenario,
       (select count(*) from truncate_target) as target_rows,
       (select count(*) from transaction_marker where id = 4) as marker_rows;
set autocommit = 1;

-- Text PREPARE is not an execution boundary; EXECUTE is.
create table prepared_truncate_target (id int primary key);
insert into prepared_truncate_target values (1), (2);
prepare truncate_prepared from 'truncate table prepared_truncate_target';
start transaction;
insert into transaction_marker values (5);
execute truncate_prepared;
rollback;
select 'prepared_execute' as scenario,
       (select count(*) from prepared_truncate_target) as target_rows,
       (select count(*) from transaction_marker where id = 5) as marker_rows;
deallocate prepare truncate_prepared;

drop database issue_28256_truncate;
