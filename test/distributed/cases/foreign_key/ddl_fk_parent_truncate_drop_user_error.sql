-- Issue #31724: TRUNCATE/DROP on an FK-referenced parent must fail with MySQL's
-- dedicated user errors instead of a generic internal error, with the constraint
-- and child names resolved from live FK metadata. The rejection itself and the
-- foreign_key_checks asymmetry are unchanged: with checks disabled DROP passes
-- while TRUNCATE keeps rejecting. The statements between the errors prove the
-- same connection stays usable after each rejection.
drop database if exists fk_trunc_drop_31724;
create database fk_trunc_drop_31724;
use fk_trunc_drop_31724;
create table parent(id int primary key, v int);
create table child(id int primary key, pid int, constraint fk_child_31724 foreign key (pid) references parent(id));
insert into parent values (1,1);
insert into child values (10,1);
truncate table parent;
insert into parent values (2,2);
drop table parent;
insert into parent values (3,3);
set session foreign_key_checks = 0;
truncate table parent;
drop table parent;
set session foreign_key_checks = 1;
drop database fk_trunc_drop_31724;
