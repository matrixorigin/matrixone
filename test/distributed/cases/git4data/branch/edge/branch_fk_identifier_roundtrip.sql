-- Regression for #26144: FK catalog values must preserve identifier bytes.
drop database if exists `br_fk_ident_src\part`;
drop database if exists br_fk_ident_branch;
drop database if exists br_fk_ident_clone;

create database `br_fk_ident_src\part`;
use `br_fk_ident_src\part`;
create table `parent\name`(id int primary key);
create table `child\name`(id int primary key, parent_id int, constraint `fk\name_one` foreign key(parent_id) references `parent\name`(id));
insert into `br_fk_ident_src\part`.`parent\name` values (1);
insert into `br_fk_ident_src\part`.`child\name` values (1, 1);

data branch create database br_fk_ident_branch from `br_fk_ident_src\part`;
create database br_fk_ident_clone clone `br_fk_ident_src\part`;
select hex(table_name), hex(constraint_name), hex(refer_table_name) from mo_catalog.mo_foreign_keys where db_name = 'br_fk_ident_branch';
select hex(table_name), hex(constraint_name), hex(refer_table_name) from mo_catalog.mo_foreign_keys where db_name = 'br_fk_ident_clone';
-- @regex("foreign key constraint fails",true)
insert into br_fk_ident_branch.`child\name` values (2, 999);
-- @regex("foreign key constraint fails",true)
insert into br_fk_ident_clone.`child\name` values (2, 999);

drop database br_fk_ident_branch;
drop database br_fk_ident_clone;
drop database `br_fk_ident_src\part`;
