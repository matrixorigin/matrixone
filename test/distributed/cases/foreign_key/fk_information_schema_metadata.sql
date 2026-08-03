-- @bvt:issue
drop database if exists mysql_compat_model11_min2;
create database mysql_compat_model11_min2;
use mysql_compat_model11_min2;

create table p (id int primary key, key p_secondary (id));
create table p_compound (id int, code int, unique key uq_p_compound (id, code), key p_compound_secondary (id, code));
create table unrelated (id int, unique key unrelated_id (id));
create table c (
  id int primary key,
  parent_id int,
  compound_parent_id int,
  parent_code int,
  restrict_parent_id int,
  constraint fk_c_p foreign key (parent_id) references p(id),
  constraint fk_c_p_compound foreign key (compound_parent_id, parent_code) references p_compound(id, code),
  constraint fk_c_p_restrict foreign key (restrict_parent_id) references p(id) on delete restrict on update restrict
);

select constraint_name, unique_constraint_name, delete_rule, update_rule
from information_schema.referential_constraints
where constraint_schema = database()
order by constraint_name;

select count(*) as referential_constraint_count
from information_schema.referential_constraints
where constraint_schema = database();

select table_name, column_name, referenced_table_name, referenced_column_name, ordinal_position
from information_schema.key_column_usage
where table_schema = database() and referenced_table_name is not null
order by table_name, constraint_name, ordinal_position;

drop database mysql_compat_model11_min2;
