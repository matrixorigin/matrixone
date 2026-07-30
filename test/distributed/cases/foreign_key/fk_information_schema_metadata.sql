-- @bvt:issue
drop database if exists mysql_compat_model11_min2;
create database mysql_compat_model11_min2;
use mysql_compat_model11_min2;

create table p (id int primary key);
create table p_compound (id int, code int, primary key (id, code));
create table c (
  id int primary key,
  parent_id int,
  compound_parent_id int,
  parent_code int,
  constraint fk_c_p foreign key (parent_id) references p(id),
  constraint fk_c_p_compound foreign key (compound_parent_id, parent_code) references p_compound(id, code)
);

select constraint_name, delete_rule, update_rule
from information_schema.referential_constraints
where constraint_schema = database()
order by constraint_name;

select table_name, column_name, referenced_table_name, referenced_column_name, ordinal_position
from information_schema.key_column_usage
where table_schema = database() and referenced_table_name is not null
order by table_name, constraint_name, ordinal_position;

drop database mysql_compat_model11_min2;
