-- @suite

-- @case
-- @desc: MySQL compatibility cases for NULL with subquery predicates
-- @label:bvt

drop database if exists mysql_compat_null_subquery;
create database mysql_compat_null_subquery;
use mysql_compat_null_subquery;

create table t_outer (
  id int primary key,
  k int
);

create table t_inner (
  grp int,
  val int
);

insert into t_outer values
  (1, 1),
  (2, 2),
  (3, 3),
  (4, null),
  (5, 4);

insert into t_inner values
  (1, 1),
  (1, 2),
  (1, null),
  (2, null),
  (3, 4);

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       k in (select val from t_inner where grp = 1) as in_null_set,
       k not in (select val from t_inner where grp = 1) as not_in_null_set,
       k in (select val from t_inner where grp = 99) as in_empty_set,
       k not in (select val from t_inner where grp = 99) as not_in_empty_set
from t_outer
order by id;

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       exists (select 1 from t_inner i where i.val = t_outer.k) as exists_eq,
       not exists (select 1 from t_inner i where i.val = t_outer.k) as not_exists_eq,
       exists (select 1 from t_inner i where i.val <=> t_outer.k) as exists_null_safe,
       not exists (select 1 from t_inner i where i.val <=> t_outer.k) as not_exists_null_safe
from t_outer
order by id;

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       k = (select max(val) from t_inner where grp = 3) as eq_scalar_nonnull,
       k = (select max(val) from t_inner where grp = 2) as eq_scalar_all_null,
       k <=> (select max(val) from t_inner where grp = 2) as null_safe_scalar_all_null,
       k = (select max(val) from t_inner where grp = 99) as eq_scalar_empty,
       k <=> (select max(val) from t_inner where grp = 99) as null_safe_scalar_empty
from t_outer
order by id;

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       k = any (select val from t_inner where grp = 1) as eq_any_null_set,
       k <> all (select val from t_inner where grp = 1) as ne_all_null_set,
       k > all (select val from t_inner where grp = 1) as gt_all_null_set,
       k < any (select val from t_inner where grp = 1) as lt_any_null_set
from t_outer
order by id;

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       k = any (select val from t_inner where grp = 99) as eq_any_empty,
       k <> all (select val from t_inner where grp = 99) as ne_all_empty,
       k > all (select val from t_inner where grp = 99) as gt_all_empty,
       k < any (select val from t_inner where grp = 99) as lt_any_empty
from t_outer
order by id;

select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       k = any (select val from t_inner where grp = 2) as eq_any_all_null,
       k <> all (select val from t_inner where grp = 2) as ne_all_all_null,
       k > all (select val from t_inner where grp = 2) as gt_all_all_null,
       k < any (select val from t_inner where grp = 2) as lt_any_all_null
from t_outer
order by id;

drop database mysql_compat_null_subquery;
