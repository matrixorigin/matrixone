-- @suite
-- @case
-- @desc: clone databases after COPY and metadata-only ALTER operations on foreign-key tables
-- @label:bvt

-- 1. Repeated unrelated ALTER operations on one FK child.
drop database if exists fk_clone_alter_child_src;
drop database if exists fk_clone_alter_child_dst;
create database fk_clone_alter_child_src;
use fk_clone_alter_child_src;
create table fk_clone_alter_child_src.parent (
    id int primary key,
    name varchar(32) not null
);
create table fk_clone_alter_child_src.child (
    id int primary key,
    parent_id int,
    payload varchar(32),
    constraint fk_child_parent foreign key (parent_id)
        references fk_clone_alter_child_src.parent(id)
);
insert into fk_clone_alter_child_src.parent values (1, 'p1'), (2, 'p2');
insert into fk_clone_alter_child_src.child values (10, 1, 'c1'), (20, 2, 'c2');
alter table fk_clone_alter_child_src.child add column note varchar(32) default 'n';
alter table fk_clone_alter_child_src.child modify column payload varchar(64);
alter table fk_clone_alter_child_src.child rename column note to remark;
alter table fk_clone_alter_child_src.child add index idx_payload(payload);
alter table fk_clone_alter_child_src.child drop index idx_payload;
alter table fk_clone_alter_child_src.child drop column remark;
select table_name, refer_table_name
from mo_catalog.mo_foreign_keys
where db_name = 'fk_clone_alter_child_src'
order by constraint_name;
delete from fk_clone_alter_child_src.parent where id = 1;
create database fk_clone_alter_child_dst clone fk_clone_alter_child_src;
select * from fk_clone_alter_child_dst.child order by id;
show create table fk_clone_alter_child_dst.child;
delete from fk_clone_alter_child_dst.parent where id = 1;
insert into fk_clone_alter_child_dst.child values (30, 999, 'invalid');
insert into fk_clone_alter_child_dst.child values (30, 1, 'valid');
select count(*) from fk_clone_alter_child_dst.child;
drop database fk_clone_alter_child_dst;
drop database fk_clone_alter_child_src;

-- 2. ALTER the parent, then the child. Child FK metadata and parent reverse
-- references must both follow the new physical table IDs.
drop database if exists fk_clone_alter_both_src;
drop database if exists fk_clone_alter_both_dst;
create database fk_clone_alter_both_src;
use fk_clone_alter_both_src;
create table fk_clone_alter_both_src.parent (
    id int primary key,
    code varchar(16) unique
);
create table fk_clone_alter_both_src.child (
    id int primary key,
    parent_id int,
    constraint fk_both foreign key (parent_id)
        references fk_clone_alter_both_src.parent(id)
);
insert into fk_clone_alter_both_src.parent values (1, 'a');
insert into fk_clone_alter_both_src.child values (1, 1);
alter table fk_clone_alter_both_src.parent add column description varchar(64);
alter table fk_clone_alter_both_src.parent modify column description varchar(128);
alter table fk_clone_alter_both_src.child add column created_at datetime;
create database fk_clone_alter_both_dst clone fk_clone_alter_both_src;
show create table fk_clone_alter_both_dst.parent;
show create table fk_clone_alter_both_dst.child;
insert into fk_clone_alter_both_dst.child(id, parent_id) values (2, 404);
select count(*) from fk_clone_alter_both_dst.child;
drop database fk_clone_alter_both_dst;
drop database fk_clone_alter_both_src;

-- 3. Multiple altered children referencing the same parent.
drop database if exists fk_clone_siblings_src;
drop database if exists fk_clone_siblings_dst;
create database fk_clone_siblings_src;
use fk_clone_siblings_src;
create table fk_clone_siblings_src.parent (id int primary key);
create table fk_clone_siblings_src.child_a (
    id int primary key,
    parent_id int,
    constraint fk_sibling_a foreign key (parent_id)
        references fk_clone_siblings_src.parent(id)
);
create table fk_clone_siblings_src.child_b (
    id int primary key,
    parent_id int,
    constraint fk_sibling_b foreign key (parent_id)
        references fk_clone_siblings_src.parent(id)
);
alter table fk_clone_siblings_src.child_a add column a1 int default 1;
alter table fk_clone_siblings_src.child_b add column b1 int default 2;
alter table fk_clone_siblings_src.child_a add column a2 int default 3;
create database fk_clone_siblings_dst clone fk_clone_siblings_src;
show create table fk_clone_siblings_dst.child_a;
show create table fk_clone_siblings_dst.child_b;
insert into fk_clone_siblings_dst.child_a values (1, 100, 1, 3);
insert into fk_clone_siblings_dst.child_b values (1, 100, 2);
drop database fk_clone_siblings_dst;
drop database fk_clone_siblings_src;

-- 4. Multi-level FK chain with ALTER at every level.
drop database if exists fk_clone_chain_src;
drop database if exists fk_clone_chain_dst;
create database fk_clone_chain_src;
use fk_clone_chain_src;
create table fk_clone_chain_src.root (id int primary key);
create table fk_clone_chain_src.middle (
    id int primary key,
    root_id int,
    constraint fk_middle_root foreign key (root_id)
        references fk_clone_chain_src.root(id)
);
create table fk_clone_chain_src.leaf (
    id int primary key,
    middle_id int,
    constraint fk_leaf_middle foreign key (middle_id)
        references fk_clone_chain_src.middle(id)
);
insert into fk_clone_chain_src.root values (1);
insert into fk_clone_chain_src.middle values (1, 1);
insert into fk_clone_chain_src.leaf values (1, 1);
alter table fk_clone_chain_src.root add column root_value varchar(20);
alter table fk_clone_chain_src.middle add column middle_value varchar(20);
alter table fk_clone_chain_src.leaf add column leaf_value varchar(20);
create database fk_clone_chain_dst clone fk_clone_chain_src;
select leaf.id, middle.id, root.id
from fk_clone_chain_dst.leaf leaf
join fk_clone_chain_dst.middle middle on leaf.middle_id = middle.id
join fk_clone_chain_dst.root root on middle.root_id = root.id;
insert into fk_clone_chain_dst.leaf(id, middle_id) values (2, 999);
drop database fk_clone_chain_dst;
drop database fk_clone_chain_src;

-- 5. Composite foreign key after repeated child ALTER.
drop database if exists fk_clone_composite_src;
drop database if exists fk_clone_composite_dst;
create database fk_clone_composite_src;
use fk_clone_composite_src;
create table fk_clone_composite_src.parent (
    tenant_id int,
    object_id int,
    primary key (tenant_id, object_id)
);
create table fk_clone_composite_src.child (
    id int primary key,
    tenant_id int,
    object_id int,
    constraint fk_composite foreign key (tenant_id, object_id)
        references fk_clone_composite_src.parent(tenant_id, object_id)
);
insert into fk_clone_composite_src.parent values (1, 1);
insert into fk_clone_composite_src.child values (1, 1, 1);
alter table fk_clone_composite_src.child add column payload varchar(32);
alter table fk_clone_composite_src.child modify column payload varchar(64);
create database fk_clone_composite_dst clone fk_clone_composite_src;
show create table fk_clone_composite_dst.child;
insert into fk_clone_composite_dst.child(id, tenant_id, object_id) values (2, 1, 999);
drop database fk_clone_composite_dst;
drop database fk_clone_composite_src;

-- 6. One altered child referencing two different parents.
drop database if exists fk_clone_multi_parent_src;
drop database if exists fk_clone_multi_parent_dst;
create database fk_clone_multi_parent_src;
use fk_clone_multi_parent_src;
create table fk_clone_multi_parent_src.parent_a (id int primary key);
create table fk_clone_multi_parent_src.parent_b (id int primary key);
create table fk_clone_multi_parent_src.child (
    id int primary key,
    parent_a_id int,
    parent_b_id int,
    constraint fk_multi_a foreign key (parent_a_id)
        references fk_clone_multi_parent_src.parent_a(id),
    constraint fk_multi_b foreign key (parent_b_id)
        references fk_clone_multi_parent_src.parent_b(id)
);
alter table fk_clone_multi_parent_src.child add column payload int default 0;
alter table fk_clone_multi_parent_src.child add column note varchar(32);
create database fk_clone_multi_parent_dst clone fk_clone_multi_parent_src;
show create table fk_clone_multi_parent_dst.child;
insert into fk_clone_multi_parent_dst.child(id, parent_a_id, parent_b_id) values (1, 1, 1);
drop database fk_clone_multi_parent_dst;
drop database fk_clone_multi_parent_src;

-- 7. Self-referencing FK remains valid after ALTER and clone.
drop database if exists fk_clone_self_src;
drop database if exists fk_clone_self_dst;
create database fk_clone_self_src;
use fk_clone_self_src;
create table fk_clone_self_src.node (
    id int primary key,
    parent_id int,
    constraint fk_node_parent foreign key (parent_id)
        references fk_clone_self_src.node(id)
);
insert into fk_clone_self_src.node values (1, null), (2, 1);
alter table fk_clone_self_src.node add column label varchar(32);
alter table fk_clone_self_src.node add index idx_label(label);
create database fk_clone_self_dst clone fk_clone_self_src;
select id, parent_id from fk_clone_self_dst.node order by id;
show create table fk_clone_self_dst.node;
insert into fk_clone_self_dst.node(id, parent_id) values (3, 999);
drop database fk_clone_self_dst;
drop database fk_clone_self_src;

-- 8. Drop and recreate an FK around other ALTER operations.
drop database if exists fk_clone_recreate_fk_src;
drop database if exists fk_clone_recreate_fk_dst;
create database fk_clone_recreate_fk_src;
use fk_clone_recreate_fk_src;
create table fk_clone_recreate_fk_src.parent (id int primary key);
create table fk_clone_recreate_fk_src.child (
    id int primary key,
    parent_id int,
    constraint fk_recreated foreign key (parent_id)
        references fk_clone_recreate_fk_src.parent(id)
);
alter table fk_clone_recreate_fk_src.child drop foreign key fk_recreated;
alter table fk_clone_recreate_fk_src.child add column payload varchar(32);
alter table fk_clone_recreate_fk_src.child
    add constraint fk_recreated foreign key (parent_id)
        references fk_clone_recreate_fk_src.parent(id);
alter table fk_clone_recreate_fk_src.child add column note varchar(32);
create database fk_clone_recreate_fk_dst clone fk_clone_recreate_fk_src;
show create table fk_clone_recreate_fk_dst.child;
insert into fk_clone_recreate_fk_dst.child(id, parent_id) values (1, 999);
drop database fk_clone_recreate_fk_dst;
drop database fk_clone_recreate_fk_src;

-- 9. A catalog-only forward reference must survive COPY ALTER until its
-- parent is created.
drop database if exists fk_clone_forward_src;
create database fk_clone_forward_src;
use fk_clone_forward_src;
set foreign_key_checks = 0;
create table fk_clone_forward_src.child (
    id int primary key,
    parent_id int,
    constraint fk_forward foreign key (parent_id)
        references fk_clone_forward_src.parent(id)
);
alter table fk_clone_forward_src.child add column payload int;
select table_name, constraint_name, refer_table_name
from mo_catalog.mo_foreign_keys
where db_name = 'fk_clone_forward_src'
order by constraint_name;
create table fk_clone_forward_src.parent (id int primary key);
set foreign_key_checks = 1;
show create table fk_clone_forward_src.child;
insert into fk_clone_forward_src.child values (1, 404, 0);
drop database fk_clone_forward_src;

-- 10. Dropping and recreating a parent under FOREIGN_KEY_CHECKS=0 is another
-- catalog-only forward-reference path.
drop database if exists fk_clone_forward_drop_src;
create database fk_clone_forward_drop_src;
use fk_clone_forward_drop_src;
create table fk_clone_forward_drop_src.parent (id int primary key);
create table fk_clone_forward_drop_src.child (
    id int primary key,
    parent_id int,
    constraint fk_forward_drop foreign key (parent_id)
        references fk_clone_forward_drop_src.parent(id)
);
set foreign_key_checks = 0;
drop table fk_clone_forward_drop_src.parent;
alter table fk_clone_forward_drop_src.child add column payload int;
select table_name, constraint_name, refer_table_name
from mo_catalog.mo_foreign_keys
where db_name = 'fk_clone_forward_drop_src'
order by constraint_name;
create table fk_clone_forward_drop_src.parent (id int primary key);
set foreign_key_checks = 1;
show create table fk_clone_forward_drop_src.child;
insert into fk_clone_forward_drop_src.child values (1, 404, 0);
drop database fk_clone_forward_drop_src;

-- 11. A self-referencing parent with another child must have one canonical
-- reverse-reference definition after COPY ALTER.
drop database if exists fk_clone_self_external_src;
create database fk_clone_self_external_src;
use fk_clone_self_external_src;
create table fk_clone_self_external_src.parent (
    id int primary key,
    parent_id int,
    constraint fk_self_external_self foreign key (parent_id)
        references fk_clone_self_external_src.parent(id)
);
create table fk_clone_self_external_src.child (
    id int primary key,
    parent_id int,
    constraint fk_self_external_child foreign key (parent_id)
        references fk_clone_self_external_src.parent(id)
);
alter table fk_clone_self_external_src.parent add column payload int;
select table_name, constraint_name, refer_table_name
from mo_catalog.mo_foreign_keys
where db_name = 'fk_clone_self_external_src'
order by table_name, constraint_name;
drop table fk_clone_self_external_src.child;
drop table fk_clone_self_external_src.parent;
drop database fk_clone_self_external_src;

-- 12. FK catalog column names must follow COPY ALTER column renames on both
-- the child and parent sides.
drop database if exists fk_clone_rename_columns_src;
drop database if exists fk_clone_rename_columns_dst;
create database fk_clone_rename_columns_src;
use fk_clone_rename_columns_src;
create table fk_clone_rename_columns_src.parent (old_id int primary key);
create table fk_clone_rename_columns_src.child (
    id int primary key,
    old_parent_id int,
    constraint fk_rename_columns foreign key (old_parent_id)
        references fk_clone_rename_columns_src.parent(old_id)
);
alter table fk_clone_rename_columns_src.child
    rename column old_parent_id to parent_id;
alter table fk_clone_rename_columns_src.parent
    rename column old_id to id;
select table_name, column_name, refer_table_name, refer_column_name
from mo_catalog.mo_foreign_keys
where db_name = 'fk_clone_rename_columns_src'
order by constraint_name;
create database fk_clone_rename_columns_dst clone fk_clone_rename_columns_src;
show create table fk_clone_rename_columns_dst.child;
insert into fk_clone_rename_columns_dst.child values (1, 404);
drop database fk_clone_rename_columns_dst;
drop database fk_clone_rename_columns_src;

-- 13. Clone an explicit database snapshot taken after ALTER.
drop snapshot if exists fk_clone_alter_snapshot;
drop database if exists fk_clone_snapshot_src;
drop database if exists fk_clone_snapshot_dst;
create database fk_clone_snapshot_src;
use fk_clone_snapshot_src;
create table fk_clone_snapshot_src.parent (id int primary key);
create table fk_clone_snapshot_src.child (
    id int primary key,
    parent_id int,
    constraint fk_snapshot foreign key (parent_id)
        references fk_clone_snapshot_src.parent(id)
);
insert into fk_clone_snapshot_src.parent values (1);
insert into fk_clone_snapshot_src.child values (1, 1);
alter table fk_clone_snapshot_src.child add column before_snapshot varchar(32);
create snapshot fk_clone_alter_snapshot for database fk_clone_snapshot_src;
alter table fk_clone_snapshot_src.child add column after_snapshot varchar(32);
insert into fk_clone_snapshot_src.parent values (2);
insert into fk_clone_snapshot_src.child(id, parent_id) values (2, 2);
create database fk_clone_snapshot_dst clone fk_clone_snapshot_src
    {snapshot = 'fk_clone_alter_snapshot'};
show create table fk_clone_snapshot_dst.child;
select id, parent_id from fk_clone_snapshot_dst.child order by id;
insert into fk_clone_snapshot_dst.child(id, parent_id) values (3, 999);
drop database fk_clone_snapshot_dst;
drop database fk_clone_snapshot_src;
drop snapshot fk_clone_alter_snapshot;

-- 10. Clone, ALTER the cloned child, then clone again.
drop database if exists fk_clone_chain_clone_src;
drop database if exists fk_clone_chain_clone_mid;
drop database if exists fk_clone_chain_clone_dst;
create database fk_clone_chain_clone_src;
use fk_clone_chain_clone_src;
create table fk_clone_chain_clone_src.parent (id int primary key);
create table fk_clone_chain_clone_src.child (
    id int primary key,
    parent_id int,
    constraint fk_clone_chain foreign key (parent_id)
        references fk_clone_chain_clone_src.parent(id)
);
alter table fk_clone_chain_clone_src.child add column source_value int;
create database fk_clone_chain_clone_mid clone fk_clone_chain_clone_src;
use fk_clone_chain_clone_mid;
alter table fk_clone_chain_clone_mid.child add column middle_value int;
create database fk_clone_chain_clone_dst clone fk_clone_chain_clone_mid;
show create table fk_clone_chain_clone_dst.child;
insert into fk_clone_chain_clone_dst.child(id, parent_id) values (1, 999);
drop database fk_clone_chain_clone_dst;
drop database fk_clone_chain_clone_mid;
drop database fk_clone_chain_clone_src;

-- 11. Rename FK columns and tables before cloning.
drop database if exists fk_clone_rename_src;
drop database if exists fk_clone_rename_dst;
create database fk_clone_rename_src;
use fk_clone_rename_src;
create table fk_clone_rename_src.parent (id int primary key);
create table fk_clone_rename_src.child (
    id int primary key,
    parent_id int,
    constraint fk_rename foreign key (parent_id)
        references fk_clone_rename_src.parent(id)
);
alter table fk_clone_rename_src.parent rename column id to parent_key;
alter table fk_clone_rename_src.child rename column parent_id to parent_key;
rename table fk_clone_rename_src.parent to fk_clone_rename_src.renamed_parent;
rename table fk_clone_rename_src.child to fk_clone_rename_src.renamed_child;
alter table fk_clone_rename_src.renamed_child add column payload varchar(32);
create database fk_clone_rename_dst clone fk_clone_rename_src;
show create table fk_clone_rename_dst.renamed_child;
insert into fk_clone_rename_dst.renamed_child(id, parent_key) values (1, 999);
drop database fk_clone_rename_dst;
drop database fk_clone_rename_src;
