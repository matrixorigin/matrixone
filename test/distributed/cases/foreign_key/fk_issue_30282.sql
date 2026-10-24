drop database if exists fk_issue_30282;
create database fk_issue_30282;
use fk_issue_30282;

create table parent (id int primary key);

-- #30282: ON DELETE SET NULL over a NOT NULL child column must be rejected (MySQL error 1830)
create table child_del (
    id int primary key,
    parent_id int not null,
    key idx_parent (parent_id),
    constraint fk_parent_del foreign key (parent_id) references parent (id) on delete set null
);

-- same for ON UPDATE SET NULL
create table child_upd (
    id int primary key,
    parent_id int not null,
    key idx_parent (parent_id),
    constraint fk_parent_upd foreign key (parent_id) references parent (id) on update set null
);

-- primary key implies NOT NULL: same rejection even without an explicit NOT NULL
create table child_pk (
    parent_id int primary key,
    constraint fk_parent_pk foreign key (parent_id) references parent (id) on delete set null
);

-- self-referencing foreign key over a NOT NULL column
create table self_ref (
    id int primary key,
    pid int not null,
    key idx_pid (pid),
    constraint fk_self foreign key (pid) references self_ref (id) on update set null
);

-- the invalid definition cannot enter through ALTER TABLE ADD CONSTRAINT either
create table child_notnull (
    id int primary key,
    parent_id int not null,
    key idx_parent (parent_id)
);
alter table child_notnull
    add constraint fk_parent_alter foreign key (parent_id) references parent (id) on delete set null;
alter table child_notnull
    add constraint fk_parent_alter foreign key (parent_id) references parent (id) on update set null;

-- COPY-mode ALTER (mixed with another action) must reject it too
alter table child_notnull add column val int, add constraint fk_parent_copy foreign key (parent_id) references parent (id) on delete set null;

-- confirm none of the rejected constraints was installed
select count(*) as fk_count from information_schema.table_constraints where constraint_schema = 'fk_issue_30282' and constraint_type = 'FOREIGN KEY';

-- NULLABLE pivots, both actions allowed
create table child_ok (
    id int primary key,
    parent_id int,
    key idx_parent (parent_id)
);
alter table child_ok add constraint fk_parent_ok foreign key (parent_id) references parent (id) on delete set null on update set null;
create table child_ok2 (
    id int primary key,
    parent_id int,
    key idx_parent (parent_id),
    constraint fk_parent_ok2 foreign key (parent_id) references parent (id) on delete cascade on update set null
);
select count(*) as fk_count from information_schema.table_constraints where constraint_schema = 'fk_issue_30282' and constraint_type = 'FOREIGN KEY';

drop table child_notnull;
drop table child_ok;
drop table child_ok2;
drop table parent;

-- review: an unresolved (forward-referenced) child declaring ON DELETE SET NULL
-- over a NOT NULL column under foreign_key_checks=0 must fail when the parent
-- creation binds the pair (MySQL error 1830)
set foreign_key_checks = 0;
create table fwd_child (
    id int primary key,
    pid int not null,
    key idx_pid (pid),
    constraint fk_fwd foreign key (pid) references fwd_parent (id) on delete set null
);
set foreign_key_checks = 1;
create table fwd_parent (id int primary key);

-- same flow over a NULLABLE column stays legal
set foreign_key_checks = 0;
create table fwd_child_ok (
    id int primary key,
    pid int,
    key idx_pid (pid),
    constraint fk_fwd_ok foreign key (pid) references fwd_parent (id) on delete set null
);
-- drop the rejected child, then the parent creation succeeds and binds fwd_child_ok
drop table fwd_child;
create table fwd_parent (id int primary key);
set foreign_key_checks = 1;
select count(*) as fk_count from information_schema.table_constraints where constraint_schema = 'fk_issue_30282' and constraint_type = 'FOREIGN KEY';

-- review: ALTER tightening the child column under a SURVIVING ON DELETE
-- SET NULL constraint must be rejected in every form (MySQL error 1830)
create table alt_parent (id int primary key);
create table alt_child (
    id int primary key,
    pid int,
    key idx_pid (pid),
    constraint fk_alt_setnull foreign key (pid) references alt_parent (id) on delete set null
);
alter table alt_child modify pid int not null;
alter table alt_child change pid pid int not null;

-- same via ADD PRIMARY KEY on a table without an explicit primary key
create table alt_child_np (
    id int,
    pid int,
    key idx_pid (pid),
    constraint fk_alt_np foreign key (pid) references alt_parent (id) on delete set null
);
alter table alt_child_np add primary key (pid);

-- dropping the constraint unblocks the tightening
alter table alt_child drop foreign key fk_alt_setnull;
alter table alt_child modify pid int not null;
select count(*) as non_null_pid from information_schema.columns where table_schema = 'fk_issue_30282' and table_name = 'alt_child' and column_name = 'pid' and is_nullable = 'NO';

-- dropping the constraint first unblocks ADD PRIMARY KEY as well
alter table alt_child_np drop foreign key fk_alt_np;
alter table alt_child_np add primary key (pid);
select count(*) as np_fk_count from information_schema.table_constraints where constraint_schema = 'fk_issue_30282' and table_name = 'alt_child_np' and constraint_type = 'FOREIGN KEY';

-- CASCADE over the same column stays legal when tightening
create table alt_child_c (
    id int primary key,
    pid int,
    key idx_pid (pid),
    constraint fk_alt_c foreign key (pid) references alt_parent (id) on delete cascade
);
alter table alt_child_c modify pid int not null;
select count(*) as c_non_null_pid from information_schema.columns where table_schema = 'fk_issue_30282' and table_name = 'alt_child_c' and column_name = 'pid' and is_nullable = 'NO';

-- nullable-preserving MODIFY stays legal
create table alt_child_n (
    id int primary key,
    pid int,
    key idx_pid (pid),
    constraint fk_alt_n foreign key (pid) references alt_parent (id) on delete set null
);
alter table alt_child_n modify pid int;
select count(*) as n_nullable_pid from information_schema.columns where table_schema = 'fk_issue_30282' and table_name = 'alt_child_n' and column_name = 'pid' and is_nullable = 'YES';

-- review round 3: reusing the NAME of a column dropped or renamed away by
-- the same ALTER must not let the fk column inherit the dropped column's
-- NOT NULL exemption (CHANGE/RENAME preserve the column identity)
create table nr_parent (id int primary key);
create table nr_child (
    id int primary key,
    pid int,
    other int not null,
    key idx_pid (pid),
    constraint fk_nr_setnull foreign key (pid) references nr_parent (id) on delete set null
);
alter table nr_child drop column other, change pid other int not null;
alter table nr_child change other tmp int null, change pid other int not null;

-- once the constraint is gone the very same rename-and-tighten is legal
alter table nr_child drop foreign key fk_nr_setnull;
alter table nr_child change other tmp int null, change pid other int not null;
select count(*) as nr_non_null_other from information_schema.columns where table_schema = 'fk_issue_30282' and table_name = 'nr_child' and column_name = 'other' and is_nullable = 'NO';
select count(*) as nr_fk_count from information_schema.table_constraints where constraint_schema = 'fk_issue_30282' and table_name = 'nr_child' and constraint_type = 'FOREIGN KEY';

drop table nr_child;
drop table nr_parent;

drop table fwd_child_ok;
drop table fwd_parent;
drop table alt_child;
drop table alt_child_np;
drop table alt_child_c;
drop table alt_child_n;
drop table alt_parent;
drop database fk_issue_30282;
