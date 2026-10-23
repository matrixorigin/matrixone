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
drop database fk_issue_30282;
