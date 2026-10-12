-- Issue #27117: RESTORE TABLE must not report success for a referenced table.
drop snapshot if exists issue27117_control_snapshot;
drop snapshot if exists issue27117_parent_snapshot;
drop database if exists issue27117_control;
drop database if exists issue27117_fk;

create database issue27117_control;
create table issue27117_control.t (id int primary key, v varchar(16));
insert into issue27117_control.t values (1, 'before');
create snapshot issue27117_control_snapshot for table issue27117_control t;
update issue27117_control.t set v = 'changed';
restore table issue27117_control.t{snapshot='issue27117_control_snapshot'};
select 'control_table_restore' as case_name, v from issue27117_control.t;

create database issue27117_fk;
create table issue27117_fk.parent_t (id int primary key, v varchar(16));
create table issue27117_fk.child_t (
    cid int primary key,
    pid int,
    constraint fk_parent foreign key (pid) references issue27117_fk.parent_t(id)
);
insert into issue27117_fk.parent_t values (1, 'before');
insert into issue27117_fk.child_t values (1, 1);
create snapshot issue27117_parent_snapshot for table issue27117_fk parent_t;
update issue27117_fk.parent_t set v = 'changed';

select 'fk_snapshot_read' as case_name, v from issue27117_fk.parent_t{snapshot='issue27117_parent_snapshot'};
-- @regex("not supported: can not restore table .* referenced by some foreign key constraint",true)
restore table issue27117_fk.parent_t{snapshot='issue27117_parent_snapshot'};
select 'fk_table_restore' as case_name, v from issue27117_fk.parent_t;
select 'fk_child_unchanged' as case_name, cid, pid from issue27117_fk.child_t;

drop snapshot if exists issue27117_control_snapshot;
drop snapshot if exists issue27117_parent_snapshot;
drop database if exists issue27117_control;
drop database if exists issue27117_fk;
