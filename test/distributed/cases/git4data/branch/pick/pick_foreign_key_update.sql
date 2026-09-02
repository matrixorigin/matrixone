-- PICK must apply non-key updates to referenced rows without delete/insert replay.
drop database if exists branch_pick_foreign_key_update;
create database branch_pick_foreign_key_update;
use branch_pick_foreign_key_update;

create table child_t (id int primary key, note varchar(32));
create table parent_t (
    id int primary key,
    child_id int,
    constraint fk_parent_child foreign key (child_id) references child_t(id)
);
insert into child_t values (1, 'one'), (2, 'two');
insert into parent_t values (1, 1), (2, 2);

data branch create table leaf_pick from child_t;
update leaf_pick set note = 'leaf-pick-clean' where id = 1;
update leaf_pick set note = 'leaf-pick' where id = 2;

-- A non-conflicting picked update to a referenced row succeeds.
data branch pick leaf_pick into child_t keys(1) when conflict accept;
select id, note from child_t order by id;

-- Ordinary UPDATE succeeds; FAIL keeps the destination atomic, ACCEPT then wins.
update child_t set note = 'direct-control' where id = 2;
select id, note from child_t order by id;
data branch pick leaf_pick into child_t keys(2) when conflict fail;
select id, note from child_t order by id;
data branch pick leaf_pick into child_t keys(2) when conflict accept;
select id, note from child_t order by id;

drop database branch_pick_foreign_key_update;
