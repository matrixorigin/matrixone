-- Branch-side non-key updates must not transiently delete referenced rows.
drop database if exists branch_merge_foreign_key_update;
create database branch_merge_foreign_key_update;
use branch_merge_foreign_key_update;

create table child_t (id int primary key, note varchar(32));
create table parent_t (
    id int primary key,
    child_id int,
    constraint fk_parent_child foreign key (child_id) references child_t(id)
);
insert into child_t values (1, 'one'), (2, 'two');
insert into parent_t values (1, 1), (2, 2);

data branch create table leaf_merge from child_t;
update leaf_merge set note = 'leaf-merge' where id = 2;

-- A non-conflicting branch update to a referenced row succeeds.
data branch merge leaf_merge into child_t when conflict accept;
select id, note from child_t order by id;

-- The ordinary UPDATE control succeeds, then ACCEPT overwrites it in place.
update child_t set note = 'direct-control' where id = 2;
select id, note from child_t order by id;
data branch merge leaf_merge into child_t when conflict accept;
select id, note from child_t order by id;

drop database branch_merge_foreign_key_update;
