drop database if exists fk_issue_26831;
create database fk_issue_26831;
use fk_issue_26831;

create table tree (
    id int primary key,
    parent_id int null,
    constraint fk_self foreign key (parent_id) references tree(id)
        on delete cascade on update cascade
);

insert into tree values (1, null), (2, 1), (3, 2), (4, 2);
delete from tree where id = 4;
select count(*) as rows_after_leaf_delete from tree;
insert into tree values (4, 2);
delete from tree where id = 1;
select count(*) as remaining_rows from tree;
select count(*) as orphan_count
from tree c
where c.parent_id is not null
  and not exists (select 1 from tree p where p.id = c.parent_id);

insert into tree values (1, null), (2, 1), (3, 2), (4, 2);
update tree set id = 10 where id = 1;
select id, parent_id from tree order by id;
select count(*) as orphan_count
from tree c
where c.parent_id is not null
  and not exists (select 1 from tree p where p.id = c.parent_id);

delete from tree where id = 10;
insert into tree values (1, null);
update tree set parent_id = 1 where id = 1;
update tree set id = 10 where id = 1;
select id, parent_id from tree;
select count(*) as orphan_count
from tree c
where c.parent_id is not null
  and not exists (select 1 from tree p where p.id = c.parent_id);

drop database fk_issue_26831;
