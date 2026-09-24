drop table if exists issue_28258_l;
drop table if exists issue_28258_r;
drop view if exists issue_28258_v;
drop table if exists issue_28258_ctas;

create table issue_28258_l(id int primary key, lv varchar(10), lonly int);
create table issue_28258_r(id int primary key, rv varchar(10), ronly int);
insert into issue_28258_l values (1, 'L1', 11), (2, 'L2', 22);
insert into issue_28258_r values (1, 'R1', 111), (3, 'R3', 333);

select * from issue_28258_l l right join issue_28258_r r using (id) order by id;
select * from issue_28258_l l natural right join issue_28258_r r order by id;
select * from issue_28258_l l right join issue_28258_r r on l.id = r.id order by r.id;

create view issue_28258_v as
select * from issue_28258_l right join issue_28258_r using (id);
select column_name, ordinal_position
from information_schema.columns
where table_schema = database() and table_name = 'issue_28258_v'
order by ordinal_position;

create table issue_28258_ctas as
select * from issue_28258_l right join issue_28258_r using (id);
select column_name, ordinal_position
from information_schema.columns
where table_schema = database() and table_name = 'issue_28258_ctas'
order by ordinal_position;

prepare issue_28258_ps from
'select * from issue_28258_l right join issue_28258_r using (id) where id >= ? order by id';
set @issue_28258_p = 1;
execute issue_28258_ps using @issue_28258_p;
deallocate prepare issue_28258_ps;

drop view issue_28258_v;
drop table issue_28258_ctas;
drop table issue_28258_l;
drop table issue_28258_r;
