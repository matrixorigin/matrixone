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
-- Nested NATURAL JOIN star order must follow visible operands (#32599).
drop view if exists issue_32599_v;
drop table if exists issue_32599_sink;
drop table if exists issue_32599_t1;
drop table if exists issue_32599_t2;
drop table if exists issue_32599_t3;

create table issue_32599_t1(c int, b int);
create table issue_32599_t2(a int, b int);
create table issue_32599_t3(b int, c int);
insert into issue_32599_t1 values (10,1),(3,1),(3,2);
insert into issue_32599_t2 values (2,1);
insert into issue_32599_t3 values (1,3),(1,10);

-- @metacmp(true)
select * from issue_32599_t1 natural join (issue_32599_t2 natural join issue_32599_t3) order by c,b,a;
-- The left-associated control intentionally has a different star order.
-- @metacmp(true)
select * from issue_32599_t1 natural join issue_32599_t2 natural join issue_32599_t3 order by c,b,a;

create view issue_32599_v as
select * from issue_32599_t1 natural join (issue_32599_t2 natural join issue_32599_t3);
select column_name, ordinal_position
from information_schema.columns
where table_schema = database() and table_name = 'issue_32599_v'
order by ordinal_position;
-- @metacmp(true)
select * from issue_32599_v order by 1,2,3;

prepare issue_32599_ps from 'select * from issue_32599_v where b = ? order by 1,2,3';
set @issue_32599_b = 1;
-- @metacmp(true)
execute issue_32599_ps using @issue_32599_b;
deallocate prepare issue_32599_ps;
set @issue_32599_b = null;

create table issue_32599_sink(first_col int, second_col int, third_col int);
insert into issue_32599_sink select * from issue_32599_v;
select * from issue_32599_sink order by 1,2,3;

-- error ER_BAD_FIELD_ERROR
select missing_col from issue_32599_v;
select * from issue_32599_v where c = 3;

drop view issue_32599_v;
drop table issue_32599_sink;
drop table issue_32599_t1;
drop table issue_32599_t2;
drop table issue_32599_t3;
select table_name from information_schema.tables
where table_schema = database() and table_name in
('issue_32599_v','issue_32599_sink','issue_32599_t1','issue_32599_t2','issue_32599_t3')
order by table_name;
