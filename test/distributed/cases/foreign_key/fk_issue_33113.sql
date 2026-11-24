drop database if exists fk_issue_33113;
create database fk_issue_33113;
use fk_issue_33113;

-- #33113: a parent DELETE can reach one grandchild both through an
-- ON DELETE CASCADE path and through an ON DELETE SET NULL that then
-- triggers ON UPDATE CASCADE. The cascade-deleted row must not survive
-- as the updated replacement.
create table p (id int primary key, code varchar(10) unique);
create table c1 (id int primary key, pid int, foreign key (pid) references p(id) on delete cascade);
create table c2 (id int primary key, x varchar(10) unique, foreign key (x) references p(code) on delete set null);
create table g (id int primary key, c1id int, x varchar(10), foreign key (c1id) references c1(id) on delete cascade, foreign key (x) references c2(x) on update cascade);
insert into p values (1, 'P1'), (2, 'P2');
insert into c1 values (10, 1), (20, 2);
insert into c2 values (100, 'P1'), (200, 'P2');
insert into g values (1, 20, 'P1'), (2, 10, 'P2'), (3, 10, 'P1');
delete from p where id = 1;
select id, c1id, x from g order by id;
select id, pid from c1 order by id;
select id, x from c2 order by id;
select id, code from p order by id;

drop database fk_issue_33113;
