create table if not exists indup_01 (a int primary key, b int);

insert into indup_01 values (1,1);
insert into indup_01 values (1,2),(2,2) on duplicate key ignore;
select * from indup_01;

insert into indup_01 values (3,1),(3,2),(3,3) on duplicate key ignore;
select * from indup_01;

insert into indup_01 values (null,1),(null,2),(null,null) on duplicate key ignore;
select * from indup_01;

drop table if exists indup_02_multi_uk_ignore;
create table indup_02_multi_uk_ignore (
    a int,
    b int,
    c int,
    value int,
    unique key uk_b_ignore (b),
    unique key uk_c_ignore (c)
);

insert into indup_02_multi_uk_ignore values (1,10,20,100), (2,11,21,200);
select * from indup_02_multi_uk_ignore order by a;

insert into indup_02_multi_uk_ignore values
    (99,10,20,999),
    (100,10,21,555),
    (3,12,22,300),
    (4,12,23,400),
    (5,13,22,500),
    (6,14,24,600)
on duplicate key ignore;
select * from indup_02_multi_uk_ignore order by a;

drop table indup_02_multi_uk_ignore;
