-- drop sequence not supported yet, use drop table instead first.
drop table if exists s1;
create sequence s1 as smallint increment by -10 minvalue 30 maxvalue 100 cycle; 
select * from s1;
drop table if exists s1;
create sequence s1 as bigint unsigned increment by -1000 maxvalue 300;
select * from s1;