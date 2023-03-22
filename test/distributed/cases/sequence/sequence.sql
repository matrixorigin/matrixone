drop sequence if exists s1;
create sequence s1 as smallint increment by -10 minvalue 30 maxvalue 100 cycle; 
select * from s1;
drop sequence if exists s1;
create sequence s1 as bigint unsigned increment by -1000 maxvalue 300;
select * from s1;
drop table s1;
show sequences;