-- drop sequence not supported yet, use drop table instead first.
drop sequence if exists s1;
create sequence s1 as smallint increment by -40 minvalue 30 maxvalue 100 cycle; 
select nextval('s1');
select nextval('s1');
select nextval('s1');
select currval('s1');
select lastval();
select setval('s1', 50);
select lastval();
select nextval('s1');