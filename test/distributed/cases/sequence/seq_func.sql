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

-- matrixone#30650: SETVAL(..., false) must store is_called so the next NEXTVAL advances by the increment, even on a never-called sequence.
drop sequence if exists s3;
create sequence s3 increment by 3 start with 10;
select setval('s3', 20, false);
select nextval('s3');
select setval('s3', 50, true);
select nextval('s3');
drop sequence if exists s3;
drop sequence if exists s4;
create sequence s4 increment by 3 start with 10;
select setval('s4', 60, true);
select nextval('s4');
select setval('s4', 70, true);
select nextval('s4');
drop sequence if exists s4;
