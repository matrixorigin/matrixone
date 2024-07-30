
select enable_fault_injection();

begin;
select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'create table xx');
create table xx (a int);
select remove_fault_point('runOnce_fail');

select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'drop table xx');
drop table xx;
select remove_fault_point('runOnce_fail');


create table bb (a int);
insert into bb values (1);


select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'alter table bb');
alter table bb rename to xx;
select remove_fault_point('runOnce_fail');

show tables;

select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'alter table xx');
alter table xx add primary key (a);
select remove_fault_point('runOnce_fail');

select * from xx;
commit;
select * from xx;
show create table xx;

drop table xx;


drop table if exists foreign01;
create table foreign01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references foreign01(col1));
insert into foreign01 values(1,'sfhuwe',1,1);
insert into foreign01 values(2,'37829901k3d',2,2);
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);


begin;
select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'insert into foreign01');
insert into foreign01 values(3,'anykind',3,3);
select remove_fault_point('runOnce_fail');

-- rollback rename & update constraints in one stamtment
select add_fault_point('runOnce_fail', ':::', 'echo', 0, 'alter table foreign01');
alter table foreign01 drop column col2;
select remove_fault_point('runOnce_fail');

select * from foreign01;
commit;
select * from foreign01;
show columns from foreign01;

drop table foreign02;
drop table foreign01;


select disable_fault_injection();
