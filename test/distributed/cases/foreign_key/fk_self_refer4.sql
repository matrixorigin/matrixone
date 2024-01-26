drop database if exists fk_self_refer4;
create database fk_self_refer4;
use fk_self_refer4;

drop table if exists t1;
--alter table add fk
create table t1(a int primary key,b int);
show tables;
show create table t1;
insert into t1 values (1,2),(3,4),(5,6);


--error. alter table drop fk
alter table t1 add constraint fk1 foreign key (b) references t1(a);

--should be error. duplicate foreign key
alter table t1 add constraint fk1 foreign key (b) references t1(a);


drop database if exists fk_self_refer4;