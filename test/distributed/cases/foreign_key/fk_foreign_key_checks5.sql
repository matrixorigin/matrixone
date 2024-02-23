drop database if exists fk_foreign_key_checks5;
create database fk_foreign_key_checks5;

drop database if exists fk_foreign_key_checks5_db0;
create database fk_foreign_key_checks5_db0;

drop database if exists fk_foreign_key_checks5_db1;
create database fk_foreign_key_checks5_db1;

create table fk_foreign_key_checks5_db0.t1(a int primary key);

create table fk_foreign_key_checks5_db1.t2(b int, constraint c1 foreign key (b) references fk_foreign_key_checks5_db0.t1(a));

--error
drop database fk_foreign_key_checks5_db0;

drop database if exists fk_foreign_key_checks5_db2;
create database fk_foreign_key_checks5_db2;

create table fk_foreign_key_checks5_db2.t1(a int primary key);
create table fk_foreign_key_checks5_db2.t2(b int, constraint c1 foreign key (b) references fk_foreign_key_checks5_db2.t1(a));

--no error
drop database fk_foreign_key_checks5_db2;

drop table fk_foreign_key_checks5_db1.t2;

--no error
drop database fk_foreign_key_checks5_db0;

drop database if exists fk_foreign_key_checks5;
drop database if exists fk_foreign_key_checks5_db0;
drop database if exists fk_foreign_key_checks5_db1;
drop database if exists fk_foreign_key_checks5_db2;