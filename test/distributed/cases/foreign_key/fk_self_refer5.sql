drop database if exists fk_self_refer5;
create database fk_self_refer5;
use fk_self_refer5;


create table names(id int PRIMARY KEY,name VARCHAR(255),age int,b int,constraint `c1` foreign key (b) references names(id));

-- no error
replace into names(id, name, age, b) values(1,"Abby", 24,1);

--error
replace into names(id, name, age, b) values(3,"Abby", 24,2);

--no error
replace into names(id, name, age, b) values(3,"Abby", 24,1);

--error
replace into names(id, name, age) values(1,"Bobby", 25);

--no error
replace into names set id = 2, name = "Ciro";

--no error
replace into names set id = 2, name = "Ciro", b = 3;

--error
replace into names set id = 2, name = "Ciro", b = 5;

--no error
replace INTO names values (2, "Bob", 19,NULL);

--error
replace INTO names values (2, "Bobx", 19,4);

--error
replace INTO names values (1, "Bobx", 19,2);

--no error
replace INTO names values (2, "Bobx", 19,3);

--no error
replace INTO names values (2, "Bobx", 19,1);

--no error
replace INTO names values (2, "Bobx", 19,3);

--error
replace INTO names values (3, "Jack", 19,2);

--error
replace INTO names values (1, "Join", 19,2);

drop table if exists names;
drop database if exists fk_self_refer5;