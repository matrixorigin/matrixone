drop table if exists test1;
drop table if exists test2;
drop table if exists test3;

create table test1 (
id int(10) primary key,
uname varchar(20) unique key,
sex varchar(10),
birth date,
department varchar(20),
address varchar (50)
);

INSERT INTO test1 VALUES (7369,'SMITH', 'Male', '1980-12-17','ACCOUNTING','NEW YORK');
INSERT INTO test1 VALUES (7499,'ALLEN', 'Female', '1981-02-20','RESEARCH','DALLAS');
INSERT INTO test1 VALUES (7521,'WARD', 'Male', '1981-02-22','SALES','CHICAGO');
INSERT INTO test1 VALUES (7839,'KING', 'Female', '1981-11-17','ACCOUNTING','NEW YORK');
INSERT INTO test1 VALUES (7844,'TURNER', 'Male', '1981-09-08','RESEARCH', 'DALLAS');
INSERT INTO test1 VALUES (7902,'FORD', 'Male', '1981-12-03','ACCOUNTING','NEW YORK');

select * from test1;
update test1 set id = 1200 where uname = 'TURNER';
drop table test1;


create table test2 (
id int(10),
uname varchar(20)  unique key,
sex varchar(10),
birth date,
department varchar(20),
address varchar (50),
primary key(id)
);

INSERT INTO test2 VALUES (7369,'SMITH', 'Male', '1980-12-17','ACCOUNTING','NEW YORK');
INSERT INTO test2 VALUES (7499,'ALLEN', 'Female', '1981-02-20','RESEARCH','DALLAS');
INSERT INTO test2 VALUES (7521,'WARD', 'Male', '1981-02-22','SALES','CHICAGO');
INSERT INTO test2 VALUES (7839,'KING', 'Female', '1981-11-17','ACCOUNTING','NEW YORK');
INSERT INTO test2 VALUES (7844,'TURNER', 'Male', '1981-09-08','RESEARCH', 'DALLAS');
INSERT INTO test2 VALUES (7902,'FORD', 'Male', '1981-12-03','ACCOUNTING','NEW YORK');

update test2 set id = 1200 where uname = 'TURNER';
select * from test2;
drop table test2;

create table test3 (
id int unsigned,
uname varchar(20),
sex varchar(10),
birth date unique key,
department varchar(20),
address varchar (50),
primary key(id,uname)
);

INSERT INTO test3 VALUES (7369,'SMITH', 'Male', '1980-12-17','ACCOUNTING','NEW YORK');
INSERT INTO test3 VALUES (7499,'ALLEN', 'Female', '1981-02-20','RESEARCH','DALLAS');
INSERT INTO test3 VALUES (7521,'WARD', 'Male', '1981-02-22','SALES','CHICAGO');
INSERT INTO test3 VALUES (7839,'KING', 'Female', '1981-11-17','ACCOUNTING','NEW YORK');
INSERT INTO test3 VALUES (7844,'TURNER', 'Male', '1981-09-08','RESEARCH', 'DALLAS');
INSERT INTO test3 VALUES (7902,'FORD', 'Male', '1981-12-03','ACCOUNTING','NEW YORK');
update test3 set id = 1200 where uname = 'TURNER';
select * from test3;
drop table test3;