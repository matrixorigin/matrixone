create database if not exists test;
use test;
drop table if exists tlonglonglong;
# 版本:220608 无外键 有自增长 有所有默认值 图片表取消自增长

# add table01
CREATE TABLE table01(
                              c1 BIGINT NOT NULL AUTO_INCREMENT,
                              c2 VARCHAR(200) NULL,
                              c3 VARCHAR(200) NULL,
                              c4 VARCHAR(200) NULL,
                              c5 VARCHAR(200) NULL,
                              c6 TINYINT NULL,
                              c7 DATETIME NULL,
                              c8 VARCHAR(200) NULL,
                              c9 TINYINT NOT NULL,
                              PRIMARY KEY (`c1`) USING BTREE
)ENGINE=INNODB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

show tables;
show create table table01;
drop table table01;

# add table02
drop table if exists table02;
create table table02(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into table02 values(1,"Abby", 24);
insert into table02 values(2,"Bob", 25);
insert into table02 values(3,"Carol", 23);
insert into table02 values(4,"Dora", 29);
create unique index idx on table02(name);
select * from table02;
show create table table02;
drop table table02;

# drop database
drop database test;