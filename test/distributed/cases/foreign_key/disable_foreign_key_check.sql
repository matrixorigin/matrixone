-- drop table
SET FOREIGN_KEY_CHECKS = 0;
drop database if exists test;
create database test;
use test;

create table if not exists products (id int primary key auto_increment, name varchar(10));
create table if not exists orders (id int primary key auto_increment, product_id INT, FOREIGN KEY (product_id) REFERENCES products(id));

delete FROM PRODUCTS;
DROP TABLE ORDERS;

drop database test;
SET FOREIGN_KEY_CHECKS = 1;

-- drop database
SET FOREIGN_KEY_CHECKS = 0;
drop database if exists test;
create database test;
use test;

create table if not exists products (id int primary key auto_increment, name varchar(10));
create table if not exists orders (id int primary key auto_increment, product_id INT, FOREIGN KEY (product_id) REFERENCES products(id));

delete FROM PRODUCTS;
drop database test;
SET FOREIGN_KEY_CHECKS = 1;