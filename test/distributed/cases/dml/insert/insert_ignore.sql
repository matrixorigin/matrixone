-- insert ignore autoincrement primary key
-- @bvt:issue#15365
create table insert_ignore_01(c1 int not null auto_increment primary key,c2 varchar(10));
insert into insert_ignore_01(c2) values("a"),("b"),("c"),("d");
insert ignore into insert_ignore_01 values(3,"e"),(6,"f"),(1,"g");
insert ignore into insert_ignore_01(c2) values("h"),("g"),("k");
insert ignore into insert_ignore_01 values(NULL,NULL);
select * from insert_ignore_01;
drop table insert_ignore_01;
-- @bvt:issue
-- insert ignore multi primary key
create table insert_ignore_01 (part_id INT NOT NULL,color VARCHAR(20) NOT NULL,quantity INT,PRIMARY KEY (part_id, color));
insert ignore into insert_ignore_01 (part_id, color, quantity)values(1, 'Red', 10),(1, 'Blue', 20),(2, 'Green', 15),(1, 'Red', 5);
select * from insert_ignore_01;

-- insert ignore unique key
create table insert_ignore_02(c1 int,c2 decimal(6,2),unique key(c1));
insert into insert_ignore_02 values(100,1234.56),(200,2345.67),(300,3456.78),(400,4567.89),(NULL,33.00);
insert ignore into insert_ignore_02 values(100,1234.56),(200,23.7),(500,56.7),(600,6.9);
insert ignore into insert_ignore_02 values(700,1.56),(800,3.7);
insert ignore into insert_ignore_02 values(NULL,44.56);
select * from insert_ignore_02;

-- insert ignore secondary key
-- @bvt:issue#15365
create table insert_ignore_03(c1 int auto_increment primary key,c2 int,key(c2));
insert into insert_ignore_03(c2) values(2),(2),(5),(10),(12),(NULL);
insert ignore into insert_ignore_03(c2) values(7),(2),(5),(10),(12),(NULL);
select * from insert_ignore_03;
-- @bvt:issue
-- insert ignore not null and default constraint
-- @bvt:issue#15358
create table insert_ignore_04 (product_id INT NOT NULL AUTO_INCREMENT,product_name VARCHAR(255) NOT NULL,quantity_in_stock INT DEFAULT 0,price DECIMAL(10, 2) NOT NULL,PRIMARY KEY (product_id));
insert ignore into insert_ignore_04(product_name, price) VALUES('Laptop', 1200.00),('Monitor', 150.00),('Keyboard', NULL),('Mouse', 15.00);
-- @bvt:issue
-- @bvt:issue#15345
insert ignore into insert_ignore_04(product_name, quantity_in_stock,price) VALUES(NULL, 5,1200.00),('board',6, NULL),('phone',NULL,1500.00);
select * from insert_ignore_04;
-- @bvt:issue
-- insert ignore foreign key constraint
-- @bvt:issue#15345
create table parent_table(parent_id INT AUTO_INCREMENT PRIMARY KEY,parent_name VARCHAR(255) NOT NULL);
create table child_table(child_id INT AUTO_INCREMENT PRIMARY KEY,child_name VARCHAR(255) NOT NULL,parent_id INT,FOREIGN KEY (parent_id) REFERENCES parent_table(parent_id)
);
insert ignore into parent_table (parent_name) VALUES ('Parent 1'), ('Parent 2'), ('Parent 3');
insert ignore into child_table (child_name, parent_id) VALUES('Child 1', 1),('Child 2', 2),('Child 3', 4),('Child 4', 1);
select * from parent_table;
select * from child_table;
-- @bvt:issue
-- syntax check
insert ignore into insert_ignore_02 values(1234.56);
-- @bvt:issue#15345
insert ignore into insert_ignore_02 values("abc",1234.56);
insert ignore into insert_ignore_02 select "abc",34.22;
-- @bvt:issue
insert ignore into insert_ignore values("abc",1234.56);

-- insert ignore values out of type range
-- @bvt:issue#15345
create table insert_ignore_05(id TINYINT,created_at DATETIME);
insert ignore INTO insert_ignore_05 (id, created_at) VALUES(130, '2024-04-03 10:00:00'),(-129, '2024-04-03 11:00:00'),(100, '2024-04-03 12:00:00');
insert ignore INTO insert_ignore_05 (id, created_at) VALUES(50, '9999-12-31 23:59:59'), (50, '2000-02-29 10:00:00'),(50, '2024-04-03 13:00:00');
select * from insert_ignore_05;
-- @bvt:issue

-- insert ignore partition table
-- @bvt:issue#15355
create table insert_ignore_06 (sale_id INT AUTO_INCREMENT,product_id INT,sale_amount DECIMAL(10, 2),sale_date DATE,PRIMARY KEY (sale_id, sale_date))PARTITION BY RANGE (year(sale_date)) (PARTITION p0 VALUES LESS THAN (1991),PARTITION p1 VALUES LESS THAN (1992),PARTITION p2 VALUES LESS THAN (1993),PARTITION p3 VALUES LESS THAN (1994));
insert ignore into insert_ignore_06 (product_id, sale_amount, sale_date) VALUES(1, 1000.00, '1990-04-01'),(2, 1500.00, '1992-05-01'),(3, 500.00, '1995-06-01'),(1, 2000.00, '1991-07-01');
select * from insert_ignore_06;
-- @bvt:issue
-- insert ignore select from table
-- @bvt:issue#15349
create table insert_ignore_07(c1 int primary key auto_increment, c2 int);
insert into insert_ignore_07(c2) select result from generate_series(1,100000) g;
create table insert_ignore_08(c1 int primary key, c2 int);
insert into insert_ignore_08 values(20,45),(21,55),(1,45),(6,22),(5,1),(1000,222),(99999,19);
insert ignore into insert_ignore_08 select * from insert_ignore_07;
select count(*) from insert_ignore_08;
select * from insert_ignore_08 where c2 in (45,55,22,1,222,19);
-- @bvt:issue
