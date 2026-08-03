-- @suite
-- @case
set @old_cte_max_memory_bytes = @@session.cte_max_memory_bytes;
set @old_cte_max_recursion_depth = @@session.cte_max_recursion_depth;
drop database if exists cte_memory_11297;
create database cte_memory_11297;
use cte_memory_11297;
show variables like 'cte_max_memory_bytes';
set session cte_max_memory_bytes = 0;
select @@session.cte_max_memory_bytes;

set session cte_max_memory_bytes = 16777216;
set session cte_max_recursion_depth = 600;
with recursive seq(n) as (select 1 union all select n + 1 from seq where n < 500) select count(*) from seq;

drop table if exists product;
create table product (id int primary key, p_id int, p_name varchar(25), price decimal(10,3));
insert into product values (3,2,'bed',3560.98),(2,null,'chair',1599.00),(4,1,'desk',2999.99),(5,3,'door',8123.09),(6,3,'mirrors',698.00),(7,4,'tv',5678);
-- @regex("recursive CTE memory quota exceeded on this CN: projected [0-9]+ bytes, query limit 16777216 bytes; increase @@cte_max_memory_bytes or rewrite the query to converge",true)
with recursive cte_ab_8(productID,price) as (select p_id,price from product union all select c.productID,p.price from product p join cte_ab_8 c on p.p_id = c.productID) select * from cte_ab_8;
select 1;

set session cte_max_memory_bytes = 1073741824;
set session cte_max_recursion_depth = 3;
with recursive cte_ab_8(productID,price) as (select p_id,price from product union all select c.productID,p.price from product p join cte_ab_8 c on p.p_id = c.productID) select * from cte_ab_8;

drop table product;
set session cte_max_memory_bytes = @old_cte_max_memory_bytes;
set session cte_max_recursion_depth = @old_cte_max_recursion_depth;
drop database cte_memory_11297;
