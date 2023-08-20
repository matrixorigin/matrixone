CREATE TABLE emp(id INT PRIMARY KEY NOT NULL,name VARCHAR(100) NOT NULL,manager_id INT NULL,INDEX (manager_id),FOREIGN KEY (manager_id) REFERENCES employees_mgr(id));
INSERT INTO emp VALUES(333, "总经理", NULL), (198, "副总1", 333), (692, "副总2", 333),(29, "主任1", 198),(4610, "职员1", 29),(72, "职员2", 29),(123, "主任2", 692);
create table employees_mgr(id int primary key not null,name varchar(25));
insert into employees_mgr values(333,'ami'),(198,'lucky'),(29,'jack'),(692,'sammi');
create table product (id int primary key,p_id int,p_name varchar(25),price decimal(10,3));
insert into product values (3,2,"bed",3560.98),(2,null,"chair",1599.00),(4,1,"desk",2999.99),(5,3,"door",8123.09),(6,3,"mirrors",698.00),(7,4,"tv",5678);

-- non recursive cte
with non_cte_1 as(select manager_id,id,name from emp  order by manager_id ) select * from non_cte_1;
with non_cte_2 as(select a.manager_id,b.name as manger_name,a.id,a.name as job_name from emp a join employees_mgr b on a.manager_id = b.id order by manager_id ) select manager_id,count(id)  from non_cte_2 group by manager_id having count(id)>1;
with non_cte_3(manager_id,manager_name,employee_id,employee_name)as  (select a.manager_id,b.name as manger_name,a.id,a.name as job_name from emp a join employees_mgr b on a.manager_id = b.id order by manager_id ) select * from non_cte_3 order by employee_id;
with non_cte_4 as (select count(id) as emp_num,manager_id from emp group by manager_id) select manager_id from non_cte_4 where emp_num>1;
with non_cte_5(manager_id,job_name,employee_id)as (select a.manager_id,name,id from emp a where exists (select id from employees_mgr b where a.manager_id=b.id))select * from non_cte_5;
with non_cte_6(manager_id,job_name,employee_id) as (select a.manager_id,name,id from emp a where exists (select id from employees_mgr b where a.manager_id=b.id))select manager_id,job_name,count(employee_id) as emp_num from non_cte_6 group by manager_id,job_name order by manager_id,job_name;
with non_cte_7(manager_id,emp_num)as (select manager_id ,count(id) from emp where manager_id is not null group by manager_id) select avg(emp_num) as "average emp per manager" from non_cte_7;

-- drop table ,truncate table
truncate table non_cte_6;
drop table non_cte_7;

--  multi non recursive cte
with non_cte_8(manager_id,name) as
    (SELECT a.manager_id,
        a.name AS job_name
    FROM emp a), non_cte_9(id,name) as
    (SELECT m.id,
         m.name
    FROM employees_mgr m
    WHERE m.name != "lucky")
SELECT *
FROM non_cte_8 a
JOIN non_cte_9 b
    ON a.manager_id = b.id
ORDER BY  a.manager_id

--expression_name: key word,number
with date as(select manager_id,id,name from emp  order by manager_id )select * from date;
with 111 as(select manager_id,id,name from emp  order by manager_id )select * from 111;

-- recursive cte
CREATE TABLE MyEmployees
(
EmployeeID SMALLINT NOT NULL,
FirstName NVARCHAR(30) NOT NULL,
LastName NVARCHAR(40) NOT NULL,
Title NVARCHAR(50) NOT NULL,
DeptID SMALLINT NOT NULL,
ManagerID SMALLINT NULL,
CONSTRAINT PK_EmployeeID PRIMARY KEY CLUSTERED (EmployeeID ASC),
CONSTRAINT FK_MyEmployees_ManagerID_EmployeeID FOREIGN KEY (ManagerID) REFERENCES MyEmployees (EmployeeID)
);

INSERT INTO MyEmployees VALUES
(1, N'Ken', N'Sánchez', N'Chief Executive Officer',16, NULL)
,(273, N'Brian', N'Welcker', N'Vice President of Sales', 3, 1)
,(274, N'Stephen', N'Jiang', N'North American Sales Manager', 3, 273)
,(275, N'Michael', N'Blythe', N'Sales Representative', 3, 274)
,(276, N'Linda', N'Mitchell', N'Sales Representative', 3, 274)
,(285, N'Syed', N'Abbas', N'Pacific Sales Manager', 3, 273)
,(286, N'Lynn', N'Tsoflias', N'Sales Representative', 3, 285)
,(16, N'David', N'Bradley', N'Marketing Manager', 4, 273)
,(23, N'Mary', N'Gibson', N'Marketing Specialist', 4, 16)

WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
)
SELECT ManagerID, EmployeeID, Title, EmployeeLevel
FROM DirectReports
ORDER BY ManagerID;

-- recursive cte :concat function
WITH  RECURSIVE DirectReports(Name, Title, EmployeeID, EmployeeLevel)
AS (SELECT concat(e.FirstName," ",e.LastName) as name,
        e.Title,
        e.EmployeeID,
        1 as EmployeeLevel
    FROM MyEmployees AS e
    WHERE e.ManagerID IS NULL
    UNION ALL
    SELECT concat(e.FirstName," ",e.LastName) as name,
        e.Title,
        e.EmployeeID,
        EmployeeLevel + 1
    FROM MyEmployees AS e
    JOIN DirectReports AS d ON e.ManagerID = d.EmployeeID
    )
SELECT EmployeeID, Name, Title, EmployeeLevel
FROM DirectReports order by EmployeeID;

-- multi recursive cte
WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
),
emp_cte as (
    select id,manager_id,name from emp where manager_id is null
    union all
    select ec.id,ec.manager_id,ec.name from emp_cte ec join emp e on ec.manager_id = e.id
    )
SELECT ManagerID, EmployeeID, Title
FROM DirectReports union all select id,manager_id,name from emp_cte ORDER BY ManagerID;

-- recursive cte :insert cte
create table cte_insert_table (c1 int,c2 int ,c3 varchar(50),c4 int);
insert into cte_insert_table  WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
)
SELECT ManagerID, EmployeeID, Title, EmployeeLevel
FROM DirectReports
ORDER BY ManagerID;

-- recursive cte: update cte
update cte_insert_table set c3= (WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
)
select title from  DirectReports where ManagerID=16) where c2=274;

-- recursive cte: delete
delete from  cte_insert_table where  c3 = (WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
)
select Title from  DirectReports where ManagerID=16);

-- recursive cte: create view as
create view cte_view as(
WITH  RECURSIVE DirectReports(Name, Title, EmployeeID, EmployeeLevel)
AS (SELECT concat(e.FirstName," ",e.LastName) as name,
        e.Title,
        e.EmployeeID,
        1 as EmployeeLevel
    FROM MyEmployees AS e
    WHERE e.ManagerID IS NULL
    UNION ALL
    SELECT concat(e.FirstName," ",e.LastName) as name,
        e.Title,
        e.EmployeeID,
        EmployeeLevel + 1
    FROM MyEmployees AS e
    JOIN DirectReports AS d ON e.ManagerID = d.EmployeeID
    )
SELECT EmployeeID, Name, Title, EmployeeLevel
FROM DirectReports order by EmployeeID);
select * from cte_view order by EmployeeLevel;

-- recursive cte abnormal: multi select cte?
WITH RECURSIVE test(id, name, path)
AS
(SELECT id, name, CAST(id AS CHAR(200))
FROM emp WHERE manager_id IS NULL
UNION ALL
SELECT e.id, e.name, CONCAT(ep.path, ',', e.id)
FROM test AS ep JOIN emp AS e  ON ep.id = e.manager_id)SELECT * FROM test ORDER BY path;

-- recursive cte abnormal:  recursive_query include sum/avg/count/max/min,group by,having,order by
with recursive cte_ab_1(id, productID,price) as (SELECT p.id, p.p_id, p.price FROM product p where p.p_id is null UNION all SELECT p.id, p.p_id, avg(p.price) FROM product p JOIN cte_ab_1 c  ON p.id = c.productID  GROUP BY c.id, c.productID )SELECT * FROM cte_ab_1;
with recursive cte_ab_2(id, productID,price) as (SELECT p.id, p.p_id, p.price FROM product p where p.p_id is null UNION all SELECT p.id, p.p_id, sum(p.price) FROM product p JOIN cte_ab_2 c  ON p.id = c.productID  GROUP BY c.id, c.productID )SELECT * FROM cte_ab_2;
with recursive cte_ab_3(id, productID,price) as (SELECT p.id, p.p_id, p.price FROM product p where p.p_id is null UNION all SELECT p.id, p.p_id, count(p.price) FROM product p JOIN cte_ab_3 c  ON p.id = c.productID  GROUP BY c.id, c.productID )SELECT * FROM cte_ab_3;
with recursive cte_ab_4(id, productID,price) as (SELECT p.id, p.p_id, p.price FROM product p where p.p_id is null UNION all SELECT p.id, p.p_id, max(p.price) FROM product p JOIN cte_ab_4 c  ON p.id = c.productID  GROUP BY c.id, c.productID )SELECT * FROM cte_ab_4;
with recursive cte_ab_5(id, productID,price) as (SELECT p.id, p.p_id, p.price FROM product p where p.p_id is null UNION all SELECT p.id, p.p_id, min(p.price) FROM product p JOIN cte_ab_5 c  ON p.id = c.productID  GROUP BY c.id, c.productID )SELECT * FROM cte_ab_5;
with recursive cte_ab_6(id,manager_id,name) as(select p.id,p.manager_id,p.name from emp p where manager_id is null union all select p.id,p.manager_id,p.name from emp p join cte_ab_6 c on p.manager_id= c.id order by c.id) select * from cte_ab_6 order by id;

-- recursive cte abnormal:  recursive_query include distinct ,limit, subquery , left join,right join,out join
with recursive cte_ab_7(id,manager_id,name) as(select p.id,p.manager_id,p.name from emp p where manager_id is null union all select distinct p.id,p.manager_id,p.name from emp p join cte_ab_7 c on p.manager_id= c.id) select * from cte_ab_7;
with recursive cte_ab_8(id,manager_id,name,levels) as(select p.id,p.manager_id,p.name,0 as level from emp p where manager_id is null union all select p.id,p.manager_id,p.name,levels+1 from emp p join cte_ab_8 c on p.manager_id= c.id limit 5) select * from cte_ab_8;
with recursive cte_ab_9(id,manager_id,name,levels) as(select p.id,p.manager_id,p.name,0 as level from emp p where manager_id is null union all select p.id,p.manager_id,p.name,levels+1 from emp p where p.manager_id in(select c.id from cte_ab_9 c)) select * from cte_ab_9;

-- abnormal: SELECT list and column names list have different column counts
with recursive cte_ab_10(id,manager_id,name,levels) as(select p.id,p.manager_id,p.name from emp p where manager_id is null union all select p.id,p.manager_id,p.name from emp p join cte_ab_10 c on p.manager_id= c.id ) select * from cte_ab_10 order by id;
with non_cte_8(manager_id,name) as(SELECT a.manager_id, a.id,a.name AS job_name FROM emp a), non_cte_9(id,name) as(SELECT m.id, m.name FROM employees_mgr m WHERE m.name != "lucky") select * FROM non_cte_8 a JOIN non_cte_9 b ON a.manager_id = b.id ORDER BY  a.manager_id

-- recursive cte abnormal: with as update
WITH RECURSIVE DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS
(
    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel
    FROM MyEmployees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1
    FROM MyEmployees AS e
        INNER JOIN DirectReports AS d
        ON e.ManagerID = d.EmployeeID
)
update DirectReports set Title='manager assistant' where ManagerID=273;
