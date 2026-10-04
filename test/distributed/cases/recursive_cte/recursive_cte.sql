drop table if exists t1;
create table t1(a bigint primary key, b int unique key);
insert into t1 values (1, 2), (3, 4), (5, 6);
drop table if exists t3;
create table t3(a bigint, b int);
insert into t3 values (1, 2), (3, 4), (5, 6);
drop table if exists t2;
create table t2(a bigint, b int);
insert into t2 values (1, 2), (3, 4), (5, 6);
with recursive c as (select a from t1 union all select a+1 from c where a < 2) select * from c order by a;
with recursive c as (select a from t1 union all select a+1 from c where a < 2), d as (select a from c union all select a+1 from d where a < 2) select distinct tt.* from ( SELECT * FROM c UNION ALL SELECT * FROM d) tt order by tt.a;
with recursive c as (select a from t1 union all select a+1 from c where a < 200) select * from c;
with recursive c as (select a from t1 union all select c.a+1 from c, t1 as k1, t1 as k2 where c.a = k1.a and c.a = k2.a) select * from c order by a;
with recursive c as (select t1.a from t1, t2, t3 where t1.a = t2.a or t1.a = t3.a union all select a+1 from c where a < 6) select count(*) from c;
with recursive c as (select t1.a from t1, t2, t3 where t1.a = t2.a or t1.a = t3.a union all select c.a+1 from c, t2, t3 where c.a = t2.a and c.a = t3.a and c.a < 6) select count(*) from c;
with recursive c as (select t1.a from t1 union all select c.a+1 from c, t3 where c.a = t3.a and c.a < 2) select count(*) from c;
with recursive c as (select a from t1 union all select a+1 from c where a < 3 union all select a+1 from c where a < 4) select count(*) from c;
with recursive c as (select a from t1 union all select a+1 from c where a < 4) select * from c order by a;
with recursive c as (select a from t1 union all select a+1 from c where a < 3) select * from c order by a;
with recursive c as (select t1.a, 0 as level from t1 union all select t1.a, 0 as level from t1 join t2 on t1.a = t2.a where t1.a = 1 union all select c.a, c.level + 1 from c join t3 on c.a = t3.a where c.level < 6) select count(*) from c;
with recursive c as (select t1.a, 0 as level from t1 union all select t1.a, 0 as level from t1 join t2 on t1.a = t2.a where t1.a = 1 union all select c.a, c.level + 1 from c join t3 on c.a = t3.a where c.level < 6) select count(*) from c;
with recursive c as (select t1.a, 0 as level from t1 union all select t1.a, 0 as level from t1 join t2 on t1.a = t2.a where t1.a = 1 union all select c.a, c.level + 1 from c join t3 on c.a = t3.a where c.level < 6) select count(*) from c;

CREATE TABLE Person(ID int, Name VARCHAR(30), Mother INT, Father INT);
INSERT Person VALUES(1, 'Sue', NULL, NULL),(2, 'Ed', NULL, NULL),(3, 'Emma', 1, 2),(4, 'Jack', 1, 2),(5, 'Jane', NULL, NULL),(6, 'Bonnie', 5, 4),(7, 'Bill', 5, 4);
WITH recursive Generation (ID) AS (SELECT Mother FROM Person WHERE Name = 'Bonnie' UNION SELECT Father FROM Person WHERE Name = 'Bonnie' UNION ALL SELECT Person.Father FROM Generation, Person WHERE Generation.ID=Person.ID UNION ALL SELECT Person.Mother FROM Generation, Person WHERE Generation.ID=Person.ID) SELECT Person.ID, Person.Name, Person.Mother, Person.Father FROM Generation, Person WHERE Generation.ID = Person.ID order by person.ID;
CREATE TABLE employees_hierarchy (id INT PRIMARY KEY, name VARCHAR(50),manager_id INT);
INSERT INTO employees_hierarchy (id, name, manager_id) VALUES(1, 'Alice', NULL), (2, 'Bob', 1),(3, 'Charlie', 1),(4, 'David', 2),(5, 'Eve', 2),(6, 'Frank', 3);
WITH RECURSIVE employee_hierarchy_cte (id, name, manager_id, level) AS (SELECT id, name, manager_id, 0 FROM employees_hierarchy WHERE name = 'Alice' UNION ALL SELECT e.id, e.name, e.manager_id, eh.level + 1 FROM employees_hierarchy AS e JOIN employee_hierarchy_cte AS eh ON e.manager_id = eh.id) SELECT name, level FROM employee_hierarchy_cte;
WITH RECURSIVE employee_hierarchy_cte (id, name, manager_id, level) AS (SELECT id, name, manager_id, 0 FROM employees_hierarchy WHERE name = 'Alice' UNION ALL SELECT e.id, e.name, e.manager_id, eh.level + 1 FROM employees_hierarchy AS e JOIN employee_hierarchy_cte AS eh ON e.manager_id = eh.id) SELECT t.name, t.level FROM employee_hierarchy_cte as t;
drop table if exists t1;
create table t1(id bigint primary key, parent_id bigint, tenant_id varchar(50));
insert into t1 select *,*,* from generate_series(1000000) g;
WITH recursive tb (id, parent_id) AS (SELECT id,parent_id FROM t1 WHERE id IN ( 1937478033946447874, 1,2,3) AND tenant_id != '000000' UNION ALL SELECT c.id, c.parent_id FROM t1 c JOIN tb t ON c.id = t.parent_id WHERE c.tenant_id != '000000') select count(*) from tb;
drop table if exists t1;

-- test for cte_max_recursion_depth variable
drop table if exists t_cte_depth;
create table t_cte_depth(a int);
insert into t_cte_depth values (1);
set cte_max_recursion_depth = 200;
with recursive c as (select a from t_cte_depth union all select a+1 from c where a < 150) select count(*) from c;
set cte_max_recursion_depth = 50;
with recursive c as (select a from t_cte_depth union all select a+1 from c where a < 100) select count(*) from c;
-- test for cte_max_recursion_depth = 0 (should prevent any recursion)
set cte_max_recursion_depth = 0;
with recursive c as (select a from t_cte_depth union all select a+1 from c where a < 5) select count(*) from c;
set cte_max_recursion_depth = 100;
drop table if exists t_cte_depth;

-- recursive CTE consumers can be independently rebound under explicit aliases
with recursive seq(n) as (select 1 union all select n + 1 from seq where n < 3) select count(*) as pairs, sum(a.n + b.n) as checksum from seq as a cross join seq as b;

-- a recursive member can reference an earlier non-recursive CTE in the same WITH clause
with recursive limits(lo, hi) as (select 3, 9), seq(n) as (select lo from limits union all select n + 1 from seq, limits where n < hi) select count(*), sum(n), min(n), max(n) from seq;

-- an empty preceding CTE terminates recursion after the anchor row
with recursive empty_limit(lo, hi) as (select 3, 9 where false), seq(n) as (select 1 union all select n + 1 from seq, empty_limit where n < hi) select count(*), sum(n), min(n), max(n) from seq;

-- keep the non-equality loop join when the empty CTE is table-backed
drop table if exists t_empty_limits;
create table t_empty_limits(lo int, hi int);
with recursive empty_limit(lo, hi) as (select lo, hi from t_empty_limits), seq(n) as (select 1 union all select n + 1 from seq, empty_limit where n < hi) select count(*), sum(n), min(n), max(n) from seq;
with recursive empty_limit(lo, hi) as (select lo, hi from t_empty_limits), seq(n) as (select 1 union all select n + 1 from seq join empty_limit on n = lo where n < hi) select count(*), sum(n), min(n), max(n) from seq;
drop table t_empty_limits;

-- issue #26812: a filtered preceding CTE joined by a recursive member must not
-- be partially shared across recursive steps
drop table if exists cte_26812_seed_src;
create table cte_26812_seed_src(id int primary key, n int);
insert into cte_26812_seed_src values (1, 1), (2, 2);
with recursive base as (select n from cte_26812_seed_src where id = 2), r(n) as (select n from base union all select r.n + base.n from r cross join base where r.n < 8) select * from r order by n;
with recursive base as (select n from cte_26812_seed_src where id = 2), r(n) as (select n from base union all select r.n + base.n from r cross join base where r.n < 8) select n from r order by n;
with recursive base as (select n from cte_26812_seed_src where id = 2), r(n) as (select n from base union all select r.n + base.n from r cross join base where r.n < 8) select count(*), sum(n), min(n), max(n) from r;
with recursive base as (select n from cte_26812_seed_src where id = 2), r(n) as (select n from base union all select r.n + base.n from r cross join base where r.n < 8) select sum(n) from r;

-- filtered preceding CTE used only by the anchor remains a valid control
with recursive base as (select n from cte_26812_seed_src where id = 2), r(n) as (select n from base union all select n + 2 from r where n < 8) select count(*), sum(n), min(n), max(n) from r;

-- unfiltered preceding CTE joined by the recursive member remains valid
drop table if exists cte_26812_unfiltered_step;
create table cte_26812_unfiltered_step(n int);
insert into cte_26812_unfiltered_step values (2);
with recursive base as (select n from cte_26812_unfiltered_step), r(n) as (select n from base union all select r.n + base.n from r cross join base where r.n < 8) select count(*), sum(n), min(n), max(n) from r;

-- placing the source predicate at the CTE consumers remains valid
with recursive base as (select id, n from cte_26812_seed_src), r(n) as (select n from base where id = 2 union all select r.n + base.n from r join base on base.id = 2 where r.n < 8) select count(*), sum(n), min(n), max(n) from r;
drop table cte_26812_unfiltered_step;
drop table cte_26812_seed_src;
