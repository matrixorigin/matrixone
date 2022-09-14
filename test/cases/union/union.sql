drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int);
insert into t1 values (1, 10),(2, 20);
create table t2(aa int, bb varchar(20));
insert into t2 values (11, "aa"),(22, "bb");

(select a from t1) order by b desc;
(((select a from t1) order by b desc));
(((select a from t1))) order by b desc;
(((select a from t1 order by b desc) limit 1));
(((select a from t1 order by b desc))) limit 1;
(select a from t1 union select aa from t2) order by a desc;

(select a from t1 order by a) order by a;
(((select a from t1 order by a))) order by a;
(((select a from t1) order by a)) order by a;
(select a from t1 limit 1) limit 1;
(((select a from t1 limit 1))) limit 1;
(((select a from t1) limit 1)) limit 1;
(select a from t1 union select aa from t2) order by aa;
select a from t1 union select a from t1;