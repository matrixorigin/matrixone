
-- @suite
-- @setup

-- @case
-- @desc:test for stddev_pop
-- @label:bvt

drop table if exists t1;
CREATE TABLE t1 (id int(11),value1 float(10,2));
INSERT INTO t1 VALUES (1,0.00),(1,1.00), (1,2.00), (2,10.00), (2,11.00), (2,12.00), (2,13.00);
select id, stddev_pop(value1)  from t1 group by id;
select id, variance(value1)  from t1 group by id;
drop table t1;

drop table if exists t;
CREATE TABLE t(txt char(1), i INT);
INSERT INTO t VALUES ('a', 2), ('b', 8), ('b', 0), ('c', 2);
SELECT txt, STDDEV_POP(i) s FROM t GROUP BY txt ORDER BY s, txt;
drop table t;


