drop database if exists db;
create database db;
use db;

CREATE TABLE t(f1 INTEGER);
insert into t values(1),(2),(3),(6);
CREATE VIEW v AS SELECT f1 FROM t;
select f1 from v;

-- direct recursive reference
ALTER VIEW v AS SELECT f1 FROM (SELECT f1 FROM v) AS dt1 NATURAL JOIN v dt2 WHERE f1 > 5;

-- indirect recursive reference
create view v2 as select f1 from v;
select f1 from v2;
ALTER VIEW v AS SELECT f1 FROM (SELECT f1 FROM v2) AS dt1 NATURAL JOIN v2 dt2 WHERE f1 > 5;

drop database if exists db2;
create database db2;
use db2;

create view v3 as select f1 from db.v;
select f1 from v3;

use db;
ALTER VIEW v AS SELECT f1 FROM (SELECT f1 FROM db2.v3) AS dt1 NATURAL JOIN db2.v3 dt2 WHERE f1 > 5;

select * from vx;

alter view v as select f1 from t;
select * from v;

drop database if exists db;
drop database if exists db2;