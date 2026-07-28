set global enable_privilege_cache = off;
--success
create table t1 (a int, b int, c int) COMMENT='before cluster by' PARTITION BY hash(a+b) PARTITIONS 2  CLUSTER BY (a,b);
--error
create table t2 (a int, b int, c int) CLUSTER BY (a,b) PARTITION BY hash(a+b) PARTITIONS 2   COMMENT='after cluster by';
--error
create table t3 (a int, b int, c int) PARTITION BY hash(a+b) PARTITIONS 2  COMMENT='before cluster by' CLUSTER BY (a,b);
set global enable_privilege_cache = on;