### Build
```shell
cd ..
cd ..
cd ..
make
./mo-service -debug-http :9876 -launch ./etc/launch/launch.toml >out.log  2>err.log;
```

### MySQL Client
```shell
mysql -h 127.0.0.1 -P 6001 -udump -p111

DROP DATABASE IF EXISTS a;

create database a;
use a;

create table t1(a int, b vecf32(3), c vecf64(3));

insert into t1 values(1, "[1,2,3]", "[4,5,6]");

select * from t1;
select b+b from t1;
select b-b from t1;
select b*b from t1;
select b/b from t1;

select abs(b) from t1;
select sqrt(b) from t1;
select summation(b) from t1;
select cast("[1,2,3]" as vecf32(3));
select b + "[1,2,3]" from t1;

# vector ops
select l1_norm(b) from t1;
select l2_norm(b) from t1;
select vector_dims(b) from t1;
select inner_product(b,"[1,2,3]") from t1;
select cosine_similarity(b,"[1,2,3]") from t1;

# top K
select * FROM t1 ORDER BY cosine_similarity(b, '[3,1,2]') LIMIT 5;

# cast
select b + sqrt(b) from t1;
select * from t1 where b= "[1,2,3]";
select * from t1 where b> "[1,2,3]";
select * from t1 where b< "[1,2,3]";
select * from t1 where b>= "[1,2,3]";
select * from t1 where b<= "[1,2,3]";
select * from t1 where b!= "[1,2,3]";
select * from t1 where b= cast("[1,2,3]" as vecf32);
 
exit;
```