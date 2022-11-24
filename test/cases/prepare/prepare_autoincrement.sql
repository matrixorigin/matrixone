drop table if exists t1;
create table t1(
a int auto_increment,
b varchar(100)
);

set @a=1;
set @b='a';
prepare s1 from 'insert into t1 values (?,?)';
execute s1 using @a,@b;
select * from t1;
deallocate prepare s1;

prepare s2 from 'insert into t1 values ()';
execute s2;
select * from t1;
deallocate prepare s2;

set @a=100;
set @b='a=b';
prepare s3 from 'insert into t1 values (?,?)';
execute s3 using @a,@b;
select * from t1;
deallocate prepare s3;

prepare s4 from 'insert into t1 values ()';
execute s4;
select * from t1;
deallocate prepare s4;