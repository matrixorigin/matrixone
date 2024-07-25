select load_file(cast('file://$resources/file_test/normal.txt' as datalink));
select load_file(cast('file://$resources/file_test/normal.txt?offset=0&size=3' as datalink));

create table t1(a int, b datalink);
insert into t1 values(1, "wrong datalink url");
insert into t1 values(1, 'file://$resources/file_test/normal.txt?offset=0&size=3');
insert into t1 values(2, 'file://$resources/file_test/normal.txt');
select * from t1;
select a, load_file(b) from t1;