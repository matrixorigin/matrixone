-- 1. call datalink directly in load_file function
select load_file(cast('file://$resources/file_test/normal.txt' as datalink));
select load_file(cast('file://$resources/file_test/normal.txt?offset=0&size=3' as datalink));

-- create a table with datalink column
create table t1(a int, b datalink);

-- 2. insert invalid datalink url
insert into t1 values(1, "wrong datalink url");
insert into t1 values(2, 'git://repo/normal.txt?offset=0&size=3');

-- 3. insert valid datalink url
insert into t1 values(1, 'file://$resources/file_test/normal.txt?offset=0&size=3');
insert into t1 values(2, 'file://$resources/file_test/normal.txt');

-- 4. insert with size alone
insert into t1 values(3, 'file://$resources/file_test/normal.txt?size=3');

-- 5. insert with offset alone
insert into t1 values(4, 'file://$resources/file_test/normal.txt?offset=0');

-- 6. insert with wrong size (expected response - we will read the whole file)
insert into t1 values(5, 'file://$resources/file_test/normal.txt?offset=0&size=-100');

select a, load_file(b) from t1;