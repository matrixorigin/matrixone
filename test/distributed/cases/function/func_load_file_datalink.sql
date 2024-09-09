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

-- stage tests
create stage filestage URL='file://$resources/file_test/';
create stage outfilestage URL='file://$resources/into_outfile/';

-- 1. call datalink directly in load_file function
select load_file(cast('stage://filestage/normal.txt' as datalink));
select load_file(cast('stage://filestage/normal.txt?offset=0&size=3' as datalink));

-- 2. write datalink to file
select save_file(cast('stage://outfilestage/datalink/1.txt' as datalink), 'this is a test.');
select load_file(cast('stage://outfilestage/datalink/1.txt' as datalink));

-- create a table with datalink column
create table t2(a int, b datalink, c varchar);
insert into t2 values(1, cast('stage://outfilestage/datalink/varchar1.txt' as datalink), 'this is a varchar test 1'),
(2, cast('stage://outfilestage/datalink/varchar2.txt' as datalink), 'this is a varchar test 2'),
(3, cast('stage://outfilestage/datalink/varchar3.txt' as datalink), NULL),
(4, NULL, 'this is a varchar test 4');

select a, save_file(b, c) from t2;

create table t3(a int, b datalink, c char(128));
insert into t3 values(1, cast('stage://outfilestage/datalink/char1.txt' as datalink), 'this is a char test 1'),
(2, cast('stage://outfilestage/datalink/char2.txt' as datalink), 'this is a char test 2'),
(3, cast('stage://outfilestage/datalink/char3.txt' as datalink), NULL),
(4, NULL, 'this is a char test 4');

select a, save_file(b, c) from t3;

create table t4(a int, b datalink, c text);
insert into t4 values(1, cast('stage://outfilestage/datalink/text1.txt' as datalink), 'this is a text test 1'),
(2, cast('stage://outfilestage/datalink/text2.txt' as datalink), 'this is a text test 2'),
(3, cast('stage://outfilestage/datalink/text3.txt' as datalink), NULL),
(4, NULL, 'this is a text test 4');

select a, save_file(b, c) from t4;

drop stage filestage;
drop stage outfilestage;
