-- error out
select load_file(cast('file://$resources/file_test/normal.txt?offset=0&size=a' as datalink));
select load_file(cast('file://$resources/file_test/normal.txt?offset=b&size=3' as datalink));

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
-- stage
select save_file(cast('stage://outfilestage/datalink/1.txt' as datalink), 'this is a test.');
select load_file(cast('stage://outfilestage/datalink/1.txt' as datalink));

-- file
select save_file(cast('file://$resources/into_outfile/datalink/2.txt' as datalink), 'this is a test.');
select load_file(cast('file://$resources/into_outfile/datalink/2.txt' as datalink));

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

-- failed cast
select cast(cast('file://xxx' as bool) as datalink);
select cast(cast('file://xxx' as bigint) as datalink);
select cast(cast('file://xxx' as int) as datalink);
select cast(cast('file://xxx' as float) as datalink);
select cast(cast(cast('file://xxx' as char) as datalink) as bool);
select cast(cast(cast('file://xxx' as char) as datalink) as bigint);
select cast(cast(cast('file://xxx' as char) as datalink) as int);
select cast(cast('file://xxx' as decimal(10,2)) as datalink);
select cast(cast('file://xxx' as double) as datalink);
select cast(cast('file://xxx' as date) as datalink);
select cast(cast('file://xxx' as datetime) as datalink);
select cast(cast('file://xxx' as timestamp) as datalink);
select cast(cast('file://xxx' as time) as datalink);
select cast(cast('file://xxx' as year) as datalink);
select cast(cast('file://xxx' as tinyint) as datalink);
select cast(cast('file://xxx' as smallint) as datalink);
select cast(cast('file://xxx' as mediumint) as datalink);
select cast(cast('file://xxx' as bigint unsigned) as datalink);
select cast(cast('file://xxx' as float(10,2)) as datalink);
select cast(cast('file://xxx' as enum('a','b')) as datalink);
select cast(cast('{"k":1}' as json) as datalink);
select cast(cast('[1,2,3]' as json) as datalink);
select cast(cast('file://xxx' as fulltext) as datalink);
select cast(cast(cast('file://xxx' as varchar) as datalink) as decimal(12,3));
select cast(cast(cast('file://xxx' as varchar) as datalink) as float);
select cast(cast(cast('file://xxx' as varchar) as datalink) as date);
select cast(cast(cast('file://xxx' as varchar) as datalink) as datetime);
select cast(cast(cast('file://xxx' as varchar) as datalink) as timestamp);

-- success cast
select cast(cast('file://xxx' as binary) as datalink);
select cast(cast('file://xxx' as varchar) as datalink);
select cast(cast('file://xxx' as char) as datalink);
select cast(cast('file://xxx' as text) as datalink);
select cast(cast(cast('file://xxx' as char) as datalink) as text);
select cast(cast(cast('file://xxx' as char) as datalink) as varchar);
select cast(cast(cast('file://xxx' as char) as datalink) as char);

select cast('' as datalink);
select cast('   ' as datalink);
select cast(cast('file://xxx' as varchar(100)) as datalink);
select cast(cast('file://xxx' as char(20)) as datalink);
select cast(cast('file://xxx' as text) as datalink);
select cast(cast(x'66696C653A2F2F787878' as binary) as datalink); -- 'file://xxx' 的hex
select cast(null as datalink);
select cast(cast(null as varchar) as datalink);
select cast(cast(cast(null as varchar) as datalink) as varchar);
select cast(cast('file://短路径' as char(32)) as datalink);
select cast('file:///etc/hosts#L10' as datalink);


drop stage filestage;
drop stage outfilestage;
