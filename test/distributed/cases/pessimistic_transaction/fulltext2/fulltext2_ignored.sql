-- Rows a FULLTEXT2 index cannot read (#31583). Each section proves the claim of
-- docs/design/20261028-index-ignored-rows.md it is headed by.
drop database if exists ft2_ignored;
create database ft2_ignored;
use ft2_ignored;
set experimental_fulltext2_index = 1;

-- ---------------- I19: by default a build fails on text that is not JSON -----
create table j(id bigint primary key, body varchar(200));
insert into j values (1,'{"a":"red"}'),(2,'{not json'),(3,'{"a":"blue"}');
create fulltext2 index fj on j(body) with parser json;
show index ignored from j;
-- MAX_BAD_RECORDS lets the build leave out that many rows, with the reason of each
create fulltext2 index fj on j(body) with parser json max_bad_records 2;
-- @ignore:4
show index ignored from j;
-- a build that can store none of the rows fails within the limit
create table z(id bigint primary key, body varchar(200));
insert into z values (1,'{x'),(2,'[y');
create fulltext2 index fz on z(body) with parser json max_bad_records 10;

-- ---------------- I20: the sync records the rows and follows the source -----
insert into j values (4,'[broken'),(5,'{"a":"green"}');
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from j;
update j set body = '{"a":"gray"}' where id = 2;
update j set body = '{bad' where id = 1;
delete from j where id = 4;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from j;
-- one row more than the limit stops the sync
insert into j values (6,'{six'),(7,'{seven');
-- @ignore:6
-- @wait_expect(2, 60)
show index status from j;
-- REINDEX with a higher limit rebuilds it and starts it again
alter table j alter reindex fj fulltext2 max_bad_records 5;
-- @ignore:4
show index ignored from j;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from j;
show create table j;

-- ---------------- I22: the reason of a row is bounded -----
-- a malformed value as long as the column builds, and the sync records another
create table big(id bigint primary key, body text);
insert into big values (1,'{"a":"red"}'),(2,concat('{', repeat('x', 65534)));
create fulltext2 index fbig on big(body) with parser json max_bad_records 2;
-- @ignore:3,4
show index ignored from big where length(`Reason`) <= 1024;
insert into big values (3,concat('[', repeat('y', 65534)));
-- @ignore:3,4
-- @wait_expect(2, 60)
show index ignored from big where length(`Reason`) <= 1024;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from big;

-- ---------------- I8: an index on text that needs no parsing is unchanged -----
create table p(id bigint primary key, body varchar(200));
insert into p values (1,'{not json but plain text}');
create fulltext2 index fp on p(body);
show index ignored from p;
select id from p where match(body) against('plain');

drop database ft2_ignored;
