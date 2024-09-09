-- TODO: run all tests with both experimental_fulltext_index = 0 and 1
-- TODO: GENERATE the test case to cover all combinations of types (varchar, char and text)
set experimental_fulltext_index=1;

create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'red', 't1'), (1, 'yellow', 't2'), (2, 'blue', 't3'), (3, 'blue red', 't4');

create fulltext index ftidx on src (body, title);

select * from src where match(body, title) against('red');
select src.*, match(body, title) against('blue') from src;

drop table src;

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2));

insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4');

create fulltext index ftidx2 on src2 (body, title);
select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

drop table src2;
