-- External CSV tables return the same rows as LOAD DATA into a native table:
-- quoted separators, doubled quotes, \N, empty fields, leading/trailing spaces,
-- gzip (explicit and auto), a BOM + CRLF header, and a file without a final newline.
-- Also covers read-only rejections and queries that combine external tables.
drop database if exists ext_read_consistency;
create database ext_read_consistency;
use ext_read_consistency;

create external table e1(id int, s varchar(20), d date, f double) infile{'filepath'='$resources/external_table_file/ext_read_1030/part1.csv'} fields terminated by ',' enclosed by '"';
select id, s, concat('[', s, ']'), d, f from e1 order by id;
select count(*), count(s), count(d), count(f), sum(id) from e1;
select id from e1 where s is null or s = '';
select id from e1 where s like '%"%';

create table r1(id int, s varchar(20), d date, f double);
load data infile '$resources/external_table_file/ext_read_1030/part1.csv' into table r1 fields terminated by ',' enclosed by '"';
select count(*) from e1 e join r1 r on e.id = r.id and e.s <=> r.s and e.d <=> r.d and e.f <=> r.f;

create external table e1gz(id int, s varchar(20), d date, f double) infile{'filepath'='$resources/external_table_file/ext_read_1030/part1.csv.gz','compression'='gzip'} fields terminated by ',' enclosed by '"';
create external table e1auto(id int, s varchar(20), d date, f double) infile{'filepath'='$resources/external_table_file/ext_read_1030/part1.csv.gz','compression'='auto'} fields terminated by ',' enclosed by '"';
select count(*) from e1gz g join e1 e on g.id = e.id and g.s <=> e.s and g.d <=> e.d and g.f <=> e.f;
select count(*) from e1auto a join e1 e on a.id = e.id and a.s <=> e.s and a.d <=> e.d and a.f <=> e.f;

create external table eall(id int, s varchar(20), d date, f double) infile{'filepath'='$resources/external_table_file/ext_read_1030/part*.csv'} fields terminated by ',' enclosed by '"';
select substring_index(__mo_filepath, '/', -1) file, count(*), min(id), max(id) from eall group by file order by file;
select id, s from eall where substring_index(__mo_filepath, '/', -1) = 'part2.csv' order by id;

create external table ebom(id varchar(10), name varchar(10)) infile{'filepath'='$resources/external_table_file/ext_read_1030/bom_header.csv'} fields terminated by ',' lines terminated by '\r\n' ignore 1 lines;
select id, hex(id), name, hex(name) from ebom order by id;
create external table enonl(id int, s varchar(10)) infile{'filepath'='$resources/external_table_file/ext_read_1030/no_final_newline.csv'} fields terminated by ',';
select * from enonl order by id;

-- queries over external tables
create view ve as select id, s from e1 where id > 0;
select * from ve order by id;
select a.id, b.id from e1 a join e1 b on a.id = b.id + 1 order by a.id;
select e.id, n.s from e1 e join enonl n on e.id = n.id order by e.id;
select id from e1 where id in (select id from enonl) order by id;
select id from e1 union select id from enonl order by id;
create table copy1 as select * from e1;
select count(*) from copy1;
insert into copy1 select * from e1 where id = 1;
select count(*) from copy1 where id = 1;
select table_type from information_schema.tables where table_schema = 'ext_read_consistency' and table_name = 'e1';

-- external tables are read only
insert into e1 values (9,'a','2024-01-01',1);
update e1 set s = 'x';
delete from e1;
create index ie on e1(id);
create external table epk(id int primary key, s varchar(10)) infile{'filepath'='$resources/external_table_file/ext_read_1030/part2.csv'} fields terminated by ',';

drop view ve;
drop database ext_read_consistency;
