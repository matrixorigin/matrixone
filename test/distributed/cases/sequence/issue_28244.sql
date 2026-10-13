drop database if exists issue28244;
create database issue28244;
use issue28244;

-- A table with the same columns as sequence storage is still an ordinary table.
create table fake_seq(
    last_seq_num smallint,
    min_value smallint,
    max_value smallint,
    start_value smallint,
    increment_value bigint,
    cycle bool,
    is_called bool
);
insert into fake_seq values (10, 1, 100, 10, 1, false, false);
select setval('fake_seq', 20, true);
select * from fake_seq;
select nextval('fake_seq');

-- The check must not depend on the first column's type either.
create table fake_varchar(
    first_value varchar(20),
    min_value int,
    max_value int,
    start_value int,
    increment_value bigint,
    cycle bool,
    is_called bool
);
insert into fake_varchar values ('not-a-sequence', 1, 100, 10, 1, false, false);
select setval('fake_varchar', 20, true);
select * from fake_varchar;

-- A real sequence continues to use the existing SETVAL/NEXTVAL behavior.
create sequence real_seq as int start with 10;
select setval('real_seq', 20, true);
select nextval('real_seq');

drop database issue28244;
