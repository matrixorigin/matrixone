-- update enum column with single-column secondary index
drop table if exists enum_idx05;
create table enum_idx05 (id int primary key, status enum('new','paid','shipped'), key idx_status(status));
insert into enum_idx05 values (1,'new'),(2,'paid');
update enum_idx05 set status='paid' where id=1;
select id, status from enum_idx05 order by id;
select id from enum_idx05 where status='new';
select id from enum_idx05 where status='paid' order by id;
drop table enum_idx05;

-- update enum column with composite secondary index
drop table if exists enum_idx06;
create table enum_idx06 (id int primary key, status enum('new','paid','shipped'), note varchar(20), key idx_note_status(note, status));
insert into enum_idx06 values (1,'new','n1'),(2,'paid','n2');
update enum_idx06 set status='paid', note='updated' where id=1;
select id, status, note from enum_idx06 order by id;
drop table enum_idx06;

-- update enum column with single-column unique index
drop table if exists enum_idx07;
create table enum_idx07 (id int primary key, status enum('new','paid','shipped'), note varchar(20), unique key uk_status(status));
insert into enum_idx07 values (1,'new','n1'),(2,'paid','n2');
update enum_idx07 set status='shipped' where id=1;
select id, status, note from enum_idx07 order by id;
select id from enum_idx07 where status='new';
select id from enum_idx07 where status='shipped';
-- unique conflict is properly raised; key name differs across exec modes, match prefix only
-- @regex("Duplicate entry", true)
update enum_idx07 set status='paid' where id=1;
select id, status, note from enum_idx07 order by id;
drop table enum_idx07;

-- update enum column with composite unique index
drop table if exists enum_idx08;
create table enum_idx08 (id int primary key, status enum('new','paid','shipped'), note varchar(20), unique key uk_note_status(note, status));
insert into enum_idx08 values (1,'new','n1'),(2,'paid','n2');
update enum_idx08 set status='shipped', note='updated' where id=1;
select id, status, note from enum_idx08 order by id;
-- both queries use the (note, status) composite unique index (two leading equal conds);
-- they verify the old index key was deleted and the new one written, not just the main table
select id from enum_idx08 where note='updated' and status='shipped';
select id from enum_idx08 where note='n1' and status='new';
-- composite unique conflict is properly raised; key name differs across exec modes, match prefix only
-- @regex("Duplicate entry", true)
update enum_idx08 set status='paid', note='n2' where id=1;
select id, status, note from enum_idx08 order by id;
drop table enum_idx08;

-- update set column with single-column secondary index
drop table if exists set_idx01;
create table set_idx01 (id int primary key, opts set('a','b','c'), key idx_opts(opts));
insert into set_idx01 values (1,'a'),(2,'b');
update set_idx01 set opts='c' where id=1;
select id, opts from set_idx01 order by id;
select id from set_idx01 where opts='a';
select id from set_idx01 where opts='c' order by id;
drop table set_idx01;

-- update set column with composite secondary index
drop table if exists set_idx02;
create table set_idx02 (id int primary key, opts set('a','b','c'), note varchar(20), key idx_note_opts(note, opts));
insert into set_idx02 values (1,'a','n1'),(2,'b','n2');
update set_idx02 set opts='c', note='updated' where id=1;
select id, opts, note from set_idx02 order by id;
drop table set_idx02;

-- update set column with single-column unique index (SET not allowed in unique key, verify error)
-- create table set_idx03 (id int primary key, opts set('a','b','c'), unique key uk_opts(opts));
