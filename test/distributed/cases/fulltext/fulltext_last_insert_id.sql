-- #29903: a FULLTEXT index carries a hidden auto_increment fake primary key on
-- its index table. That internal value must NOT surface through LAST_INSERT_ID;
-- the reported insert id must be the base table's business auto_increment key.

drop table if exists id_probe;
create table id_probe(id bigint unsigned auto_increment primary key, label varchar(40));
create fulltext index label_ft on id_probe(label);
insert into id_probe(label) values ('small');
-- base id is 1; LAST_INSERT_ID must be the base key, not the hidden index key.
select id, last_insert_id() from id_probe;
insert into id_probe(label) values ('second row here');
-- next base key is 2.
select last_insert_id();

-- Latent case: the base auto_increment starts high while the hidden index fake
-- key starts at 1. LAST_INSERT_ID must still be the base key (1000), never 1.
drop table if exists id_probe_hi;
create table id_probe_hi(id bigint unsigned auto_increment primary key, label varchar(40)) auto_increment = 1000;
create fulltext index label_ft on id_probe_hi(label);
insert into id_probe_hi(label) values ('alpha beta gamma');
select id, last_insert_id() from id_probe_hi;

-- Multi-row insert: LAST_INSERT_ID is the FIRST generated base key.
drop table if exists id_probe_multi;
create table id_probe_multi(id bigint unsigned auto_increment primary key, label varchar(40));
create fulltext index label_ft on id_probe_multi(label);
insert into id_probe_multi(label) values ('one one one'), ('two two'), ('three');
select last_insert_id();
select group_concat(id order by id) as ids from id_probe_multi;

drop table if exists id_probe;
drop table if exists id_probe_hi;
drop table if exists id_probe_multi;
