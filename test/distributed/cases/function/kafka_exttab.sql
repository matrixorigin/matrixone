-- ENGINE = KAFKA external table: DDL, SHOW CREATE, guards, and read-control
-- validation. Everything here resolves at parse/plan/compile time — no Kafka
-- broker is needed (connected reads are covered by Go unit tests against an
-- in-process fake cluster).
drop database if exists kafka_exttab;
create database kafka_exttab;
use kafka_exttab;

-- create with defaults; the group defaults per table
create external table kt (a int, b varchar(100)) engine = kafka with ('brokers' = '127.0.0.1:19092', 'topic' = 'events');
show create table kt;

-- explicit options round-trip through SHOW CREATE
create external table kt2 (a int, b varchar(100)) engine = kafka with ('brokers' = 'h1:9092,h2:9092', 'topic' = 't2', 'partition' = '3', 'autocommit' = 'true', 'group' = 'g2', 'format' = 'jsonl');
show create table kt2;

-- csv separator option
create external table kt3 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't3', 'separator' = '|');
show create table kt3;

-- DDL validation errors
create external table bad1 (a int) engine = kafka;
create external table bad2 (a int) engine = kafka with ('brokers' = 'h:9092');
create external table bad3 (a int) engine = kafka with ('topic' = 't');
create external table bad4 (a int) engine = kafka with ('brokers' = 'nohostport', 'topic' = 't');
create external table bad5 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'partition' = '-1');
create external table bad6 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'format' = 'xml');
create external table bad7 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'format' = 'jsonl', 'separator' = '|');
create external table bad8 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'separator' = '||');
create external table bad9 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'bogus' = 'v');
create external table bad10 (a int) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't', 'topic' = 't2');

-- reserved synthetic column names are rejected, on kafka and ordinary tables
create external table bad11 (a int, __mo_message_id bigint) engine = kafka with ('brokers' = 'h:9092', 'topic' = 't');
create table bad12 (a int, __mo_read_start_id bigint);
alter table kt3 add column __mo_message_ts timestamp;

-- ALTER on a kafka external table is rejected
alter table kt add column c int;

-- read-control validation fails at compile, before any broker dial:
-- autocommit=false (the default) requires a start id
select * from kt;
-- start id below -1
select * from kt where __mo_read_start_id = -2;
-- non-positive size
select * from kt where __mo_read_start_id = 0 and __mo_read_size = 0;
-- negative timeout
select * from kt where __mo_read_start_id = 0 and __mo_read_timeout = -1;
-- contradictory duplicate controls
select * from kt where __mo_read_start_id = 1 and __mo_read_start_id = 2;
-- overflow caps: values that would wrap arithmetic are rejected at compile
select * from kt where __mo_read_start_id = 9223372036854775807;
select * from kt where __mo_read_start_id = 0 and __mo_read_timeout = 10000000000;
select * from kt where __mo_read_start_id = 0 and __mo_read_size = 9223372036854775807;

-- writes into a kafka external table are rejected
insert into kt values (1, 'x');

-- DESC shows only the declared columns (synthetic columns are bind-time)
desc kt;

-- CREATE TABLE LIKE copies only the declared columns and yields an ordinary table
create table kt_like like kt;
desc kt_like;
insert into kt_like values (1, 'x');
select * from kt_like;
drop table kt_like;

-- an ordinary table may use ENGINE = kafka as a plain table option, and
-- kafka stays usable as an identifier
create table plain_t (a int) engine = kafka;
drop table plain_t;
create table kafka (kafka int);
insert into kafka values (1);
select kafka from kafka;
drop table kafka;

-- LAST_KAFKA_MESSAGE_ID() is NULL before any completed kafka scan
select last_kafka_message_id();

drop database kafka_exttab;
