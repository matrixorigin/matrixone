-- @label:bvt

drop database if exists group_wide_text;
create database group_wide_text;
use group_wide_text;

-- Declared LONGTEXT widths must never select the no-key aggregate path.
create table plain_keys(a longtext, b longtext, latency int not null);
insert into plain_keys values ('a','x',10),('a','x',30),('b','y',20),(null,'y',40),(null,'y',60);
select a,b,count(*) as observations,avg(latency) as avg_latency
from plain_keys group by a,b order by a is not null,a,b;

-- The same key metadata is reachable through ordinary STORED JSON projections.
create table stored_keys(
    data json not null,
    latency int not null,
    a longtext character set utf8mb4 collate utf8mb4_bin generated always as(data->>'$.a') stored,
    b longtext character set utf8mb4 collate utf8mb4_bin generated always as(data->>'$.b') stored
);
insert into stored_keys(data,latency) values
    ('{"a":"a","b":"x","extra":{"keep":[1,"x",null]}}',10),
    ('{"a":"a","b":"x","extra":{"keep":[1,"x",null]}}',30),
    ('{"a":"b","b":"y","extra":{"keep":[1,"x",null]}}',20),
    ('{"b":"y","extra":{"keep":[1,"x",null]}}',40),
    ('{"b":"y","extra":{"keep":[1,"x",null]}}',60);
select a,b,count(*) as observations,avg(latency) as avg_latency
from stored_keys group by a,b order by a is not null,a,b;

drop database group_wide_text;
