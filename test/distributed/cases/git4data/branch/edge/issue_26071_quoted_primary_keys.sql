-- Regression for issue #26071.
-- DATA BRANCH must quote user-defined primary-key columns in LCA reconstruction.
drop database if exists bvt_issue_26071;
create database bvt_issue_26071;
use bvt_issue_26071;

-- Ordinary identifier control.
create table control_base(id int primary key, value int);
insert into control_base values (1, 10), (2, 20);
data branch create table control_src from control_base;
data branch create table control_dst from control_base;
update control_src set value = 200 where id = 2;
data branch pick control_src into control_dst keys(2);

-- Reserved-word single primary key. Exercise the LCA delete path in PICK/MERGE.
create table quoted_base(`order` int primary key, value int);
insert into quoted_base values (1, 10), (2, 20);
data branch create table quoted_src from quoted_base;
data branch create table quoted_left from quoted_base;
data branch create table quoted_pick from quoted_base;
data branch create table quoted_merge from quoted_base;
delete from quoted_src where `order` = 1;
update quoted_src set value = 200 where `order` = 2;
data branch diff quoted_src against quoted_left output summary;
data branch diff quoted_src against quoted_left output as quoted_diff;
select __mo_diff_source, __mo_diff_flag, `order`, value
from quoted_diff order by `order`;
data branch pick quoted_src into quoted_pick keys(1, 2);
data branch merge quoted_src into quoted_merge;

-- Reserved-word and punctuation composite primary key.
create table composite_base(
    `select` int,
    `key-name` varchar(10),
    value int,
    primary key(`select`, `key-name`)
);
insert into composite_base values
    (1, 'a', 10),
    (2, 'b', 20),
    (3, 'c', 30);
data branch create table composite_left from composite_base;
data branch create table composite_src from composite_base;
data branch create table composite_pick from composite_base;
data branch create table composite_merge from composite_base;
update composite_src set value = 200
where `select` = 2 and `key-name` = 'b';
delete from composite_src
where `select` = 3 and `key-name` = 'c';
insert into composite_src values (4, 'd', 40);
data branch diff composite_src against composite_left output summary;
data branch diff composite_src against composite_left output as composite_diff;
select __mo_diff_source, __mo_diff_flag, `select`, `key-name`, value
from composite_diff order by `select`, `key-name`;
data branch pick composite_src into composite_pick
keys((2, 'b'), (3, 'c'));
data branch merge composite_src into composite_merge;

select id, value from control_dst order by id;
select `order`, value from quoted_pick order by `order`;
select `order`, value from quoted_merge order by `order`;
select `select`, `key-name`, value
from composite_pick order by `select`, `key-name`;
select `select`, `key-name`, value
from composite_merge order by `select`, `key-name`;

drop database bvt_issue_26071;
