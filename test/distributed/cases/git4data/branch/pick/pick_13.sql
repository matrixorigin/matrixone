drop database if exists test_pick_year;
create database test_pick_year;
use test_pick_year;

create table year_base(
y year primary key,
note varchar(20)
);
insert into year_base values
(2020, 'base'),
(2021, 'base2');

data branch create table year_dst from year_base;
data branch create table year_src from year_base;

insert into year_src values
(2024, 'literal'),
(2025, 'subquery');

data branch pick year_src into year_dst keys('2024');
select * from year_dst order by y;

data branch pick year_src into year_dst keys(select y from year_src where y = 2025);
select * from year_dst order by y;

drop table year_src;
drop table year_dst;
drop table year_base;
drop database test_pick_year;
