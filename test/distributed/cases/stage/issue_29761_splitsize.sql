-- Regression for issue #29761: SPLITSIZE must split between complete CSV rows.
drop database if exists issue_29761_splitsize;
create database issue_29761_splitsize;
use issue_29761_splitsize;

drop stage if exists issue_29761_split_stage;
create stage issue_29761_split_stage URL='file://$resources/issue_29761_splitsize';
select count(*) from stage_list('stage://issue_29761_split_stage/');

create table source_rows (id int, payload varchar(40));
insert into source_rows values
    (1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'),
    (2, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'),
    (3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');

select id, payload from source_rows order by id
into outfile 'stage://issue_29761_split_stage/split_%d.csv'
format 'csv' splitsize '64';

select substring_index(f.file, '/', -1) as file from stage_list('stage://issue_29761_split_stage/') as f order by file;
select length(load_file(cast('stage://issue_29761_split_stage/split_0.csv' as datalink)));
select length(load_file(cast('stage://issue_29761_split_stage/split_1.csv' as datalink)));
select length(load_file(cast('stage://issue_29761_split_stage/split_2.csv' as datalink)));

create table copied_rows (id int, payload varchar(40));
load data infile 'stage://issue_29761_split_stage/split_0.csv' into table copied_rows fields terminated by ',' ignore 1 lines;
load data infile 'stage://issue_29761_split_stage/split_1.csv' into table copied_rows fields terminated by ',' ignore 1 lines;
load data infile 'stage://issue_29761_split_stage/split_2.csv' into table copied_rows fields terminated by ',' ignore 1 lines;
select count(*), min(id), max(id), min(length(payload)), max(length(payload)) from copied_rows;
select id, payload from copied_rows order by id;

remove files from stage if exists 'stage://issue_29761_split_stage/split_*.csv';
select count(*) from stage_list('stage://issue_29761_split_stage/');
drop stage issue_29761_split_stage;
drop database issue_29761_splitsize;
