-- Explicit hidden path components are selection, not ordinary glob discovery.
-- #28270: local LOAD and external tables share the local ETL filesystem.
drop database if exists load_hidden_paths;
create database load_hidden_paths;
use load_hidden_paths;

-- Literal LOAD remains a control for the same hidden directory.
create table loaded (a int);
load data infile '$resources/load_data/hidden_paths/.data/part-1.csv' into table loaded;
select a from loaded order by a;

-- A plain glob must not start discovering hidden directories or hidden files.
create external table visible_glob (a int)
infile{'filepath'='$resources/load_data/hidden_paths/*/part-*.csv', 'format'='csv'};
select a from visible_glob order by a;
create external table hidden_dir (a int)
infile{'filepath'='$resources/load_data/hidden_paths/.data/*.csv', 'format'='csv'};
select a from hidden_dir order by a;

-- A dot-prefixed pattern and an exact hidden filename both explicitly opt in.
create external table hidden_glob (a int)
infile{'filepath'='$resources/load_data/hidden_paths/.data/.part-*.csv', 'format'='csv'};
select a from hidden_glob order by a;
create external table hidden_file (a int)
infile{'filepath'='$resources/load_data/hidden_paths/.top.csv', 'format'='csv'};
select a from hidden_file order by a;

-- Missing literal components still match nothing.
create external table missing_path (a int)
infile{'filepath'='$resources/load_data/hidden_paths/.missing/*.csv', 'format'='csv'};
select count(*) from missing_path;

drop database load_hidden_paths;
