drop database if exists external_nullif_rebind;
create database external_nullif_rebind;
use external_nullif_rebind;

create external table direct_nulls(col1 int,col2 float,col3 varchar,col4 blob,col6 date,col7 bool) infile{'filepath'='$resources/external_table_file/ex_table_null.csv'} fields terminated by ',' set col3=nullif(col3,'null');
select max(col3) as max_value,min(col3) as min_value,count(col3) as nonnull_rows,count(*) as total_rows from direct_nulls;

prepare nullif_create from "create external table prepared_nulls(col1 int,col2 float,col3 varchar,col4 blob,col6 date,col7 bool) infile{'filepath'='$resources/external_table_file/ex_table_null.csv'} fields terminated by ',' set col3=nullif(col3,'null')";
execute nullif_create;
select max(col3) as max_value,min(col3) as min_value,count(col3) as nonnull_rows,count(*) as total_rows from prepared_nulls;
drop table prepared_nulls;
execute nullif_create;
select max(col3) as max_value,min(col3) as min_value,count(col3) as nonnull_rows,count(*) as total_rows from prepared_nulls;
deallocate prepare nullif_create;

drop database external_nullif_rebind;
