drop table if exists load_data_year_t;
create table load_data_year_t (
  id int primary key,
  y year
);

load data infile '$resources/load_data/year.csv'
into table load_data_year_t
fields terminated by ','
lines terminated by '\n'
ignore 1 lines;

select count(*) from load_data_year_t;
select id, cast(y as char) as y from load_data_year_t order by id;

drop table load_data_year_t;
