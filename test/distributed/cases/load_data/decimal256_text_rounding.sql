-- Decimal256 text longer than 76 digits must be rounded once, to the column
-- scale (ids 1-4 are 1.4999..95e-64 -> 1e-64, also exponent-spelled; not 2e-64).
-- d is rendered as non-numeric text: the result checker compares numbers with a tolerance.
drop table if exists d256_round_i;
drop table if exists d256_round_c;
drop table if exists d256_round_j;
create table d256_round_i(id int primary key, d decimal(65,64));
insert into d256_round_i values (1,'0.000000000000000000000000000000000000000000000000000000000000000149999999999995'), (2,'-0.000000000000000000000000000000000000000000000000000000000000000149999999999995'), (3,'0.000000000000000000000000000000000000000000000000000000000000000149999999999995e0'), (4,'-0.000000000000000000000000000000000000000000000000000000000000000149999999999995E+0'), (5,'0.0000000000000000000000000000000000000000000000000000000000000001500000000000000000000'), (6,'1.444444444444444444444444444444444444444444444444444444444444444499999999999999999999');
select id, concat('d=', d) as d_text from d256_round_i order by id;
create table d256_round_c(id int primary key, d decimal(65,64));
load data infile '$resources/load_data/decimal256_text_rounding.csv' into table d256_round_c fields terminated by ',';
select id, concat('d=', d) as d_text from d256_round_c order by id;
create table d256_round_j(id int primary key, d decimal(65,64));
load data infile {'filepath'='$resources/load_data/decimal256_text_rounding.jl','format'='jsonline','jsondata'='object'} into table d256_round_j;
select id, concat('d=', d) as d_text from d256_round_j order by id;
select concat('d=', cast('0.000000000000000000000000000000000000000000000000000000000000000149999999999995' as decimal(65,64))) as cast_text;
drop table d256_round_i;
drop table d256_round_c;
drop table d256_round_j;
