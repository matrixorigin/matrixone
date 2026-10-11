-- issue #29841: Decimal128 text must not count integer leading zeros toward the
-- 38-digit limit, and discarded digits must be rounded once to the column scale.
-- d is rendered as non-numeric text: the result checker compares numbers with a tolerance.
set @saved_sql_mode = @@sql_mode;
set sql_mode = 'STRICT_TRANS_TABLES';
drop table if exists issue_29841_i38;
drop table if exists issue_29841_c38;
drop table if exists issue_29841_j38;
drop table if exists issue_29841_i37;
drop table if exists issue_29841_c37;
drop table if exists issue_29841_j37;

create table issue_29841_i38(id int primary key, d decimal(38,38));
insert into issue_29841_i38 values (1,'0.00000000000000000000000000000000000001'), (2,'0.99999999999999999999999999999999999999'), (3,'-0.99999999999999999999999999999999999999'), (4,'.00000000000000000000000000000000000001'), (5,'0.111111111111111111111111111111111111114'), (6,'0.111111111111111111111111111111111111115'), (7,'-0.111111111111111111111111111111111111115'), (8,'0.111111111111111111111111111111111111114 e-1'), (9,'-0.111111111111111111111111111111111111114+e-1'), (10,'0.11111111111111111111111111111111111111054e0');
select id, concat('d=', d) as d_text from issue_29841_i38 order by id;
create table issue_29841_c38(id int primary key, d decimal(38,38));
load data infile '$resources/load_data/issue_29841_decimal38_38.csv' into table issue_29841_c38 fields terminated by ',';
select id, concat('d=', d) as d_text from issue_29841_c38 order by id;
create table issue_29841_j38(id int primary key, d decimal(38,38));
load data infile {'filepath'='$resources/load_data/issue_29841_decimal38_38.jl','format'='jsonline','jsondata'='object'} into table issue_29841_j38;
select id, concat('d=', d) as d_text from issue_29841_j38 order by id;

-- downscale: 1.45e-37 / 1.49e-37 must round once to 1e-37 in DECIMAL(38,37),
-- also when spelled with an exponent (ids 7-10); id 11 = 1.45e-36 rounds to 1.5e-36
create table issue_29841_i37(id int primary key, d decimal(38,37));
insert into issue_29841_i37 values (1,'0.000000000000000000000000000000000000145'), (2,'-0.000000000000000000000000000000000000145'), (3,'0.000000000000000000000000000000000000149'), (4,'-0.000000000000000000000000000000000000149'), (5,'0.000000000000000000000000000000000000144'), (6,'0.000000000000000000000000000000000000150'), (7,'0.000000000000000000000000000000000000145e0'), (8,'-0.000000000000000000000000000000000000145e0'), (9,'0.000000000000000000000000000000000000149E+0'), (10,'-0.000000000000000000000000000000000000149E+0'), (11,'0.000000000000000000000000000000000000145e1');
select id, concat('d=', d) as d_text from issue_29841_i37 order by id;
create table issue_29841_c37(id int primary key, d decimal(38,37));
load data infile '$resources/load_data/issue_29841_decimal38_37.csv' into table issue_29841_c37 fields terminated by ',';
select id, concat('d=', d) as d_text from issue_29841_c37 order by id;
create table issue_29841_j37(id int primary key, d decimal(38,37));
load data infile {'filepath'='$resources/load_data/issue_29841_decimal38_37.jl','format'='jsonline','jsondata'='object'} into table issue_29841_j37;
select id, concat('d=', d) as d_text from issue_29841_j37 order by id;


-- Destination precision applies after half-away-from-zero rounding.
create table issue_29841_range(d decimal(38,0));
insert into issue_29841_range values ('99999999999999999999999999999999999999.4'), ('-99999999999999999999999999999999999999.4');
insert into issue_29841_range values ('99999999999999999999999999999999999999.5');
insert into issue_29841_range values ('99999999999999999999999999999999999999.7');
insert into issue_29841_range values ('-99999999999999999999999999999999999999.5');
insert into issue_29841_range values ('-99999999999999999999999999999999999999.7');
select concat('d=',d) d_text from issue_29841_range order by d;
insert into issue_29841_range values ('1');
select count(*) from issue_29841_range;
drop table issue_29841_range;

drop table issue_29841_i38;
drop table issue_29841_c38;
drop table issue_29841_j38;
drop table issue_29841_i37;
drop table issue_29841_c37;
drop table issue_29841_j37;
set sql_mode = @saved_sql_mode;
