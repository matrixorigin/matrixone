-- issue #29840: an empty / whitespace ENUM member and the empty SET are values,
-- not NULL. LOAD must read back exactly what native INSERT stores, in every
-- applicable LOAD mode; explicit NULL still loads as NULL.
-- SET('0','a') makes a zero-filled empty SET visible as member '0'.
-- SET(' a','a','中文') checks that a leading-space member is not trimmed into 'a'.
set @saved_sql_mode = @@sql_mode;
set sql_mode = 'STRICT_TRANS_TABLES';
drop table if exists issue_29840_ins;
drop table if exists issue_29840_csv;
drop table if exists issue_29840_csv_par;
drop table if exists issue_29840_csv_local;
drop table if exists issue_29840_jl;
drop table if exists issue_29840_jl_par;
create table issue_29840_ins(id int primary key, e enum(' ','red',''), s set('0','a'), t set(' a','a','中文'));
insert into issue_29840_ins values (1,'','',' a'), (2,' ','','中文'), (3,'red','a','a'), (4,null,null,null), (5,'','0',' a,a'), (6,'red','0,a','a ');
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_ins order by id;
create table issue_29840_csv like issue_29840_ins;
load data infile '$resources/load_data/issue_29840_enum_set.csv' into table issue_29840_csv fields terminated by ',' optionally enclosed by '"';
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_csv order by id;
create table issue_29840_csv_par like issue_29840_ins;
load data infile '$resources/load_data/issue_29840_enum_set.csv' into table issue_29840_csv_par fields terminated by ',' optionally enclosed by '"' parallel 'true';
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_csv_par order by id;
set sql_mode = '';
create table issue_29840_csv_local like issue_29840_ins;
load data local infile '$resources/load_data/issue_29840_enum_set.csv' into table issue_29840_csv_local fields terminated by ',' optionally enclosed by '"';
set sql_mode = 'STRICT_TRANS_TABLES';
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_csv_local order by id;
create table issue_29840_jl like issue_29840_ins;
load data infile {'filepath'='$resources/load_data/issue_29840_enum_set.jl','format'='jsonline','jsondata'='object'} into table issue_29840_jl;
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_jl order by id;
create table issue_29840_jl_par like issue_29840_ins;
load data infile {'filepath'='$resources/load_data/issue_29840_enum_set.jl','format'='jsonline','jsondata'='object'} into table issue_29840_jl_par parallel 'true';
select id, concat('[',e,']') e, e+0 e_idx, e is null e_null, concat('[',s,']') s, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_jl_par order by id;
-- every LOAD table must equal the INSERT control row by row (expect 0 each)
select * from (select 'csv' t, count(*) diff from issue_29840_ins i join issue_29840_csv x using(id) where not (i.e+0 <=> x.e+0 and i.s+0 <=> x.s+0 and i.t+0 <=> x.t+0)
union all select 'csv_par', count(*) from issue_29840_ins i join issue_29840_csv_par x using(id) where not (i.e+0 <=> x.e+0 and i.s+0 <=> x.s+0 and i.t+0 <=> x.t+0)
union all select 'csv_local', count(*) from issue_29840_ins i join issue_29840_csv_local x using(id) where not (i.e+0 <=> x.e+0 and i.s+0 <=> x.s+0 and i.t+0 <=> x.t+0)
union all select 'jl', count(*) from issue_29840_ins i join issue_29840_jl x using(id) where not (i.e+0 <=> x.e+0 and i.s+0 <=> x.s+0 and i.t+0 <=> x.t+0)
union all select 'jl_par', count(*) from issue_29840_ins i join issue_29840_jl_par x using(id) where not (i.e+0 <=> x.e+0 and i.s+0 <=> x.s+0 and i.t+0 <=> x.t+0)) d order by t;
-- Explicit NULLIF mappings override otherwise-valid empty ENUM/SET values.
truncate table issue_29840_csv;
load data infile '$resources/load_data/issue_29840_enum_set.csv' into table issue_29840_csv fields terminated by ',' optionally enclosed by '"' set e=NULLIF(e,''), s=NULLIF(s,'');
select id, e+0 e_idx, e is null e_null, s+0 s_bits, s is null s_null, concat('[',t,']') t, t+0 t_bits from issue_29840_csv order by id;
drop table issue_29840_ins;
drop table issue_29840_csv;
drop table issue_29840_csv_par;
drop table issue_29840_csv_local;
drop table issue_29840_jl;
drop table issue_29840_jl_par;
set sql_mode = @saved_sql_mode;
