drop database if exists issue_29849;
create database issue_29849;
use issue_29849;

create table load_user_variable (
    id int primary key,
    region_id int not null,
    label varchar(32) not null,
    payload_legacy varchar(64),
    payload varchar(64),
    payload_empty varchar(64),
    payload_json json
);

-- A row variable must be listed as an input field before it can be used in SET.
load data inline format='csv', data='1,10,unknown,legacy,value,true'
into table load_user_variable
fields terminated by ','
(id, region_id, label, payload_legacy)
set payload = nullif(@missing_variable, 'NULL');

load data infile '$resources/load_data/issue_29849_user_variables.csv'
into table load_user_variable
fields terminated by ','
(id, region_id, label, payload_legacy, @user_variable, @json_value)
set payload_legacy = nullif(payload_legacy, 'NULL'),
    payload = nullif(@user_variable, 'NULL'),
    payload_empty = nullif(@user_variable, ''),
    payload_json = nullif(@json_value, 'null');

select id, region_id, label,
       payload_legacy is null as legacy_is_null, payload_legacy,
       payload is null as payload_is_null, payload,
       payload_empty is null as empty_is_null, payload_empty,
       payload_json is null as json_is_null, payload_json
from load_user_variable
order by id;

drop database issue_29849;
show databases like 'issue_29849';
