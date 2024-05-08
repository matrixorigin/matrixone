-- separate execution
select @@sql_select_limit;
set sql_select_limit=default;
select @@sql_select_limit;

-- set sql_select_limit after autocommit
set autocommit=0;
begin;
select @@sql_select_limit;
set sql_select_limit=default;
commit;
select @@sql_select_limit;

-- set sql_mode after autocommit
begin;
select @@sql_mode;
set sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select @@sql_mode;
commit;
set @@sql_mode=default;


set autocommit=1;