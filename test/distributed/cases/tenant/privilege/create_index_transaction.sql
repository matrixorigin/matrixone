drop database if exists index_transaction_auth;
drop user if exists index_txn_owner_user, index_txn_granted_user;
drop role if exists index_txn_owner, index_txn_granted;

create database index_transaction_auth;
create table index_transaction_auth.existing_table(id int primary key, payload varchar(32), index keep_payload(payload));
create role index_txn_owner, index_txn_granted;
create user index_txn_owner_user identified by '123456' default role index_txn_owner;
create user index_txn_granted_user identified by '123456' default role index_txn_granted;
grant create table on database index_transaction_auth to index_txn_owner;
grant index on table index_transaction_auth.existing_table to index_txn_granted;

-- @session:id=1&user=sys:index_txn_owner_user:index_txn_owner&password=123456
set session experimental_fulltext_index = 1;
begin;
create table index_transaction_auth.new_table(id int primary key, payload varchar(128));
create fulltext index ft_payload on index_transaction_auth.new_table(payload) with parser ngram;
create index discard_payload on index_transaction_auth.new_table(payload);
drop index discard_payload on index_transaction_auth.new_table;
commit;

-- The ownership of a new table cannot authorize an index on another table.
begin;
create table index_transaction_auth.rolled_back(id int primary key);
create index unauthorized on index_transaction_auth.existing_table(payload);
drop index keep_payload on index_transaction_auth.existing_table;
rollback;

-- @session

-- @session:id=2&user=sys:index_txn_granted_user:index_txn_granted&password=123456
create index allowed on index_transaction_auth.existing_table(payload);
drop index allowed on index_transaction_auth.existing_table;
-- @session

select table_name, index_name, index_type from information_schema.statistics
where table_schema = 'index_transaction_auth' and index_name in ('ft_payload', 'discard_payload', 'keep_payload', 'allowed')
order by table_name, index_name;
select count(*) from information_schema.tables
where table_schema = 'index_transaction_auth' and table_name = 'rolled_back';

drop database index_transaction_auth;
drop user index_txn_owner_user, index_txn_granted_user;
drop role index_txn_owner, index_txn_granted;
