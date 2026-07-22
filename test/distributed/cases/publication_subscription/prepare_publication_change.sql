drop account if exists prepared_subscriber;
create account prepared_subscriber admin_name = 'admin' identified by '111';
drop account if exists prepared_other;
create account prepared_other admin_name = 'admin' identified by '111';

drop database if exists prepared_pub_db1;
create database prepared_pub_db1;
create table prepared_pub_db1.src (a int);
drop database if exists prepared_pub_db2;
create database prepared_pub_db2;
create table prepared_pub_db2.src (b bigint);
drop publication if exists prepared_pub1;
create publication prepared_pub1 database prepared_pub_db1 account prepared_subscriber;
drop publication if exists prepared_pub2;
create publication prepared_pub2 database prepared_pub_db2 account prepared_subscriber;

-- @session:id=1&user=prepared_subscriber:admin&password=111
prepare deleted_publication_stmt from
    'create database prepared_deleted_sub from sys publication prepared_pub1';
-- @session

drop publication prepared_pub1;

-- @session:id=1&user=prepared_subscriber:admin&password=111
execute deleted_publication_stmt;
show databases like 'prepared_deleted_sub';
deallocate prepare deleted_publication_stmt;
-- @session

create publication prepared_pub1 database prepared_pub_db1 account prepared_subscriber;

-- @session:id=1&user=prepared_subscriber:admin&password=111
prepare revoked_publication_stmt from
    'create database prepared_revoked_sub from sys publication prepared_pub1';
-- @session

alter publication prepared_pub1 account prepared_other;

-- @session:id=1&user=prepared_subscriber:admin&password=111
execute revoked_publication_stmt;
show databases like 'prepared_revoked_sub';
deallocate prepare revoked_publication_stmt;

create database prepared_work;
use prepared_work;
-- @session

alter publication prepared_pub1 account prepared_subscriber;

-- @session:id=1&user=prepared_subscriber:admin&password=111
create database prepared_identity_sub from sys publication prepared_pub2;
prepare subscription_identity_stmt from
    'create table prepared_like like prepared_identity_sub.src';
drop database prepared_identity_sub;
create database prepared_identity_sub from sys publication prepared_pub1;
execute subscription_identity_stmt;
show columns from prepared_like;
deallocate prepare subscription_identity_stmt;
drop database prepared_identity_sub;

create database prepared_target;
create database prepared_clone_source;
create table prepared_clone_source.src (a int);
prepare target_sequence_stmt from 'create sequence prepared_target.seq as bigint';
prepare target_clone_stmt from
    'create table prepared_target.cloned clone prepared_clone_source.src';
drop database prepared_target;
create database prepared_target from sys publication prepared_pub1;
execute target_sequence_stmt;
execute target_clone_stmt;
show tables from prepared_target;
deallocate prepare target_sequence_stmt;
deallocate prepare target_clone_stmt;
drop database prepared_target;
drop database prepared_clone_source;
drop table prepared_like;
drop database prepared_work;
-- @session

drop publication prepared_pub1;
drop publication prepared_pub2;
drop database prepared_pub_db1;
drop database prepared_pub_db2;
drop account prepared_subscriber;
drop account prepared_other;
