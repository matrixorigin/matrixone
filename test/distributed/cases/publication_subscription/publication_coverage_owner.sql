-- Issue 30950: SHOW PUBLICATION COVERAGE must allow the publication's own
-- owner, keep subscriber access, and still reject unrelated accounts.
drop account if exists cov_owner;
drop account if exists cov_reader;
drop account if exists cov_stranger;
create account cov_owner admin_name = 'admin' identified by '111';
create account cov_reader admin_name = 'admin' identified by '111';
create account cov_stranger admin_name = 'admin' identified by '111';

-- @session:id=1&user=cov_owner:admin&password=111
create database cov_db;
create table cov_db.t1 (id int primary key, v int);
create table cov_db.t2 (id int primary key);
-- The owner is not in the subscriber list: only cov_reader is authorized.
create publication cov_pub database cov_db account cov_reader;
-- A table-scoped publication publishes an explicit table list.
create publication cov_pub_t1 database cov_db table t1 account cov_reader;
-- The owner can inspect the coverage of its own publications.
show publication coverage cov_pub;
show publication coverage cov_pub_t1;

-- @session:id=2&user=cov_reader:admin&password=111
-- The subscriber keeps its access to the publication coverage.
show publication coverage cov_pub;
show publication coverage cov_pub_t1;

-- @session:id=3&user=cov_stranger:admin&password=111
-- Unrelated accounts stay rejected.
show publication coverage cov_pub;
-- @session

-- The sys account can inspect any publication's coverage.
show publication coverage cov_pub;

-- An identically named publication owned by another account must not shadow
-- the caller's own publication.
-- @session:id=2&user=cov_reader:admin&password=111
create database cov_db2;
create table cov_db2.shadow (id int primary key);
create publication cov_pub database cov_db2 account cov_stranger;
-- cov_reader observes its own identically named publication, not cov_owner's.
show publication coverage cov_pub;
-- @session:id=1&user=cov_owner:admin&password=111
-- cov_owner still observes its own publication's coverage.
show publication coverage cov_pub;
-- @session:id=3&user=cov_stranger:admin&password=111
-- cov_stranger cannot access cov_owner's cov_pub, but it is the subscriber of
-- cov_reader's identically named publication, so the name resolves to the
-- publication it can access.
show publication coverage cov_pub;
-- @session

-- @session:id=1&user=cov_owner:admin&password=111
drop publication cov_pub;
drop publication cov_pub_t1;
drop database cov_db;
-- @session:id=2&user=cov_reader:admin&password=111
drop publication cov_pub;
drop database cov_db2;
-- @session
drop account cov_stranger;
drop account cov_reader;
drop account cov_owner;
