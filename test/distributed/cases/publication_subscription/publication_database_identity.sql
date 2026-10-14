-- A publication belongs to sys but its physical source belongs to another account.
drop publication if exists pub_identity_guard;
drop account if exists pub_identity_tenant;
drop database if exists pub_identity_a;
create account pub_identity_tenant admin_name = 'admin' identified by '111';

-- @session:id=1&user=pub_identity_tenant:admin&password=111
create database pub_identity_a;
create database pub_identity_b;
create table pub_identity_b.marker (v int);
prepare drop_b from 'drop database pub_identity_b';
-- @session

create publication pub_identity_guard database pub_identity_a account pub_identity_tenant;
-- The publisher's same-name database is a different physical object.
create database pub_identity_a;
drop database pub_identity_a;
alter publication pub_identity_guard comment 'same source';

-- @session:id=1&user=pub_identity_tenant:admin&password=111
drop database pub_identity_a;
-- @session

alter publication pub_identity_guard database pub_identity_b;
-- @session:id=1&user=pub_identity_tenant:admin&password=111
drop database pub_identity_a;
execute drop_b;
-- @session

-- DROP ACCOUNT must roll back when another account still publishes its DB.
drop account pub_identity_tenant;
-- @session:id=1&user=pub_identity_tenant:admin&password=111
insert into pub_identity_b.marker values (1);
deallocate prepare drop_b;
-- @session

drop publication pub_identity_guard;
drop account pub_identity_tenant;

-- A prepared plan can name an older incarnation of a database. Its index
-- cleanup must use the new incarnation actually removed at execution.
create database pub_identity_replaced;
prepare drop_replaced from 'drop database pub_identity_replaced';
drop database pub_identity_replaced;
create database pub_identity_replaced;
create table pub_identity_replaced.t (v int, key pub_identity_idx (v));
execute drop_replaced;
select count(*) from mo_catalog.mo_indexes where name = 'pub_identity_idx';
deallocate prepare drop_replaced;
