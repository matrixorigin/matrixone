-- RENAME USER preserves stable identity, grants and existing sessions across name reuse.
-- SHOW GRANTS is unordered: use the tester default full-row multiset comparison.
drop database if exists ru30467_db;
drop user if exists ru30467_a, ru30467_b, ru30467_mid, ru30467_moved;
drop role if exists ru30467_reader;
create database ru30467_db;
create table ru30467_db.marker (v int);
insert into ru30467_db.marker values (42);
create role ru30467_reader;
grant connect on account * to ru30467_reader;
grant select on table ru30467_db.marker to ru30467_reader;
create user ru30467_a identified by 'ru30467_password', ru30467_b identified by 'ru30467_password';
grant ru30467_reader to ru30467_a;
set default role ru30467_reader to ru30467_a;
-- @capture:ru30467_a_id
select user_id from mo_catalog.mo_user where user_name='ru30467_a';
-- @capture:ru30467_b_id
select user_id from mo_catalog.mo_user where user_name='ru30467_b';
select {{ru30467_a_id}} <> {{ru30467_b_id}} as distinct_ids;
set @ru30467_auth = (select authentication_string from mo_catalog.mo_user where user_id={{ru30467_a_id}});
set @ru30467_default = (select default_role from mo_catalog.mo_user where user_id={{ru30467_a_id}});
-- @session:id=11&user=sys:ru30467_a&password=ru30467_password
-- @capture:ru30467_old_conn
select connection_id();
select current_user_id()={{ru30467_a_id}} as source_identity;
select current_role();
select v from ru30467_db.marker;
show grants;
-- @session
-- The first pair writes; the second collides with that destination.
rename user ru30467_a to ru30467_mid, ru30467_b to ru30467_mid;
select count(*)=1 as source_rolled_back from mo_catalog.mo_user where user_name='ru30467_a' and user_id={{ru30467_a_id}};
select count(*)=1 as second_unchanged from mo_catalog.mo_user where user_name='ru30467_b' and user_id={{ru30467_b_id}};
select count(*)=0 as intermediate_absent from mo_catalog.mo_user where user_name='ru30467_mid';
rename user ru30467_a to ru30467_mid, ru30467_mid to ru30467_moved;
select count(*)=1 as identity_credential_default_preserved from mo_catalog.mo_user where user_name='ru30467_moved' and user_id={{ru30467_a_id}} and authentication_string=@ru30467_auth and default_role=@ru30467_default;
select count(*)=1 as grant_preserved from mo_catalog.mo_user_grant g join mo_catalog.mo_role r on g.role_id=r.role_id where g.user_id={{ru30467_a_id}} and r.role_name='ru30467_reader';
select count(*)=0 as old_name_absent from mo_catalog.mo_user where user_name='ru30467_a';
-- @session:id=12&user=sys:ru30467_moved&password=ru30467_password
select connection_id() <> {{ru30467_old_conn}} as fresh_login;
select current_user_id()={{ru30467_a_id}} as moved_identity;
select current_role();
select v from ru30467_db.marker;
-- @session
-- The original JDBC session ID is reused, not reauthenticated.
-- @session:id=11&user=sys:ru30467_a&password=ru30467_password
select connection_id()={{ru30467_old_conn}} and current_user_id()={{ru30467_a_id}} as original_session;
show grants;
show grants for current_user;
show grants for current_user();
-- @session
rename user ru30467_b to ru30467_a;
-- @session:id=13&user=sys:ru30467_a&password=ru30467_password
select connection_id() <> {{ru30467_old_conn}} as fresh_reused_name_login;
select current_user_id()={{ru30467_b_id}} as reused_name_identity;
select current_role();
select v from ru30467_db.marker;
rename user ru30467_moved to ru30467_b;
-- @session
-- @session:id=11&user=sys:ru30467_a&password=ru30467_password
select connection_id()={{ru30467_old_conn}} and current_user_id()={{ru30467_a_id}} as original_session_after_reuse;
select v from ru30467_db.marker;
show grants;
show grants for current_user;
show grants for current_user();
show grants for 'ru30467_a';
show grants for 'ru30467_moved';
show grants for 'ru30467_moved'@'%';
show grants for role ru30467_reader;
-- @session
rename user ru30467_moved to ru30467_a;
rename user ru30467_missing to ru30467_mid;
rename user root to ru30467_mid;
rename user ru30467_moved to root;
rename user ru30467_moved@'remote' to ru30467_mid;
rename user ru30467_moved to 'ru30467?invalid';
select count(*)=1 as source_still_moved from mo_catalog.mo_user where user_name='ru30467_moved' and user_id={{ru30467_a_id}};
select count(*)=1 as reused_binding_still_b from mo_catalog.mo_user where user_name='ru30467_a' and user_id={{ru30467_b_id}};
drop user if exists ru30467_a, ru30467_b, ru30467_mid, ru30467_moved;
drop role if exists ru30467_reader;
drop database if exists ru30467_db;
set @ru30467_auth = null;
set @ru30467_default = null;
