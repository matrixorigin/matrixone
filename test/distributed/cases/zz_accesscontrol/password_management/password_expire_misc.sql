-- password expire / account lock options of create user and alter user
-- are applied to mo_user instead of being silently dropped (issue #30698).

-- create user with password expire variants
drop user if exists pe_u1;
create user pe_u1 identified by 'Start123a' password expire;
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u1';
select expired_time <= current_timestamp() from mo_catalog.mo_user where user_name = 'pe_u1';
select status from mo_catalog.mo_user where user_name = 'pe_u1';

drop user if exists pe_u2;
create user pe_u2 identified by 'Start123a' password expire interval 3 day;
select expired_time > current_timestamp() from mo_catalog.mo_user where user_name = 'pe_u2';

drop user if exists pe_u3;
create user pe_u3 identified by 'Start123a' password expire never;
select expired_time > current_timestamp() + interval 100 year from mo_catalog.mo_user where user_name = 'pe_u3';

drop user if exists pe_u4;
create user pe_u4 identified by 'Start123a' password expire default;
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u4';

-- alter user with password expire variants
drop user if exists pe_u5;
create user pe_u5 identified by 'Start123a';
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u5';
alter user pe_u5 password expire;
select expired_time <= current_timestamp() from mo_catalog.mo_user where user_name = 'pe_u5';
alter user pe_u5 password expire interval 2 day;
select expired_time > current_timestamp() from mo_catalog.mo_user where user_name = 'pe_u5';
alter user pe_u5 password expire never;
select expired_time > current_timestamp() + interval 100 year from mo_catalog.mo_user where user_name = 'pe_u5';
alter user pe_u5 password expire default;
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u5';

-- identified with password expire combination is rejected with a clear error
alter user pe_u5 identified by 'Other999b' password expire;
-- Empty/random credentials must not be silently discarded while changing
-- expiration or locking the account. The rejected statement is atomic.
alter user pe_u5 password expire;
set @pe_saved_auth = (select authentication_string from mo_catalog.mo_user where user_name = 'pe_u5');
set @pe_saved_expiry = (select expired_time from mo_catalog.mo_user where user_name = 'pe_u5');
alter user pe_u5 identified by '' password expire;
alter user pe_u5 identified by random password password expire;
alter user pe_u5 identified by '' password expire default;
alter user pe_u5 identified by random password password expire default;
alter user pe_u5 identified by '' account lock;
alter user pe_u5 identified by random password account lock;
select authentication_string = @pe_saved_auth as password_unchanged, expired_time = @pe_saved_expiry as expiration_unchanged, status = 'unlock' as unlocked from mo_catalog.mo_user where user_name = 'pe_u5';
alter user pe_u5 password expire default;
-- unsupported misc options keep the historical error
alter user pe_u5 password history default;

-- a password change clears the explicit expiration but keeps EXPIRE NEVER
alter user pe_u1 password expire;
alter user pe_u1 identified by 'Fresh456b';
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u1';
alter user pe_u3 identified by 'Fresh456b';
select expired_time > current_timestamp() + interval 100 year from mo_catalog.mo_user where user_name = 'pe_u3';

-- account lock / unlock on create user and alter user
drop user if exists pe_u6;
create user pe_u6 identified by 'Start123a' account lock;
select status from mo_catalog.mo_user where user_name = 'pe_u6';
alter user pe_u6 account unlock;
select status from mo_catalog.mo_user where user_name = 'pe_u6';
alter user pe_u6 account lock;
select status from mo_catalog.mo_user where user_name = 'pe_u6';
alter user pe_u6 unlock;
select status from mo_catalog.mo_user where user_name = 'pe_u6';

-- an administrator can rescue an expired account by resetting its password;
-- the explicit expiration is cleared by the password change.
drop user if exists pe_u7;
create user pe_u7 identified by 'Start123a' password expire;
select expired_time <= current_timestamp() from mo_catalog.mo_user where user_name = 'pe_u7';
alter user pe_u7 identified by 'Second234b';
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u7';
alter user pe_u7 password expire default;
select expired_time is null from mo_catalog.mo_user where user_name = 'pe_u7';

-- Legacy UTC digit convention: a -08:00 write made one hour before a
-- hypothetical cutover at 13:00 UTC stores 20:00 UTC, after the cutover.
-- At 13:00 UTC the next day it is 25 hours old, not 17 hours old.
set @pe_time_zone = @@session.time_zone;
drop database if exists pe_lifetime_clock;
create database pe_lifetime_clock;
create table pe_lifetime_clock.pwd_clock (id int, password_last_changed timestamp);
set time_zone = '-08:00';
insert into pe_lifetime_clock.pwd_clock values (1, '2026-01-01 12:00:00'), (2, '2026-01-01 13:00:00'), (3, '2026-01-01 17:00:00'), (4, null);
set time_zone = '+00:00';
select cast(password_last_changed as datetime) as stored_utc from pe_lifetime_clock.pwd_clock where id = 1;
set time_zone = '-08:00';
select id, password_last_changed is not null and cast(password_last_changed as datetime) < cast('2026-01-02 13:00:00' as datetime) - interval 1 day as expired from pe_lifetime_clock.pwd_clock order by id;
set time_zone = @pe_time_zone;
drop database pe_lifetime_clock;

-- Exercise the real pre-upgrade login boundary with the default policy on.
-- No convention table or migration may be required to reach authentication.
set @pe_password_lifetime = @@global.default_password_lifetime;
set global default_password_lifetime = 1;
-- @session:id=21&user=sys:pe_u4&password=Start123a
select 1;
set password = 'Fresh456b';
select 1;
-- @session
set global default_password_lifetime = @pe_password_lifetime;

drop user if exists pe_u1;
drop user if exists pe_u2;
drop user if exists pe_u3;
drop user if exists pe_u4;
drop user if exists pe_u5;
drop user if exists pe_u6;
drop user if exists pe_u7;
