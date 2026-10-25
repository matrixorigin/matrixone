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

drop user if exists pe_u1;
drop user if exists pe_u2;
drop user if exists pe_u3;
drop user if exists pe_u4;
drop user if exists pe_u5;
drop user if exists pe_u6;
drop user if exists pe_u7;
