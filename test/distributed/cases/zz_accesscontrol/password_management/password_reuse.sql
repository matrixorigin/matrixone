SET GLOBAL password_history = 5;
SET GLOBAL password_reuse_interval = 180;

CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password5';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password5';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';

ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password6';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';

ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password7';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';

ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';

ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';

DROP USER 'test_user'@'localhost';
SET GLOBAL password_history = default;
SET GLOBAL password_reuse_interval = default;
select @@global.password_history;
select @@global.password_reuse_interval;


SET GLOBAL password_history = 3;
select @@global.password_history;
select @@global.password_reuse_interval;
CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
DROP USER 'test_user'@'localhost';
SET GLOBAL password_history = default;
select @@global.password_history;
select @@global.password_reuse_interval;

SET GLOBAL password_reuse_interval = 1;
select @@global.password_history;
select @@global.password_reuse_interval;
CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password2';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password3';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password4';
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
-- @ignore:0
select password_history from mo_catalog.mo_user where user_name  = 'test_user';
select @@global.password_history;
select @@global.password_reuse_interval;
ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'password1';
DROP USER 'test_user'@'localhost';

drop user if exists uarontestuser1;
create user uarontestuser4 identified by 'M@PasswordTestInTheFutur_129';
alter user uarontestuser4 identified by 'M@PasswordTestInTheFutur_130';
alter user uarontestuser4 identified by 'M@PasswordTestInTheFutur_131';
alter user uarontestuser4 identified by 'M@PasswordTestInTheFutur_129';
alter user uarontestuser4 identified by 'M@PasswordTestInTheFutur_130';
alter user uarontestuser4 identified by 'M@PasswordTestInTheFutur_131';
drop user uarontestuser4;

SET GLOBAL password_reuse_interval = default;
select @@global.password_history;
select @@global.password_reuse_interval;
