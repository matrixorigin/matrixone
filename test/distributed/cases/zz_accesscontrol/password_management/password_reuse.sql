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
