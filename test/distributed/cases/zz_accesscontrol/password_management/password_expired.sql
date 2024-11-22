SET GLOBAL default_password_lifetime = default;
select @@default_password_lifetime;
select @@global.default_password_lifetime;


drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '123456';
-- @session:id=1&user=sys:user1&password=123456
select @@global.default_password_lifetime;
-- @session

SET GLOBAL default_password_lifetime = 1;
select @@global.default_password_lifetime;
alter user 'user1' identified by '12345678';
-- @session:id=2&user=sys:user1&password=12345678
select @@global.default_password_lifetime;
-- @session

drop user if exists user1;

SET GLOBAL default_password_lifetime = default;
