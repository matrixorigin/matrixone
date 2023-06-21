set global enable_privilege_cache = off;
drop account if exists acc1;
drop account if exists accx;
--no error, no account nosys
alter account if exists nosys admin_name 'root' identified by '1234';

--error
alter account nosys admin_name 'root' identified by '1234';

--new account acc1
create account acc1 admin_name "root1" identified by "111";

--error, wrong admin_name "rootx" for acc1
alter account acc1 admin_name "rootx" identified by "111";

--error, wrong admin_name "rootx" for acc1
alter account if exists acc1 admin_name "rootx" identified by "111";

--no error
alter account acc1 admin_name "root1" identified by "1234";

--no error
alter account if exists acc1 admin_name "root1" identified by "1234";

--error, do not support IDENTIFIED BY RANDOM PASSWORD
alter account if exists acc1 admin_name "root1" IDENTIFIED BY RANDOM PASSWORD;

--error, do not support IDENTIFIED WITH 'abc'
alter account if exists acc1 admin_name "root1" IDENTIFIED WITH 'abc';

--error, at most one option at a time
alter account if exists acc1 admin_name "root1" identified by "1234" suspend;

--error, at most one option at a time
alter account if exists acc1 suspend comment "acc1";

--error, at most one option at a time
alter account if exists acc1 admin_name "root1" identified by "1234" comment "acc1";

--error, at least one option at a time
alter account if exists acc1;

--no error
alter account acc1 comment "new accout";

--no error, no account accx
alter account if exists accx comment "new accout";

--new account accx
create account accx admin_name "root1" identified by "111";

--no error
alter account accx comment "new accout";

--no error
alter account accx suspend;

--no error
alter account accx open;

drop account if exists acc1;
drop account if exists accx;
set global enable_privilege_cache = on;