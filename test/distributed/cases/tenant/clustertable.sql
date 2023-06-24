set global enable_privilege_cache = off;
drop account if exists tenant_test;
create account tenant_test admin_name = 'root' identified by '111' open comment 'tenant_test';

use mo_catalog;
drop table if exists a;
create cluster table a(a int);
insert into a values(0, 0),(1, 0),(2, 0),(3, 0);
insert into a values(0, 1),(1, 1),(2, 1),(3, 1);
update a set account_id=(select account_id from mo_account where account_name="tenant_test") where account_id=1;
select a from a;

-- @session:id=2&user=tenant_test:root&password=111
use mo_catalog;
select a from a;
-- @session

drop account if exists tenant_test;

select a from a;
drop table if exists a;

drop account if exists tenant_test;
set global enable_privilege_cache = on;