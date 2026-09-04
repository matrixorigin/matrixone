-- A tenant's ANALYZE result covers only its implicit account-filtered subset.
-- It must never replace the system account's statistics for the shared
-- physical cluster table.
set global enable_privilege_cache = off;
drop account if exists analyze_stats_tenant_a;
drop account if exists analyze_stats_tenant_b;
create account analyze_stats_tenant_a admin_name = 'root' identified by '111';
create account analyze_stats_tenant_b admin_name = 'root' identified by '111';

use mo_catalog;
drop table if exists analyze_stats_domain_guard;
create cluster table analyze_stats_domain_guard(v int);
insert into analyze_stats_domain_guard values
    (100, 0),
    (1, 0),
    (2, 0),
    (3, 0),
    (4, 0);
update analyze_stats_domain_guard
set account_id = (select account_id from mo_account where account_name = 'analyze_stats_tenant_a')
where v in (1, 2);
update analyze_stats_domain_guard
set account_id = (select account_id from mo_account where account_name = 'analyze_stats_tenant_b')
where v in (3, 4);

analyze table analyze_stats_domain_guard(v);
select table_cnt, json_extract(ndv_map, '$.v') as v_ndv
from table_stats('mo_catalog.analyze_stats_domain_guard', 'get', 'normal') g;

-- @session:id=2&user=analyze_stats_tenant_a:root&password=111
use mo_catalog;
analyze table analyze_stats_domain_guard(v);
-- @session
select table_cnt, json_extract(ndv_map, '$.v') as v_ndv
from table_stats('mo_catalog.analyze_stats_domain_guard', 'get', 'normal') g;

-- @session:id=3&user=analyze_stats_tenant_b:root&password=111
use mo_catalog;
analyze table analyze_stats_domain_guard(v);
-- @session
select table_cnt, json_extract(ndv_map, '$.v') as v_ndv
from table_stats('mo_catalog.analyze_stats_domain_guard', 'get', 'normal') g;

drop table analyze_stats_domain_guard;
drop account analyze_stats_tenant_a;
drop account analyze_stats_tenant_b;
set global enable_privilege_cache = on;
