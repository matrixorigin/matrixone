-- Feature limit BVT: privilege check for sys-only functions.
-- This case verifies:
-- 1) sys account can execute mo_feature_registry_upsert/mo_feature_limit_upsert
-- 2) non-sys accounts are rejected with "only support sys account"

drop account if exists fl_priv_acc;
create account fl_priv_acc admin_name = 'admin' identified by '111';

set @fl_priv_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_priv_acc');

-- sys should be able to execute both functions.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);
-- @ignore:0
select mo_feature_limit_upsert(@fl_priv_id, 'snapshot', 'table', 1);

-- @session:id=1&user=fl_priv_acc:admin&password=111
-- non-sys should NOT be able to execute these sys-only functions.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);
-- @ignore:0
select mo_feature_limit_upsert(0, 'snapshot', 'table', 1);

-- @session
drop account fl_priv_acc;

