-- Feature limit BVT: parameter validation for upsert functions.
-- This case verifies:
-- 1. mo_feature_registry_upsert behavior with various parameters
-- 2. mo_feature_limit_upsert behavior with various parameters

drop account if exists fl_params_acc;
create account fl_params_acc admin_name = 'admin' identified by '111';
set @acc_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_params_acc');

-- ----------------------------------------------------------------------
-- 1. mo_feature_registry_upsert tests
-- ----------------------------------------------------------------------

-- 1.1 Normal case: new feature registration
-- @ignore:0
select mo_feature_registry_upsert('test_feat_1', 'Test Feature 1', '{"allowed_scope": ["account", "database"]}', true);

-- 1.2 Normal case: update existing feature (change desc, scopes, enabled status)
-- @ignore:0
select mo_feature_registry_upsert('test_feat_1', 'Updated Desc', '{"allowed_scope": ["table"]}', false);

-- 1.3 Edge case: Feature with empty allowed scope
-- @ignore:0
select mo_feature_registry_upsert('test_feat_2', 'Empty Scope', '{"allowed_scope": []}', true);

-- 1.4 Edge case: Feature with long description
-- @ignore:0
select mo_feature_registry_upsert('test_feat_3', 'Very long description string that might exceed some display limits but should be stored correctly in the registry for administrative purposes', '{"allowed_scope": ["account"]}', true);

-- 1.5 Edge case: Case sensitivity in feature code (usually uppercase stored)
-- @ignore:0
select mo_feature_registry_upsert('Test_Feat_4', 'Mixed Case Code', '{"allowed_scope": []}', true);

-- 1.6 Edge case: JSON formatting (extra spaces)
-- @ignore:0
select mo_feature_registry_upsert('test_feat_5', 'JSON Spaces', '  { "allowed_scope" : [ "account" , "table" ] }  ', true);

-- Re-enable test_feat_1 for limit tests
-- @ignore:0
select mo_feature_registry_upsert('test_feat_1', 'Re-enabled', '{"allowed_scope": ["table", "database"]}', true);

-- ----------------------------------------------------------------------
-- 2. mo_feature_limit_upsert tests
-- ----------------------------------------------------------------------

-- 2.1 Normal case: set limit for account
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'table', 10);

-- 2.2 Normal case: update existing limit
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'table', 20);

-- 2.3 Edge case: Unlimited quota (-1)
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'table', -1);

-- 2.4 Edge case: Disabled quota (0)
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'table', 0);

-- 2.5 Edge case: Negative quota other than -1 (Testing behavior)
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'table', -100);

-- 2.6 Edge case: Empty scope string
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', '', 5);

-- 2.7 Edge case: Feature not yet registered (or non-existent)
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'unknown_feature', 'table', 5);

-- 2.8 Edge case: Scope not in allowed_scope list of the feature
-- 'test_feat_1' was updated to only allow 'table' and 'database'
-- @ignore:0
select mo_feature_limit_upsert(@acc_id, 'test_feat_1', 'account', 5);

-- 2.9 Edge case: Using 0 as account_id (sys account)
-- @ignore:0
select mo_feature_limit_upsert(0, 'test_feat_1', 'table', 999);

-- 2.10 Edge case: Negative account_id values
-- @ignore:0
select mo_feature_limit_upsert(-1, 'test_feat_1', 'table', 5);
-- @ignore:0
select mo_feature_limit_upsert(-100, 'test_feat_1', 'table', 5);

-- Cleanup
drop account fl_params_acc;
