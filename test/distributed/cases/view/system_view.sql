--test mo_sessions

-- @session:id=1{
begin;
-- @session}

select count(*) > 0 from mo_sessions() t;
-- Wait until the async session's open transaction is visible. The transaction
-- remains open on session 1 until the explicit commit below.
-- @wait_expect(1, 10)
select count(*) > 0 from mo_sessions() as t where txn_id != '';
select count(*) > 0  from mo_transactions() t join mo_sessions() s on t.txn_id = s.txn_id;

-- @session:id=1{
commit;
-- @session}


-- test mo_cache

select count(*) > 0 from mo_cache() c;

-- test mo_configurations

select count(*) >0 from mo_configurations() t;
select count(*) >0 from mo_configurations() t where node_type = 'cn';

select distinct node_type,default_value  from mo_configurations() t where  name like '%frontend.port';
select count(*) > 0  from mo_configurations() t where internal = 'advanced';
