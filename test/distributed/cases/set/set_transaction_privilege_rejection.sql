-- Public-path regression coverage for non-admin mixed SESSION/GLOBAL SET.
-- Use an isolated account so the test cannot mutate the sys account defaults.
drop account if exists issue29185_txn_acc;
create account issue29185_txn_acc
    ADMIN_NAME 'issue29185_admin' IDENTIFIED BY '111';

-- @session:id=1&user=issue29185_txn_acc:issue29185_admin&password=111{
create role issue29185_txn_role;
grant connect on account * to issue29185_txn_role;
create user issue29185_txn_user identified by '111'
    default role issue29185_txn_role;
set global transaction_isolation = 'REPEATABLE-READ';
set global transaction_read_only = 0;
select @@global.transaction_isolation,
       @@global.tx_isolation,
       @@global.transaction_read_only,
       @@global.tx_read_only;
-- @session}

-- The connect-only user is allowed to change SESSION state, but must not be
-- able to partially apply a mixed SESSION/GLOBAL assignment.
-- @session:id=2&user=issue29185_txn_acc:issue29185_txn_user:issue29185_txn_role&password=111{
select @@session.transaction_isolation,
       @@session.tx_isolation,
       @@session.transaction_read_only,
       @@session.tx_read_only;
select @@global.transaction_isolation,
       @@global.tx_isolation,
       @@global.transaction_read_only,
       @@global.tx_read_only;

set session transaction_read_only = 1,
    global transaction_isolation = 'READ-COMMITTED';
select @@session.transaction_isolation,
       @@session.tx_isolation,
       @@session.transaction_read_only,
       @@session.tx_read_only;
select @@global.transaction_isolation,
       @@global.tx_isolation,
       @@global.transaction_read_only,
       @@global.tx_read_only;

-- Reverse the assignment order to prove that GLOBAL validation also happens
-- before the earlier SESSION assignment can mutate the connection.
set global transaction_isolation = 'READ-COMMITTED',
    session transaction_read_only = 1;
select @@session.transaction_isolation,
       @@session.tx_isolation,
       @@session.transaction_read_only,
       @@session.tx_read_only;
select @@global.transaction_isolation,
       @@global.tx_isolation,
       @@global.transaction_read_only,
       @@global.tx_read_only;
-- @session}

-- Check the account-global values from the account administrator connection,
-- independently of the rejected user's session.
-- @session:id=1&user=issue29185_txn_acc:issue29185_admin&password=111{
select @@global.transaction_isolation,
       @@global.tx_isolation,
       @@global.transaction_read_only,
       @@global.tx_read_only;
-- @session}

drop account issue29185_txn_acc;
