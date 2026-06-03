-- datalink CAS garbage collection: dropping an account reclaims its pinned
-- content-addressed store namespace (datalink_cas/<accountID>/). This verifies
-- that an account can pin content into the per-account CAS, that DROP ACCOUNT
-- succeeds while pinned blobs exist (the per-account prefix cleanup runs
-- best-effort during the drop), and that recreating a same-named account starts
-- from a clean, independent namespace.

-- an account pins content into a table, reads it back from its CAS, then is dropped
create account cas_gc_acc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=cas_gc_acc:admin:accountadmin&password=123456
create database cas_gc_db;
use cas_gc_db;
create table t(id int, dl datalink);
insert into t values(1, datalink_pin(cast('file://$resources/file_test/normal.txt' as datalink)));
insert into t values(2, datalink_pin(cast('file://$resources/file_test/normal.txt?offset=0&size=5' as datalink)));
select id, load_file(dl) as content from t order by id;
-- @session

-- dropping the account with pinned blobs present must succeed
drop account cas_gc_acc;

-- a same-named account recreated afterwards has its own clean CAS namespace and
-- can pin again without interference from the dropped account
create account cas_gc_acc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=3&user=cas_gc_acc:admin:accountadmin&password=123456
select load_file(datalink_pin('file://$resources/file_test/normal.txt')) as repin;
-- @session
drop account cas_gc_acc;
