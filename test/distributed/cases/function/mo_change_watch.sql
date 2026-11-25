-- A successful assertion belongs to the caller's explicit read transaction.
select mo_assert_snapshot_after('1-0');
prepare watch_check from 'select mo_assert_snapshot_after(?) as verified';
set @watch_old = '1-0';
start transaction read only;
execute watch_check using @watch_old;
commit;

-- The statement's own snapshot gives a deterministic equality counterexample.
start transaction read only;
select mo_assert_snapshot_after(watermark) from change_watermark() w;
rollback;

start transaction read only;
select mo_assert_snapshot_after('1-01');
rollback;
deallocate prepare watch_check;

-- The new source function matches the public, alias-free syntax.
select * from mo_changed_tables(NULL, NULL);

-- The deployment must explicitly provision and enable the change watch.
select * from mo_changed_tables(NULL, 'watch_bvt_observer');
