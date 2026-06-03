-- datalink_pin: freeze the bytes referenced by a datalink into the immutable
-- content-addressed store, so the value stays reproducible by its content hash
-- even if the external object is later changed.

-- 1. pin then read returns the referenced content (datalink / varchar / text)
select load_file(datalink_pin(cast('file://$resources/file_test/normal.txt' as datalink))) as pin_datalink;
select load_file(datalink_pin('file://$resources/file_test/normal.txt')) as pin_varchar;
select load_file(datalink_pin(cast('file://$resources/file_test/normal.txt' as text))) as pin_text;

-- 2. pinning an already-pinned value is idempotent
select load_file(datalink_pin(datalink_pin(cast('file://$resources/file_test/normal.txt' as datalink)))) as pin_idempotent;

-- 3. offset/size: pin freezes the sliced bytes
select load_file(datalink_pin(cast('file://$resources/file_test/normal.txt?offset=0&size=5' as datalink))) as pin_slice;

-- 4. store pinned datalinks in a table and read them back from the CAS
create table pin_t(id int, dl datalink);
insert into pin_t values(1, datalink_pin(cast('file://$resources/file_test/normal.txt' as datalink)));
insert into pin_t values(2, datalink_pin(cast('file://$resources/file_test/normal.txt?offset=0&size=5' as datalink)));
select id, load_file(dl) as content from pin_t order by id;

-- 5. a pinned value is decoupled from its original path: reading by contenthash is
--    served from the CAS even when the original path no longer resolves
select load_file(cast('file:///bogus/nonexistent/path.txt?contenthash=c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a' as datalink)) as cas_decoupled;

-- 6. a contenthash with no stored object errors out (never falls back to the live file)
select load_file(cast('file://$resources/file_test/normal.txt?contenthash=0000000000000000000000000000000000000000000000000000000000000000' as datalink)) as missing_cas;

-- 7. an ill-formed contenthash is rejected
select load_file(cast('file://$resources/file_test/normal.txt?contenthash=notavalidhash' as datalink)) as bad_hash;

-- 8. pinning an invalid url scheme errors out
select datalink_pin(cast('unknownscheme://x/y' as datalink)) as pin_bad_scheme;

-- 9. NULL in -> NULL out
select datalink_pin(cast(null as datalink)) as pin_null;

drop table pin_t;

-- 10. core regression: pin, then change what the reference resolves to, and
--     verify the pinned value still reads the original bytes while the live
--     reference reads the new bytes. The external object is "overwritten" out of
--     band by repointing the stage to a different directory (the file service is
--     write-once, so the same path cannot be overwritten in place).
create stage pin_ow_st URL='file://$resources/into_outfile/pin_ow_a/';
select save_file(cast('stage://pin_ow_st/f.txt' as datalink), 'version-ONE') as ow_setup_v1;
create table pin_ow(id int, dl datalink);
insert into pin_ow values(1, datalink_pin(cast('stage://pin_ow_st/f.txt' as datalink)));
drop stage pin_ow_st;
create stage pin_ow_st URL='file://$resources/into_outfile/pin_ow_b/';
select save_file(cast('stage://pin_ow_st/f.txt' as datalink), 'version-TWO') as ow_setup_v2;
select load_file(cast('stage://pin_ow_st/f.txt' as datalink)) as ow_live_read;
select id, load_file(dl) as ow_pinned_read from pin_ow where id = 1;
drop table pin_ow;
drop stage pin_ow_st;

-- 11. cross-account isolation: a pinned datalink's CAS object is namespaced by
--     account, so a contenthash is not a global bearer token. The sys account
--     pinned normal.txt ('Hello world!', hash c0535e..) in the cases above; a
--     separate account reading the same contenthash is served from its own
--     (empty) namespace and errors out, never reaching the sys account's bytes.
create account pin_acc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=pin_acc:admin:accountadmin&password=123456
select load_file(cast('file:///x.txt?contenthash=c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a' as datalink)) as cross_account_blocked;
-- @session
drop account pin_acc;

-- 12. writes to a pinned (contenthash) datalink are rejected: the pinned value
--     addresses an immutable CAS object whose internal key is not a writable
--     external path, so save_file must error rather than clobber the wrong path.
select save_file(cast('file:///bogus/path.txt?contenthash=c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a' as datalink), 'should-fail') as pin_write_rejected;

-- 13. pinning with an offset past EOF errors out (mirrors load_file), instead of
--     silently pinning zero bytes and minting the empty-content hash.
select datalink_pin(cast('file://$resources/file_test/normal.txt?offset=100' as datalink)) as pin_offset_past_eof;

-- 14. a live path must not use the reserved datalink_cas/ prefix: otherwise
--     file://datalink_cas/<acct>/<hh>/<hash> would be mistaken for a pinned CAS
--     key and read straight from shared storage, bypassing account isolation.
select load_file(cast('file://datalink_cas/0/c0/c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a' as datalink)) as reserved_prefix_rejected;
select datalink_pin(cast('file://datalink_cas/0/c0/c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a' as datalink)) as reserved_prefix_pin_rejected;

-- 15. a duplicated contenthash parameter (mixed case) folds nondeterministically,
--     so it is rejected outright by both the parser and datalink_pin.
select load_file(cast('file:///x.txt?contenthash=c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a&ContentHash=0000000000000000000000000000000000000000000000000000000000000000' as datalink)) as dup_contenthash_rejected;
select datalink_pin(cast('file://$resources/file_test/normal.txt?contenthash=c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a&ContentHash=0000000000000000000000000000000000000000000000000000000000000000' as datalink)) as dup_contenthash_pin_rejected;
