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
