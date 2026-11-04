# Pre-fix DUMP TABLE compatibility fixture

Produced by the actual MatrixOne server using frozen main `12eeeb0205b5e87faef48660ddbcd8cb5e18baea`, before PR #31069 changed SHOW CREATE comment escaping. Both intended production files were pinned to their main contents by Go overlay; the binary's checkout metadata is `e7ceb3c519` and must not be confused with its overlaid production identity. Go1.27.1, linux/amd64, GOAMD64=v3, GOEXPERIMENT=simd, verified CPU native build.

The two-row source has column/table comments containing backslashes and doubled quotes, a backslash default, and a secondary index with a backslash/quote comment:

```sql
CREATE TABLE src (id int primary key, v varchar(32) default 'd\\ef', c int comment 'C:\\dir\\new it''s', key idx_v(v) comment 'idx\\q''s') comment 'x\\ty''z';
INSERT INTO src VALUES (1,'one',10),(2,'two',20);
```

The main and secondary-index relations were flushed before real DUMP TABLE. This is the original complete version1 archive, not a current-version archive with an edited hash. All files (including READY) were copied byte for byte; the manifest records the object sizes and SHA256. It contains two main rows and two index entries, under4KiB in total.

Manifest SHA256: `37d31ea2e400f0014c0a62b7fb84832bf886c601e18ef4000ba4c0d7ad29718d`.

`comment/comment_backslash_roundtrip.sql` loads this fixture through the public SQL frontend into a separately created table and asserts rows and an indexed predicate. The same case also checks current DUMP/LOAD, comment replay and mismatch rejection. Detailed original producer/consumer commands, binary identities and cross-version receipts are retained with the PR acceptance evidence. Keep the archive immutable; changing formatter output must not regenerate this compatibility oracle.
