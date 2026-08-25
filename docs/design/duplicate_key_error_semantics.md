# How a duplicate key is reported, and what it does to a transaction

Three changes, one story: what a client is told when a statement violates a
unique constraint, what that does to an open transaction, and what a stored
procedure can do about it.

## 1. SQLSTATE now matches MySQL

MO reported error **1062** with SQLSTATE **HY000**. MySQL pairs 1062 with
**23000** — the integrity-constraint-violation class — and clients route on
that class:

- **JDBC** picks the `SQLException` subclass from the SQLSTATE class, so with
  HY000 a duplicate key arrived as a plain `SQLException`, never as
  `SQLIntegrityConstraintViolationException`.
- Frameworks that classify retryable vs fatal errors by SQLSTATE class saw
  "general error" for every constraint violation.

MO already carried MySQL's own table in `pkg/common/moerr/mysql_error_define.go`,
so the fix is to stop ignoring it. 29 errors used `MySQLDefaultSqlState` where
MySQL defines a specific SQLSTATE; all 29 now use MySQL's, including:

| error | MySQL code | was | now |
|---|---|---|---|
| `ErrDuplicateEntry` | 1062 | HY000 | 23000 |
| `ErrNoSuchTable` | 1146 | HY000 | 42S02 |
| `ErrBadDB` | 1049 | HY000 | 42000 |
| `ErrDivByZero` | 1365 | HY000 | 22012 |
| `ErrOutOfRange` | 1264 | HY000 | 22003 |
| `ErrSyntaxError` / `ErrParseError` | 1064/1149 | HY000 | 42000 |

`TestSqlStateMatchesMySQL` keeps the two tables from drifting again: for every
error carrying a MySQL error number, the SQLSTATE must be the one MySQL pairs
with that number.

**One deliberate exception.** SQLSTATE class `01` is the *warning* class. MySQL
attaches it to a warning, never to an ERR packet, and a client that sees class
01 reads it as "succeeded, with a warning". MO delivers `ErrWarnDataTruncated`
(MySQL 1265, SQLSTATE 01000) as an error, so copying the warning SQLSTATE would
misreport it; HY000 stays. The guard test skips class 01 for this reason.

Nothing inside MO branches on SQLSTATE — it is only forwarded to the wire
(`protocol.go`, `util.go`) — so this changes what clients see and nothing else.

## 2. `mo_rollback_txn_on_duplicate_key`

A duplicate key rolls back **the failing statement**, not the transaction:

```sql
BEGIN;
INSERT INTO t VALUES (20, 'twenty');
INSERT INTO t VALUES (1, 'dup');     -- ERROR 1062 (23000)
INSERT INTO t VALUES (21, 'twentyone');
COMMIT;                              -- 20 and 21 are both committed
```

That is MySQL's behaviour and remains MO's **default**. Some applications treat
a constraint violation as fatal to the unit of work and would rather the
transaction end there, so a session can opt in:

```sql
SET mo_rollback_txn_on_duplicate_key = 1;
```

With it on, the whole transaction is rolled back, including work done *before*
the failure — in the example above, row 20 is gone too.

Scope: session (and global), dynamic, boolean, default `0`.

**Why a session variable and not the static set.** The decision between
statement-level and transaction-level rollback runs through
`TxnHandler.rollback`, and the set of errors that end a transaction lives in
`errCodeRollbackWholeTxn` (`pkg/frontend/util.go`). Every member of that set is
an *infrastructure* failure — deadlock, lock timeout, a backend that went away,
unknown transaction state — after which the transaction genuinely cannot
continue. A duplicate key is a *data* error, and putting it in that set would:

- change the default away from MySQL for everyone, breaking the common
  "try the insert, catch the duplicate, carry on" pattern;
- apply to background and internal executors too, where a duplicate key can be
  benign, letting it destroy an enclosing transaction.

So the static set is left alone and the opt-in is consulted beside it, in
`sessionRollsBackTxnOnError`, scoped to `ErrDuplicateEntry`. An unrelated
statement error still rolls back only the statement, whatever the setting.

## 3. `mo.sql` returns a structured error

Starlark has no exceptions, so MO's `mo` module returns `[result, ok]` with `ok`
`None` on success. `ok` used to be `err.Error()` — a bare string — so a
procedure could only match on message text:

```python
if err != None and "Duplicate entry" in err:   # brittle
```

The error value now carries its codes while still behaving exactly like the
message string:

```python
rs, err = mo.sql("insert into t values (1, 'dup')")
if err != None and err.code == 1062:           # the error CLASS
    ...
```

| expression | value |
|---|---|
| `err.code` | MySQL error number, e.g. `1062` (`0` when the failure is not a moerr) |
| `err.sqlstate` | `"23000"` |
| `err.message` | the message text |
| `str(err)`, `"x: " + err`, `err + " y"` | the message |
| `bool(err)` | `True` |
| `err == None` | `False` on failure, `True` on success |
| `out_param = err` | the message string |

Everything a procedure could already do with the value keeps working — it is
truthy, concatenates with strings, and converts to its message when assigned to
an OUT parameter — so this is additive rather than a breaking change.

### A note for whoever writes the next Starlark BVT

mo-tester strips leading whitespace from each line of a case, and Starlark is
indentation-sensitive, so an indented procedure body reaches the server
dedented and fails to parse with `got identifier, want indent`. The existing
`sp_ins2_sum` golden in `starlark_sql.result` records exactly that failure. The
cases here therefore use conditional *expressions* rather than `if`/`else`
blocks. Fixing mo-tester would be the better answer, but it lives in another
repository.

## Tests

| Level | What it proves |
|---|---|
| `pkg/common/moerr` `TestSqlStateMatchesMySQL` | every error carrying a MySQL error number reports MySQL's SQLSTATE, with the warning class excluded and the reason recorded |
| `pkg/common/moerr` `TestDuplicateEntrySqlState` | 1062 / 23000 specifically |
| `pkg/frontend` `TestSessionRollsBackTxnOnError` | the opt-in is per session, applies only to a duplicate key, and is off by default |
| `pkg/frontend` `TestDuplicateKeyNotInStaticRollbackSet` | a duplicate key is not in the infrastructure set, while a real infrastructure error still ends the transaction |
| BVT `pessimistic_transaction/dup_key_rollback_scope` | end to end: default keeps the transaction (rows before and after the failure commit), opted in discards it including the row inserted before, an unrelated error stays statement-scoped either way, and the setting can be turned back off |
| BVT `procedure/starlark_sql_error` | `err.code` / `err.sqlstate` / `err.message`, `dir(err)`, truthiness, concatenation, `None` on success, and the OUT-parameter form still yielding the message |
