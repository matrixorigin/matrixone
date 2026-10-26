# Query execution and target identity

Revision 1, approved 2026-10-26 by `gpt-6.1-sol` / `xhigh` before implementation.
This extends the existing SDK quality contracts for legacy execution, async
export, database routing, recursive CTEs and query composition.

A retained public entry point must dispatch through its supplied execution
owner. Legacy `Query` retains its constructor, insertion methods, materialized
SELECT results and eager deletion. Its explicit `_table_name` convention stays
supported alongside standard SQLAlchemy models. Mapped schema/name determines
physical identity, and returned physical columns map back to mapper attribute
keys in result order. It delegates execution to the
client without acquiring a connection or interpreting database errors.

Modern SELECT, UPDATE and DELETE share one pure target renderer. Database and
alias selection must agree with predicates, SET expressions and ordering;
compilation must not temporarily mutate `_table_name` or issue `USE`. Existing
expression refresh also covers SET expressions, including `update().alias()`.
An explicit database conflicting with a qualified table fails before dispatch;
a matching prefix is included once. Statement-local CTE names remain local.
Missing target errors never fall back to a default-database table. Joined
mutations fail before dispatch because this API does not implement join-DML;
dropping the JOIN would broaden the write.

SDK buffered results expose column names as data; SQLAlchemy CursorResult
exposes `columns()` as a projection method and `keys()` as metadata. String-table
materialization uses the correct metadata interface, fetches rows once and
retains the complete-column-width check. Empty, explicit-projection and mapped
model branches retain their contracts.

Generated `_build_sql()` output has already crossed its input validation
boundary. Export and `logical_in` consume its remaining binds with the shared
lexical substituter, preserving generated dollar strings and literal question
marks. Modern raw predicate fragments retain their backslash ambiguity guard.
Explicit whole-SQL and raw CTE inputs retain their existing low-level contract.

Each CTE descriptor carries its recursive intent. Any recursive descriptor
selects one `WITH RECURSIVE` prefix for the list; absent flags mean ordinary WITH.
Descriptor and parameter ordering remain unchanged. Recursion and execution
budgets belong to the server and existing execution owner.

`AsyncClient.export` lazily owns the existing AsyncExportManager, which delegates
to that same client's execute path. No new pool, task, retry, transaction or
cancellation owner is introduced. Query and driver errors propagate normally.

Acceptance covers direct legacy read/count/insert/update/delete, both alias call
orders, actual changed rows, async client/session database isolation and missing
target failures, recursive results with an ordinary CTE control, generated
fulltext composition, async CSV/JSONL contents and reuse after export failure.
Native checks run against release 4.2.5 and the identified local source binary.
The runner records content hashes, checks execution-time source drift and stores
observed server identity alongside dependency versions and nonempty JUnit
results. Portable reports identify the tested binary independently of SDK HEAD.

Prior snapshot restrictions, native file limitations and cancellation/cleanup
contracts remain as documented in the preceding design documents. This change
adds no join-DML feature and makes no throughput claim.

The driver-preserving dialect adapter retains native Decimal binding. Legacy
SQL literal rendering must additionally use fixed-point Decimal tokens: a
scientific token can change server comparison semantics. Decimal.is_finite
classifies values without a float conversion. Before formatting, tuple digits
and exponent predict the expanded length; representations exceeding 4096
characters (including sign/decimal point) raise ValueError. This is an output
expansion budget, not a claim about server precision. Positive-exponent zero
representations may be conservatively rejected by that same budget. Float
rendering and SQLAlchemy type processors retain their existing contracts.
Native large and very small adjacent-decimal controls assert exact read and
mutation scope on both release and source, through sync and async paths.
