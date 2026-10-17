# User-variable assignment expressions

Owner issue: #29909. Implementation PR: #29966. This document is the
versioned design record for the `@name := expr` expression implemented by the
frontend and execution engine.

Version: v1.2 (2026-10-15)

Status: Accepted for implementation

Decision: PASS

Decision scope: the semantics and safety boundaries in this document are the
implementation contract for PR #29966. A later design revision is required if
the feature is extended to DML, aggregate/window side effects, remote
execution, or system-variable writes.

## Goals and non-goals

The feature makes a user-variable assignment an expression in a supported
`SELECT` projection, so that a query can both return the expression result and
update a MySQL user variable. It supports prepared execution and nested
assignments while preserving the existing user-variable type/domain rules.

The feature does not add assignments to `WHERE`, `JOIN` conditions, grouping,
ordering, limits, DML, views, generated columns, stored programs, or system
variables. The binder rejects a plan that would allow an assignment to escape
the coordinator-only projection scope. An assignment is a user-variable side
effect; it is never a write to catalog or transactional table data.

The public `BuildPlan` entry point also performs a statement-level DML fence
before table resolution or fallback planning. If an assignment AST occurs
anywhere in INSERT (including VALUES and INSERT SELECT), multi-table INSERT,
REPLACE, UPDATE, DELETE, MERGE, or LOAD, planning fails immediately. This
prevents a direct value binder such as INSERT's `buildValueScan` from bypassing
the SELECT projection validator and guarantees that no DML plan can mutate data
or publish a variable before the scope error is returned.

CREATE VIEW, CREATE OR REPLACE VIEW, and ALTER VIEW reject an assignment in
their defining SELECT before view inference or catalog mutation. This keeps an
existing usable view intact when a rejected replacement is attempted and avoids
persisting a definition that ordinary non-root view expansion must reject.

The same boundary applies to persisted table expressions. CREATE TABLE and
ALTER TABLE reject an assignment anywhere in a DEFAULT, CHECK, generated-column,
or other catalog-owned expression before table binding or catalog mutation.
Assignments are not evaluated later by DML as an implicit session side effect.

## SQL and binding contract

The parser represents `@name := expr` as `UserVarAssignmentExpr`. Assignment
has low precedence and is right associative, so:

```sql
@a := @b := value
```

is bound as `@a := (@b := value)`. Names are canonicalized case-insensitively
for the statement-local overlay and for publication. The right-hand expression
is bound with the same type, prepared-parameter, collation, and runtime-string
domain rules as an ordinary user-variable read.

The plan contains a versioned `UserVarAssignment` expression node. Plan
construction preserves nested children, and plan cloning/structural identity
includes the assignment name and child expression. The immutable plan contains
no session values and is safe to reuse for prepared statements; values are
resolved from the execution `Process` at `EXECUTE` time.

The planner rejects a filtered projection, unsafe filter/order/limit scopes,
and ambiguous multiple assignment projections. These restrictions are
intentional: they avoid making side-effect order depend on optimizer rewrites.
The supported form is a deterministic left-to-right projection list with at
most one side-effecting assignment at a given projection level; nested
assignments use the explicit right-associative expression tree.

An ordinary projection sibling is evaluated over the complete physical batch.
Consequently, a read of variable `x` in an earlier sibling than the projection
that assigns `x` is rejected. Such a read can otherwise observe the prior
batch's complete row history and make the result depend on physical batching.
A read after the assignment projection remains supported because the current
batch has a complete staged row vector by then.
The assignment write set is the complete expression subtree: every nested or
conditional assignment target is checked against every earlier user-variable
read, with names canonicalized case-insensitively.

HAVING is also a pre-projection consumer. If it reads a variable in the
assignment write set, the statement is rejected before binding; otherwise its
filter would observe a previous physical batch and change the returned rows.
Reads of unrelated user variables remain allowed.

## Logical-row and batch semantics

The logical row, not the physical vector batch, is the semantic unit.

* The coordinator evaluates an assignment one logical row at a time.
* A child expression receives a one-row cloned batch. This prevents evaluating
  a volatile or sequence-producing RHS once per physical batch and then
  replaying it for every row.
* Physical batch boundaries are invisible. At the beginning of each top-level
  assignment invocation, row histories for every variable are resized/reset to
  the current batch while scalar values are retained. Thus a statement split
  into batches has the same result as one batch.
* A nested assignment inherits the parent logical row. It writes into the
  statement-sized row history, rather than using the one-row scratch batch
  index.
* A read at the current logical row sees the row-local value already staged by
  an earlier nested/preceding assignment. A read outside a row-wise assignment
  sees the complete row vector only when every row has a valid snapshot; it
  otherwise falls back to the scalar session value.
* A skipped conditional branch records a snapshot of the value visible before
  the branch for that row, but it does not mark the skipped assignment as a
  write. Consequently skipped branches do not publish a new user variable,
  while later projections still see a complete row history.

Conditional-expression snapshot traversal is enabled only for expression trees
that actually contain an assignment node. Ordinary IF/CASE/COALESCE evaluation
does not allocate assignment bookkeeping or walk the executor tree, preserving
the baseline fast path for queries that do not use this feature.

The observable result of a nested self-reference is therefore stable across
batch sizes. For example, with `@b = 0`, `SELECT @a := @b := @b + 1 FROM t`
publishes and returns 1, 2, ... in logical-row order, regardless of how `t` is
chunked.

## Overlay ownership and publication

Each execution `Process` owns a statement-local overlay with:

* a canonical-name to scalar `UserVariableAssignment` map;
* per-variable row values and validity bits for the current batch; and
* a current logical-row selector.

The overlay is not shared with another session or stored in a plan. The
frontend installs a statement-boundary publisher before engine execution and
removes it in a `defer`. `CommitUserVariableAssignments` takes a locked
snapshot, clears all overlay maps/row buffers/current-row state, sorts names for
deterministic diagnostics, and then invokes the session setter once per final
variable value. A reused `Process` therefore cannot retain a prior statement's
row history.

Only actual `StageUserVariableAssignment` calls enter the scalar map. A
snapshot made for a skipped branch is row history only and is never published.
System-variable resolution is deliberately separate: a user-variable overlay
cannot shadow reads such as `@@autocommit`.

## Partial errors, cancellation, panic, and reset

Assignment values are session state, not transaction data. Execution stages the
successful logical-row prefix. If a later row, nested expression, or input
clone fails, the frontend still performs the statement-boundary commit in its
deferred cleanup. The successfully evaluated prefix is published once; no
uncomputed suffix is invented. This is the explicit error-prefix contract and
is covered by executor and frontend regression tests.

The commit operation clears the overlay before calling the session setter. A
setter failure therefore cannot leak the overlay into a retry or a later
statement; it can only leave the already-published setter prefix visible. The
original execution error is retained when both execution and setter fail.

Cancellation is checked before each logical row. Cancellation, context failure,
or a panic follows the same deferred cleanup path: no row history is left
attached to the `Process`, and a subsequent statement starts with a clean
overlay. Each one-row clone is released by a per-row terminal owner, so an
RHS panic during stack unwinding returns its memory to the execution pool
without double-cleaning success or error paths. `DiscardUserVariableAssignments`
is available to explicit abort/reset paths and is idempotent. `Reset`/`Free`
of an executor releases retained result vectors through the process memory
pool.

## Prepared statements and retries

Prepared plans contain only the assignment expression tree and type metadata.
The current user-variable values, binary-string flag, and runtime string domain
are read from the session at bind/execute time as required by existing
prepared-statement rules. Re-executing the same prepared plan uses a fresh
process overlay and cannot reuse row values from a prior execution.

The compiler marks a plan containing an assignment as non-retryable. Retrying a
partially executed physical pipeline could repeat a volatile RHS or publish a
side-effecting prefix twice. A retryable error is consequently surfaced to the
client instead of silently replaying the assignment. The plan cache may retain
the immutable plan shape, but never the overlay or its published values.

## Coordinator, remote, and mixed-version boundaries

User-variable assignment is coordinator-only. Placement forces the projection
to the frontend/coordinator, and the remote send/receive validator rejects any
pipeline containing the assignment expression. This second fence is deliberate
defence against a future optimizer rewrite or an older receiver silently
dropping the protobuf oneof field.

The expression is serialized in the plan proto with its own field. A mixed
version deployment must fail closed: an older CN must not receive or execute a
plan containing this field. There is no fallback that evaluates the side effect
remotely, and a remote retry is not allowed. The feature is therefore enabled
only after the coordinator and all possible plan receivers understand the
versioned plan field; otherwise the query is rejected with a not-supported
error at the coordinator/remote validation boundary.

## Resource bounds and ownership

Row history is bounded by the current input batch row count. It is reset for
all variables at every top-level batch, while the scalar map contains only
variables actually assigned by the statement. One-row child batches and
temporary vectors are allocated from the execution memory pool and cleaned on
both success and error. No pointer to a child batch, vector, or process-local
overlay is stored in the plan or session after publication.

## Validation map

The public contract is covered at the following layers:

| Contract | Evidence |
|---|---|
| low precedence/right associativity and parser acceptance | `pkg/sql/parsers/dialect/mysql/user_var_assignment_test.go` |
| plan shape, nesting, scope rejection, prepared identity | `pkg/sql/plan/user_var_assignment_test.go` |
| coordinator placement, remote fence, and retry suppression | `pkg/sql/compile/user_var_assignment_test.go` and `remoterun_test.go` |
| row-wise evaluation, nested assignment, skipped rows, cancellation, cleanup | `pkg/sql/colexec/user_var_assignment_test.go` |
| overlay publication/discard and batch reset | `pkg/vm/process/user_var_assignment_test.go` |
| real frontend execution and prepared execution | `pkg/frontend/mysql_cmd_executor_test.go`, `computation_wrapper_test.go`, and the user-variable integration cases |
| plan serialization | generated `pkg/pb/plan/plan.pb.go` from `proto/plan.proto` |
| DML scope admission and no-side-effect rejection | `pkg/sql/plan/user_var_assignment_test.go` (`TestUserVariableAssignmentRejectsDMLBeforeBinding`) |
| read-before-assignment and view-authoring rejection | `pkg/sql/plan/user_var_assignment_test.go` and `pkg/tests/sqlintegration/user_var_assignment_test.go` |
| persisted DEFAULT/CHECK/generated-expression rejection and HAVING dependency fence | `pkg/sql/plan/user_var_assignment_test.go` |
| RHS panic clone cleanup | `pkg/sql/colexec/user_var_assignment_test.go` (`TestUserVariableAssignmentExecutorReleasesClonesOnChildPanic`) |

The regression set includes empty input, multiple physical batches, nested
self-reference, skipped conditional rows, prepared reuse, cancellation, and
remote-boundary rejection. Any future change to DML scope, optimizer ordering,
remote execution, or system-variable writes must extend this document and add
a new versioned design decision before code changes are approved.

## Revision history

* **v1, 2026-10-13** — initial implementation contract for #29909/#29966;
  includes nested row-history reset, statement-boundary publication,
  coordinator-only placement, no-retry policy, and mixed-version fail-closed
  behavior.
* **v1.1, 2026-10-14** — closes the read-before-assignment, view-authoring,
  and RHS-panic cleanup boundaries found in review.
* **v1.2, 2026-10-15** — closes persisted table-expression and HAVING
  admission gaps, and gates conditional bookkeeping on actual assignment use.
