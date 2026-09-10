# Row-dependent expression defaults

## Scope and trigger

This design covers issue [#28247](https://github.com/matrixorigin/matrixone/issues/28247)
and implementation PR [#28450](https://github.com/matrixorigin/matrixone/pull/28450).
The change is a user-visible SQL capability spanning the planner, value-scan
executor, DDL/catalog metadata, all write paths, and the configured protocol
compatibility boundary. It changes more than 500 production lines and therefore
requires a design-first review.

## Problem and goals

An expression default such as `b INT DEFAULT (a + 1)` needs the value of `a`
from the same row. The existing default machinery treats an omitted default as
a standalone expression: dependent defaults can be expanded repeatedly, a
volatile source such as `RAND()` can be evaluated again, and a VALUES batch can
be evaluated once for every row-local expression. Those behaviors can produce a
wrong row image or unbounded planner/executor work.

The feature must support row-dependent defaults consistently for CREATE/ALTER,
INSERT, multi-row VALUES, INSERT SELECT, REPLACE, UPDATE, ON DUPLICATE KEY
UPDATE, LOAD DATA, CTAS, and LIKE. It must preserve explicit values, including
explicit `NULL`, and must keep the existing constant-default behavior.

Non-goals are generated-column semantics, automatic mixed-version discovery,
reordering user declarations to make an invalid dependency legal, and a new
catalog serialization format.

## Semantic contract

For each table schema, ordinary non-generated, non-auto-increment columns form a
directed dependency graph. An edge `x -> y` means the default for `x` reads the
row value of `y`.

The following definitions are the contract:

1. A self-reference, cycle, reference to a generated or auto-increment column,
   or reference to an invalid column is rejected before catalog publication.
2. A reference to a later column with an expression default is rejected. This
   keeps declaration order and persisted SQL replayable. A later ordinary
   column without an expression default remains a valid source where the
   existing SQL contract permits it.
3. For one input row, an omitted/defaulted column is evaluated after its
   dependencies. Each dependency level is materialized once. A dependent
   default reads the materialized value in that row, so volatile sources have
   one identity per row.
4. An explicit input value, including `NULL`, is the row value and is never
   replaced by its default. `DEFAULT` requests the dependency evaluation for
   that column.
5. A failed row or failed statement releases temporary vectors and executor
   state, and a reused executor starts with no values from the previous row or
   batch.

The first owner of the row image is the write-path projection. Planner binding
owns dependency validation and coordinate mapping; the projection/value-scan
executor owns per-row materialization; the persisted `plan.Default` owns the
replayable expression and target coordinate. No new background worker, queue, or
shared mutable cache is introduced.

## Planner and executor design

CREATE/ALTER binds defaults against the complete target schema, records local
column references, validates the graph, and rejects the protocol version before
any metadata side effect. DML builders construct dependency levels from the
target schema. Independent columns in one level share a projection boundary;
dependent levels consume the preceding row image. This removes recursive
expression-tree expansion and prevents repeated evaluation of volatile source
defaults.

VALUES and multi-insert preserve one row image per tuple. The value-scan path
uses a borrowed one-row window for a row-local expression, copies the result to
the destination batch, and releases the window and result on every return path.
It never evaluates an expression against the whole input batch for each row.
UPDATE, REPLACE, and ODKU use the same dependency ordering while preserving their
existing old-row/new-row semantics. LOAD DATA first normalizes file-field order
to table coordinates, then applies the same levels; already typed defaults are
not recast as file fields.

Generated columns remain computed by their existing target-table path. A default
cannot depend on a generated column, and CTAS does not insert a source value for
a destination generated column.

## Coordinate and SQL reconstruction rules

Persisted local references use target table coordinates. ALTER ADD/CHANGE/MODIFY
and column moves construct a complete old-position to new-position map and apply
it to both default and generated expressions. The map includes the moved column
itself, so deleting an old slot cannot leave a stale self-reference.

CTAS has two independent schemas: SELECT output order and explicit target
declaration order. Inherited source defaults are first mapped from source-table
coordinates to SELECT output coordinates, then the final target order is
constructed and all surviving defaults are rebound against final target types
and names. Persisted SQL is rewritten together with executable references so
aliases remain replayable by SHOW CREATE, LIKE, and metadata restoration.

When a CTAS target-only column has a local-reference default, CTAS omits all
target-only columns from its internal source projection and supplies the source
columns through an explicit target column list. The ordinary INSERT path then
materializes the omitted defaults after the target table exists. This is required
for chains such as `a DEFAULT 1, b DEFAULT (a + 1)` and ensures a volatile
target-only source is evaluated once. An inherited source default whose required
source column is absent remains a DDL error. CTAS conflict modes retain their
existing INSERT IGNORE/REPLACE behavior.

## Persistence and compatibility

The implementation reuses the existing `plan.Default.Expr` and
`plan.Default.OriginString` fields; no catalog format migration is added. A
local-column reference is admitted only when the configured protocol version is
at least `MORPCVersion60`. Constant defaults remain available at older versions.
Version 59 remains reserved for the existing typed numeric FORMAT behavior.

The protocol check occurs during binding, before catalog publication. Operators
must keep the deployment protocol below 60 until every participating CN can
read and execute local-reference defaults. After such metadata is admitted,
running an older binary, downgrading without removing the metadata, or restoring
a backup containing it into an older deployment is unsupported. Rollback of a
failed CREATE/ALTER leaves no partially published table metadata. Mixed-version,
upgrade, downgrade, backup, and restore qualification remains an operational
release test; the planner gate prevents accidental admission but cannot discover
an unconfigured old CN.

## Cost, bounds, and unhappy paths

For `C` columns and dependency depth `D`, planning and materialization are
bounded by the dependency levels, with O(C × D) projection entries in the
worst-case chain and shared work for independent branches. No recursive
deep-copy expansion is used as the dependency boundary. Row-local VALUE_SCAN
work is O(number of rows × local expressions), with one bounded row window at a
time. Temporary vectors are owned by the current projection/executor and are
released on success, error, cancellation, reset, and reuse. The design adds no
unbounded retry, goroutine, channel, or cache.

Invalid dependency, unsupported protocol, type-rebinding, missing CTAS source,
parse/replay, and executor errors fail before or during the owning operation and
follow existing transaction rollback. A partial VALUES/LOAD batch does not
publish temporary default vectors to the next statement. A CTAS population error
is returned through the existing DDL path and leaves the create operation's
normal rollback behavior in force.

## Alternatives and decision

* Recursive substitution with memoized deep copies keeps the old executor shape,
  but repeated references still grow exponentially and volatile roots are
  re-evaluated. It is rejected on correctness and resource bounds.
* A single full-width projection for every default is simple, but repeats work
  for independent columns and retains oversized row-local vectors. It is rejected
  on the batch cost and cleanup contract.
* Rejecting every column reference avoids compatibility work but does not solve
  the requested SQL capability. It is rejected on product behavior.
* The selected staged materialization design keeps the public SQL contract,
  gives one owner the row image, preserves healthy parallel projection of
  independent columns, and reuses existing catalog/protocol mechanisms.

## Acceptance evidence

The implementation is accepted only when the following distinct contracts are
covered:

* planner UTs for valid chains, explicit values/NULL, self/cycle/forward and
  generated/auto-increment rejection, coordinate remapping, protocol 58/59/60,
  and malformed metadata;
* executor UTs for multi-row VALUES, mixed explicit/default rows, one-row
  cleanup/reuse, and volatile source identity;
* public SQL/BVT comparison for INSERT SELECT, UPDATE, REPLACE, ODKU, LOAD
  field reordering, CTAS type/alias/override/target-only defaults, ALTER moves,
  LIKE, and persisted read-back;
* focused race/package/vet checks for the changed planner and executor packages;
* deployment-managed mixed-version and upgrade/downgrade/backup/restore tests
  before enabling protocol 60 in a release.

The current implementation PR records the local UT/race/vet and public fixture
evidence available at review time. The live mixed-version and rollback matrix is
an explicit release qualification item, not silently claimed by planner tests.

## Decision record

The design owner accepts the configured-protocol rollout restriction, the
declaration-order rule for forward expression-default references, the
O(C × D) worst-case dependency cost, and the requirement that CTAS target-only
defaults flow through ordinary INSERT materialization. The implementation must
be re-reviewed if it changes any of those decisions or introduces a new
catalog/wire representation.
