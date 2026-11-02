# Aggregate-root decimal multiply/subtract fusion

Approved in the 2026-11-01 conversation; owner #30860. This replaces the
unpublished global-expression prototype with an independent aggregate-only PR.

## Contract

Admit a complete aggregate argument x*(literal-y), including commuted multiply:
Decimal64 columns with same-scale, lossless implicit widening to Decimal128;
a bound non-NULL integer/decimal literal with at most one domain-preserving
implicit cast; Decimal128 subtraction and product without scale adjustment.
Literal normalization must fit int64; product scale is 0..12. Preserve bound
precision, including DECIMAL(37,4). Parameters, variables, shared memo nodes,
explicit/fallible casts, grouping sentinels, mismatched physical types/scales or
wider physical precision retain generic evaluation. CASE, tax/Decimal256 and
outer subtraction roots do not recursively specialize their children.

## Ownership and boundaries

Group preparation makes ordinary evaluators, then cheaply checks only each
aggregate argument root. FuseDecimalAggregateArgument may wrap that root.
Generic constructors, ColumnExpressionExecutor.Eval, writable-result handling
and row-alignment helpers are unchanged. The wrapper cannot appear as a child
of a generic function. Group calls arguments with nil masks; any unexpected
mask and ownership-transferring evaluation delegate to the original tree.

The wrapper owns the generic root, borrows its column executors and immutable
literal coefficient, and allocates output only through the root result wrapper.
Reset/free delegate once to that root through existing ExprEvalVector lifecycle.
No cache, pool, asynchronous task, new SQL function or persistent state is added.
Column Eval supplies existing NULL normalization and errors. Allocation failure
propagates; reuse after failure must succeed without leaked account charges.

The full signed int64 difference fits 65 bits and
|x*(c-y)| <= 2^63*(2^64-1) < 2^127. Two exact 64x64 products handle wide
differences without Decimal128 overflow. Flat/NULL/constant kernels skip cast
and subtraction intermediates while retaining the generic bound result type.

## Validation and release gate

Kernel math/big boundary oracle; helper admission, generic nested controls,
NULL/constant/masked fallback, physical-width error, allocation failure/reuse,
reset/free; actual Group SUM consumer and COUNT generic control. Minimal SQL
aggregate, prepared reuse and metadata checks run with normal mo-tester results.
No new race dimension: execution and ownership remain pipeline local.

Measure components then independent current-main/candidate normal release
binaries: unchanged SF10 Q1-Q22, four balanced pairs, five warm calls/query,
cold excluded, profiles separate. Q1/Q14/Q15 are exposure candidates; Q21 is a
mandatory independent regression gate. Same local eight physical CPUs, 16 GiB
CN cache, 32 GiB whole-application cap; no input/result verification in timing.
A confirmed slowdown blocks publication. No gain elsewhere waives it. Retain
old failed branch and evidence; broader SF100/mixed-I/O issue acceptance stays open.
