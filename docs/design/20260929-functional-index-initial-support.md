# Initial functional index implementation

This implementation follows the hidden-generated-column approach described in
`docs/rfcs/20260904_functional_indexes.md`. It is an independent implementation,
not a continuation of draft PR #28274.

## Supported SQL

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(80),
    INDEX name_lower ((lower(name)))
);
CREATE INDEX id_next ON users ((id + 1));
ALTER TABLE users ADD INDEX name_upper ((upper(name)));
DROP INDEX name_upper ON users;
```

The first increment supports named, single-expression, non-unique ordinary
BTREE indexes on persistent ordinary tables. The expression allowlist is
deliberately small: integer `+`, `-`, `*`, `abs`, text `lower`/`upper`, identity
casts and same-signedness integer widening. Generated-column dependencies must
pass the same eligibility check recursively. Result types are integers or
CHAR/VARCHAR. Volatile, temporal/session-dependent, JSON extraction/conversion,
aggregate, parameter and subquery expressions are not enabled by this change.
Unique/composite functional keys, directions, prefixes, INCLUDE columns and
special index algorithms are unsupported.

## Ownership and execution

Each functional index owns one hidden virtual generated column named from the
normalized index name's SHA-256 prefix. Its value is materialized using existing
generated-column DML machinery; ordinary secondary-index maintenance owns the
physical index. Explicit INSERT/UPDATE assignments to the reserved internal
name are rejected, including DEFAULT. SELECT-star and public column metadata
omit the backing column.

Standalone creation, addition and removal use atomic COPY ALTER. Source-column
MODIFY/CHANGE/RENAME on tables with functional indexes also require COPY so a
source type change cannot leave an old inferred key type behind. This costs a
table rebuild and does not provide online/in-place index construction. Original
expression SQL is replayed against the final schema, including reordered
columns; source types and the strict allowlist determine the new binding.

## Query planning

Only typed structural matches of equality-to-literal expressions are eligible.
Matching ignores relation provenance, inferred nullability and optimizer
statistics, while retaining function IDs, constants and value-bearing type
metadata. It adds an indexable hidden-column equality but retains the original
base-row predicate. Functional indexes use equality backfill, never covering
or range access in this increment. Prepared parameters remain valid SQL but
are not guaranteed this optimization.

## Catalog and compatibility

SHOW CREATE emits expression syntax. SHOW INDEX and STATISTICS expose a NULL
column name and the original expression. Checkpoint export reconstructs
functional keys from generated metadata and fails closed when it is missing.
Cluster protocol 101 gates creation. A dedicated 4.0.11 tenant upgrade (minimum
4.0.10, protocol 101) refreshes existing STATISTICS views; old tenant workers
cannot claim this semantic-version task.

## Validation

Planner tests cover DDL forms, rejected expressions, protocol gating, SQL
round-trip and equality index access with a residual. The real-client SQL
integration regression covers writes, rollback, prepared insertion, backfill,
column reorder/type change, SQL mode changes, LIKE cloning, metadata and DROP
cleanup. Checkpoint tests cover expression rendering and missing-metadata
rejection; upgrade tests cover version metadata and error propagation.

Issue #29300 does not contain the exact failing InvenTree index expression.
This change therefore supplies the supported functional-index foundation but
does not assert that all InvenTree migrations or MySQL functional expressions
are now supported.
