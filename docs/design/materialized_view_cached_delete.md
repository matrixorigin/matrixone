# Cached DELETE and materialized-view dependencies

## Requirement and reproducer

A source DELETE must remain valid row DML after another session creates an MV
that pins its physical identity. MV creation does not change the source schema
version. A previously prepared `CanTruncate` decision therefore cannot establish
that replacement is still legal.

The native counterexample prepares DELETE on CN1, creates and refreshes an MV on
CN2, then executes the original handle. Before this correction, EXECUTE returns
the dependency rejection and leaves two source rows; a fresh direct DELETE
succeeds with two affected rows. Stored MV rows and publication remain unchanged.
The original same-session test advanced the owner's session DDL generation and
rebuilt its plan, masking this cache boundary.

```sql
-- Session A, after creating facts and inserting two rows:
PREPARE remove_facts FROM 'DELETE FROM facts';
-- Session B, same database on another CN:
CREATE MATERIALIZED VIEW stored REFRESH MANUAL AS SELECT id FROM facts;
REFRESH MATERIALIZED VIEW stored;
-- Session A, after the existing synccommit frontier handoff:
EXECUTE remove_facts; -- two affected rows; stored still has its last two rows
```

## Ownership and change

One pure planner predicate recognizes a DELETE query with an actual DELETE node
whose `DeleteCtx.CanTruncate` is true. The existing frontend cache owners use it
to reject ordinary SQL cache admission, avoid retaining a physical prepared
compile, and rebind the prepared logical plan on EXECUTE. Existing rebuild and
compile cleanup publish the resulting generation. The dependency lookup remains
at the physical eligibility owner; there is no second lookup or dependency cache.

This applies while creation is disabled too: existing MVs still pin identity.
Filtered/row DELETE and ordinary SELECT keep their existing policy. Explicit
TRUNCATE retains its dependency rejection. Once a plan uses row deletion, later
dependency removal does not make that plan unsafe. Existing runtime guards still
protect the physical replacement boundary during concurrent DDL.

Ordinary SELECT rejects the predicate by statement kind in constant time.
Otherwise eligible unrestricted DELETE pays logical rebinding and its existing
bounded indexed lookup per execution. No new worker, state, retry loop, schema
mutation, per-row work or zero-overhead claim is introduced. No error is recast.

## Tests and evidence

| Layer | Oracle |
| --- | --- |
| Frontend policy | Truncating DELETE rebuilds and bypasses both caches; row DELETE, missing context, SELECT and another node retain their policy. All three consumers share the predicate. |
| Native negative/control | Old binary rejects the cross-CN prepared handle while fresh direct DELETE succeeds; two stored rows and the full publication tuple are unchanged. |
| Existing consumer mode | Another session/CN creates and refreshes the MV. EXECUTE returns two affected rows with creation disabled; source is empty, MV rows/publication are intact, unique/secondary maintenance and explicit TRUNCATE protection still hold. |
| Transaction modes | Final consumer and unindexed prepared reproducer pass on two CNs explicitly configured for PESSIMISTIC/RC and OPTIMISTIC/SI. Both services exit0 and all independent cleanup counts are zero in each run. |

The BVT entrypoint passes its existing second CN to `-consumer-check`. Standalone
use without `-second-dsn` opens another session on the primary endpoint. Existing
`synccommit` handoffs establish cross-CN visibility without sleeps or query retries.
The fixture uses the existing deployment, two rows and indexed source; the
additional connection and prepared handle close before independent30-second
fixture cleanup.

Complete planner/frontend suites pass (6,332 root passes); focused cache controls,
owning vet and configured incremental lint pass with zero issues. The cache-fix
product binary passes the complete112-check two-CN entrypoint. Two causal SELECT
handoffs were then added only to the consumer harness; final10/10 consumer checks
and the plain reproducer pass in each explicit transaction mode. Product sources
are unchanged across those runs. This distinguishes final harness proof from
the earlier full-entry capture instead of repeating unchanged functional modes.

[Execution receipts](materialized_view_cached_delete_results.json) retain the
negative, exact terminal results, configurations, cleanup, source fingerprint and
binary identity. The binary is5c25c816 plus working changes with modified=true,
not a pristine publication-head build. Prior crash/upgrade and performance
captures remain historical. New-head CI and reviewer acceptance are separate.
