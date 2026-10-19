# MV catalog4.0.18 integration

Tracker [#30079](https://github.com/matrixorigin/matrixone/issues/30079),
PR [#30085](https://github.com/matrixorigin/matrixone/pull/30085).
Parent4e537923 merges main `def799e1260dd547cae2e0e54323ea20ec1b7685`.

## Requirement and design

Main allocated4.0.17 to the INFORMATION_SCHEMA.COLUMNS MEDIUMINT tenant repair.
The feature previously used that same version for MV cluster catalogs. Preserve
main's complete4.0.17 handler and tenant entries byte for byte; do not repurpose
an upstream migration. Move the feature handler/test to the next version4.0.18,
with minimum4.0.17 and offset5 (four global catalogs plus legacy policy fencing).
Keep protocol120, definition format2, executor17 and all MV runtime owners.

Register4.0.17 followed by4.0.18. From4.0.16, queue both the tenant repair and MV
catalog hops. A late tenant repair still updates COLUMNS before advancing its
persisted account version; a failed repair is retryable and does not mark the
tenant complete. From a ready main4.0.17/offset1 deployment, queue only4.0.18.
Retain the MV handler's explicit common-protocol check before any catalog DDL,
transactional IF NOT EXISTS catalogs and format1 periodic-policy pause/fence.
The moved handler adds no lock, retry, task, callback or persistent format.

Update the native recovery driver's current catalog, old control defaults and
interrupted-upgrade stack symbol. Its startup fault remains test-tag-only. The
normal production binary has no startup environment fault initializer. Earlier
experimental migration allocations remain historical captures; they are not
relabeled as proof for this new target.

## Validation and reuse map

- Extend the existing route matrix with main4.0.17/offset1 and require the exact
  retained intermediate hop for older inputs. The existing late-tenant fixture
  now verifies both4.0.17 and4.0.18 account-version transitions, preserves failed
  repair/retry/idempotence assertions and checks the final4.0.18 metadata.
- Move the existing MV handler matrix intact. Assert4.0.18/min4.0.17/offset5,
  old119/current120 inventories, no DDL on old CN or inventory failure, bounded
  DDL failure and the complete five-statement successful migration.
- Run full bootstrap, shared versions, historical4.0.14 handler, new4.0.18,
  sysview, planner/compiler/frontend suites plus incremental static checks.
- Build a clean main def799 old service and normal/tagged new services. Use the
  real old4.0.17/119 cluster as the control, never fabricated catalog/version rows.
  Existing destructive driver covers mixed-node exclusion, staged-DDL SIGKILL,
  recovery/repeated upgrade, real old-binary downgrade refusal, commit-response
  loss, both refresh commit crash windows, task replay and independent cleanup.
- Reuse the final two-CN consumer fixture/client, including comparison DDL before
  PREPARE and branch work on the other session. Run17 cases each in explicit RC
  and SI against the normal4.0.18 binary and independently assert ready4.0.18/5.
  The old native branch-reader counterexample remains valid for its unchanged
  endpoint owner, with its exact prior binary/source scope in the branch ledger.

[Result ledger](materialized_view_catalog418_results.json) records actual exits,
source/binary identities and terminal outcomes. This compatibility closure is R3
because the persisted migration allocation changes; the migration algorithm,
service synchronization and publication owners are unchanged. Prior owner race
proof remains scoped to those identical implementations. No new benchmark claim
is made. New-head remote CI is a separate required gate.
