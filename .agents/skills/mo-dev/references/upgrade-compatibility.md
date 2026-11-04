# Upgrade compatibility contract

MatrixOne supports **shutdown upgrade only**: stop every service in the cluster,
replace the binaries, start the cluster. Rolling upgrade, where old and new
CN/TN/LogService/proxy binaries serve one cluster at the same time, is not
supported. Downgrade (starting an older binary on data written by a newer one)
is not supported.

Apply this contract when designing, implementing, or reviewing any change that
touches RPC messages, plan/pipeline encoding, persisted state, or upgrade.

## Catalog upgrades follow release boundaries

The supported upgrade boundary is a **released deployment to the target
release**, not one Git commit to the next. Product release numbers and internal
catalog versions are separate identifiers; resolve the catalog identity from
the actual supported source release.

- Maintain one pending catalog target for the upcoming release. Before that
  target is released, consolidate necessary migrations under its existing
  owners. Do not create a new catalog version, intermediate route, compatibility
  branch or compatibility test solely to upgrade from an earlier unreleased main commit.
- Freeze the catalog version, offset, entry ordering and migration effects when
  released. Do not let a published migration change indirectly through mutable
  shared SQL definitions. Subsequent changes belong to the next release's
  target; retain published steps needed by supported upgrade paths.
- Unreleased main catalogs are not supported upgrade sources. Development
  environments may need to be rebuilt when their catalog layout changes. Do
  not infer a supported path merely because two commits have different offsets,
  or assume identical version/offset markers identify identical unreleased
  layouts.
- For a new upgrade version or compatibility branch, name the supported released
  source and the persisted input that requires it. Compatibility only with an
  unpublished commit is not sufficient justification. Any exception requires an
  explicit supported deployment and upgrade contract.

Validate fresh installation and upgrades from supported releases, using pinned
release tags and resolved commits as source fixtures. Use the latest supported
release as the default baseline; retain older-source cases where their supported
paths exercise distinct contracts. Do not use the previous commit or a moving
main branch as the release compatibility baseline. Synthetic versions in unit
tests may exercise framework behavior; they do not establish supported
deployment sources.

This boundary does not weaken recovery within a supported upgrade: still verify
transaction rollback, retry, committed partial progress, restart and completion
without duplicate effects. Reuse the existing upgrade owners and recovery paths
rather than adding a separate state machine for development history.

## In-cluster RPC: no mixed-version obligations

Peers inside one cluster always run the same binary. Therefore:

- Do not add a per-feature `MORPCVersion` constant, bump `MORPCLatestVersion`,
  or gate a feature on `AllCNsSupportProtocol`, a heartbeat-reported protocol
  version, or any other "all peers support X" capability check.
- Do not add legacy encode/decode paths, neutral fallbacks, or downgraded plans
  for an older peer.
- A new or changed proto field, plan node, operator, table function, function
  overload, query-service method, or `mo_ctl` command needs no version gate.
- Reviewers must not request these mechanisms or block a change for lacking
  them. "An old CN would not understand this" is not a finding.

Existing gates stay until a dedicated cleanup PR removes them. Do not extend
them for new features, and do not remove unrelated gates inside a feature PR.

## When to bump MORPCVersion

`MORPCVersion` is a protocol generation, like the NFS major version. It changes
rarely, only when:

1. the RPC framing, handshake, or message envelope changes so that two different
   generations would misdecode each other's bytes; the bump exists so that a
   misconfigured mixed deployment fails at connect time instead of corrupting
   data; or
2. the project deliberately reintroduces mixed-version operation as a supported
   mode, which is a design-level decision recorded in a design document.

Adding features, fields, operators, or functions is never by itself a reason
to bump.

## What still must stay compatible

Shutdown upgrade still means a new binary starts on data an older binary
wrote. These remain hard compatibility contracts and valid review findings:

- **Persisted state:** catalog rows and system tables, TAE object/block and
  checkpoint formats, WAL/LogService records, persisted views/plans/expressions,
  index metadata and index files, configuration files. A new binary must read
  persisted data from supported released sources, either directly or through a
  bootstrap upgrade step. A supported deployment's retained data and snapshots
  remain in scope even when originally created by an earlier release. New on-disk
  formats are opt-in; never flip a global default.
- **Upgrade steps:** catalog/schema migrations run once at startup, are
  idempotent, and survive a crash part-way through followed by a restart.
- **Backup/restore:** a backup taken by an older binary restores on a newer one.
- **External boundaries:** the MySQL client protocol, and anything that crosses
  a cluster boundary (cross-cluster publication/subscription, CDC sinks,
  external tools) keep their own compatibility rules. Shutdown upgrade of one
  cluster does not make the other side the same version.

A valid compatibility finding names one of these surfaces and a concrete input
written by the older binary that the newer binary misreads, rejects, or loses.
