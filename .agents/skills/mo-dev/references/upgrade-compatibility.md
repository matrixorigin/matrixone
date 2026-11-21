# Upgrade compatibility contract

MatrixOne supports **shutdown upgrade only**: stop every service in the cluster,
replace the binaries, start the cluster. Rolling upgrade, where old and new
CN/TN/LogService/proxy binaries serve one cluster at the same time, is not
supported. Downgrade (starting an older binary on data written by a newer one)
is not supported.

Use this reference when a change can affect persisted state, upgrade/restore,
or protocol and plan encoding. Classify the changed boundary once in the
existing change map. A changed function, type, ID or default can alter what
persisted bytes mean even when the format itself is unchanged. Trace a real
writer and reader to distinguish durable state, an external contract and
same-binary traffic. Only an affected release boundary needs release lookup
and upgrade evidence. Docs, test-only or internal changes with no effect on
that boundary proceed through the normal workflow.

## Resolve the source release only when needed

- Use the applicable source release specified by the user or the project's
  upgrade policy for the target release line. Otherwise, use the upstream
  latest stable release as the default when it is a supported source for the
  target. Resolve it at task time, for example with
  `gh api repos/matrixorigin/matrixone/releases/latest --jq .tag_name`. For a
  maintenance branch, use the latest published supported predecessor for that
  line when the upstream latest release is outside its upgrade path.
- Verify that the release is published and pin the tag and resolved commit
  (or identify the official binary used) in the task evidence. A newer-dated
  hotfix or prerelease does not automatically replace that baseline. If the
  source cannot be verified, report the precise gap and continue the checks
  that do not depend on it. Do not substitute a branch tip or a guessed tag.
- Check distinct older supported sources only when their data or upgrade paths
  exercise a different contract. Reuse valid author, CI or previous task
  evidence for the same source and affected behavior.

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

## Preserve or explicitly migrate released semantics

- Identify what the release wrote and how upgraded code consumes it. Preserve
  the meaning of existing definitions by default, including when later writes
  reevaluate them. Use the existing owner to distinguish released definitions
  from newly bound ones. Prefer binding, decoding or migration time to avoid
  per-row compatibility work. Remove superseded development-only branches
  in the changed scope.
- When a fix deliberately changes an existing definition's behavior, state
  the intended result and use the existing migration or rebuild owner. Verify
  consistency across the definition, materialized values and dependent indexes
  where affected. Exercise interruption and retry if the change can leave
  durable partial progress. Keep decoding a released ID consistently while
  any supported artifact can still carry it.
- Choose the smallest test that distinguishes the two behaviors. For a
  reachable change to released interpretation or migration, exercise a real
  artifact from the verified release (or reuse equivalent evidence), open it
  on the target, and check affected reads and new writes or recomputation.
  Compare with a fresh target definition where semantics differ. Exercise a
  fresh install if bootstrap changes. Include dump/restore, restart or
  interrupted upgrade where the changed contract reaches those paths. Use a
  synthetic fixture for decoder edge cases and release provenance for claims
  about what a release actually wrote.
- If changed writes or migrations affect a rollback plan, state when the
  pre-upgrade backup must be taken, how to restore it, and which post-upgrade
  changes that restore would lose. Keep forward-upgrade evidence separate
  from any downgrade experiment. Do not require a downgrade matrix for
  normal changes.

For a changed migration, verify applicable transaction rollback, retry,
committed partial progress, restart and completion without duplicate effects.
Reuse the existing upgrade owners and recovery paths.

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

A valid compatibility finding names one of these surfaces and a concrete
released or external input that the target misreads, rejects, loses or
reevaluates incorrectly.
