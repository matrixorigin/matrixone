# Declared catalog metadata: publication admission and account repair

Status: revision 15 (2026-10-30) - editorial consolidation of the
independently approved revision 14; no design decision, invariant,
oracle or validation entry changes. Owning issue:
matrixorigin/matrixone#30936 (carrying #30935 and #30937).
Implementation: PR matrixorigin/matrixone#31489.

Design-first approval and the delivered vehicle (this document's
commitment to the delivery record):

- The change trips the mandatory design triggers (section 1), and the
  design-first order was enforced end to end: implementation of a
  round proceeded only after that round's independent design review
  returned PASS. Each reviewed revision's exact content hash and
  disposition is recorded in the revision history at the end of this
  document.
- The delivered design content is revision 14 (sha256
  22aaf464a18a8be7e7d4889e605850c0792bca6df7bd234e97569445b0c84c66):
  independently reviewed and APPROVED (PASS) on 2026-10-30 before
  implementation, and implemented at PR #31489 head
  61da7f8ef005f2ee14a3ea44e738e6baee24876d. The previously delivered
  content revisions were revision 9 (sha256
  57043aac71c74a7cb63f0a8ac047a4ac898bf974791644569e5c57f4bd2838d9,
  PASS 2026-10-28, implemented at rework head 9f454cb5d0) and
  revision 12 (sha256
  47ebfe15a45940221c3d2d5ad6955e11a5c89a1a909fa9d4e382e1ac9db0a599,
  PASS 2026-10-29, implemented at head efb7234682).
- Revision 15 is a documentation-only consolidation requested by the
  PR's implementation review: the D13/D14 amendments are folded into
  the standing sections (2.2, 3, 5.2, 5.5, 5.6, 6, 7, 8), and the
  per-round status correspondence, the pre-revision-8 withdrawn
  delivery-model blocks, and the completed rebases' resolution/run
  narratives are removed from this tracked file - they stay recorded
  in the PR's review threads and its retained review evidence. Every
  surviving decision keeps its approved wording in the standing
  decision record, and the approved revision-14 wording governs
  wherever this consolidation could be read as a change.
- Delivered vehicle: the stored-view refresh hop is
  `pkg/bootstrap/versions/v4_0_19` (version "4.0.19", minimum upgrade
  version "4.0.7", required protocol 118, offset 2), registered on
  main's consolidated ladder behind upstream's `v4_0_18` handle. Its
  COLUMNS leg publishes the merged identity (upstream's
  attributed-DECIMAL COLUMN_TYPE decoration composed over the
  label-aware chain with the unbounded generated-expression
  projection) and its STATISTICS leg
  publishes the prefix-aware identity (5.2); historical readers
  recognize every recent published generation including the frozen
  `InformationSchemaColumns4018DDL()` identity (5.5); population
  routes converge per section 6 and the validation census is
  section 7's.

## 1. Classification and gate record

Ordinary-fix exemption: withdrawn. The reworked change - the declared-label
metadata helpers and overloads, the corrected persisted information_schema
identities, the SHOW INDEX prefix projection, and one version-gated
tenant-migration refresh hop - still crosses the complexity triggers of
`.agents/skills/mo-dev/references/feature-design-review.md`:

1. It affects upgrade, migration, restart, backup and restore of
   persisted state: the per-account information_schema COLUMNS/STATISTICS
   identities an older binary wrote move to the corrected identities
   through a bootstrap upgrade step, and the shutdown-only contract of
   `.agents/skills/mo-dev/references/upgrade-compatibility.md` governs
   that surface.
2. It crosses more than two subsystem and ownership boundaries (declared
   labels in the persisted catalog, the function registry,
   information_schema publication, the bootstrap upgrade framework,
   SHOW INDEX) and changes a client-visible SQL semantics family
   (information_schema per MySQL).
3. It exceeds the default size triggers as well (hundreds of production
   lines across more than five owning packages, excluding tests,
   generated files and documentation).

The triggers earlier revisions carried - a recurring repair pass with
cursor state, and a wire/capability-protocol fence - are withdrawn with
their machinery by revision 8 (section 4.5). This document remains the
required reviewed artifact under `docs/design/`, and review order is
design first: implementation of a round proceeds only after that
round's independent design PASS, with each reviewed revision's exact
content hash and disposition recorded in the revision history.

## 2. Problem, evidence, and invariants

### 2.1 Observed problem (recorded evidence)

Three catalog-metadata projections never consult the metadata that owns their
answers, and two publications paths could not deliver the corrected projections
safely. All five facts are reproduced on the assigned base and recorded:

- **Metadata columns.** `information_schema.COLUMNS` computed
  CHARACTER_MAXIMUM_LENGTH/CHARACTER_OCTET_LENGTH/NUMERIC_PRECISION/
  NUMERIC_SCALE from the serialized physical column type (`mo_columns.atttyp`)
  alone. Declared ENUM columns (persisted as T_enum, outside the
  MySQL-string OID set) returned SQL NULL for all four fields; declared SET
  columns (persisted as the T_uint64 bitmask `build_util` maps them to)
  returned NULL lengths and a false NUMERIC_PRECISION=20/NUMERIC_SCALE=0,
  misreporting SET as numeric. Recorded in the issue investigation
  and retained with the PR's review evidence: a base probe log
  (ASCII, multibyte `('中','长长')`, reversed `('z','a')`, empty-label
  and control columns) and an embedded-cluster red run (exit 1).
- **Prefix index parts.** The declared prefix lengths are retained in
  `mo_catalog.mo_indexes.algo_params` as `{"prefix_lengths":"v:10"}` (visible
  in SHOW INDEX `Index_params`), but the STATISTICS template projected the
  literal `NULL AS SUB_PART` and SHOW INDEX never projected the retained
  value, so both surfaces returned NULL for prefix parts and indistinguishable
  NULLs for whole-column parts. Recorded: same probe pair.
- **Generated expressions.** The COLUMNS GENERATION_EXPRESSION projection
  wrapped `mo_show_visible_bin(mc.attr_generated, 5)` in nested varchar(500)
  casts, so a 630-character expression was clipped to exactly 500 bytes on
  information_schema while SHOW CREATE and the evaluated value kept the full
  text. Recorded: same probe pair (`length(generation_expression)=500` on
  base vs 630 on the fixed tree, byte-identical to SHOW CREATE).
- **Persisted identities written by older binaries.** Every account's
  information_schema COLUMNS/STATISTICS definitions are persisted tenant
  DDL written by the binary that created or last upgraded the account, so
  an account last written by a pre-correction binary keeps projections
  that cannot consult the declared labels - the reporting case's wrong
  values. The persisted-state contract requires a new binary to read
  everything an older binary wrote "either directly or through a
  bootstrap upgrade step"; this tree's established vehicle for moving a
  persisted information_schema definition is the version-gated tenant
  hop (upstream's merged #30362 report-MEDIUMINT correction shipped the
  same class of COLUMNS metadata value correction as a v4_0_17 hop over
  the shared `versions.InformationSchemaColumnsUpgradeEntry`). Recorded:
  the shared entry and the 4.0_17 runner shape at the base. The
  mixed-fleet publication-gap findings of earlier revisions are
  withdrawn by revision 8 (sections 3, 4.5).

### 2.2 Invariants

- **I1 (declared-domain metadata).** For a row whose declared column domain is
  ENUM or SET, COLUMNS metadata derives from the declared label list
  (`mo_columns.attr_enum`): character length = longest member label (ENUM) or
  the sum of member labels plus N-1 separators (SET); octet length = 4 bytes
  per character (utf8mb4 domain, the repository's varchar convention);
  NUMERIC_PRECISION/NUMERIC_SCALE are SQL NULL. ENUM('') and SET('') are
  declared zero-length domains and report 0/0 with NULL numeric metadata; an
  empty or NULL label list on a T_uint64 row keeps the ordinary unsigned
  integer identity (20/0) - the empty-label case is decided by the declared
  column OID. Ordinary types (int, varchar, decimal, text, geometry SRID,
  json labels) are unchanged. **I1 is exact for every declaration the persisted
  catalog represents unambiguously** - which is every accepted SET declaration
  by construction (section 2.3) and every ENUM declaration whose labels
  contain no comma; the residual ENUM collision class follows the exactly
  defined boundary interpretation of section 2.3, not a silent guess.
- **I2 (unconditional publication).** Every publication path on this
  binary generation - bootstrap system init, CREATE ACCOUNT, tenant
  publication and the subscription wrapper - persists exactly the
  corrected current identities (label-aware COLUMNS, prefix-aware
  STATISTICS) through the base protocol ladder; the ladder's
  protocol-aware rungs stay the grandfathered ones (the <126 legacy
  list, VIEWS at 126, the collation identity at 109). There is no
  capability survey, no admission condition and no authoring-floor
  branch beyond what base already has.
- **I3 (hop convergence).** The v4_0_19 tenant hop ("4.0.19"; the
  vehicle of revisions 13-14, registered behind upstream's `v4_0_18`
  handle) is the single refresh
  vehicle for identities persisted by older binaries: for every account
  below 4.0.19 it rewrites both definitions once (CREATE OR REPLACE
  VIEW inside the hop's account transaction, per the consolidated
  shared-builder shape), guarded by identity
  CheckFuncs - a leg already at the published identity is skipped, so a
  completed hop never rewrites. A crash partway through repairs through
  the framework's existing marker plus re-probe rule, so the hop is
  idempotent across restarts.
- **I4 (pre-refresh readability).** Until a given account's hop leg
  completes, its definitions persisted by an older binary keep binding on
  this binary - every function identity those definitions reference
  remains implemented - and reads report the pre-correction values.
  Within a shutdown-upgraded fleet every peer resolves every published
  identity; no below-floor substitution exists.
- **I5 (contract-conforming fencing).** The change adds no
  `MORPCVersion` constant, moves no `MORPCLatestVersion`, and
  introduces no all-peers capability condition anywhere. The
  pre-existing planes - the 118 view-metadata admission handoff, the
  126 VIEWS authoring floor, the 130-134 upstream gates and the
  bootstrap maintenance passes - keep their base shape and base values.

- **I6 (historical snapshot continuity, added by revision 9).** Any
  information_schema.COLUMNS definition a recent binary published as its
  current generation - the corrected identity, the #31461 NULL-selector
  MEDIUMINT template, and the pre-#31461 'binary' MEDIUMINT template -
  binds in the snapshot readers under any live definition of the
  migration-gate family: `historicalViewColumnsSQL` binds the snapshot's
  recorded DDL byte-stably (the adapter's predecessor branches return
  the recorded text unchanged), the derived view-metadata path knows the
  same set, and unknown or user-edited definitions stay rejected at
  every seam. Snapshot binding has no write path: the recorded
  definition, projections and timestamp are preserved; the hop rewrites
  only the live catalog rows.

Non-goals: changing stored ENUM/SET values, SHOW CREATE labels, index
storage or algo_params format; changing the upgrade framework's
versioning semantics; supporting in-cluster mixed-version operation,
downgrade or per-feature protocol gates (out of scope by the
shutdown-only upgrade contract); retiring pre-existing
`MORPCVersion`-gated machinery outside a dedicated cleanup change;
adding new metrics or operator controls; repairing arbitrary or unknown
view identities; fixing #30931 (ENUM/SET boolean value semantics).

### 2.3 Declaration-source fidelity and the encoding-defect boundary

The metadata sources differ in declared-label fidelity, and this design's
scope boundary is defined against that difference:

- **Lossless sources.** The parser's in-memory `EnumValues` list is lossless,
  and the original CREATE TABLE statement is persisted losslessly as
  `mo_catalog.mo_tables.rel_createsql` (T_text) - the only persisted copy of
  the original text; the rendering surfaces do not use it for column types.
- **Lossy persisted source.** `mo_columns.attr_enum` for ENUM is
  `strings.Join(labels, ",")` with no escaping (`pkg/sql/plan/build_util.go`,
  MYSQL_TYPE_ENUM case) persisted unchanged by `pkg/catalog/tuplesGen.go`
  (MO_COLUMNS_ATT_ENUM_IDX). MySQL permits a comma inside a quoted ENUM
  member, so `ENUM('a,b')` (one member, declared maximum character length 3)
  and `ENUM('a','b')` (two members, length 1) both persist as `a,b` and are
  indistinguishable to every attr_enum reader - and every existing
  attr_enum reader already renders the split interpretation for this class:
  SHOW COLUMNS and SHOW CREATE TABLE build the column type through
  `mo_show_visible_bin_enum(atttyp, attr_enum)` (pkg/sql/plan/build_show.go),
  so the original declaration survives only in the parser tree and in
  `rel_createsql`. This is a **pre-existing catalog encoding defect** on the
  assigned base, and it is **out of this metadata correction's scope**:
  fixing it means changing the persisted attr_enum format for future writers
  with a mixed-version migration fence of its own - a separate, gated change
  (recorded in section 8 as the encoding follow-up).
- **SET is unambiguous by construction.** `types.NormalizeSetValues` rejects
  any SET member containing `,` at declaration time ("set member %q contains
  ','"), and the empty-domain corner is encoded distinctly (`,` for the single
  empty member). Every accepted SET declaration therefore survives attr_enum
  exactly; I1's SET semantics are exact with no residual class.
- **Boundary interpretation for the ENUM collision class.** The metadata
  helpers interpret the persisted string under its documented format - the
  comma-separated member list as stored. For `a,b` that is the two-member
  list {a, b}: ENUM('a','b') reports character length 1 (exactly MySQL), and
  ENUM('a,b') also reports 1 (MySQL: 3) because the declaration no longer
  exists in the persisted representation the metadata layer may read. The
  interpretation is deliberately the same one every existing attr_enum
  consumer uses (stored-value validation, SHOW COLUMNS, SHOW CREATE TABLE),
  so the corrected metadata agrees with what the system renders for the
  ambiguous class instead of inventing a second reading. The deviation is
  deterministic, bounded to ENUM declarations with at least one
  comma-containing label, identical across all four COLUMNS projections
  (mo_columns, mo_view_columns, mo_subscription_columns,
  mo_subscription_view_columns read the same attr_enum through the same
  helper, so base tables, views over them and subscription replicas report
  the same value), and strictly narrower than the status quo, where the same
  row reported SQL NULL for all four fields. Existing columns, views and
  subscriptions are untouched: the boundary changes no persisted bytes and no
  stored values - it only defines what a reader derives from the already
  persisted, already ambiguous string.

## 3. Standards, precedent, and MatrixOne constraints

- MySQL 8.3 information_schema semantics for ENUM/SET character metadata,
  STATISTICS.SUB_PART and SHOW INDEX Sub_part are the interoperability oracle
  (reference values quoted in the issue). They are mandatory compatibility
  precedent for I1.
- Repository precedent: upstream's merged #30362 report-MEDIUMINT
  correction (v4_0_17 tenant hop over the shared
  `versions.InformationSchemaColumnsUpgradeEntry` - pre-existing gate
  values, idempotent CheckFunc, PreSql-guarded create in the hop's
  account transaction, marker-driven restart safety) is the established
  mechanism for refreshing persisted per-account information_schema
  definitions when the current identity moves; this design reuses that
  shape verbatim (D8-3) instead of inventing a second style.
- The shared upgrade entries remain the single
  source of the current definitions and their
  CheckFuncs - the COLUMNS leg is the upstream shared
  `versions.InformationSchemaColumnsUpgradeEntry` and the STATISTICS leg
  is the upstream shared-builder form revision 10 specifies (drop
  statements with them: the consolidated entries author
  CREATE OR REPLACE VIEW) - so the v4_0_19 hop calls them (as does the upstream
  v4_0_18 handle, whose shared COLUMNS entry resolves the same identity
  symbol in the merged tree), and there is exactly one semantics per
  definition.
- Constraints taken as given (revision 8): MatrixOne supports shutdown
  upgrade only - stop every service, replace the binaries, start - so
  in-cluster peers always run one binary, mixed-version operation and
  downgrade are unsupported, and a new function overload or a changed
  persisted view text needs no version gate
  (`.agents/skills/mo-dev/references/upgrade-compatibility.md`).
  Persisted-state closure remains binding: a new binary reads
  everything an older binary wrote, directly or through an upgrade step,
  and backups taken by older binaries restore on newer ones.

## 4. Alternatives (decision log)

### 4.1 Repair for finding 1 (withdrawn by revision 8 - the unreachable-account premise only exists on mixed fleets; see 4.5)

- **A1 Status quo.** No repair: final-version mixed-fleet accounts keep all
  three reported defects forever. Rejected: defeats the acceptance contract
  and finding 1.
- **A2 Offset-only upgrade rerun.** Make the final hop
  `FromVersion == ToVersion` so `shouldRunTenantUpgrade` reruns equal-version
  tenants. Rejected: it couples the repair to the catalog version schedule
  (every later offset bump drags it along), reruns whole entry lists
  version-blind (the identity check needed to avoid churn would have to be
  added anyway), and adds a version/offset artifact every future handler
  must reproduce. The repair semantics are per-account identity convergence,
  not a schema migration between catalog versions.
- **A3 Repair at account login/first-use.** Lazy per-account repair on
  session paths. Rejected: DDL on the hot session path, latency coupled to
  fleet state, and failure modes surface as account-visible errors.
- **A4 Selected: bounded background maintenance in the VIEWS-maintenance
  style.** A recurring pass in the same periodic slot as the VIEWS maintenance
  (already only one process-local maintenance owner), gated at 135 on both
  halves of admission, identity-guarded, page-bounded, transactional per page,
  driven by the durable identities themselves. Wins on: zero durable state
  (I8), zero hot-path cost (I7), established precedent and shared entry
  semantics, single-writer discipline, and trivially provable idempotence.

### 4.2 Publication selection (withdrawn by revision 8 - publication is unconditional; see 4.5)

- **B1 Capability-only selection.** Select 135-level text if peers answer 135
  (pre-revision shape). Rejected: violates I3 in the capability-before-
  admission window; aborting CREATE ACCOUNT on the write gate is an
  availability regression on an otherwise healthy fleet.
- **B2 Local-floor-only selection.** Publish 135 text as soon as the local
  floor admits it. Rejected: violates I2 when a peer is still pre-135 - the
  text would be persisted but unresolvable remotely.
- **B3 Selected: twin half-gates.** Publish 135 identities only when the
  survey is 135 everywhere and the local authoring floor is admitted at 135;
  otherwise publish the frozen protocol-126 set. This is the write gate's
  decisions mirrored into the publisher (including the missing-floor-key
  standalone fallback), and the account is repaired later by A4 (I4).
- **B4 Deferring account creation until admission.** Wait/retry CREATE
  ACCOUNT until the floor rises. Rejected: couples account availability to
  admission timing and still needs a floor at the moment of retry.

### 4.3 Declaration-encoding boundary (decision)

- **C1 Fix the attr_enum encoding inside this change.** Escape commas for
  future writers and read both encodings. Rejected for this change: it turns
  a metadata correction into a persisted catalog-format migration with its
  own mixed-version writer/reader fence, diverts SHOW CREATE and stored-value
  consumers that share the encoding, and compounds the protocol fencing
  already carried here. The comma-label class keeps the pre-existing NULL
  metadata problem until a dedicated change fixes the encoding.
- **C2 Selected: document the boundary, prove it, and schedule the encoding
  fix separately.** I1 is exact for every unambiguously persisted declaration
  (all SET by construction; all comma-free ENUM), the collision class gets
  the exactly defined interpretation of section 2.3, both colliding
  declarations join the metadata oracle (section 7) so the boundary is
  pinned, and the encoding fix is recorded as a separately designed change
  (section 8). This keeps the metadata correction decoupled from a
  catalog-format migration it does not need.

### 4.4 Tenant-login compatibility across the rollout (decision, revision 4; withdrawn by revision 8 - see 4.5)

The reviewer's login finding: the version-gated hop runs synchronously at
login (`pkg/frontend/mysql_protocol.go`, the `MaybeUpgradeTenant` call inside
the authenticate path), and its two information-schema entries are the
135-gated shared ones, whose runner fails login twice over below admission
(`pkg/bootstrap/versions/upgrade_strategy.go`: the survey error at
~189-191; past the survey, the CREATE VIEW fails
`RequirePersistedProtocolVersionForAuthoring` through the mirrored floor,
`pkg/sql/plan/build_ddl.go:~335` + `pkg/sql/plan/persisted_ip.go:~262-280`).
Revision 3's CN-first sequence is a routing contract; this decision makes it
a service contract.

- **D1 Status quo: fail login below admission.** The entry runner's current
  behavior: every pre-final account fails login on every upgraded CN for the
  entire mixed-phase + pause duration (a 134 peer blocks the survey; the
  134 authoring floor blocks the DDL at the pause). Rejected: it converts
  the rollout window into a login outage for exactly the accounts the
  repair exists to serve, and it voids the pause's usability promise.
- **D2 Defer the tenant's version bump below admission.** Candidates:
  downgrade the hop or refuse to mark. Rejected as infeasible: the login
  loop requires `from == currentCN.Version` to return nil
  (service_upgrade_tenant.go), and the version compare rejects a tenant
  above the CN's version - deferring the marker means login fails anyway,
  with a framework-shaped error instead of a gate-shaped one.
- **D3 Rework the framework to a deferred final marker.** Rejected:
  changing the upgrade framework's version semantics is a non-goal (section
  2.2), it couples the marker schedule to admission state, and the bounded
  repair already guarantees convergence without framework changes.
- **D4 Selected: admission-scoped, login-safe entries in the hop.** The
  hop runs its two information-schema entries only when both halves of the
  135 admission hold - the same condition the repair probes at page start
  (the capability survey, and the local authoring floor at 135, i.e. the
  exact twin of I3). Below that, each entry completes without writes and
  the account is marked final with the frozen identities; `MaybeUpgradeTenant`
  returns nil and login succeeds. Only the two gate conditions skip; every
  other error (transaction, DDL, catalog) stays visible to login exactly as
  today. Convergence for every such account is the bounded repair's
  existing contract (I4, I6) - no new writer, no durable marker, unchanged
  identity guards. This is a required production delta recorded in
  section 9 (implemented with the post-PASS ladder's new login-boundary
  cases L1-L3).

### 4.5 Upgrade-contract rework (decision, revision 8)

- **E1 Status quo of revisions 1-7.** Keep the 135 fence, survey-gated
  publication, the repair pass, the hop admission-skip and below-floor
  serving. Rejected: the repository contract excludes the whole family
  of mechanisms (shutdown upgrade only; no new per-feature
  `MORPCVersion` constant; no `AllCNsSupportProtocol`-style gates; no
  legacy fallback paths for older peers; do not extend grandfathered
  gates), and the contract equally forbids reviewers from requesting
  such mechanisms. MORPCVersion132-134 are upstream planes; this change
  extends the same fenced files for a new feature, which is the exact
  extension the contract prohibits.
- **E2 Deliver without an upgrade step.** Publish corrected identities
  only for fresh content. Rejected: existing accounts keep every wrong
  value of section 2.1 - the wrong projections live in their persisted
  view text, and the persisted-state contract requires the new binary
  to move that state forward.
- **E3 Selected: unconditional publication plus one version-gated hop.**
  Publish the corrected identities everywhere this binary publishes
  (D8-2) and refresh pre-existing accounts through the tenant
  hop in the upstream #30362 shape (D8-3; the vehicle is the
  post-consolidation successor handle, renumbered onto the merged
  ladder as v4_0_19 by revision 13); delete every
  feature-specific fence, selector, reader substitution and repair pass
  (D8-1, D8-4, D8-5). Wins: the change abides by the repository's
  upgrade contract; delivery uses the two shapes the repository already
  owns (publication, one hop); the recurring-state, admission-ordering,
  pause, login and restore failure modes cease to exist because their
  machinery is deleted rather than managed. Withdrawn by this
  decision: A4, B3, D4, the revision-7 reader half and the previous
  I2-I4/I7-I10 invariants. Reusing the VIEWS-maintenance precedent
  never required the 135 fence, and retiring the grandfathered
  126-plane machinery remains a dedicated future change.

## 5. Selected design details

### 5.1 Publication

Publication is base behavior with corrected content. Bootstrap system
init, CREATE ACCOUNT, tenant publication and the subscription wrapper
carry the corrected current identities through the pre-existing
protocol ladder exactly as base carries its own current identities; the
ladder's protocol-aware rungs stay the grandfathered ones (the <126
legacy list, VIEWS at 126, the collation identity at 109).
`protocolVersionForTenantInitializationWithContext` and
`createTablesInInformationSchemaOfGeneralTenantWithProtocol` keep
their base shape: no survey branch, no authoring-floor branch, no new
rung. The corrected definitions author under the floor base already
applies to their content; the label-aware overloads and
MO_INDEX_SUB_PART, like every other function overload, carry no new
protocol requirement.

### 5.2 The v4_0_19 tenant hop (retained stored-view migration, merged-ladder numbering)

The handler is a standard version handle on the merged consolidated
ladder (`versions.Version{Version: "4.0.19", MinUpgradeVersion: "4.0.7",
UpgradeCluster: No, UpgradeTenant: Yes, RequiredProtocolVersion: 118,
VersionOffset: 2}` - its two tenant entries, registration appended
after upstream's `v4_0_18.Handler`, numeric order 4.0.7 -> 4.0.18 ->
4.0.19), and HandleTenantUpgrade is the plain per-account entry loop,
invoking `entry.Upgrade` per entry with the framework's logging - no
paging walker, no skip, no new durable progress. For an account that
crossed the upstream v4_0_18 handle in the same startup the COLUMNS leg
re-checks content-equal and writes nothing while the STATISTICS leg
installs the prefix-aware identity (D13-1). Two MODIFY_VIEW entries,
both in the upstream shared-builder form revision 10 specifies:

- `versions.InformationSchemaColumnsUpgradeEntry()`: the upstream
  shared-builder form (authors `CREATE OR REPLACE VIEW` on
  `sysview.InformationSchemaColumnsDDL`, RequiredProtocolVersion 109,
  canonicalized CheckFunc through
  `InformationSchemaViewDefinitionMatches`) - and the identity symbol
  now carries the merged corrected identity (upstream's
  attributed-DECIMAL COLUMN_TYPE decoration composed verbatim over the
  declared-label projections and the unbounded generated-expression
  row, D13-2), so bootstrap publication, the upstream v4_0_18 handle
  and the historical hops observe exactly one current identity.
- `versions.InformationSchemaViewUpgradeEntry("STATISTICS",
  sysview.InformationSchemaStatisticsPrefixSubPartDDL,
  defines.MORPCVersion41)`: publishes the prefix-aware identity with
  the same shared-builder authoring and CheckFunc, at the historical
  STATISTICS refresh gate revision 9 chose (41, a pre-existing
  constant); `sysview.InformationSchemaStatisticsDDL` keeps its
  historical value so main's own 4.0.7 statistics entry stays
  upstream-identical.

Terminal paths and failure behavior - all inherited from the
framework's base-hop semantics:

- **Leg current:** the CheckFunc skips it; no write; a completed hop
  never touches the view again.
- **Leg stale or missing:** the entry rewrites it to the published
  identity inside the hop's account transaction (the shared builder's
  CREATE OR REPLACE VIEW covers the missing case; neither leg drops
  a view).
- **Hop error (transaction, DDL, catalog):** the error stays visible to
  the framework exactly as every historical hop exposes failures; the
  account's marker is untouched, so the next login or startup tenant
  pass re-enters, and re-probes make the retry a no-op except for the
  failed legs.
- **Crash partway through a pass:** restart re-runs the hop from the
  marker; completed accounts are skipped, and an interrupted account
  repeats only its transaction.
- **Concurrency:** one driver per account - the framework's existing
  login/startup discipline; the change adds no coordination, cursor or
  lock.

### 5.3 Ownership, bounds, and hot paths

The upgrade framework owns the refresh (version handle, shared
entries, tenant task and login paths); the change adds no runtime
state, background pass, goroutine, queue, cursor, config flag, metric
or operator surface. Request paths are untouched: corrected identities
bind at view-bind time on the base path, and the hop is DDL bounded by
the account count, once per account. Observability stays the
framework's existing per-entry logging.

### 5.4 Security and tenant isolation

The hop and the publication paths run in bootstrap's existing system
transactions with fixed repository string constants (the shared DDL and
DROP statements); no user input is handled, so the change introduces no
injection or privilege surface and writes only account-owned
information_schema definitions.

### 5.5 Historical snapshot readers (added by revision 9)

`historicalViewColumnsSQL`
(pkg/sql/plan/view_columns_history.go) binds a snapshot's recorded COLUMNS
definition as-is when the definition is current (or exactly the tolerated
selector drift of current), and otherwise - once the tenant's live COLUMNS
view belongs to the on-demand migration family
(`IsInformationSchemaColumnsMigrationDDL` on the live definition) - either
adapts it through `AdaptLegacyInformationSchemaColumnsDDL` or refuses with
"unsupported historical information_schema.COLUMNS definition".
`metadataViewColumnsSQL`'s derived view-metadata seam
(`sysview.ViewMetadataColumnsDDL`) holds the same recognition discipline
for explicitly owned derived-metadata statements. This change moves the
live current identity to the corrected projections, so the holders of the
previous current generations become the historical set, and the design
records three recognition seams together (D9-1):

- `AdaptLegacyInformationSchemaColumnsDDL` recognizes the complete
  historical family: the byte-stable V100/V109/V41/V46/V46Upgrade/V58
  templates (unchanged), the V110 MEDIUMINT 'binary' template (kept),
  the #31461 NULL-selector MEDIUMINT template (added - a byte-stable
  branch: the identity base-era services persisted as their current
  definition), and the frozen upstream 4.0.18 published identity
  `InformationSchemaColumns4018DDL()` (revision 14 - the
  attributed-DECIMAL decoration applied verbatim over the frozen
  binary-null pre-attributes template, built from frozen bytes and
  never from the mutable current constructor; a byte-stable
  pass-through branch, because the 4.0.18 identity already carries
  current column authority, projections and selector).
- `ViewMetadataColumnsDDL`'s known set carries the same MEDIUMINT-era
  pair and the frozen 4.0.18 identity beside current, V100 and V109.
- `IsInformationSchemaColumnsMigrationDDL` grants gate membership to the
  on-demand-format generations recent binaries persisted as their
  current definition - V100, V110, the NULL-selector variant, the
  frozen 4.0.18 identity, and the corrected current - so a
  not-yet-refreshed base-era account keeps the gate base granted it,
  and a refreshed account joins the corrected generation.

The historical readers stay read-only: they return bound SQL text and
acquire no DDL path, so a snapshot's recorded definition, its own
(pre-correction) projections and its timestamp stay exactly as recorded;
the hop refreshes only the live rows. Recognition is exact-template at
every seam (canonical view-name case only), so unknown or user-edited
definitions remain refused - the D8-1 withdrawal of mixed-fleet machinery
removes no rejection behavior, and the D9-1/D14-1 additions open no
backdoor. `IsCurrentInformationSchemaColumnsDDL` keeps excluding the
4.0.18 text (no replay-drift tolerance extends to it - it is not the
merged current), so recorded 4.0.18 text always routes to the adapter
where it now binds, and the undecorated label-aware intermediate stays
dev-lineage-only and unrecognized (D14-1).

### 5.6 Preserved upgrade admission and on-demand continuation for the successor final (revisions 11-13)

Registering the successor final silences four upgrade-admission guards
the pre-rework upstream keyed on the literal
`final.Version == "4.0.7"`. Current main itself re-keys them onto the
consolidated-line `>= 4.0.7` comparison, the registered frozen-handle
identity and the `crossingPending`-in-route predicates, and this
design adopts that re-keying verbatim while keeping the recovery-path
guards (D11-1/D11-2 as amended by D13-3):

- **Legacy CDC admission** (`rejectLegacyCDCTasks`) runs before any
  catalog mutation whenever this binary admits a source below the
  frozen stage or resumes an unfinished route to it: at route
  creation with `final` in the consolidated line and the recorded
  source below 4.0.7 or an unready 4.0.7, at recheck of a non-ready
  retained row in the same line, and at recovery before returning the
  resume verdict for a retained 4.0.7-stage route. Sources containing
  pre-4.0.7 CDC tasks (paused tasks included) terminate with the
  upstream message; an empty task catalog admits; a catalog-read
  error stays retryable. The capped diagnostic and the wrapped error
  of pkg/bootstrap/service_upgrade_cdc.go are unchanged.
- **Frozen source validation:** a ready (4.0.7, offset) row whose
  offset differs from `v4_0_7.Handler.Metadata().VersionOffset`
  terminates with upstream's "4.0.7 is frozen" message before route
  creation, so no earlier-generation 4.0.7 catalog can leap directly
  to 4.0.19 and skip the consolidated migrations; an unready
  (4.0.7, frozen-offset) route carrying a 4.0.7 -> 4.0.7 offset-only
  step terminates with the "unsupported in-progress 4.0.7 offset-only
  upgrade" message, at the pre-mutation check and again in
  `checkUpgradeRecovery` (same-version steps for the frozen target are
  rejected instead of resumed); unready (4.0.7, other-offset) rows
  keep terminating through `canResumeUpgrade`'s handle-offset
  comparison exactly as upstream does.
- **Allowed transitions, unchanged from a current-main binary:** a
  ready source below 4.0.7 chains through the frozen 4.0.7 stage with
  CDC admission and the view-metadata catalog prerequisites (both
  `>= 4.0.7`-keyed admissions now agree); a ready 4.0.7 at the frozen
  offset takes the single direct [4.0.7 -> 4.0.19] hop; an interrupted
  normal semantic route (e.g. [4.0.6 -> 4.0.7]) resumes with CDC
  admission enforced; fresh clusters initialize at 4.0.19. Every other
  recorded state terminates before mutation with upstream's message
  text, error type and finalVersionCompleted behavior - including the
  crossed-route non-rejection upstream's pending-scan permits (a route
  whose 4.0.7 step crossed with its own admission is not re-rejected).

These are source-validation admissions, not protocol mechanisms: no
version constant moves, no capability checks appear, and nothing about
peer negotiation changes (upgrade-compatibility.md holds). The oracles
pinning them are listed in section 7's admission entries.

**On-demand continuation past the paged stage (revisions 12-13).**
The login-compensation walk (`MaybeUpgradeTenant`) and the manual
walk (`UpgradeOneTenant`) inherit the paged-4.0.7 special case:
without a caller-owned transaction they break the register walk at
`v4_0_7.Handler` and complete through `upgradeV407TenantOnDemand`,
which advances the account only to 4.0.7. With this branch's
successor registered, both walks must reach it: current main owns
this through re-entrant planning loops (both walks re-enter planning
after each paged crossing and step every registered successor handle,
skipping the frozen handle in-walk when the account is already at
4.0.7), this design adopts those loops verbatim, and it adds the
manual walk's in-transaction completion check
(`if !pendingV407 && from != currentCN.Version` ->
"cannot upgrade tenant version %s to current cn version %s",
absent upstream; D13-4 supersedes revision 12's service-owned
continuation transaction). Three invariants hold for both walks:

- **Completion equals the final:** an on-demand or manual completion
  (and its success caching) requires the account's persisted
  create_version to equal `getFinalVersionHandle().Metadata().Version`;
  no path caches success with the account left below it.
- **Failure resumes from the stamp:** a later-stage error leaves the
  account at its last committed create_version, the success cache
  unset, and the next entry resumes the walk from there without
  duplicating committed legs (the existing per-account authority rule:
  mo_account.create_version is the sole progress marker).
- **The paged stage keeps its ownership:** the special case exists to
  release planning transactions before 4.0.7's bounded per-page work;
  the continuation changes neither the stage's transactions nor its
  completion owner, and the successor hop needs no paging (two
  idempotent view legs bounded by one account's DDL).

The route-based startup pass is unaffected: its upgrade rows already
drive the registered final for snapshot-captured accounts, and
`shouldRunTenantUpgrade` ignores the paged special case.

## 6. Compatibility and upgrade model (shutdown-only)

MatrixOne supports shutdown upgrade only - stop every service, replace
the binaries, start the cluster - and does not support downgrade
(`upgrade-compatibility.md`). In-cluster peers always run one binary,
so this change neither needs, offers nor relies on mixed-version
behavior. Compatibility map of the change:

| Surface | Older binary wrote | This binary's behavior |
|---|---|---|
| Stored rows (mo_columns, mo_indexes.algo_params, ENUM/SET values) | unchanged formats (a non-goal touches none) | reads unchanged |
| Persisted COLUMNS/STATISTICS identities | the pre-correction projections (section 2.1's wrong values) | binds and serves them unchanged (I4) until the 4.0.19 hop rewrites both (I3) |
| Fresh publication (bootstrap init, CREATE ACCOUNT, subscriptions) | - | publishes corrected identities unconditionally (I2) |
| Remote dispatch and persisted placements | - | no new protocol condition exists (I5); every peer of a started fleet is this binary |
| Historical snapshots | pre-upgrade COLUMNS texts recorded under previous current generations (the NULL-selector and 'binary' MEDIUMINT templates, the upstream 4.0.18 published identity; older known layers) | bind through the D9-1/D14-1 recognition family (I6, 5.5); recorded text, projections and timestamp preserved |

- **Writer gates:** inherited values only - the entries'
  RequiredProtocolVersion (109 for the shared COLUMNS leg, 41 for the
  STATISTICS leg) and the handler's 118, all enforced by the framework
  inside hop transactions exactly as for every historical hop. No
  admission condition, survey or authoring floor appears anywhere in
  this change. The consolidated shared builder the legs use matches a
  stored definition against the published identity with upstream's
  authoring-form normalization (`CREATE VIEW`/`CREATE OR REPLACE
  VIEW`); that normalization is exact-template comparison - an
  unknown or user-edited definition still fails the check.
- **Reader continuity (I4/I6):** every function identity a previously
  persisted definition references remains implemented (one-argument
  INTERNAL_* metadata forms, the historical view families); the
  two-argument declared-label forms, MO_INDEX_SUB_PART and the
  unbounded GENERATION_EXPRESSION projection are additive. A live
  definition either is readable or refreshes to a readable identity.
  Historical snapshots record their own DDL text: the reader
  recognition family covers every recent current generation (I6, 5.5) -
  including the base-era NULL-selector MEDIUMINT identity - and binding
  returns the recorded text unchanged; the read path gains no error
  class and no write path.
- **Backup/restore:** a backup taken by an older binary restores on this
  binary as at base, carrying its persisted identities and tenant
  markers; markers restore unchanged, so the 4.0.19 hop refreshes the
  migrated definitions exactly as on the source cluster (I3), and a
  backup taken by a main-era 4.0.18 binary restores through the same
  refresh - its COLUMNS identity is the frozen 4018 generation (5.5). The
  admission/restore model of revisions 4-5 is withdrawn with its
  machinery; nothing in this change moves any admission-state value, so
  a restored cluster derives admission exactly as base does.
- **Consolidated 4.0.7 stage (upstream behavior; admission preserved by revision 11):** a
  route from below 4.0.7 walks main's consolidated 4.0.7 entries first;
  those entries publish the identities their recorded constants name
  (main's COLUMNS entry publishes this branch's corrected current
  symbol; main's STATISTICS entry publishes the kept historical base
  identity), and on a chained route the v4_0_18 hop's COLUMNS leg
  already installs the merged identity first (its shared entry
  resolves the same identity symbol in the merged tree, D13-2). The
  4.0.19 legs run afterwards and only write on failed
  checks, so every routed account converges on the corrected
  identities (D10-4); a current account is untouched by the hop. The
  admission guards the pre-rework upstream keyed on
  `final.Version == "4.0.7"` (the legacy
  CDC-task prerequisite and the two frozen-offset rejections;
  view-metadata catalogs already use the `>= 4.0.7` line) are re-keyed
  by current main onto the consolidated-line/route predicates and the
  registered-handle identity, adopted verbatim (D13-3) with this
  design's recovery-path guards (D11), so the successor preserves
  exactly the acceptance and rejection outcomes a current-main binary
  produces for every recorded source state (5.6); the message texts,
  error types and ordering before catalog mutation are unchanged.
- **Pre-existing machinery untouched:** the 118 view-metadata admission
  handoff, the 126 VIEWS authoring floor and publication rungs, the
  130-134 upstream protocol planes, the VIEWS maintenance and the
  columns/tables maintenance keep base behavior, base constants and
  their existing coverage; retiring any of them is a dedicated cleanup
  change's scope (D8-5).
- **Rollout:** the repository's shutdown procedure itself.
  Population convergence at final 4.0.19 follows the route mechanisms
  (D13-1/D14-2): a ready 4.0.7 cluster's background route queues the
  single direct [4.0.7 -> 4.0.19] upgrade executed by this handler
  alone; a main-era 4.0.18 stamp queues the direct [4.0.18 -> 4.0.19]
  upgrade; below-4.0.7 sources chain their per-handle rows through
  the frozen 4.0.7 stage and the 4.0.18 handle; the on-demand login
  and manual walks step every successor handle (5.6), so first-login
  and manual convergence reaches the final before success is cached;
  fresh accounts initialize at 4.0.19 with the identities already
  installed, and no hop leg runs for them. No operational ordering,
  pause point or per-component schedule belongs to this design.

## 7. Verification plan (cheapest deterministic validation per invariant)

Suites that pinned the withdrawn machinery - the V-A/V-B/V-C
admission-boundary sets, the L1-L3 login-boundary cases, the R1-R4
restore matrix, the m1-m8 repair-lifecycle harness and its adaptive
race plan, the serving-floor adapter UT, and the
DeclaredCatalogMetadataFunctions protocol UTs - are deleted with the
code they cover; they have no subject under revision 8. The retained
plan under the reworked invariants:

- **I1/I4 SQL and component oracles (carry over verbatim,
  [recorded])**: `TestIssue30936EnumSetColumnCharacterMetadata`,
  `TestIssue30936IndexPrefixSubPartMetadata`,
  `TestIssue30936GeneratedExpressionMetadataLength`,
  `TestIssue30936DeclaredLabelEncodingBoundary` (pkg/tests/issues;
  bug-class red on pristine base and green on the fixed tree; probes
  recorded in the PR evidence), the component UTs for the label-aware
  helpers and MO_INDEX_SUB_PART (pkg/sql/plan/function), and the SHOW
  INDEX Sub_part projection cases.
- **I1 encoding-boundary oracle (carry over, [recorded])**: the
  colliding-declaration matrix on attr_enum-seeded stores (section
  2.3), with the SET rejection control.
- **I2 publication tests (retargeted)**: fresh-runtime tests prove
  CREATE ACCOUNT and bootstrap init install exactly the corrected
  COLUMNS/STATISTICS identities, with no survey, knob or
  authoring-floor seeding. On the merged base, account creation
  installs the complete current definition set or aborts (the old
  protocol ladder and its fallback rungs are gone upstream), and
  `TestCreateTablesInformationSchemaPublishesDeclaredMetadataUnconditionally`
  publishes this unconditional contract beside upstream's own
  account-creation contract matrix (peer-answer rejection cases).
- **I3 hop tests (retargeted, pkg/bootstrap)**: drive the v4_0_19
  handler and the tenant-upgrade transactional harness - a tenant
  below 4.0.19 with pre-correction identities is rewritten to exactly
  the published identities; a current tenant is a no-op; a mid-hop DDL
  error leaves the marker untouched and the next pass repairs it; the
  shared entries keep the consolidated builder's CheckFunc semantics
  (authoring-form-normalized identity match; unknown text fails). The
  existing columns/statistics upgrade suites - merged with upstream's
  #31497 harness rework - the v4_0_19 hop suite and upstream's merged
  v4_0_18 suite carry these cases; the deleted 4.0_14/4.0_15/4.0_17-era
  superseded hop suites stay deleted with main's consolidation. Route
  fixtures cover the appended-registration ordering, the three-leg
  chained census for a below-4.0.7 source ([.. -> 4.0.7 -> 4.0.18 ->
  4.0.19]), the single direct [4.0.7 -> 4.0.19] row for the ready
  valid frozen identity, and the direct [4.0.18 -> 4.0.19] row for a
  main-era stamp (D14-3). The "ready 4.0.7 at an older offset routes
  directly" oracle earlier rounds planned is withdrawn: those offsets
  terminate (next entry).
- **Admission oracles (revision 11, re-derived at the merged
  registration by revision 13)**: with the successor final registered
  at "4.0.19" on the merged three-handle ladder, upstream's guards
  keep firing.
  TestLegacyCDCAdmissionBeforeUpgradeMutation, extended to the merged
  registration, drives a ready 4.0.6 source and a created
  (4.0.7, frozen-offset) source: remaining paused tasks terminate with
  the upstream CDC message before any catalog mutation (the harness's
  unexpected-SQL ordering proof keeps asserting that no mutation SQL
  precedes the rejection), an empty task catalog admits onward, a
  catalog-read error stays the retryable wrapped error, and the
  recovery/resume path for an unfinished [4.0.6 -> 4.0.7] route
  enforces the same admission before the resume verdict.
  TestV407RejectsDifferentOffsetBeforeCreatingRoute and
  TestV407RejectsInterruptedOffsetOnlyRouteBeforeMutation are
  re-derived at the 4.0.19 final (including the crossed-route
  non-rejection upstream's pending-scan permits, D13-3): ready
  (4.0.7, 1) and (4.0.7, 1000) terminate with "4.0.7 is frozen"
  before mutation; an interrupted
  (4.0.7, frozen-offset) route with a 4.0.7 -> 4.0.7 step terminates
  with "unsupported in-progress 4.0.7 offset-only upgrade" at both
  the pre-mutation check and the recovery validation (a
  canResumeUpgrade-eligible unready row); valid semantic-route
  recovery resumes only with the admission enforced. Upstream's
  terminalUpgradeError type, finalVersionCompleted=false and message
  assertions carry over verbatim, and a successful direct-succession
  case (ready (4.0.7, frozen offset) -> route created) pins the allowed
  transition.
- **Continuation oracles (added by revision 12, deterministic - using
  the real v4_0_7 handler plus the successor registration, not the
  synthetic-handler substitution TestUpgradeTenant uses)**:
  (a) late-account login: `MaybeUpgradeTenant` with no caller
  transaction for an account at create_version 4.0.6 in a ready
  final-4.0.19 cluster ends at create_version 4.0.19 with the
  corrected persisted identities observed, the success cache set, and
  a second login an idempotent no-op; (b) manual already-at-4.0.7:
  `UpgradeOneTenant` under the 4.0.19 final executes the successor
  legs (at upstream's re-entrant loop shape, ending in-txn with the
  completion check; the register walk does not break at the v4_0_7
  branch and cache success with nothing done) and stamps 4.0.19;
  (c) manual from 4.0.6 chains the paged 4.0.7 stage and the
  successor; (d) successor-leg
  DDL failure leaves the account at its last committed version, the
  success cache unset, and a retry completing without duplicated
  account-visible effects - proving no premature caching and exact
  register-resume semantics. The suite asserts ordering: no successor
  SQL runs before the 4.0.7-stage completion commits.
- **I5 no-gate regressions (kept green)**: the CI-owned multicn
  regression `TestDatabaseDefaultsTwoCNPreparedFreshness` (prepared
  binds of information_schema at a lowered negotiated knob) returns to
  its base passing behavior - with the PR-specific requirement
  mappings removed, the requirement ladders are byte-equivalent to
  base, and the failure that motivated revision 7 is structurally
  impossible.
- **I6 historical-transition cases (added by revision 9,
  deterministic - the reader-predecessor regression the design review
  required)**: H1a (pkg/util/sysview suites
  `view_columns_history_test.go` / `predefined_test.go`): the snapshot
  adapter and the derived view-metadata seam recognize both
  predecessor identities (`InformationSchemaColumnsV110DDL`,
  `InformationSchemaColumnsBinaryFamilyNullDDL`) byte-stably, the
  migration-gate predicate accepts both predecessors beside V100 and
  the corrected current, and an unknown definition stays refused at
  every seam - exactly the recognition set of 5.5. H1b (pkg/sql/plan
  `view_columns_history_test.go`, the `historicalViewColumnsSQL`
  driver): with the live COLUMNS view at the corrected identity (the
  post-hop gate), a snapshot recorded at the base-era current
  definition binds successfully - the returned bound SQL is the
  snapshot's recorded text (its own pre-correction projections and its
  recorded timestamp preserved; the snapshot is deep-copied before the
  call and compared unchanged after it), a lowercase view-name variant
  adapts identically, and an unknown definition still fails with
  exactly "unsupported historical information_schema.COLUMNS
  definition". Revision 14 extends both to the 4.0.18 generation:
  every seam recognizes the frozen 4018 identity byte-stably with the
  unknown-definition refusal intact, a snapshot recorded while the
  4.0.18 identity was live binds after the 4.0.19 refresh with its
  recorded text, recorded projections and recorded timestamp preserved
  (the modified or unrelated definition still failing with exactly
  "unsupported historical information_schema.COLUMNS definition"),
  and the pre-refresh leg pins a live 4.0.18 definition still
  gating legacy snapshot adaptation while passing the migration gate -
  no silent pass-through regression (D14-4). The pre-upgrade-snapshot
  -> corrected-live -> historical-query transition is pinned
  deterministically at both seams; the end-to-end AS OF legs ride the
  repository's existing snapshot/history suites in CI.
- **CI selection (unchanged scope)**: the bootstrap package suite, the
  sqlintegration reconcile, the upgrade e2e suite, sysview suites, and
  the distributed BVT result files for the corrected identities
  ([recorded] green at head).
- **Static scope:** gofmt/vet/lint over the changed closure per the
  repository's incremental rules.

Acceptance criteria: every retained invariant maps to a deterministic
oracle (I1/I4 defect-value and boundary matrices; I2
unconditional-identity installation; I3 rewrite/no-op/marker behavior;
I5 base-equivalent gates; I6 historical-transition cases H1a/H1b);
revision 11's admission oracles pin every preserved guard (legacy CDC
admission at creation, recheck and recovery; frozen-source rejection;
interrupted offset-only rejection; direct succession from the valid
frozen identity) at the actual successor registration; revision 12's
continuation oracles pin the on-demand walks reaching the successor
(late-login convergence, already-4.0.7 manual upgrades, failure/retry
without premature caching) at the real-handler plus successor
registration; the
base-red/tree-green defect discriminators keep holding - the defect
corrections are unchanged by revisions 8-9; the deleted suites are
removed in the same implementation turn as the code they cover, leaving
nothing failing or orphaned; no sleeps, live services or new harnesses
are introduced.

## 8. Open decisions and risks

- **Encoding follow-up (non-blocking, separate change, unchanged):**
  the attr_enum comma-escaping defect of section 2.3 keeps its own
  owner, scope and decision point; this design pins the boundary with
  the section-7 oracle instead of claiming parity.
- **Version markers (framework behavior, unchanged):** accounts marked
  4.0.19 refuse older binaries under exactly the startup/login rules
  every prior version bump introduced, and backups restore under the
  framework's marker rules; revision 8 adds no new restore dimension
  because no admission state or protocol generation moves. The
  vehicle renumbers (4.0.18 -> 4.0.8 at revision 10, 4.0.8 -> 4.0.19
  at revision 13) affect only runtimes that already recorded the
  unreleased branch's marker - this project's own validation clusters,
  which rebuild their runtime data; every released binary records
  4.0.18 or lower and routes validly (a 4.0.18-stamped tenant crosses
  the 4.0.19 refresh directly, 5.6).
- **Convergence window (accepted, framework-timed):** until a given
  account's hop leg completes, that account reports pre-correction
  values (I4) - the same convergence timing every version-held
  correction (e.g. #30362) already has; the hop runs at each
  account's first post-upgrade pass.
- **Grandfathered gates (accepted, explicit):** the pre-existing
  per-feature gates and admission machinery remain in the tree at
  their base values under their existing coverage; this change must not
  be read as extending their pattern (D8-5).
- **Admission preservation (resolved by revisions 11-13; the earlier
  non-goal note is withdrawn):** main's `checkUpgrade` tied the legacy
  CDC-task prerequisite and the two frozen-4.0.7 rejections to
  `final.Version == "4.0.7"`, which any successor final silences. The
  pre-rework branch recorded that as an upstream non-goal and was
  corrected: the successor owns preserving the safety its renumbering
  would disable. Current main re-keys the guards onto the
  consolidated-line/route predicates and the registered-handle
  identity, this design adopts that verbatim, keeps the recovery-path
  guards and extends the admission oracles (D11-1/D11-2/D13-3,
  sections 5.6 and 7). Upstream-identical outcomes - terminal
  rejection of ready off-frozen 4.0.7 sources, interrupted
  offset-only routes and unadmitted CDC-task sources, and admission
  of every supported transition including the crossed-route
  non-rejection - hold at the 4.0.19 registration; the
  docs/guides/upgrade-4.2-to-main.md promise ("The new CN also
  rejects startup if tasks remain") keeps holding.
- No blocking question remains for design approval; any reviewer-added
  decision enters this document as a new revision before implementation
  changes.

## 9. Implementation conformance and deviations

The implementation of this design is delivered by PR
matrixorigin/matrixone#31489 as one squashed semantic change, rebased
per the design's rework rounds onto main's consolidated ladder; the
head that carried the revision-14 implementation was
61da7f8ef005f2ee14a3ea44e738e6baee24876d (this revision 15 changes
documentation only). The committed production change is the design's
selected shape:

- the additive declared-label metadata helpers and the new
  MO_INDEX_SUB_PART function (id 623, FUNCTION_END_NUMBER 624);
- the merged current COLUMNS identity (upstream's attributed-DECIMAL
  decoration composed over the declared-label projections and the
  unbounded GENERATION_EXPRESSION row) and the prefix-aware STATISTICS
  identity, published unconditionally through bootstrap init, CREATE
  ACCOUNT and the subscription wrapper (5.1);
- the v4_0_19 refresh hop with the two shared-builder legs (5.2),
  upstream's re-keyed admission guards adopted verbatim with this
  design's recovery-path guards, and the re-entrant on-demand planning
  loops with the manual-walk completion check (5.6);
- the byte-stable historical recognition family, including the frozen
  `InformationSchemaColumns4018DDL()` identity, at the three reader
  seams (5.5);
- the withdrawn mixed-version machinery of the pre-revision-8 rounds
  deleted with its suites, and the grandfathered gates left at their
  base values (4.5, I5).

Deviations from the approved design: none. The oracle and fixture
re-derivations the approved decisions plan (D13-7, D14-3) included
re-deriving upstream's crossing-admission fixtures to this delivery's
composed route matrix - planned work involving no production change.
The per-round rebase resolution maps, validation receipts and
red/green logs are retained with the PR's review evidence rather than
in this document. Any later implementation change that deviates from
these decisions requires a new revision of this document - and design
re-review for a material deviation - before it lands.

## Standing decision record

The blocks below keep each approved round's decisions in their exact
wording, newest first, together with the tree facts they were
verified against. A later block amends an earlier decision only
where it says so, and the standing sections above already apply those
amendments; version literals in older blocks name the vehicle as that
round defined it (4.0.18 at revision 9, 4.0.8 at revisions 10-12,
4.0.19 at revisions 13-14). The blocks exist so review can audit
decisions against their recorded wording and evidence.

Verified facts (checked at origin/main 97b2261005 before revision 14
was written):

1. A binary that served the upstream 4.0.18 published identity
   (`InformationSchemaColumnsDDL =
   informationSchemaColumnsWithUnsignedDecimalDDL()`,
   pkg/util/sysview/predefined.go:731 at 97b2261005) persists exactly
   that identity tenant by tenant, and field binaries do persist
   intermediate identities (the upstream contract's own record: "On a
   v4.2.6 source, unmodified main reached 4.0.17"). Once the merged
   constructor composes, no live constant equals the published 4.0.18
   text, so the refresh's recognition family must carry a frozen copy
   of it.
2. Refreshing live views cannot repair recorded snapshot text: with
   the 4.0.18 text unrecognized, the historical reader returns ("",
   false) and pkg/sql/plan/view_columns_history.go:87-92 fails with
   "unsupported historical information_schema.COLUMNS definition";
   before a 4.0.18-stamped tenant crosses the refresh, the same gate
   skips adaptation silently (the recorded definition passes
   un-adapted).
3. The background route creator appends a single direct row for a
   final-eligible latest (`if final.CanDirectUpgrade(latest) {
   append(final) }`, pkg/bootstrap/service_upgrade.go:442), each row
   executed by exactly the handle at its ToVersion
   (service_upgrade.go:798; service_upgrade_tenant.go:327), and the
   chained else-branch exists only for a latest below final's
   MinUpgradeVersion - a ready 4.0.7 cluster takes one
   [4.0.7 -> 4.0.19] row, and the on-demand handle walks are a
   separate mechanism from the route record.

Revision-14 decisions:

- **D14-1 (amends D13-2: freeze the upstream 4.0.18 identity and
  recognize it at every seam).** Add a frozen constructor for the
  identity the upstream 4.0.18-era binaries published and persist
  (`sysview.InformationSchemaColumns4018DDL()` or an equivalent frozen
  name): upstream's attributed-DECIMAL branch-0 decoration (the exact
  `columnTypeExpr -> decoratedColumnTypeExpr` replacement, applied to
  the first union branch only, including the varchar(65535) pin and
  the att_is_unsigned guard) applied verbatim over the frozen
  binary-null pre-attributes template - built from frozen bytes, NOT
  from the mutable current constructor, so later revisions of the
  merged current cannot drift it. A UT pins its text byte-stable
  (built only from the frozen T0 pair and the literal decoration,
  mirroring upstream's own "a test pins it byte-identical to the
  live pre-decoration derivation" rule), and the rebase round
  verifies the constructed text byte-identical to
  `informationSchemaColumnsWithUnsignedDecimalDDL()` as evaluated at
  97b2261005. Recognize it at every seam exactly as the base-era pair
  is recognized: `AdaptLegacyInformationSchemaColumnsDDL` gains a
  byte-stable pass-through branch (the 4.0.18 identity already
  carries current column authority and projections, the same rule as
  upstream's pre-attributes pass-through), `ViewMetadataColumnsDDL`
  adds it to the known set, and
  `IsInformationSchemaColumnsMigrationDDL` adds it to the gate set
  beside V100, the base-era pair and the merged current. The complete
  recognition union becomes: the historical templates
  (V41/V46/V46-upgrade/V58/V100/V109), the base-era frozen pair (this
  branch's `InformationSchemaColumnsV110DDL()` /
  `InformationSchemaColumnsBinaryFamilyNullDDL()` plus upstream's
  byte-identical pre-attributes pair), the frozen 4.0.18 identity, and
  the merged current identity. `IsCurrentInformationSchemaColumnsDDL`
  keeps excluding the 4.0.18 text (it is not the merged current and
  no #31160-class replay-drift tolerance extends to it), so recorded
  4.0.18 text always routes to the adapter where it now binds. The
  undecorated label-aware intermediate stays dev-lineage-only and
  unrecognized (this branch never shipped it in any binary); unknown
  or user-edited definitions stay refused at every seam;
  view_columns_history.go:87-92 keeps its rejection line unchanged.
- **D14-2 (amends D13-1: population routes at the merged ladder, per
  mechanism).** Background route census at final 4.0.19: a ready-4.0.7
  cluster takes the single direct [4.0.7 -> 4.0.19] row
  (service_upgrade.go:442), executed by the v4_0_19 handler alone
  (COLUMNS leg -> merged identity; STATISTICS leg -> prefix-aware
  identity); a main-era 4.0.18-stamped tenant takes the single direct
  [4.0.18 -> 4.0.19] row (`CanDirectUpgrade` accepts from = 4.0.18),
  whose COLUMNS leg rewrites the decorated-plain live text (the
  frozen 4018 identity) to the merged identity and whose STATISTICS
  leg installs the prefix-aware identity; a below-4.0.7 source falls
  to the chained else-branch rows ([.. -> 4.0.7 via the frozen
  handler, 4.0.7 -> 4.0.18, 4.0.18 -> 4.0.19]), each executed by its
  own handler, the 4.0.18 row installing the merged COLUMNS identity
  that the 4.0.19 row then re-checks content-equal; fresh clusters
  initialize at 4.0.19 and run no leg. The on-demand walks remain the
  separate login-time/manual mechanism that steps the handle list
  numerically - a ready-4.0.7 account walks both successor handlers
  in-walk (D13-4 unchanged). Section 6's population table is annotated
  with the direct-row form so no reader mistakes the route record for
  the handle chain (the repairs reach every population either way,
  because the v4_0_19 handler carries both legs).
- **D14-3 (amends D13-7: the direct-route validation census).** Route
  fixtures re-derive as: ready-4.0.7 latest -> exactly one
  VersionUpgrade {FromVersion "4.0.7", ToVersion "4.0.19",
  FinalVersion "4.0.19"} (the service_upgrade_test /
  service_statistics_upgrade_test chain fixtures become the direct
  single-row census; checkUpgradeRecovery's chaining invariants -
  UpgradeOrder positions and FromVersion == previous ToVersion - hold
  trivially at one row); below-4.0.7 source -> the three-row chained
  census retained, including recovery-resume cases across the frozen
  row; 4.0.18-stamped latest -> the direct [4.0.18 -> 4.0.19] census;
  on-demand continuation fixtures unchanged (D13-4). All other D13-7
  census entries (the v4_0_19 upgrade_test, columns_upgrade_test, the
  cdc/recovery lattices, embed/catalog_view_identity_test, the
  sqlintegration registry-derived final, function_id_test, and the
  view_columns_history merged-known-set fixtures) stand as written.
- **D14-4 (extends section 7's I6/H1a/H1b: the 4.0.18 snapshot
  transition oracle).** H1a (pkg/util/sysview suites): every seam
  recognizes the frozen 4018 identity byte-stably beside the base-era
  pair - adapter pass-through equality, `ViewMetadataColumnsDDL`
  known-set membership, migration-gate membership - and an unknown
  definition stays refused at every seam. H1b (pkg/sql/plan
  view_columns_history_test.go, the historicalViewColumnsSQL driver):
  with the live COLUMNS view at the merged corrected identity (the
  post-4.0.19-refresh gate), a snapshot recorded while the 4.0.18
  identity was live binds successfully - the returned bound SQL is the
  snapshot's recorded 4.0.18 text with its recorded projections and
  recorded timestamp preserved (the snapshot is deep-copied before the
  call and compared unchanged after it, as the base-era H1b case
  does); the lowercase-authoring variant adapts identically; a
  modified or unrelated definition still fails with exactly
  "unsupported historical information_schema.COLUMNS definition". The
  pre-refresh leg is also pinned: with the live view still at the
  4.0.18 identity (a 4.0.18-stamped tenant before the crossing), the
  migration gate accepts that live definition and a legacy snapshot
  definition still adapts - no silent pass-through regression. CI's
  end-to-end AS OF legs ride the repository's existing
  snapshot/history suites.

Verified upstream facts at origin/main 97b2261005, checked in the tree
before deciding:

1. **Main consumed the post-consolidation successor slot as 4.0.18.**
   `initUpgrade` (pkg/bootstrap/upgrade.go) registers
   `..., v4_0_7.Handler, v4_0_18.Handler`; the handle at
   pkg/bootstrap/versions/v4_0_18/upgrade.go carries
   `{Version: "4.0.18", MinUpgradeVersion: "4.0.7", UpgradeCluster: No,
   UpgradeTenant: Yes, VersionOffset: uint32(len(tenantUpgEntries)) (= 1),
   RequiredProtocolVersion: defines.MORPCVersion126}` (MORPCVersion126
   pre-exists at the merge base, pkg/defines/const.go), and its sole entry
   is `versions.InformationSchemaColumnsUpgradeEntry()` - the same shared
   entry revision 12 carries. The upstream contract that merged with it
   (docs/design/20261028-catalog-upgrade-407.md, issue #31490) fixes the
   numbering rule revision 10 read differently: "Do not register the
   unpublished 4.0.8-4.0.17 identities as new upgrade steps" and "assign
   later migrations a new semantic catalog version", the same doc also
   records "On a v4.2.6 source, unmodified main reached 4.0.17" - i.e.
   intermediate identities WERE recorded by field binaries, so D10-1's
   rationale ("no released binary ever recorded 4.0.8..4.0.17") is fact
   superseded, and the minimum legal successor is 4.0.19.
2. **Registering "4.0.8" in any position is now deadlocked.**
   Appending it last (final "4.0.8" < the registered "4.0.18") makes
   `MaybeUpgradeTenant`'s advance guard (pkg/bootstrap/
   service_upgrade_tenant.go: `versions.Compare(currentCN.Version,
   version) < 0` -> "tenant version %s is greater than current cn version
   %s") reject every tenant a main-era binary stamped 4.0.18 - the
   population the merged line must upgrade - and every legacy
   4.0.9..4.0.17 stamp. Parking it before v4_0_18 leaves main's handle
   as final: tenants a main-era binary already stamped 4.0.18 equal the
   final, take the version-equality short-circuit (`currentCN.Version ==
   version` -> nil), cross no hop, and never receive this PR's
   STATISTICS Sub_Part identity nor the label-aware COLUMNS identity -
   the exact defects #31489 exists to repair persist for that population,
   and the contract's numbering rule is violated outright.
3. **Upstream re-keyed the same four admission guards D11 re-keyed, with
   a more precise CDC predicate.** pkg/bootstrap/service_upgrade.go at
   97b2261005 replaces the `final.Version == "4.0.7"` literals with
   `versions.Compare(final.Version, "4.0.7") >= 0` comparisons, keys both
   frozen-identity rejections on `v4_0_7.Handler.Metadata().VersionOffset`,
   and gates both `rejectLegacyCDCTasks` call sites on the new
   `crossingPending`/`crossingPendingInRoute` helpers (pkg/
   bootstrap/service_upgrade.go:66-109): rejection fires only while the
   retained route still owes the 4.0.7 crossing (a route whose 4.0.7 step
   is Ready crossed with its own admission and is not re-rejected).
   D11-1's branch predicates are subsumed; D11-1's recovery-path addition
   and D11-2's recovery offset-only rejection are absent upstream and
   remain necessary (a final above 4.0.7 reaches checkUpgradeRecovery
   without any other admission for a retained 4.0.7 stage).
4. **Upstream restructured both on-demand walks D12 patched, as
   re-entrant planning loops.** `MaybeUpgradeTenant`
   (service_upgrade_tenant.go) and `UpgradeOneTenant`
   (custom_upgrade.go) wrap the planning transaction in a `for { pendingV407
   := false; ...; if !pendingV407 { break }; s.upgradeV407TenantOnDemand
   (...) }` loop that re-enters planning after each paged crossing, skip
   the frozen handle in-walk when the account is already at 4.0.7
   (`continue`), and (MaybeUpgradeTenant only) terminate in-txn with
   `if !pendingV407 && from != currentCN.Version` -> "cannot upgrade
   tenant version %s to current cn version %s". The continuation D12
   built through the service-owned `upgradeTenantToFinalVersionOnDemand`
   transaction is therefore owned upstream by the re-planned walks;
   the manual loop upstream still lacks the in-txn completion check.
5. **The merged COLUMNS identity composes string-wise.** Upstream
   defines the current identity as `InformationSchemaColumnsDDL =
   informationSchemaColumnsWithUnsignedDecimalDDL()`, which splits
   `informationSchemaCurrentColumnsDDL()` at " UNION ALL ", and replaces
   only the branch-0 COLUMN_TYPE expression with the attributed-DECIMAL
   decoration (ancient constructs only, cast varchar(65535), legacy
   unsigned-integer rows byte-identical); frozen pre-change identities
   `informationSchemaColumnsPreAttributesDDL()` (V109 lineage +
   MEDIUMINT arm) and `...BinaryNullDDL()` (its #31461-era NULL-selector
   variant) are wired into `AdaptLegacyInformationSchemaColumnsDDL` and
   `ViewMetadataColumnsDDL`. This PR's derivers rewrite disjoint anchors -
   the four metadata projections `internal_char_length/internal_char_size/
   internal_numeric_precision/internal_numeric_scale` on all four
   branches, and the unbounded GENERATION_EXPRESSION row - so taking
   upstream's assignment verbatim over the revision-12
   `informationSchemaCurrentColumnsDDL()` yields the merged current:
   DECIMAL-decorated branch 0 with label-aware metadata and unbounded
   generated expressions. Upstream's `InformationSchemaColumnsUpgradeEntry()`
   references `sysview.InformationSchemaColumnsDDL` directly
   (pkg/bootstrap/versions/information_schema_columns.go:24), so both
   hops converge on the merged value at runtime. The undecorated
   label-aware intermediate and the decorated plain-projection intermediate
   are dev-lineage-only texts ("unreleased intermediate catalogs" are out
   of scope by the same contract) and enter no recognition set.
6. **Function ids 620-622 are allocated.** upstream reserved
   `UNSIGNED_DECIMAL_ADMISSION = 620`, `_STRICT = 621`, `_IGNORE = 622`
   (FUNCTION_END_NUMBER 623); revision 12's `MO_INDEX_SUB_PART = 621`
   conflicts. The id is internal-only (plant function wiring, view DDL
   references the name), so renumbering is safe.
7. **The BVT identity rows re-derive from the merged binary.**
   four .result rows render the COLUMNS identity or its descriptor:
   test/distributed/cases/dml/show/show.result,
   test/distributed/cases/zz_accesscontrol/account_restricted.result
   (both already conflicting: upstream's decoration grows Create View to
   [12,18848,0]), and the two this-rebase-only generations
   test/distributed/cases/database/system_table.result,
   test/distributed/cases/system_variable/system_variables.result
   (GENERATION_EXPRESSION descriptor row), that must be regenerated per
   the aec8645e3a alignment precedent. Upstream's new dtype case
   (decimal_unsigned_zerofill.test) and the 12c8c6739f (#31775)
   list_builtIn.go vector overloads do not intersect this PR's seams
   (auto-merge regions, no shared anchors).

Revision-13 decisions:

- **D13-1 (vehicle: the next free semantic successor version, "4.0.19").**
  The stored-view refresh keeps its dedicated version handle, renumbered
  onto the merged consolidated ladder as folder
  `pkg/bootstrap/versions/v4_0_19` (renamed from v4_0_8), metadata
  `{Version: "4.0.19", MinUpgradeVersion: "4.0.7", UpgradeCluster: No,
  UpgradeTenant: Yes, VersionOffset: uint32(len(tenantUpgEntries)) (= 2),
  RequiredProtocolVersion: defines.MORPCVersion118}` (the historical
  grandfathered 4.0.7-protocol floor, per D10-6; no protocol constant
  moves). Registration appends `v4_0_19.Handler` after `v4_0_18.Handler`
  (numeric order: 4.0.7 -> 4.0.18 -> 4.0.19), so the final is "4.0.19"
  and every population routes validly: below-4.0.7 clusters cross the
  frozen 4.0.7 stage, then 4.0.18 (COLUMNS refreshed to the merged
  identity), then 4.0.19 (the COLUMNS leg re-checks as content-equal
  after a same-binary 4.0.18 crossing and writes nothing; the STATISTICS
  leg writes the prefix-aware identity); ready 4.0.7 tenants step
  through the same forward chain; tenants a main-era binary stamped
  4.0.18 skip the 4.0.18 hop outright (`Compare("4.0.18","4.0.18")` is
  not `> 0`) and take the direct [4.0.18 -> 4.0.19] hop
  (`CanDirectUpgrade` = `Compare(MinUpgradeVersion="4.0.7", from) <= 0`
  accepts from = 4.0.18), whose COLUMNS leg performs the label-aware
  repair their decorated-plain identity lacks and whose STATISTICS leg
  installs the prefix-aware identity; fresh clusters initialize
  at 4.0.19 with the merged identities installed in the account
  transaction, so no hop leg runs for them.
- **D13-2 (COLUMNS identity and recognition sets: compose upstream's
  decoration over the revision-12 derivers).** Take
  `informationSchemaColumnsWithUnsignedDecimalDDL()`, both frozen
  pre-change constructors and upstream's seam wiring verbatim; keep
  revision 12's `informationSchemaCurrentColumnsDDL()` (label-aware
  metadata projections + unbounded GENERATION_EXPRESSION), D9-1/D10-3
  recognition sets and every current-identity test subject, extended so
  each seam (AdaptLegacy pass-through, ViewMetadataColumnsDDL known set,
  IsInformationSchemaColumnsMigrationDDL gates, snapshot and
  view-metadata readers) holds the union: upstream's pre-attributes pair
  plus this branch's `InformationSchemaColumnsV110DDL()` and
  `InformationSchemaColumnsBinaryFamilyNullDDL()` (the binary-null pair
  pins the same field generation; duplicate memberships are harmless)
  plus the merged current identity. Historical templates and the
  STATISTICS template stay byte-stable; unknown or user-edited
  definitions stay refused at every seam; the recognized characterization
  composes upstream's `TestInformationSchemaColumnsDDL_
  UnsignedDecimalLocalBranchOnly`/`_UnsignedDecimalAncientConstructs`
  oracles with the revision-12 `TestInformationSchemaColumnsDDL_
  DeclaredLabelMetadataProjections` oracles against the merged current.
- **D13-3 (admission guards: upstream's predicates own the two CDC call
  sites; the recovery guards stand).** Take upstream's
  `crossingPending`/`crossingPendingInRoute`/`crossingPendingInSteps`
  helpers and both re-keyed frozen-identity rejections verbatim
  (subsumes D11-1's checkUpgrade-side recheck/create branches); keep
  revision 12's checkUpgradeRecovery additions verbatim - the offset-only
  4.0.7 -> 4.0.7 leg terminal rejection and the frozen-4.0.7-target
  `rejectLegacyCDCTasks` admission (a retained route whose target is
  4.0.7 owes its crossing by definition, so upstream's pending-scan
  refinement and this admission cannot disagree). D11-3's oracle set is
  re-derived at the three-handle registration: every retained/crossing
  state is exercised at final 4.0.19 including the crossed-route
  non-rejection the pending-scan now permits, and upstream's own
  crossing/cdc harnesses (service_upgrade_crossing_test.go,
  upgrade_columns_4_0_18_test.go, upgrade_columns_cdc_4_0_18_test.go)
  carry over with their recorded 4.0.18 targets re-derived at the
  merged final where the ladder continues past them.
- **D13-4 (on-demand continuation: upstream's re-planned loops own it;
  the service-owned continuation transaction is withdrawn).** Take
  upstream's re-entrant planning loops in MaybeUpgradeTenant and
  UpgradeOneTenant verbatim; delete `upgradeTenantToFinalVersionOnDemand`
  (pkg/bootstrap/service_upgrade_tenant_v407.go) and both call sites,
  because a merged second planning pass after each crossing re-runs the
  register walk over the remaining handles in the same shape D12 required;
  add the in-txn manual-walk completion check
  `if !pendingV407 && from != currentCN.Version` -> upstream's
  "cannot upgrade tenant version %s to current cn version %s" message
  (upstream's manual loop lacks it), keeping D12's manual-success-only-
  at-final oracle. Every D12 continuation oracle is re-derived at the
  upstream loop shape: late-login convergence from 4.0.6 through both
  successor hops to 4.0.19 with corrected identities, second-login
  idempotence, already-4.0.7 manual upgrade, failure between the
  successor hops leaves the cache unset and resumes from the committed
  stamp, and no-successor-SQL-before-4.0.7-completion ordering.
- **D13-5 (function-id renumber).** `MO_INDEX_SUB_PART = 623`,
  `FUNCTION_END_NUMBER = 624`; the register entry, func_builtin.go
  implementation and list_builtIn.go wiring keep their revision-12 shape
  with the renumbered id; function_id_test.go's literal table merges
  upstream's three admission ids with the renumbered entry.
- **D13-6 (BVT identity rows regenerate from the merged binary).** The
  four .result rows under fact 7 are re-validated against a merged-binary
  run and any row that diverges is regenerated (for the two
  identity-text rows this is mandatory - the decoration grows the text;
  for the descriptor rows the result may prove byte-identical, and the
  regeneration then commits nothing; row text equal to
  sysview.InformationSchemaColumnsDDL() exactly, Create View length
  per the merged text, GENERATION_EXPRESSION descriptor as served);
  no other .result rows interact with this merge.
- **D13-7 (oracle re-derivation census).** All version-literal fixtures
  move to 4.0.19 or registry-derived finals:
  pkg/bootstrap/versions/v4_0_19/upgrade_test.go (renamed folder,
  latest-version/offset-aware fixtures with the appended-registration
  ordering variant), columns_upgrade_test.go, service_upgrade_test
  harnesses (service_statistics_upgrade_test.go chains
  {{"4.0.7","4.0.18"},{"4.0.18","4.0.19"}}), service_upgrade_cdc_test.go
  and service_upgrade_recovery_test.go at the two-successor lattice,
  embed/catalog_view_identity_test.go (registry-derived final plus the
  revision-12 identity assertions), pkg/tests/sqlintegration/
  tenant_catalog_initialization_test.go (the registry-derived oracle
  stands; upstream's "4.0.18" literal drops),
  pkg/sql/plan/function/function_id_test.go, and the view_columns_history
  suites' merged known-set fixtures (`preChangeCurrentColumnsDDL()` and
  the V110/binary-null constructors both recognized). The pre-existing
  pkg/frontend/session_upgrade_test.go mock ("4.0.8" fixture value,
  pre-dating both sides) is untouched.

Verified finding (checked in origin/main's tree before deciding):
the login-compensation and manual tenant-upgrade walks stop at the
paged 4.0.7 stage and cache success before reaching the final handle,
so with a successor registered a late or manually upgraded account
never executes the successor hop. Concretely:

1. `MaybeUpgradeTenant` (pkg/bootstrap/service_upgrade_tenant.go) -
   the login/late-account compensation - walks `s.handles` upgrading
   the account per register, but when it reaches the real
   `v4_0_7.Handler` with no caller-owned transaction it sets
   `pendingV407` and breaks out of the walk; `upgradeV407TenantOnDemand`
   (pkg/bootstrap/service_upgrade_tenant_v407.go) then advances the
   account only to 4.0.7 (its ToVersion is the v4_0_7 handle version)
   and the function caches `s.mu.tenants[tenantID] = true` and returns
   success. Both cache consults at its entry return immediately once
   set, so the account is never compensated further on that CN: a late
   account at 4.0.6 in a ready 4.0.8 cluster ends at 4.0.7, never
   receives the successor's refresh legs, and keeps serving the
   historical STATISTICS identity.
2. `UpgradeOneTenant` (pkg/bootstrap/custom_upgrade.go) - the manual
   upgrade - walks handles with a `>=` register comparison, so an
   account already at 4.0.7 still enters the v4_0_7 branch, sets
   `pendingV407 = false`, breaks, and then caches success without
   executing anything; below-4.0.7 accounts take the same on-demand
   v407 stage and stop at 4.0.7. Either way a manual upgrade never
   reaches a successor stage.
3. The existing tolerance/late-account test (pkg/bootstrap/
   service_test.go TestUpgradeTenant) appends synthetic handlers
   (`newTestVersionHandler`), so the real-handler special case is
   never exercised; no test drives compensation to a final above
   4.0.7.

Revision 10's rollout promise ("accounts below 4.0.8 converge
through their hop legs at first login or the startup tenant pass")
therefore had no backing mechanism for the login and manual paths; this
revision records the mechanism and its oracles.

Revision-12 decisions:

- **D12-1 (login compensation continues past the paged stage).** After
  the on-demand 4.0.7 stage commits (the account stamped at 4.0.7),
  `MaybeUpgradeTenant` resumes the authoritative walk from the
  stamped create_version through the remaining registered handles to
  `getFinalVersionHandle().Metadata().Version`, executing the
  successor handle's tenant upgrade (a plain
  `HandleTenantUpgrade` - this hop's two idempotent legs need no
  paging) in one service-owned transaction and stamping the account's
  create_version per handle, exactly as the walk does inside the
  planning transaction. Success is cached only once the walk has
  reached the final version - extending the function's existing
  `from != currentCN.Version` invariant across the paged boundary; any
  successor failure leaves the cache unset so the next login resumes
  from the stamped version. The paged 4.0.7 stage keeps its existing
  bounded transactions and completion ownership unchanged.
- **D12-2 (manual upgrade continues to the final).**
  `UpgradeOneTenant` applies the same continuation: the v4_0_7 branch
  break is no longer terminal - when the stamped account version is
  still below the final (including an account already at 4.0.7), the
  manual walk proceeds to the successor handle after the paged stage
  (or directly when 4.0.7 already stamped), stamps the final
  create_version, and only then caches success. No path exists that
  reports a successful manual upgrade with the account left below the
  cluster's final version.
- **D12-3 (continuation oracles at the real handler and the successor
  registration).** Deterministic tests register the real v4_0_7
  handler plus the successor handle (the actual `initUpgrade()` plus
  the v4_0_8 registration - no synthetic-handler substitution):
  (a) a late account at 4.0.6 in a ready final-4.0.8 cluster logs in
  (`MaybeUpgradeTenant` with no caller transaction) and ends at
  create_version 4.0.8 with the corrected persisted identities
  observed, the success cache set, and a second login an idempotent
  no-op; (b) a manual `UpgradeOneTenant` for an account already at
  4.0.7 under the 4.0.8 final executes the successor legs (no
  break-with-nothing-done) and stamps 4.0.8; (c) a manual upgrade from
  4.0.6 chains both stages; (d) a successor-leg DDL failure leaves the
  account stamped at its last committed stage, the success cache
  unset, and a retry completing without duplication - proving no
  premature caching and exactly-once account-visible effects.
  The tests reuse the existing mem-executor harness style and assert
  ordering (no successor SQL before the 4.0.7-stage completion
  commits).

The startup/route-based tenant pass is unchanged: route rows drive
the registered final for captured accounts and need no continuation fix
(verified in service_upgrade_tenant.go's task walk and
shouldRunTenantUpgrade's register comparison). The continuation
touches only the two on-demand walks; the paged 4.0.7 machinery, the
admission guards of revision 11 and all D8-D11 decisions are untouched.

Verified findings (each checked in origin/main's tree before deciding):

1. **Legacy CDC admission dies with the successor final.**
   `rejectLegacyCDCTasks` is called at exactly two sites of
   `checkUpgrade` (pkg/bootstrap/service_upgrade.go): the recheck branch
   keys on `final.Version == "4.0.7"` before accepting/completing a
   non-ready row, and the create-admission branch keys on
   `final.Version == "4.0.7" && (v < "4.0.7" || (v == "4.0.7" &&
   !v.IsReady()))` before `EnsureViewMetadataCatalogs` and route
   creation. With final = 4.0.8 both literals are false, so a ready
   4.0.6 source holding legacy CDC tasks proceeds into catalog
   migration. That violates the behavior docs/guides/
   upgrade-4.2-to-main.md:12-36 promises ("The new CN also rejects
   startup if tasks remain") and the documented reason in
   service_upgrade_cdc.go (a pre-4.0.7 task cannot establish the target
   identity the current CDC executor requires; paused tasks included;
   the diagnostic is capped). Upstream pins the behavior with
   TestLegacyCDCAdmissionBeforeUpgradeMutation (ready 4.0.6 and created
   4.0.7 sources: terminal rejection listing paused tasks;
   TestRejectLegacyCDCTasks covers the 32-task cap and
   the "more tasks exist" suffix; a catalog-read failure stays
   retryable and non-terminal). Revision 10 wrongly recorded this as an
   upstream non-goal in D10-6/section 8: the successor PR owns
   preserving the safety its renumbering would otherwise silently
   disable.
2. **The frozen-4.0.7 source and recovery validation die the same
   way.** Both terminal rejections in `checkUpgrade` - a ready 4.0.7
   row whose offset differs from the binary's ("catalog 4.0.7 offset
   %d differs from binary offset %d; 4.0.7 is frozen and further
   migrations require a new catalog version") and an in-progress
   offset-only 4.0.7 route ("unsupported in-progress 4.0.7 offset-only
   upgrade; 4.0.7 is frozen...") - require `v.Version == final.Version`
   and `final.Version == "4.0.7"`. With final = 4.0.8, every recorded
   4.0.7 state bypasses them: `CanDirectUpgrade` compares semantic
   versions only (versions/version.go:45-46), the route-creation walk
   skips every handle <= the recorded version, so a ready 4.0.7 catalog
   at an earlier offset (e.g. 4.0.7/0) would take the direct
   [4.0.7 -> 4.0.8] route, miss the consolidated 4.0.7 migrations
   (mo_database_defaults and its COLUMNS entry among them) and receive
   only the two view replacements. Unfinished 4.0.7-stage routes now
   divert into `canResumeUpgrade` -> `checkUpgradeRecovery` (their
   recorded version differs from the new final), whose step validation
   accepts From == To steps, so an interrupted 4.0.7 -> 4.0.7 offset-only
   route - terminally rejected by upstream
   (TestV407RejectsDifferentOffsetBeforeCreatingRoute,
   TestV407RejectsInterruptedOffsetOnlyRouteBeforeMutation) - would be
   silently resumed, and `shouldRunTenantUpgrade` reruns offset-only
   steps for accounts already at the target version because
   mo_account.create_version stores no offset. Revision 10's D10-1
   population claim ("a ready 4.0.7 at any offset upgrades directly")
   is corrected by this revision: only the valid frozen identity
   succeeds; upstream's binary rejects the rest and so must this one.

Re-key precedents already in the file: the adjacent
`EnsureViewMetadataCatalogs` admission uses
`versions.Compare(final.Version, "4.0.7") >= 0` with the same source
predicate, and `canResumeUpgrade` compares the recorded offset against
the registered handle's own metadata (per-handle identity, not the
final-version literal). The re-keyed guards follow both patterns and
touch only source-validation admission - they introduce no protocol
constant, no mixed-version path and no feature gate (D8-1/D10-6
guards keep holding).

Revision-11 decisions:

- **D11-1 (CDC admission keyed on the source/retained route).** Both
  `rejectLegacyCDCTasks` call sites lose the
  `final.Version == "4.0.7"` literal: the create-admission branch
  becomes `created && versions.Compare(final.Version, "4.0.7") >= 0 &&
  (versions.Compare(v.Version, "4.0.7") < 0 || (v.Version == "4.0.7"
  && !v.IsReady()))`, mirroring the adjacent EnsureViewMetadataCatalogs
  predicate (same block, before any catalog mutation); the recheck
  branch admits by the same consolidated-line predicate for any
  non-ready retained row (the retained route's stages include the
  4.0.7 stage for every final in this line). The recovery path gains
  the admission the finding requires: `checkUpgradeRecovery` enforces
  rejectLegacyCDCTasks after validating the retained steps and before
  returning the resume verdict when the retained target is the frozen
  4.0.7 stage (a final binary above 4.0.7 can otherwise reach that
  path without any admission - false in upstream's shape, true in
  ours).
- **D11-2 (frozen source validation keyed on the registered handle).**
  The ready-source rejection becomes
  `v.IsReady() && v.Version == "4.0.7" && v.VersionOffset !=
  v4_0_7.Handler.Metadata().VersionOffset` under
  `versions.Compare(final.Version, "4.0.7") >= 0`, terminating before
  route creation or any catalog DDL with upstream's message text
  unchanged. The in-progress offset-only rejection applies to the
  frozen identity itself - an unready (4.0.7, frozen-offset) row whose
  retained steps contain a 4.0.7 -> 4.0.7 leg - both at the pre-mutation
  check in `checkUpgrade` and in `checkUpgradeRecovery`, which now
  rejects same-version steps for the frozen target instead of
  accepting them. Unready (4.0.7, off-frozen-offset) rows stay
  rejected by `canResumeUpgrade`'s existing per-handle offset
  comparison, unchanged. Allowed exactly as upstream: direct
  succession from the valid frozen identity (ready 4.0.7 at the
  registered handle's offset -> [4.0.7 -> 4.0.8]), below-4.0.7 ready
  sources chaining through the frozen 4.0.7 stage with CDC admission,
  and recovery of a normal unfinished route (e.g. [4.0.6 -> 4.0.7])
  with CDC admission enforced before the resume verdict.
- **D11-3 (admission oracles at the actual 4.0.8 final handle).**
  TestLegacyCDCAdmissionBeforeUpgradeMutation is extended to the
  successor registration and covers: a ready 4.0.6 source and a created
  (4.0.7, frozen offset) source rejected before any catalog mutation
  with the terminal CDC message listing paused tasks; an empty task
  catalog admitting the source onward; a catalog-read error surfacing
  as the retryable wrapped error with no mutation; and the
  recovery/resume path (unfinished [4.0.6 -> 4.0.7]) enforcing the
  same admission. TestV407RejectsDifferentOffsetBeforeCreatingRoute
  and TestV407RejectsInterruptedOffsetOnlyRouteBeforeMutation are
  re-derived at the 4.0.8 final: ready (4.0.7, 1) and (4.0.7, 1000)
  terminate with "4.0.7 is frozen" before mutation; an interrupted
  (4.0.7, frozen offset) route with a 4.0.7 -> 4.0.7 step terminates at
  both the pre-mutation check and the recovery validation; ready
  (4.0.7, frozen offset) creates the direct route; a ready 4.0.6 source
  chains; a valid unfinished semantic route resumes. Upstream's
  assertions (terminalUpgradeError type, finalVersionCompleted false,
  message text) carry over verbatim. Revision 10's planned
  "successful older-offset route" oracle is withdrawn and replaced by
  these deterministic rejection cases.
- **D11-4 (D10-6 correction).** The admission guards are no longer
  recorded as untouched or as an upstream non-goal: the successor
  re-keys them to preserve upstream's acceptance/rejection outcomes
  for every recorded source state, and the section-8 risk bullet that
  defers the CDC guard's evolution is withdrawn with this decision. No
  gate, protocol constant or mixed-version mechanism appears anywhere
  in the correction (upgrade-compatibility.md stays satisfied); the
  change is confined to pkg/bootstrap source-validation admission and
  its tests.

Verified upstream facts at origin/main 3cf38889bc, checked in the tree
before this revision:

1. #31497 deleted the handles v4_0_8..v4_0_17 from the ladder
   ("superseded intermediate handlers are removed; version, account and
   tenant-task tables remain the only durable progress owners"): main's
   `initUpgrade` ends at v4_0_7.Handler, and revision 9's hop
   predecessor "4.0.17" no longer exists there.
2. Main froze 4.0.7: pkg/bootstrap/service_upgrade.go terminally
   rejects an in-progress offset-only 4.0.7 route ("unsupported
   in-progress 4.0.7 offset-only upgrade; 4.0.7 is frozen and further
   migrations require a new catalog version") and any ready 4.0.7 whose
   recorded offset differs from the binary's ("catalog 4.0.7 offset %d
   differs from binary offset %d; 4.0.7 is frozen and further
   migrations require a new catalog version"), because "4.0.7 uses
   account version as its only durable per-account progress marker.
   That marker cannot distinguish two offsets of the same version."
   Appending this PR's refreshes to the consolidated 4.0.7 entry list
   (offset growth) is therefore upstream-forbidden as a delivery
   vehicle and is rejected here too.
3. The consolidated 4.0.7 handler owns the catalog steps in entry lists
   (pkg/bootstrap/versions/v4_0_7/tenant_upgrade_list.go) built on a new
   shared view-refresh builder:
   pkg/bootstrap/versions/information_schema_view.go's
   `InformationSchemaViewUpgradeEntry(name, definition, protocol)`
   authors `CREATE OR REPLACE VIEW`, carries no PreSql DROP (CREATE OR
   REPLACE covers the missing-view case), checks through
   `CheckInformationSchemaViewDefinition` (lowercase fallback) and
   `sysview.InformationSchemaViewDefinitionMatches` over
   `canonicalInformationSchemaViewDDL` authoring normalization. The
   formerly hand-rolled COLUMNS entry now builds on it, and the
   hand-rolled statistics builder revision 9 added duplicates its shape.
4. pkg/util/sysview/predefined.go renamed the seam normalizer
   (`canonicalColumnsViewName` -> `canonicalColumnsViewDDL`, wrapped by
   the authoring-form `canonicalInformationSchemaViewDDL`) and exported
   `InformationSchemaViewDefinitionMatches`; the D9-1 seam edits sit in
   exactly those functions.
5. Route mechanics (`checkUpgrade`,
   `addUpgradesToFinalVersion`, `CanDirectUpgrade` = MinUpgradeVersion
   <= recorded) hold for a version after 4.0.7: a ready 4.0.7 at any
   offset upgrades directly to the new version, the frozen-offset
   rejections never fire because the final version differs, and
   clusters below 4.0.7 chain through 4.0.7 first.

Revision-10 decisions:

- **D10-1 (vehicle: the post-consolidation successor version).** The
  stored-view refresh keeps a dedicated version handle, renumbered onto
  the consolidated ladder: folder `pkg/bootstrap/versions/v4_0_8`,
  metadata `versions.Version{Version: "4.0.8", MinUpgradeVersion:
  "4.0.7", UpgradeCluster: No, UpgradeTenant: Yes,
  RequiredProtocolVersion: 118, VersionOffset: len(tenantUpgEntries)}`
  (equal to the consolidated tenant+cluster sum: the hop adds no
  cluster entries). Registration appends after `v4_0_7.Handler` in
  `initUpgrade`; main's deletions and handle list are otherwise
  byte-identical. "4.0.8" continues the consolidated ladder
  consecutively - no released binary ever recorded 4.0.8..4.0.17, and
  every population routes validly: below-4.0.7 clusters step through
  4.0.7 first, current-main consumers and the released 4.1/4.2
  deployments (recording ready 4.0.7) take the direct 4.0.7 -> 4.0.8
  hop, and fresh clusters initialize at 4.0.8 with the corrected
  identities already installed, so no hop leg runs for them.
- **D10-2 (entries in the consolidated shared shape).** The hop carries
  exactly the two idempotent refreshes, now built on the upstream
  shared builder: the COLUMNS leg stays
  `versions.InformationSchemaColumnsUpgradeEntry()` (identity symbol
  `sysview.InformationSchemaColumnsDDL`, gate 109, upstream's
  CREATE-OR-REPLACE authoring and canonicalized CheckFunc), and the
  STATISTICS leg becomes `versions.InformationSchemaViewUpgradeEntry(
  "STATISTICS", sysview.InformationSchemaStatisticsPrefixSubPartDDL,
  defines.MORPCVersion41)` - the revision-9 historical gate choice 41
  (a pre-existing grandfathered constant) and the prefix-aware
  identity stand; the hand-rolled
  pkg/bootstrap/versions/information_schema_statistics.go carrier is
  deleted with its suite, and
  `sysview.InformationSchemaStatisticsDDL` keeps its historical value
  so main's own consolidated 4.0.7 STATISTICS entry stays byte
  upstream. The per-account HandleTenantUpgrade keeps the plain
  shared-entry loop (`entry.Upgrade(txn, accountID)` with framework
  logging); no paging walker, no skip, no new durable progress.
- **D10-3 (predefined.go seam merge rule).** Take upstream's renames
  and authoring-form normalization verbatim; the D9-1 recognition sets
  (the byte-stable V100/V109/V41/V46/V46Upgrade/V58/V110 and
  NULL-selector templates at the snapshot adapter, the derived
  view-metadata known set, the migration-gate membership and the
  current-identity test) carry over inside the renamed seams with the
  exact-template or authoring-normalized discipline; unknown or
  user-edited definitions stay refused at every seam, and no
  historical renderer changes value.
- **D10-4 (route convergence over the consolidated list).** On routes
  that walk 4.0.7 before 4.0.8, main's own 4.0.7 entries re-publish the
  recorded identities first; the hop legs run afterwards and only
  write on failed checks, so state converges to the corrected
  identities; pre-refresh accounts keep serving through I4 reader
  continuity exactly as before.
- **D10-5 (test re-derivation).** The three conflicting bootstrap
  test files merge main's #31497 harness with this branch's identity
  oracles; the hop tests move from the v4_0_18 folder to v4_0_8
  (latest-version and offset-aware fixtures per main's reworked
  mocks; the appended-registration ordering variant); every I1-I6
  oracle of section 7 survives with its subject unchanged.
- **D10-6 (standing guards).** No protocol constant moves
  (`MORPCLatestVersion` stays 134); gates 41/109/118 are pre-existing
  grandfathered values, not new per-feature gates; no mixed-version
  machinery returns; main's 4.0.7-specific admission branches
  (EnsureViewMetadataCatalogs, the legacy CDC-task prerequisite and
  the frozen-offset rejections) stay untouched.

Verified finding: `historicalViewColumnsSQL`
(pkg/sql/plan/view_columns_history.go:47-92) binds a snapshot's
recorded information_schema.COLUMNS definition only when
`sysview.IsCurrentInformationSchemaColumnsDDL` accepts it or, once the
tenant's live view passes the migration gate
(`sysview.IsInformationSchemaColumnsMigrationDDL`), when
`sysview.AdaptLegacyInformationSchemaColumnsDDL` recognizes the
recorded DDL - otherwise it fails with "unsupported historical
information_schema.COLUMNS definition". At base 7e0c9d94fc the live
current identity IS the NULL-selector MEDIUMINT template (base's
`informationSchemaCurrentColumnsDDL`; byte-identically this head's
`InformationSchemaColumnsBinaryFamilyNullDDL()`), so base-era
snapshots carry exactly that definition. After the 4.0.18 hop installs
the corrected identity, such a snapshot passes neither the
current-identity test (it differs by helper arity and the removed
varchar(500) casts, beyond the tolerated selector drift) nor the
legacy adapter (whose branches at head cover V100, V109, the V110
'binary' template and V41/V46/V58; the NULL-selector variant is
absent, and `ViewMetadataColumnsDDL`'s derived seam and the gate
membership mirror that gap), so the gate the corrected live identity
activates would turn every base-era snapshot read into the
unsupported-definition error. Refreshing the live catalog cannot
rewrite historical snapshots: the persisted-state contract requires the
new binary to read them. Revision 8's section 9 additionally planned to
delete the very identity helpers the reader needs.

Revision-9 decisions (completing the read side of D8-4; the delivery
model and D8-1..D8-5 are unchanged):

- **D9-1 (explicit predecessor recognition, historical readers).**
  Keep both predecessor identity renderers -
  `InformationSchemaColumnsV110DDL()` (MEDIUMINT, 'binary' selector)
  and `InformationSchemaColumnsBinaryFamilyNullDDL()` (MEDIUMINT,
  NULL selector - what base-era services published as their current
  definition) - as byte-stable template functions, and register both
  at every historical COLUMNS reader seam: the snapshot adapter
  `AdaptLegacyInformationSchemaColumnsDDL` gains the byte-stable
  NULL-selector branch beside V110; the derived view-metadata known
  set `ViewMetadataColumnsDDL` carries both MEDIUMINT-era templates
  beside current, V100 and V109; and the migration-gate membership
  `IsInformationSchemaColumnsMigrationDDL` grants both predecessors
  gate status beside V100 and the corrected current, restoring for
  not-yet-refreshed base-era accounts the gate base granted them as
  current-generation holders. Recognition stays exact-template
  (canonical view-name case only); unknown or user-edited definitions
  keep being rejected at every seam.
- **D9-2 (read-side continuity, completed).** D8-4 becomes two halves:
  the live half (pre-refresh definitions keep binding through
  preserved function identities) and the historical half - a COLUMNS
  definition recorded under any recent current generation keeps
  binding under the corrected live identity, with the snapshot's
  recorded text, its own (pre-correction) projections and its
  timestamp preserved. The readers acquire no write path, and the
  hop rewrites only the live catalog rows.
- **D9-3 (implementation and evidence amendments).** The helpers stay
  with their doc-comments rewritten from the withdrawn mixed-fleet
  rationale to the historical-recognition purpose (5.5); the
  deletion list shrinks to `FrozenViewServingBelowMetadataFloor` and
  the withdrawn publication/floor wiring; and section 7 gains the H1
  transition regression the finding requires - pre-upgrade snapshot
  -> corrected live identity -> historical query - asserting
  successful binding, preserved historical projections and timestamp,
  and continued rejection of unknown definitions.

The withdrawn mixed-version machinery (deleted with its suites, per
D8-1/D8-5) spans: `MORPCVersion135` and
the `MORPCLatestVersion` raise (pkg/defines/const.go); the
`DeclaredCatalogMetadataFunctions` fences (pkg/sql/compile/remoterun.go,
expression_protocol.go, expression_destination_protocol.go,
pkg/sql/plan/persisted_ip.go, pkg/pb/plan/string_literal_form.go); the
survey-gated publication ladder (pkg/frontend/authenticate.go); the
view-metadata admission handoff raise 118 -> 135
(pkg/cnservice/server.go); the below-floor frozen-identity read
substitution of revision 7 (pkg/sql/plan/persisted_view_serving_floor.go
and its three bind call sites); the recurring account-repair pass
(pkg/bootstrap/service_information_schema_metadata_maintenance.go plus
state, scheduling and strategy additions in
pkg/bootstrap/service.go, service_upgrade_tenant.go,
versions/upgrade_strategy.go); and the admission-scoped hop skip
(pkg/bootstrap/versions/v4_0_18/upgrade.go).

Revision-8 decisions keep every metadata correction of sections 2.1, 2.3
and 4.3 and rework the delivery to contract-conforming shapes:

- **D8-1 (no feature fence).** Delete `MORPCVersion135` and every wiring.
  No new protocol number, no `MORPCLatestVersion` movement, no capability
  survey, no authoring-floor extension, no remote dispatch or
  destination fence, no `DeclaredCatalogMetadataFunctions` flag; function
  overloads and persisted view text need no version gate. The
  grandfathered planes (the 118 admission handoff, the 126 VIEWS
  authoring floor and its publication rungs, the 130-134 upstream gates
  and the pre-existing maintenance passes) stay byte-identical.
- **D8-2 (unconditional publication).** Every publication path of this
  binary - bootstrap system init, CREATE ACCOUNT, tenant publication and
  the subscription wrapper - publishes the corrected current identities
  through the base protocol ladder, exactly as base publishes its own
  current identities. No survey branch, no local-floor branch, no frozen
  publication templates.
- **D8-3 (retained stored-view migration).** The v4_0_18 tenant hop
  remains the single refresh vehicle for identities persisted by older
  binaries and performs both refreshes unconditionally, in the upstream
  merged #30362 report-MEDIUMINT shape (a standard version handler at
  the framework's own required protocol, two shared MODIFY_VIEW entries
  with base-shaped gates and base CheckFunc semantics, a plain entry
  loop, one transaction per account, idempotent re-probe, no skip).
- **D8-4 (read-side continuity by preserved identities).** The
  corrected definitions add no persisted-expression protocol requirement;
  definitions persisted by older binaries keep binding until their hop
  leg runs, because every function identity they reference stays
  implemented. Pre-refresh reads keep returning the pre-correction
  values; there is no below-floor substitution and no frozen serving
  contract.
- **D8-5 (cleanup discipline).** The grandfathered gates and maintenance
  passes are neither extended nor narrowed here; retiring them belongs
  to a dedicated cleanup change under the upgrade contract.

## Revision history

- Revision 1 (2026-10-26, sha256
  79443c9313e239f233f2e03c6a742978b73056bbfdd4a3da138bc187f4cd9f07): first
  design-first version; returned REQUEST_CHANGES with three blockers: the
  lossy persisted ENUM declaration source behind I1, the undocumented durable
  admission-floor activation/rollback boundary, and the missing proof plan
  for the repair lifecycle's failure and concurrency paths.
- Revision 2 (2026-10-26, sha256
  cf51dffccdf207fe9b29d3fc76c0be4c848754082710ed02878587fb1737d885): added
  section 2.3 (declaration-source fidelity;
  the comma-collision boundary with the lossless sources named), decision 4.3
  (encoding-fix boundary, C1/C2), the defined cross-CN repair outcome in
  5.2, sections 6.1-6.3 (activation order and prerequisites, the
  irreversible floor and supported recovery, backup/restore consistency), and
  the section-7 additions (I1 encoding-boundary oracle, admission cases
  V-A/V-B, repair-lifecycle transactional harness and cases m1-m7, focused
  race pass); returned REQUEST_CHANGES with two blockers: the prescribed
  LogStore-first rollout let the raise fire while 134 CNs were live and
  mischaracterized the withdrawn CN as still serving (the readiness bit feeds
  general CN selection including proxy routes), and the race plan exempted
  production shared state from the mandatory owning-package belt.
- Revision 3 (2026-10-26, sha256
  7cc2e70e1ba437831043f579bf255bfaedc736a967aebd8faac016764812b439): corrected
  the activation prerequisites and
  rollout order (6.1 rewritten: the raise arm's wire-capability gate -
  base-advertised flags, not a 135-fleet predicate - with no live-CN guard on
  the already-enabled path, the general-routing consequence of the readiness
  withdrawal, and the CN-first sequence whose pause point is the LogStore
  binary version), fixed 6.2's pause and in-flight-activation semantics,
  extended V-A through the routing consumer with recovery-after-activation,
  added V-C (pause boundary, downgraded-leader barrier, and the corrected
  V3-versus-135 prerequisite at the logged pkg/logservice proposal seam) and
  m8, and replaced the focused-only race pass with the mandatory
  individually selected adaptive stress (m6/m7/m8, B=30s budget formula)
  plus the `pkg/bootstrap` owning-package `-race` pass through
  mo-cgo-test; returned REQUEST_CHANGES with two blockers: the retained
  routing stopped short of usable account service (the login-time tenant
  hop's 135-gated entries fail login below admission - the survey error in
  the mixed phase, the authoring-floor DDL rejection at the pause), and the
  backup/restore model assumed the standard backup captures HAKeeper
  admission state while the actual payload carries only the ID watermarks.
- Revision 4 (2026-10-26, sha256
  06abb1f2f7b1c74b2fdf13534b1f4e7cb68bcf69f487ac248ae49e830093b145): added
  invariant I10 and decision 4.4
  (admission-scoped login-safe no-ops in the version-gated hop; the
  framework's pre-existing tenant-above-CN rule documented unchanged), the
  tenant-login transition inside 6.1 with the six verified code sites
  (mysql_protocol.go login propagation, service_upgrade_tenant.go's
  synchronous loop and version refusal, upgrade_strategy.go's survey error
  and DDL, build_ddl.go/persisted_ip.go authoring floor), login-boundary
  cases L1-L3, the fully rewritten 6.3 backup/restore contract (backup
  payload = TAE storage + BackupData{NextID, NextIDByKey} only; restore
  seeds only the watermarks; admission always re-derives through the
  guarded initial-enablement arm so a restore can never withdraw a live CN;
  the restore-target constraint on post-activation backups; physical
  full-state snapshots withdrawn as an undefined mechanism), restore cases
  R1/R2, and the corresponding matrix, acceptance, section-8, and
  section-9 updates (the 4.4 hop no-op recorded as the single scheduled
  production delta); returned REQUEST_CHANGES with two restore-contract
  blockers: the recovery model ignored the persisted cluster/tenant
  version markers D4 writes at floor 134 (a pre-raise backup containing
  4.0.18 markers does not restore onto 134 binaries - the older-CN
  startup fatal over a Ready cluster row at service_upgrade.go ~204-207
  and the login version refusal at service_upgrade_tenant.go ~70-73
  fire), and the no-withdrawal claim covered only the fresh-admission
  interval (after the restored cluster first enables at 134, a 135
  LogStore leadership raises through the un-guarded Enabled arm
  (store.go ~1170-1176) and withdraws below-floor CNs from general
  routing exactly as in a live rollout).
- Revision 5 (this document): reworks 6.2's restore option and all of 6.3
  on the two points the review isolated - the backup payload carries the
  upgrade framework's persisted version markers
  (UpgradeTenantVersion at service_upgrade_tenant.go ~129;
  UpdateVersionState at service_upgrade.go ~505-506), so the
  restore-compatibility matrix now binds on view identities AND version
  markers (pre-transition backups restore onto any identity-binding
  fleet; post-4.0.18-marker backups only onto binaries whose compiled
  final version is >= 4.0.18); and admission re-derivation is stated as
  two guarded intervals (the fresh live-CN-guard interval, then the
  Enabled-state raise through the un-guarded arm on 135 LogStore
  leadership), so the restored fleet inherits 6.1's
  CN-first/LogStore-pinning prerequisites and the unconditional
  no-withdrawal claim is withdrawn. Section 7 corrects R2's overbroad
  assertions and adds R3 (version-marker compatibility through the
  login and startup seams) and R4 (fresh enable at 134 then a 135
  leader while 134 CNs remain - the withdrawal recurrence); acceptance,
  section 8, section 9, and the history are updated accordingly.
- Revision 6 (2026-10-28, sha256
  977e9bfe5e6c2f09b6ac44248b5a742b61769cab80dd27db58c24b9175cb8076): merge
  adaptation of the PASS-approved revision 5 forced by rebasing onto upstream
  main `7e0c9d94`; the label-aware publication claimed the next free
  protocol generation `MORPCVersion132 -> MORPCVersion135` past upstream's
  132-134 gates, the merged current COLUMNS identity composed the upstream
  #31461 binary-family NULL selector with the label overloads while the
  frozen V110 template stayed byte-stable, and the repair/eligibility set
  learned the upstream-persisted NULL-selector identity. No decision
  changes.
- Revision 7 (2026-10-28, sha256
  9a586eaa5ea1f2b4847e405173e81acd33f30f90cf53aaec42a18eac12e3ce65):
  read-path serving completion
  after the merged head failed the upstream multicn regression at the
  simulated 118 floor (details in the header block). Adds the reader half
  of the mixed-serving contract: below-floor binding of the exact current
  COLUMNS/STATISTICS identities substitutes the frozen identities at the
  three bind entry points (identity-guarded, requirement cleared,
  catalog identity untouched), records the reader row in the section-6
  matrix and the focused validation in section 7. Publication, repair,
  admission and rollout decisions are unchanged.

- Revision 8 (2026-10-28, sha256
  69864453903bc5ca2afcd0b751ed7cc5d9cf398c8b7e19114e35c764e1e4db4e):
  upgrade-contract rework returned by the implementation review at head
  aec8645e3a. Withdraws
  the mixed-fleet delivery model and all of its machinery -
  MORPCVersion135 and the MORPCLatestVersion raise, the
  DeclaredCatalogMetadataFunctions fences, the survey-gated publication
  ladder, the cnservice admission-handoff raise, the below-floor
  frozen-identity reader substitution, the recurring metadata-repair
  pass and the hop's admission-scoped skip - per the unchanged
  shutdown-only upgrade contract
  (.agents/skills/mo-dev/references/upgrade-compatibility.md). Retains
  the corrected identities, their metadata oracles and the stored-view
  migration, and moves existing accounts through the 4.0.18 hop
  unconditionally in the upstream #30362 shape. Decisions 4.1, 4.2 and
  4.4 are recorded as withdrawn and replaced by 4.5 (D8-1..D8-5);
  sections 1, 2.2, 3, 5, 6, 7, 8 and 9 are reworked accordingly, while
  2.1's defect facts, 2.3 and 4.3 carry over unchanged.

- Revision 9 (2026-10-28, this document): historical-snapshot reader
  completion, returned by the independent design review of revision 8
  (REQUEST_CHANGES, one P2 finding verified valid against
  view_columns_history.go and predefined.go at head and base). Adds
  invariant I6 and section 5.5 (decisions D9-1..D9-3): the corrected
  identity move registers the complete predecessor set - the MEDIUMINT
  'binary' template and the base-era NULL-selector identity - at every
  historical COLUMNS reader seam (snapshot adapter, derived
  view-metadata known set, migration gate), so base-era snapshots keep
  binding under the corrected live identity with their recorded text,
  projections and timestamp preserved, while unknown or user-edited
  definitions stay refused; section 9 keeps the helper functions
  (deletion list shrinks to the withdrawn serving-floor map and its
  wiring) and section 7 gains the H1 transition regression. The
  shutdown-only delivery model and D8-1..D8-5 are unchanged. Independently
  reviewed and APPROVED (PASS) on 2026-10-28; implemented at PR
  #31489's rework head 9f454cb5d0.

- Revision 10 (2026-10-29, this document): delivery-vehicle re-derivation
  for upstream #31497 (the catalog-upgrade consolidation), triggered by
  the PR #31489 conflict round at head 9f454cb5d0 (origin/main
  3cf38889bc; five merge-tree-conflicting paths, all framework surface).
  Verified against origin/main before deciding: main deleted the
  superseded intermediate handles v4_0_8..v4_0_17 and froze 4.0.7
  (offset-only 4.0.7 routes and offset-differing ready 4.0.7 states are
  terminally rejected with "further migrations require a new catalog
  version"), so the hop cannot ride the consolidated entry list and
  keeps a dedicated version handle; the shared view-refresh builder now
  authors CREATE OR REPLACE VIEW and matches definitions through
  authoring-form normalization; and predefined.go's seam normalizer was
  renamed upstream. Decisions D10-1..D10-6 renumber the hop to
  `pkg/bootstrap/versions/v4_0_8` (metadata {Version: "4.0.8",
  MinUpgradeVersion: "4.0.7", RequiredProtocolVersion: 118,
  VersionOffset: len(tenantUpgEntries)}), rebuild both legs on the
  upstream shared builder (COLUMNS via the shared entry at gate 109;
  STATISTICS via InformationSchemaViewUpgradeEntry with the
  prefix-aware identity at the historical gate 41, deleting the
  hand-rolled statistics carrier), adopt upstream's seam renames with
  the D9-1 recognition sets intact, keep every corrected identity,
  reader and publication decision unchanged, and record the rebase
  resolution map for the five conflicting paths plus the re-derived
  validation ladder. Sections 5.2, 6, 7 (I3), 8 and 9 are amended;
  everything else in revision 9 stands.

- Revision 11 (2026-10-29, this document): admission-preservation
  completion, returned by the independent design review of revision 10
  (REQUEST_CHANGES, two P2 findings verified valid against origin/main
  3cf38889bc). Both findings share one root: upstream keys four
  upgrade-admission guards on the literal final.Version == "4.0.7", so
  a 4.0.8 final silently disables them - legacy CDC admission (with
  docs/guides/upgrade-4.2-to-main.md promising new-CN rejection when
  tasks remain, and pre-4.0.7 tasks unable to establish the CDC
  executor's target identity), the ready 4.0.7 frozen-offset rejection,
  the interrupted 4.0.7 offset-only-route rejection (which
  checkUpgradeRecovery would now silently resume, and
  shouldRunTenantUpgrade would rerun for already-4.0.7 accounts), and
  revision 10 had additionally recorded the disabled behaviors as an
  upstream non-goal and planned a successful older-offset route. This
  revision (D11-1..D11-4, section 5.6) re-keys the guards onto the
  source/retained route and the registered v4_0_7 handle identity -
  following the file's own EnsureViewMetadataCatalogs (final >= 4.0.7)
  and canResumeUpgrade (per-handle offset) patterns - adds the CDC
  admission and the frozen offset-only rejection to the recovery path,
  corrects revision 10's population claim (direct succession only from
  the valid frozen identity), withdraws the non-goal risk bullet, and
  specifies the successor-registration admission oracles (extended
  TestLegacyCDCAdmissionBeforeUpgradeMutation; re-derived
  TestV407RejectsDifferentOffsetBeforeCreatingRoute and
  TestV407RejectsInterruptedOffsetOnlyRouteBeforeMutation) with
  upstream-identical messages, error types and pre-mutation ordering.
  No protocol constant, feature gate or mixed-version mechanism enters
  the correction (D8-1/D10-6 guards keep holding); everything else in
  revision 10 stands.

- Revision 12 (2026-10-29, this document): on-demand continuation
  completion, returned by the independent design review of revision 11
  (REQUEST_CHANGES, one P2 finding verified valid against origin/main
  3cf38889bc). The finding: upstream's login-compensation walk
  (MaybeUpgradeTenant) and manual walk (UpgradeOneTenant) break their
  register walk at the real paged v4_0_7 handler (the latter with a
  >= register comparison, so even an account already at 4.0.7 breaks
  and caches success with nothing done), and upgradeV407TenantOnDemand
  stamps only to 4.0.7; with a successor registered, late and manual
  accounts therefore never execute the successor hop, keep serving the
  historical STATISTICS identity, and the cached success suppresses
  further compensation on that CN; the existing TestUpgradeTenant uses
  synthetic handlers and bypasses the special case. This revision
  (D12-1..D12-3, section 5.6's continuation invariants) records the
  mechanism: both on-demand walks resume the authoritative register
  walk after the paged 4.0.7 stage commits (or directly from an
  already-stamped 4.0.7), execute the successor handle's tenant
  upgrade in one bounded service-owned transaction, stamp the
  account's create_version per handle, and cache success only at the
  final version - with failure-resume from the last committed stamp
  and the paged stage's ownership unchanged; the route-based startup
  pass needs no fix (its rows already drive 4.0.7 -> 4.0.8). Oracles
  are specified at the real v4_0_7 handler plus the successor
  registration: late-login convergence from 4.0.6 with corrected
  identities and second-login idempotence, already-4.0.7 manual
  upgrade, 4.0.6 manual chaining, and successor failure/retry without
  premature caching, with a no-successor-SQL-before-4.0.7-completion
  ordering assertion. Sections 5.6, 6, 7 (new continuation-oracle
  entry and acceptance criteria) and 9 (resolution-map additions)
  are amended; everything else in revision 11 stands. Independently
  reviewed and APPROVED (PASS) on 2026-10-29; implemented at PR
  #31489's head efb7234682.

- Revision 13 (2026-10-30, this document): successor-slot re-derivation
  for upstream #31739 (the attributed-DECIMAL forward handle), triggered
  by the PR #31489 conflict round at head efb7234682 (origin/main
  97b2261005; 15 merge-tree-conflicting paths, 18 overlapping). Verified
  against origin/main before deciding: main consumed the post-consolidation
  successor slot as "4.0.18" (registration..., v4_0_7.Handler,
  v4_0_18.Handler; sole entry the same shared COLUMNS entry at gate 109),
  and the merged catalog-upgrade contract
  (docs/design/20261028-catalog-upgrade-407.md) forbids registering the
  4.0.8..4.0.17 identities outright - superseding revision 10's
  "no released binary ever recorded 4.0.8..4.0.17" reading - so the
  revision-12 vehicle "4.0.8" deadlocks in any registration position
  (earlier-first final "4.0.18" strands main-era 4.0.18-stamped tenants
  unrepaired behind the version-equality short-circuit; last final
  "4.0.8" makes those tenants terminally unusable under the advance
  guard). D13-1 renumbers the hop to pkg/bootstrap/versions/v4_0_19
  (metadata {Version: "4.0.19", MinUpgradeVersion: "4.0.7",
  RequiredProtocolVersion: 118, VersionOffset: 2}) appended after
  v4_0_18.Handler, adding the main-era 4.0.18-stamped population's direct
  [4.0.18 -> 4.0.19] repair route to section 6. D13-2 keeps upstream's
  attributed-DECIMAL decoration verbatim and composes it over the
  revision-12 label-aware/unbounded derivers (merged current =
  informationSchemaColumnsWithUnsignedDecimalDDL() over the revision-12
  current constructor; recognition sets hold the frozen-identity union);
  D13-3 adopts upstream's crossingPending admission refinements while
  keeping revision 12's recovery-path guards; D13-4 withdraws the
  service-owned continuation transaction in favor of upstream's
  re-entrant planning loops (keeping the in-txn manual-walk completion
  check); D13-5 renumbers MO_INDEX_SUB_PART to 623 above upstream's
  620-622 allocation (FUNCTION_END_NUMBER 624); D13-6 regenerates the
  four COLUMNS-identity BVT rows from the merged binary; D13-7 records
  the version-literal oracle census re-derived at final 4.0.19. Sections
  5.2, 5.6, 6, 7, 9 and the revision history are amended; everything
  else in revision 12 stands. Returned REQUEST_CHANGES by the
  independent design review on 2026-10-30 (two verified findings: the
  unrecognized upstream 4.0.18 published identity broke historical
  reads; the background route census is the direct row, not the
  successor-hit chain), resolved by revision 14.

- Revision 14 (2026-10-30, this document): review revision resolving
  the two findings the independent design review returned on revision
  13.
  Both findings verified before revising: at origin/main 97b2261005
  the background route creator builds a single direct row for a
  final-eligible latest (`if final.CanDirectUpgrade(latest) {
  append(final) }`, pkg/bootstrap/service_upgrade.go:442; each row is
  executed only by the handle at `upgrade.ToVersion`,
  service_upgrade.go:798 / service_upgrade_tenant.go:327), and the
  decorated 4.0.18 published identity (the predefined.go:731
  assignment at 97b2261005) is sustained by no merged-tree constant
  once the current constructor composes, so recorded 4.0.18 snapshot
  text fails the historical reader at
  pkg/sql/plan/view_columns_history.go:87-92 ("unsupported historical
  information_schema.COLUMNS definition") while the pre-crossing gate
  skips adaptation silently. D14-1 freezes the exact upstream 4.0.18
  identity (the attributed-DECIMAL decoration applied over the frozen
  binary-null pre-attributes template, never over the mutable current)
  and adds it to the historical adapter, derived-metadata and
  migration-gate seams, with a byte-stability pin and no replay-drift
  tolerance; D14-2/D14-3 correct the population routes and validation
  census to the direct [4.0.7 -> 4.0.19] and [4.0.18 -> 4.0.19] rows
  executed by the v4_0_19 handler alone, retaining the chained
  multi-hop rows for below-4.0.7 sources and the on-demand walks as
  the separate mechanism; D14-4 extends the I6 historical-transition
  oracle with the 4.0.18 snapshot read after the 4.0.19 refresh
  (recorded projections and recorded timestamp preserved,
  unknown-definition rejection retained) plus the pre-refresh gate
  leg. The recognition-set union and sections 5.5, 6 and 7 are
  amended as recorded above; everything else in revision 13 stands.
  Independently reviewed and APPROVED (PASS) on 2026-10-30 at exactly
  this content (sha256
  22aaf464a18a8be7e7d4889e605850c0792bca6df7bd234e97569445b0c84c66);
  the rebase was implemented at PR #31489 head
  61da7f8ef005f2ee14a3ea44e738e6baee24876d.

- Revision 15 (2026-10-30, this document): documentation-only
  consolidation requested by PR #31489's implementation review.
  Records the design-first approval chain (each reviewed revision's
  exact hash and PASS/disposition, above) and the delivered 4.0.19
  vehicle in the status block; folds the approved D13/D14 amendments
  into the standing sections (2.2 I3, 3, 5.2, 5.5, 5.6, 6, 7, 8);
  replaces the stale per-round implementation/rebase diary of the old
  section 9 with the conformance record; and moves the per-round
  status correspondence, the pre-revision-8 withdrawn delivery-model
  blocks, the completed rebases' resolution maps and machine-local
  evidence paths out of this tracked document (they remain recorded in
  the PR's review threads and retained review evidence). No decision,
  invariant, oracle or validation entry changed from the approved
  revision 14 (sha256
  22aaf464a18a8be7e7d4889e605850c0792bca6df7bd234e97569445b0c84c66).
