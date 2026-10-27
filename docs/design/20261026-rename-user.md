# RENAME USER identity-preserving compatibility design

- Owning issue: https://github.com/matrixorigin/matrixone/issues/30467
- Revision: v0.6, 2026-10-27; **AFFECTED DESIGN REVIEW PENDING**
- Source baseline: `83c1e23526fc0ed31188abd7fc2ca87cd3a6d4b3` (fresh main clone).
- Implementation PR: not created. No product implementation accompanies this draft.
- Review routing: frontend owner `@XuPeng-SH`; parser owners `@iamlinjunhong` and
  `@XuPeng-SH` (CODEOWNERS). The repository does not prescribe one exclusive
  approver, but a traceable design approval is mandatory before implementation.

## Gate and scope

This is a new SQL capability, not an ordinary bug fix. The design gate in
`.agents/skills/mo-dev/references/feature-design-review.md` applies because it
changes a compatibility contract and persistent account identity state, crosses
parser/frontend/catalog consumers, and changes an authorization boundary.

The intended closure is account rename only. Parent #30464 is Related, not Fixed.
SET PASSWORD #30900, PASSWORD EXPIRE #30698, direct user privilege grants, and
other account-management features are explicitly out of scope. The historical
issue's claim that all other account statements work is not current verification.

## Evidence and problem

The report reproduces a parser rejection for `RENAME USER 'mo_u1'@'%' TO
'mo_u2'@'%'`. No `RenameUser` AST or handler was found in the inspected main.
This is source inspection, not a new SQL reproduction.

MySQL 8.0's documented contract permits comma-separated pairs, rejects missing
old or existing new accounts, uses CREATE USER privilege, defaults omitted hosts
to `%`, and preserves grants. Since 8.0.22 it guards renaming accounts referenced
as stored-object DEFINER; its SET_USER_ID override is not automatically available
in MatrixOne. Account-management changes are atomic.

References:
- https://dev.mysql.com/doc/refman/8.0/en/rename-user.html
- https://dev.mysql.com/doc/refman/8.0/en/account-management-statements.html

MatrixOne facts that prevent simply copying MySQL's catalog update:

1. `pkg/frontend/predefined.go:27-44` makes `mo_user.user_id` the primary key and
   `user_name` independently unique, with `user_host` a separate field. It cannot
   represent two accounts sharing a username with different hosts.
2. `authenticate.go:1314` resolves login identity by username only. User grants
   reference `user_id` (e.g. `authenticate.go:1343-1360`), so preserving that ID is
   essential. A drop/create emulation is incorrect.
3. `InitUser` maps omitted/`%` hosts to `rootHost` (`localhost`) around
   `authenticate.go:11646`. `set_default_role.go` already treats `%` as a
   username-target wildcard, validating explicit hosts against the stored host.
4. Account `admin_name`, name-based super-user checks, live sessions, routine
   DEFINER strings, and SQL-task creator strings are additional consumers.
   `authenticate.go:12928` recognizes root/dump by name; routine definitions store
   both creator IDs and textual definers; tasks carry both creator text and ID.
5. `doDropUser` uses pessimistic RC and blocks administrator targets, but its
   initial name-to-ID lookup is unlocked; DELETE locking alone does not protect
   that earlier binding. `doSetDefaultRole` provides a nearby explicit locking,
   current-principal-ID recheck, result-shape validation, and transaction pattern.

## Invariants

- A successful rename retains user ID, credentials, password history, lock state,
  role memberships, grant options, default role, creator IDs, and owned objects.
- Only names belonging to the current account may be resolved or updated.
- No unprivileged or stale authenticated principal can rename another identity.
- Every pair succeeds as one transaction or none becomes visible. Commit is the
  publication point; error/cancellation before it leaves no partial rename.
- A new name must not capture a protected built-in or silently orphan a definer.
- Existing authenticated sessions must not acquire another user's authority if
  their old username is later recreated.

## Alternatives

1. Drop/create with grant copying: reject. Changes stable IDs and may lose
   ownership, history, concurrent grants, task/routine references and atomicity.
2. Expand the catalog/login model to MySQL `(username, host)` identities: reject
   for this change. It requires catalog migration, authentication and every
   consumer update, mixed-version policy, and a separate larger design.
3. Preserve IDs and change only the accepted name/host representation in one
   catalog transaction: preferred direction. It fits current catalog constraints
   but requires explicit host, protected-user, definer and session decisions below.

## Recommended contract (v0.4)

Retain the current user-ID and username-unique catalog, without migration or
aliases. Omitted/`%` source or destination hosts mean the existing unique username;
any explicit host must equal the source row's stored host. Never change user_host.
Reject anonymous/invalid names using existing normalization. `normalizeName`
(`authenticate.go:3056`) trims and validates; it does not lowercase. SQL catalog
comparison/uniqueness, not a Go lowercase map, decides equality.

Protect both source and destination `root`, `dump`, `internal`, and names registered
with `isSpecialUser` (`authenticate.go:476-520`). Reject a source matching
`mo_account.admin_name` or holding moadmin/accountadmin membership. Reject target
account-admin names too. This deliberately does not rename built-ins/admins.
Known fixed bootstrap names are also reserved. Dynamic task-user names require
the authenticated-special-identity hardening and bootstrap admission rule below;
a CN-local name lookup alone is not an authorization boundary.

Require CREATE USER or account ALL through the existing privilege framework; no
self-service exception. Resolve authority using the authenticated account/user ID,
not a session's historical username. Do not implement mysql-table UPDATE as an
alternative privilege or a SET_USER_ID definer override.

Pairs are evaluated in written order in one transaction. With only `a` existing,
`a->b,b->c` succeeds. With both `a,b` existing, `a->b,b->c` fails;
`b->c,a->b` succeeds if c is absent. `a->a`, a direct swap, a repeated consumed
source, a missing source, or an occupied destination fails the whole statement.
No implicit temporary names. This is source-derived, not runtime-oracle evidence:
MySQL 8.0 `mysql_rename_user` checks destination and immediately renames each pair
in its loop (`sql/auth/sql_user.cc:3048-3113`), then uses its ACL DDL completion
path (3128-3144). MySQL documentation supplies the atomicity contract.
Source: https://raw.githubusercontent.com/mysql/mysql-server/8.0/sql/auth/sql_user.cc

## Name-consumer audit and required changes

| Consumer and baseline source | Finding and chosen handling |
|---|---|
| Login, `authenticate.go:1314`; catalog `predefined.go:27-44` | Username resolves stable ID; after commit old-name login fails unless separately recreated. All other row fields remain unchanged. |
| `doAlterUser`, `authenticate.go:3170-3285`, self checks later in handler | Current admin membership is looked up using the session name. After old-name reuse this could consult a different principal. Change current membership and self equality to stable IDs, with same-transaction principal validation. This is mandatory feature scope, not an already-fixed claim. |
| `set_default_role.go:198-310` | Locks current name then compares its resolved ID with session ID. Missing/mismatched identity fails closed. Keep that behavior; a renamed live session may need reconnect for this statement. The name-based shortcut at line 53 is not by itself proof of an authorization bypass. |
| Implicit SHOW GRANTS, `pkg/sql/plan/build_show.go:1120-1132` | Defaults target to protocol username. Change implicit self selection to current account/user ID with canonical catalog label; explicit requested names remain explicit targets. Do not globally replace login-display names. |
| Super/special names, `authenticate.go:12928`, `session.go:3288,3726`, `mysql_cmd_executor.go:4003`, `util.go:2438` | Affect bypass/routing/provenance, not just display. Prevent capture and rename of all reserved identities; cannot rely on refreshing one CN's sessions. |
| Session display, `session.go:3269`, `mysql_cmd_executor.go:6298`, `back_exec.go:478` | Protocol username and TenantInfo.User are distinct. Keep historical login display where non-authoritative; never use that text to determine current principal membership. |
| SQL tasks, `sql_task_utils.go:81-98`, `sql_task_executor.go:159`, `internal_executor.go:36-62,198+` | DefinerOpts supplies both creator text and nonzero IDs; context also carries IDs. applyOverride writes username only to responder, IDs to TenantInfo. There is no name-overrides-ID fallback here. Creator text may be stale metadata; do not rewrite tasks or claim their identity is automatically retargeted. Definer-writing operations executed by a task must use its current ID as below. |
| SQL-task creation, `sql_task_handler.go:331+`, `authenticate.go:10527` | Stores creator string plus IDs; management is currently admin-only. Nonzero persisted IDs remain authority. No new legacy-zero-ID reinterpretation. Builtin zero-ID accounts are protected. |
| CDC names, `cdc_options.go:118,366`, `pkg/sql/compile/ddl.go:7494`, `pkg/sql/plan/build_ddl.go:8540` | Capture user text in task metadata. Search of task/cdc readers found no Details.Username authorization consumer; this is a bounded negative search, not proof of all downstream/serialized consumers. Keep text as provenance, require integration coverage. |
| Partial restore, `restore_authorization.go:310-313` | Existing old-name/new-name mismatch skips user identity mapping despite equal ID. Preserve conservative failure rather than inventing an alias. Full restore remains whole-catalog recovery. |

### DEFINER inventory and race prevention

`mo_user_defined_function.definer` (`predefined.go:110`) and
`mo_stored_procedure.definer` (219) are textual fields; `build_show.go:958-960`
exposes them. `persistUserDefinedFunction` (`authenticate.go:12091+`) and
`persistStoredProcedure` (12211+) write TenantInfo.User. Reject rename if either
source OR destination canonical name has such references (destination check also
prevents adopting an already-orphaned textual definer). Do not rewrite definitions.

Views are different: `getViewSecurityInfoWithSnapshot` (`authenticate.go:5590+`)
reads mo_tables.owner as a role ID; `viewPrivilegePrincipal` keeps account/role
IDs. No username-definer rewrite or blanket view blocker is justified by this
path. Parser-tree searches found no independent explicit user-DEFINER AST option.

Required writer change: creation AND replacement of both routine kinds lock the
creator's current mo_user row by user ID in the same pessimistic-RC transaction,
read the canonical username after acquiring the lock, and persist that name.
Use an exclusive FOR UPDATE lock initially rather than assume shared-lock syntax.
The rename holds the same row lock through its reference check and commit.
If creation wins, rename subsequently sees its reference and rejects; if rename
wins, later creation persists the new name. No global active-session ban.
`InitFunction` already uses pessimistic RC and database locking (11860+);
`InitProcedure` currently prepares before begin using a default executor (12237+),
so its transaction/lookup must move inside the locked sequence. Existing routine
length limits must be preserved, not silently truncate canonical names.

## Concrete proposed algorithm and failure contract

1. Parse owned old/new pairs and normalize/quote through existing helpers. Begin
   one request-cancellable pessimistic-RC background transaction, tenant-scoped.
2. Identify existing users among all old/new names, plus current principal, then
   lock their existing rows in stable ID order and revalidate name/ID bindings.
   Resolve chained names against the transaction's own writes, not just the
   initial source set. Newly appearing bindings or changed IDs cause a conflict
   failure, never substitution of a new principal. Acquire required administrator
   role/membership exclusion consistently with adjacent account-management paths.
3. Validate each ordered pair's source, host, reserved/admin status, target
   absence, and routine-reference guard. Update only user_name by stable ID.
   Execute every pair's update, including intermediate names, inside this single
   transaction so database uniqueness handles canonical equality and conflicts.
4. Commit once. All validation, SQL, cancellation and detected uniqueness/deadlock
   errors abort the whole transaction via finishTxn. Always close the executor.
   No grant copying, new IDs, task rewrites, session broadcasts or precommit cache
   publication. Do not auto-retry semantic errors. Surface existing transaction
   retry errors for lock conflicts; preserve duplicate/missing-account categories
   without exposing other tenants' data. A lost commit acknowledgement has unknown
   client outcome: inspect catalog before retry; do not automatically reverse.

Absent destination names are NOT assumed gap-locked. Concurrent CREATE/RENAME
can win the unique username insert/update; the loser must roll back all pairs.
Existing-user DROP/restore writers must share user-row conflicts. The source
user lock is the proposed exclusion for routine creation; it is not sufficient
proof of the admin-membership phantom or mixed-writer cases listed below.

No new service, worker, wire field, cache or catalog migration. Pair storage is
O(pair count), but DEFINER scans may be O(routine count): current definer columns
are not asserted indexed. Reference checks must remain request-cancellable.
No per-query session-wide refresh is proposed.

## Bounded writer / shared-lock contract (v0.4)

Here "shared" means the SAME exclusion row, not SQL shared-lock mode. Use
exclusive `FOR UPDATE` on the stable user-ID row in pessimistic RC. This is a
small coordinated writer change, not a new lock service or distributed protocol.

| Writer / actual evidence | Required participation and order | Failure / acceptance evidence |
|---|---|---|
| RENAME handler (new) | Resolve then lock affected existing user IDs in ascending ID order; revalidate bindings; check admin membership and DEFINER references after acquisition. Update ordered names while retaining locks through commit. | Missing-row destination races rely on existing unique-key transaction enforcement. Deterministic CREATE-vs-RENAME and two-RENAME tests must show one winner and whole-statement loser rollback. No gap-lock claim. |
| User-target GRANT ROLE, `authenticate.go:6245,6295,6415+` | Current default executor and unlocked target lookup do not exclude rename. Switch this user-target path to pessimistic RC; resolve user IDs, lock/revalidate them before membership checks/inserts, retain to commit. Role-to-role behavior is not redesigned. | This closes the concrete administrator-membership phantom: grant-first makes rename reject; rename-first makes grant validate the current binding or fail, never another identity. |
| User-target REVOKE ROLE, `authenticate.go:6069-6128+` | Already pessimistic RC, but username lookup is not an exclusion lock. Take the same stable-ID target user lock before membership mutation; revalidate target. | Revocation and rename serialize; rollback/cancellation releases every acquired lock. |
| Routine create/replace, `authenticate.go:11860+,12091+,12211+,12237+` | Existing UDF database lock precedes creator-user lock; procedure switches to pessimistic RC, with persistence preparation inside transaction and creator lock before writing. Read canonical name by ID only after lock. Rename never takes a database lock, avoiding a new reverse database edge. | Both creator-first and rename-first barrier schedules; replacement included. No stale session/task text may become a definer. |
| DROP USER, `authenticate.go:4694-4755`, helper/template `1800,1314` | RC is already enabled, but initial name-to-ID lookup has no FOR UPDATE. Add the same stable-ID user-row lock, then revalidate the requested name-to-ID binding before admin checks or deleting user/grants. Pre-resolve multi-target IDs and lock in stable ID order; any changed binding aborts without deleting the renamed identity. Retain locks through commit. | Required barrier test: DROP resolves A=IDx, RENAME commits A->B, DROP resumes and must fail revalidation, not delete B. Reverse lock winner permits DROP to commit and RENAME to fail missing source. No claim that mutation-time DELETE locks serialize the earlier lookup. |
| SET DEFAULT ROLE, `set_default_role.go:198-310` | Retain its existing explicit user-row exclusion and current-principal-ID check. It currently takes role then name-ordered user locks. New grant/revoke user locks go after any role exclusion and before membership writes; no role lock may be acquired after a user lock in new paths. New multi-user paths use stable ID order. | Existing name-order vs new ID-order paths can deadlock: rely on existing engine deadlock detection, surface its retry error, roll back whole statement. Do not claim universal deadlock-free ordering or silently retry a partially observed operation. |
| Snapshot catalog UDF copy, `snapshot_catalog_restore.go:99-130`; catalog authorization restore | This path drops/recreates the catalog and copies definer strings verbatim, bypassing frontend routine writers. Treat account/catalog restore as maintenance-exclusive with RENAME and ordinary writers stopped for the affected account; do not retrofit per-user locks into a table replacement. Partial historical restore keeps current identity-continuity checks. | Operational restore admission is an explicit support prerequisite below, not an asserted existing per-user lock. Test post-restore normal rename; concurrent catalog restore is unsupported in this initial contract. |

The audit boundary is the named frontend create/replace writers plus explicit
snapshot catalog copying and account-membership writers. Additional dynamic
consumers are not claimed proved absent. Cross-CN lock/unique-key correctness uses
the existing engine contract; implementation validation must exercise it, but the
design does not require inventing a second transaction system. A discovered
nonparticipating writer invalidates this reviewed contract and requires revision.

## Upgrade / protected-identity contract (v0.4)

| Area / source evidence | Concrete recommended rule | Approval / verification boundary |
|---|---|---|
| Upgrade admission | Initial support is stopped/full upgrade only: quiesce SQL, drain all old CNs and task writers, replace every participating binary, restart verified new binaries, then admit RENAME. Never admit it to a mixed-version cluster. | Repository precedents allow operational gates: `docs/design/account_lifecycle_shared_gate.md:116-126` explicitly calls its prerequisite an operator rollout gate; `docs/rfcs/00000000_except_all.md:72-82` requires all participating CNs upgraded without a new feature gate; `docs/design/20261003-retire-transaction-data-trace.md:13` describes stopped replacement. These are precedents, not a blanket published MatrixOne support policy. Reviewer must accept this feature-specific offline-only runbook. No new MORPC version gate proposed. |
| Restore / downgrade | Stop affected-account writers for catalog restore; after restore verify account/grant identities before traffic resumes. No live downgrade after rename to name-authority binaries. Recovery requires stopped restore of a consistent pre-feature backup or a separately reviewed identity-safe migration. | Explicit operational support restriction; not an automatic inverse rename, and not a claim that user-row locks protect catalog replacement. |
| Fixed reserved identities | Block source and destination root/dump/internal plus `db_holder.MOLoggerUser` and `proxy.SQLUsername` (`proxy_user`) in every tenant. Continue blocking admin_name and admin-role holders. | Fixed special registration sites are `cnservice/server_task.go:131,192`; root/dump special checks and name-only bypass are cross-tenant concerns. Restricting only sys accounts would not suffice. |
| Dynamic special task identity | `cnservice/server_task.go:100` registers `command.User.Username` from HAKeeper; `logservice/store_hakeeper_check.go:675-698` accepts configured username or generates UUID. At offline admission, all CNs must have completed the same task-user bootstrap before SQL resumes; reserve that authoritative command's name. No replicated registry feature is added. | Reject source/destination matching the locally installed task identity after completed bootstrap. Runbook must prevent serving before bootstrap completion or changing task identity concurrently with account rename. This narrows a startup prerequisite, not an unbounded consensus problem. |
| Special authorization hardening | `SetSpecialUser` (`authenticate.go:493-507`) installs sys account, sentinel user ID `math.MaxUint32`, moadmin role. `session.go:3288` currently bypasses by username only. Require the authenticated special identity (sys account + sentinel ID + registered special name) rather than string equality alone; apply the same identity condition to special provenance classification (`util.go:2438`). Preserve explicit trusted `ses.isInternal` handling. | A catalog user renamed to a special-looking name must never gain bypass, even under registry timing differences. Normal catalog user_id is signed; the sentinel is outside its positive range. Test a normal non-sys AND sys catalog principal with the same textual name. No password/account mutations are part of design work. |

## Precise decisions still requiring review

Technical recommendation is now fixed: retain protected-admin semantics and add
the bounded user-target GRANT/REVOKE and DROP binding exclusion, rather than allowing administrator
role holders to rename as an implicit shortcut. The latter is a different product
contract, not the default.

Before design approval the reviewer must accept or reject **one operational
support package**: fully stopped upgrade; completed uniform special-task bootstrap
before public traffic; and account/catalog restore mutually exclusive with
rename/writers. Repository examples support this style but do not establish its
acceptance for this feature. If this package is unacceptable, return REQUEST_CHANGES
and design the specific missing admission mechanism; do not silently broaden to
rolling upgrade or concurrent catalog restore.

After technical review, user-facing product acceptance is limited to the stated
host/protected-user restrictions and textual-DEFINER-reference rejection. No
approval request should present these as full MySQL identity compatibility.

## Verification map

| Contract | Cheapest required evidence |
|---|---|
| Parser pairs, hosts, quoting, formatting, malformed auth syntax | Parser/AST UT |
| Privilege mapping, tenant boundary, stale current principal | Focused frontend UT |
| IDs and all unchanged account fields/grants retained | Handler UT + minimal SQL BVT |
| Missing source, collision, normalized-name collision, protected users | Table-driven UT and public error checks |
| Second-pair failure rolls back first, query/commit/cancel failure | Injected BackgroundExec failures + transaction BVT |
| Host convention and ordered pair semantics | MySQL 8.0 oracle with explicit MatrixOne deviations |
| Old/new login and live-session identity after old-name reuse | Two-client public SQL BVT; multi-CN if applicable |
| Stored definers and creation/rename race | Minimal object fixture + deterministic concurrency barrier |
| Concurrent CREATE/DROP/grant/restore | Existing lifecycle fixture, controlled barriers, no sleeps |
| Persistence and restore | Restart/restore of a small renamed-user fixture; unchanged IDs/grants |
| Delivery | gofmt, affected-package vet/lint, generated-parser diff, self-review |

Use the repository CGo wrapper for transitive frontend tests. Do not interpret a
mock-only pass as public SQL or multi-CN acceptance. All implementation tests are
currently **NOT_RUN** because implementation is held behind this design gate.

## Compatibility and rollout

No catalog format migration is proposed. Recommended initial support is a fully
upgraded cluster only, with the operational admission package above accepted before feature delivery.
Do not run RENAME during rolling upgrade or downgrade to binaries with old
name-authority paths after using it without a separate recovery review. This is
an intended contract, not a claim that a gate has been implemented.

Backup preserves the renamed rows and IDs; partial historical restore keeps the
existing conservative identity-continuity rule. Reversing names is a new account
administration operation, not guaranteed rollback (the old name may be reused).
No deployment, account mutation, or wire-version bump is authorized here.

## Review record and evidence limits

- v0.1: REQUEST_CHANGES, independent design reviewer.
- v0.2: REQUEST_CHANGES; concrete writer and operational-policy gaps.
- v0.3: REQUEST_CHANGES; DROP initial binding lacked an explicit lock.
- v0.4: narrowly corrected DROP lock/revalidation and added its two race schedules;
  NOT APPROVED; independent review and operational support acceptance pending.
- Approval must identify exact reviewed revision after blockers close. The gate
  requires traceable approval, not one exclusive named CODEOWNER. An explicit
  user approval of the technically reviewed final revision may satisfy it;
  generic approval to resume issue development does not.
- No product code changes; parser/frontend/BVT/multi-CN/oracle tests NOT_RUN.
- MySQL ordered-pair behavior is inferred from official source, not executed.
- The negative CDC reader search and routine/parser inventory are bounded static
  evidence, not exhaustive proof of every dynamically serialized consumer.
- Design-only review is not implementation readiness or a permission to publish.


## v0.5 recovery and approved compatibility addendum

This addendum supersedes the historical pending-review labels above. Exact v0.4
SHA f3bad1111eb88b10af482bac067caad07cbc288b306b2372a12a51b6e9a35030
received independent technical PASS; user accepted its operational and product
limits on 2026-10-26 (Sentinel_2d531c0090bc819191103b5869f55fa1).
The prior implementation and evidence files were not retained in the restored
workspace. This revision requires fresh implementation hashes and fresh tests;
conversation descriptions of old passing tests are not evidence for rebuilt code.

### Routine-bearing CLONE admission

Ordinary CLONE writes routines via clone_database_source.go persistence helpers;
it is not maintenance restore. Those common helpers must lock the creator ID and
persist the post-lock canonical name in the same transaction. Private CLONE
requests pessimistic RC, but getBackExecutorInternal's explicit-transaction path
returns a shared executor without applying that option. A fixed SI/optimistic
caller therefore cannot be assumed to satisfy the shared-lock invariant.

Chosen restriction: after resolving the complete cloneDatabaseSource, and before
changing foreign_key_checks or issuing target CREATE DATABASE, if either
userDefinedFuncs or storedProcedures is nonempty, require the actual background
transaction operator to be nonnil, pessimistic, and RC. Otherwise return a clear
NotSupported error. Check actual transaction metadata, not requested options or
session defaults. Apply at the common database-clone execution boundary so direct
CLONE and nested DATA BRANCH cannot bypass it. No-routine clones bypass this
new check and retain existing behavior. Existing transaction finalization owns
cleanup; do not commit/rollback unrelated explicit-transaction work or convert
its isolation mode. Earlier read/admission locks may occur, but no target catalog
mutation or session-variable mutation may precede rejection.

Alternatives rejected: silently change explicit isolation (violates caller txn),
allow stale SI canonical reads (violates identity invariant), separate side txn
(lock released before publication). This is deliberately narrower than rejecting
all non-RC CLONE. User accepted continued dot work after the exact additional
restriction and cloud validation explanation on 2026-10-27:
Sentinel_7b8d3b5825d081919aa77510cd153f72, “发布云端，允许dot继续”.
Parent provided this as explicit scope approval; publication here remains out of
scope. Tests must cover function/procedure, missing op, SI/optimistic rejection,
pessimistic-RC acceptance and no-routine control, plus entrypoint proof that
rejection occurs before CREATE DATABASE.

### Previously reviewed identity consumer closures to reconstruct

- AuthenticateUser resolves credentials once by username. All later status/lock,
  expiry reads and four bookkeeping writes use that resolved user ID, never a
  reused name. Missing/malformed user-row results fail closed. Preserve existing
  failed-login counter commit semantics. Deterministic full-entrypoint mocks must
  switch the old-name binding after credentials and test locked/deleted original,
  failed password and successful password paths.
- Authenticated special identity recognition requires sys tenant ID AND sys
  tenant name, sentinel MaxUint32 user ID, and registered special name. Routine
  canonical-name persistence may exempt that identity only when the per-call
  context account is also sys. Normal sys or non-sys same-name catalog users
  must still take their stable-ID catalog lock. Do not add internal-session or
  username-only privilege exemptions.
- All routine definer SQL templates receive a complete correctly quoted literal;
  do not mix a double-quote escaping helper with single-quoted template slots.
  Test apostrophe/backslash names for create AND replace of both routine kinds.
- Existing approved stable-ID DROP/GRANT/REVOKE locks and post-lock name-ID
  revalidation, ALTER current-ID checks, implicit SHOW GRANTS by current ID,
  ordered-pair atomic rename and all tenant/protected/DEFINER restrictions remain.

No new catalog migration, distributed protocol, online-upgrade claim, restore
concurrency support or special account creation is introduced. Real SQL, unique
index races, multi-CN and restart/restore acceptance still require independent
execution evidence, not mock/race-detector extrapolation.


## v0.6 destination login-name safety boundary

This addendum supersedes v0.5's normalizeName-only destination policy. User
Sentinel_d4a1219f51248191b7ff14c881429854 explicitly confirmed rejection of target
names that cannot reliably log in, responding to assistant
Sentinel_c90afd87634c8191850f114a8d80b58d. Parent supplied the exact approved
consumer-derived boundary; implementation follows independent affected review.

### Concrete failure and narrow closure

RENAME can currently publish a destination accepted by normalizeName but rejected
or reinterpreted on subsequent authentication. inputNameIsInvalid rejects ASCII
space/tab/backtick/single/double quotes; getSqlForPasswordOfUser uses that helper
before its double-quoted SQL template. Backslashes change the template's literal
meaning. getUserPart truncates at `?`; standalone GetTenantInfo converts exact
`%3A` into a tenant delimiter. Protocol username fields use readStringNUL, making
NUL unable to roundtrip. Merely removing the shared input guard would also admit
these names into other unescaped CREATE/ALTER/DROP/GRANT templates.

After existing normalizeName trimming/empty/:/# rules, apply a destination-only
validator: reuse inputNameIsInvalid for its exact rejected ASCII set; additionally
reject backslash, question mark, NUL and exact uppercase substring `%3A`. Reject
before obtaining a background executor or starting a transaction. Validate every
pair before any statement writes, so an invalid later destination cannot publish
an earlier rename. No credential/management helper or wire parser is loosened.

Do NOT add this validator to source names. Existing source normalization and
protected/admin/DEFINER checks remain, allowing catalog labels with quotes or
other newly-disallowed destination characters to be renamed away through the
existing safely quoted source lookup. This is not a promise that arbitrary
corrupt/unparseable catalog bytes can be addressed by SQL.

Keep other accepted names: ordinary Unicode, underscore, dash, dot, literal
percent and lowercase `%3a`. Do not generalize into ASCII-only identifiers,
case-insensitive percent rejection, all whitespace/control rejection or a new
login protocol. The invariant is this bounded proven consumer restriction, not
full MySQL username-grammar compatibility. No additional host/identity policy.

### Validation / compatibility

Tests enumerate each rejected character and exact `%3A`, plus positive Unicode,
`_`, `-`, `.`, `%`, `%3a` controls. Handler tests put the invalid destination in
the second pair, assert zero executor creation/SQL/transaction effects, and retain
normal ordered-pair atomicity tests. A legacy quoted source successfully migrates
to a safe destination while retaining its ID. Parser remains expressive; only
execution destinations gain this restriction. Re-run affected frontend focused,
owning normal/race checks and independent review. Earlier unchanged parser,
planner and engine compile evidence may be reused with exact-source justification.

The recovered v0.5 patch/head is 9022c82b20f124841a30b57fbd404512364c571eeb69c61a00c1b72aaada9e12 /
78fc21627d6799d25e1d3bdc83086c41503827f2. Restored from verified Library v1, not
reconstructed from conversation. All existing offline/bootstrap/restore, host,
admin, DEFINER, CLONE pessimistic-RC and stable-ID contracts remain unchanged.
Real SQL/multi-CN/restore acceptance remains NOT_RUN and transfer blocked until
supported execution is available; this user approval does not waive those gates.
