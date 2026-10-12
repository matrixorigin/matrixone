# MatrixOne View Privilege Support (GRANT/REVOKE ON VIEW): Final Design and Implementation

## Final Design

### 1. Goals and Scope
Provide `GRANT/REVOKE ... ON VIEW` with correct authorization semantics so users can access view results safely while respecting `SQL SECURITY` behavior.

### 2. Privilege Object and Levels
- Introduce `objectTypeView` alongside `objectTypeTable`.
- Privilege levels match table privileges: `*.*`, `db.*`, `db.view`, `view` (current database).
- Privilege types match table privileges (e.g., `SELECT/INSERT/UPDATE/DELETE/OWNERSHIP/ALL`), and `SHOW GRANTS` displays `ON VIEW`.

### 3. Metadata and Storage
- View object id is `mo_catalog.mo_tables.rel_logical_id` with `relkind='v'`.
- Store privileges in `mo_catalog.mo_role_privs` with `obj_type="view"` and `obj_id=rel_logical_id`.
- View privileges are read and checked through the same pipeline as table privileges, but with `obj_type` distinguishing view vs table.
- A table wildcard grant does not grant access to views; use `GRANT ... ON VIEW db.*` for view wildcard access.

### 4. Plan-Time View Lineage
The binder records typed `ViewStep` paths, ordered outermost to innermost. Each
step preserves its catalog tenant, complete snapshot timestamp, database/view
name and subscriber grant namespace. Query-level `ViewReferences` retain
authorization obligations for constant views and optimized-away scans; physical
node `ViewPath` checks cover matching logical prefixes without duplicate work.

### 5. Runtime Authorization Flow
For each plan node:
1. Verify every logical/physical view path in chain order (outermost to innermost).
2. For each view in the chain, apply its `SQL SECURITY` to decide the effective role for the next hop:
   - `DEFINER`: switch to the view catalog account and its definer role for the next hop.
   - `INVOKER`: keep the current account and role for the next hop.
3. After the chain, check base-table privileges using that effective principal.
   Subscriber grants use the subscription alias; publisher principals use the
   physical publisher database. Foreign-principal checks never use or populate
   the caller's privilege cache, even when numeric role IDs coincide.
4. Without a view path, use standard table privilege checks. Every object must
   pass independently; SELECT/ALL/OWNERSHIP alternatives apply to that object,
   never to the entire statement.

### 6. SQL SECURITY Semantics
- Session variable `view_security_type` controls `DEFINER` or `INVOKER` (default `DEFINER`).
- The `security_type` is persisted in viewdef JSON.
- For legacy views without `security_type`, treat as `DEFINER`.

### 7. Special Cases and Compatibility
- System view databases (`information_schema`, `mysql`) skip view privilege checks and remain read-only.
- Fully qualified `db.view` queries can run without `USE <db>` if view privilege passes.
- Role IDs are account-local: moadmin is role 0 in sys, accountadmin is role 2
  in a non-sys account.
- Cross-account CLONE assigns newly created objects to the target administrator;
  same-account CLONE keeps the caller's ownership. RESTORE retains its separate
  historical ownership policy. Invalid owners already persisted by older clones
  are not guessed or remapped at read time; an authorized owner must repair those
  definitions explicitly.

### 8. Testable Behaviors
- `GRANT/REVOKE ... ON VIEW` syntax and `SHOW GRANTS` output.
- Multi-layer view chains work with grant/revoke.
- `DEFINER/INVOKER` semantics enforced.
- Role inheritance and partial revokes work for non-system tenants.

## Implementation

### 1. Syntax and Object Model
- Extend `ObjectType.String()` to include `view`.
- Add `objectTypeView` and its privilege level mappings.

### 2. Metadata and Plan Structures
- Extend plan proto and generated code to carry view lineage fields.
- Record per-hop `ViewPath` and query-level `ViewReferences` during binding and
  preserve them through plan copies and prepared execution.

### 3. Authorization Logic
- Extract view privilege tips from plan nodes and check them before table privileges.
- Enforce `SQL SECURITY` per view in the lineage to determine the effective role for base-table checks.
- For view chains, cache-only privilege evaluation is skipped to avoid missing view metadata checks.
- Snapshot reads resolve view metadata using the snapshot tenant and `MO_TS` to ensure view chains are validated against the correct historical catalog.
- View privilege checks use the view-level snapshot (when the view is referenced with a snapshot) rather than the underlying table scan snapshot to avoid false "view not found" errors.
- Fix privilege extraction for `INSERT/UPDATE/DELETE` paths to avoid missed checks.
- Preserve existing system view and cluster table guardrails.

### 4. Tests and Results
Updated or added BVT cases:
- `test/distributed/cases/zz_accesscontrol/grant_view.sql`
- `test/distributed/cases/zz_accesscontrol/grant_view_non_sys.sql`
- `test/distributed/cases/zz_accesscontrol/grant_view_complex.sql`
- `test/distributed/cases/zz_accesscontrol/grant_view_nested_security.sql`
- Updated related `.result` files and error messages (e.g., `grant_privs_role.result`, `revoke_privs_role.result`).

## MySQL Compatibility

### Compatible
- Syntax: `GRANT/REVOKE ... ON VIEW`, `SHOW GRANTS` shows `ON VIEW`.
- Semantics: `SQL SECURITY DEFINER/INVOKER` aligns with MySQL (view privilege plus definer/invoker base-table privilege).
- Nested views: privilege checks follow the view chain with per-view `DEFINER/INVOKER` semantics.
- Isolation: granting view privilege does not grant base-table privilege.

### Differences
- DML on views: MatrixOne rejects `INSERT/UPDATE/DELETE` on views, while MySQL allows DML on updatable views.
- SQL SECURITY declaration: MatrixOne uses session variable `view_security_type` persisted in viewdef, not `CREATE VIEW ... SQL SECURITY ...`.
- No separate `SHOW VIEW` privilege type; it follows table/database privilege categories.
