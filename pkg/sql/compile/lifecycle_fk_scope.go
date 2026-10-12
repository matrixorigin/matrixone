// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type dropLifecycleIdentity struct {
	database, table   string
	databaseID        uint64
	logicalID         uint64
	parents, children []uint64
}

func equalDropLifecycleDomain(a, b map[uint64]dropLifecycleIdentity) bool {
	if len(a) != len(b) {
		return false
	}
	for id, x := range a {
		y, ok := b[id]
		if !ok || x.database != y.database || x.table != y.table || x.databaseID != y.databaseID ||
			!slices.Equal(x.parents, y.parents) || !slices.Equal(x.children, y.children) {
			return false
		}
	}
	return true
}

func sortedFKIDs(ids []uint64) []uint64 {
	return slices.Sorted(maps.Keys(fkIDSet(ids)))
}

// A root ID authorizes only the certified scalar DROP. Zero retains the broad
// capability, passed only by callers that have successfully acquired G X.
type replacementForeignKeyCatalog struct {
	target       plan2.FkReferKey
	materialized []*plan.ForeignKeyDef
}

type dropLifecycleAdmission struct {
	rootID            uint64
	accountID         uint32
	root              dropLifecycleIdentity
	createNames       map[plan2.FkReferKey]struct{}
	foreignKeyCatalog *replacementForeignKeyCatalog
}

type dropLifecycleKey struct{}
type dropLifecycleReceipt struct {
	owner     client.TxnOperator
	admission dropLifecycleAdmission
	closed    bool
}

func (c *Compile) withDropLifecycle(admission dropLifecycleAdmission, run func() error) error {
	old := c.proc.Ctx
	actor, err := defines.GetAccountId(old)
	if err != nil {
		return err
	}
	admission.accountID = actor
	receipt := &dropLifecycleReceipt{owner: c.proc.GetTxnOperator(), admission: admission}
	c.proc.Ctx = context.WithValue(old, dropLifecycleKey{}, receipt)
	defer func() { receipt.closed = true; c.proc.Ctx = old }()
	return run()
}

func (c *Compile) borrowedDropLifecycle(targets ...*plan.DropTable) (bool, error) {
	r, ok := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt)
	if !ok {
		return false, nil
	}
	if r.closed || r.owner == nil || r.owner != c.proc.GetTxnOperator() {
		return false, moerr.NewInternalError(c.proc.Ctx, "expired DROP lifecycle owner")
	}
	if !r.owner.Txn().IsPessimistic() {
		return false, nil
	}
	a := r.admission
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return false, err
	}
	if accountID != a.accountID {
		return false, moerr.NewInternalError(c.proc.Ctx, "invalid DROP lifecycle actor")
	}
	if a.rootID == 0 {
		return true, nil
	}
	if len(targets) != 1 || targets[0] == nil {
		return false, moerr.NewInternalError(c.proc.Ctx, "invalid scalar DROP lifecycle target")
	}
	q := targets[0]
	if q.Database != a.root.database || q.Table != a.root.table || q.TableId != a.rootID ||
		q.TableDef == nil || plan2.SnapshotTableID(q.TableDef) != a.root.logicalID {
		return false, moerr.NewInternalError(c.proc.Ctx, "scalar DROP exceeds its admitted root")
	}
	db, err := c.e.Database(c.proc.Ctx, q.Database, r.owner)
	if err != nil {
		return false, err
	}
	rel, err := db.Relation(c.proc.Ctx, q.Table, nil)
	if err != nil {
		return false, err
	}
	if rel.GetTableID(c.proc.Ctx) != a.rootID || rel.GetDBID(c.proc.Ctx) != a.root.databaseID ||
		plan2.SnapshotTableID(rel.GetTableDef(c.proc.Ctx)) != a.root.logicalID {
		return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return true, nil
}

func (c *Compile) scalarDropLifecycle() bool {
	r, ok := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt)
	return ok && r.admission.rootID != 0
}

func (c *Compile) borrowedBroadLifecycle() (bool, error) {
	r, ok := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt)
	if !ok {
		return false, nil
	}
	if r.admission.rootID != 0 {
		return false, nil
	}
	return c.borrowedDropLifecycle()
}

func (c *Compile) isLifecycleRC() bool {
	op := c.proc.GetTxnOperator()
	return op != nil && op.Txn().IsPessimistic() && op.Txn().IsRCIsolation()
}

func fkIDSet(ids []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id != 0 {
			set[id] = struct{}{}
		}
	}
	return set
}

// The executor removes complete FK relationships by table ID; columns/actions
// are not consumed by DROP. Compare exactly those parent/child sets, not schema
// version alone (own uncommitted definitions can have version zero).
func dropFKSetsMatch(q *plan.DropTable, def *plan.TableDef) bool {
	parents := make([]uint64, 0, len(def.GetFkeys()))
	for _, fk := range def.GetFkeys() {
		if fk == nil {
			return false
		}
		parents = append(parents, fk.ForeignTbl)
	}
	return maps.Equal(fkIDSet(q.ForeignTbl), fkIDSet(parents)) &&
		maps.Equal(fkIDSet(q.FkChildTblsReferToMe), fkIDSet(def.GetRefChildTbls()))
}

func (c *Compile) loadDropLifecycleDomain(tables []*plan.DropTable, database string) ([]lifecycleDatabaseName, map[uint64]dropLifecycleIdentity, error) {
	ctx := c.proc.Ctx
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, nil, err
	}
	names := make([]lifecycleDatabaseName, 0, len(tables)+1)
	identities := make(map[uint64]dropLifecycleIdentity)
	refs := make(map[uint64]struct{})
	ignoreFK, _ := ctx.Value(defines.IgnoreForeignKey{}).(bool)
	resolved := make(map[string]engine.Relation)
	if database != "" {
		names = append(names, lifecycleDatabaseName{accountID: accountID, name: database, mode: lock.LockMode_Exclusive})
		db, err := c.e.Database(ctx, database, c.proc.GetTxnOperator())
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				return names, identities, nil
			}
			return nil, nil, err
		}
		id, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
		if err != nil || id == 0 {
			return nil, nil, moerr.NewInternalError(ctx, "invalid DROP database identity")
		}
		// Zero is reserved for the root database receipt, never a table ID.
		identities[0] = dropLifecycleIdentity{database: database, databaseID: id}
		relations, err := db.Relations(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, name := range relations {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			rel, err := db.Relation(ctx, name, nil)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
					continue
				}
				return nil, nil, err
			}
			def := rel.GetTableDef(ctx)
			if def == nil {
				return nil, nil, moerr.NewInternalError(ctx, "missing DROP table definition")
			}
			q := &plan.DropTable{Database: database, Table: name, TableId: rel.GetTableID(ctx), TableDef: def}
			if !ignoreFK {
				for _, fk := range def.Fkeys {
					if fk == nil {
						return nil, nil, moerr.NewInternalError(ctx, "missing DROP foreign key")
					}
					q.ForeignTbl = append(q.ForeignTbl, fk.ForeignTbl)
				}
				q.FkChildTblsReferToMe = def.RefChildTbls
			}
			tables = append(tables, q)
			resolved[name] = rel
		}
	}
	for _, q := range tables {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if q == nil || q.Table == "" || q.GetTableDef().GetIsTemporary() {
			continue
		}
		if database == "" {
			if stmt, ok := c.stmt.(*tree.DropTable); ok && stmt.Temporary {
				continue
			}
			if session := c.proc.GetSession(); session != nil {
				if _, temporary := session.GetTempTable(q.Database, q.Table); temporary {
					continue
				}
			}
		}
		names = append(names, lifecycleDatabaseName{accountID: accountID, name: q.Database, mode: lock.LockMode_Shared})
		rel := resolved[q.Table]
		if rel == nil {
			db, err := c.e.Database(ctx, q.Database, c.proc.GetTxnOperator())
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					if q.IfExists && q.TableId == 0 {
						continue
					}
					if q.TableId == 0 {
						return nil, nil, moerr.NewNoSuchTable(ctx, q.Database, q.Table)
					}
					return nil, nil, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
				}
				return nil, nil, err
			}
			rel, err = db.Relation(ctx, q.Table, nil)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
					if q.IfExists && q.TableId == 0 {
						continue
					}
					if q.TableId == 0 {
						return nil, nil, moerr.NewNoSuchTable(ctx, q.Database, q.Table)
					}
					return nil, nil, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
				}
				return nil, nil, err
			}
		}
		def := rel.GetTableDef(ctx)
		if def == nil || (q.TableId != 0 && q.TableId != rel.GetTableID(ctx)) ||
			(!q.IsView && q.TableDef == nil) || (!ignoreFK && !dropFKSetsMatch(q, def)) {
			return nil, nil, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		id := rel.GetTableID(ctx)
		if id == 0 || rel.GetDBID(ctx) == 0 ||
			(database != "" && rel.GetDBID(ctx) != identities[0].databaseID) {
			return nil, nil, moerr.NewInternalError(ctx, "invalid DROP relation identity")
		}
		identities[id] = dropLifecycleIdentity{database: q.Database, table: q.Table, databaseID: rel.GetDBID(ctx),
			parents: sortedFKIDs(q.ForeignTbl), children: sortedFKIDs(q.FkChildTblsReferToMe)}
		for _, ids := range [][]uint64{q.ForeignTbl, q.FkChildTblsReferToMe} {
			for _, id := range ids {
				if id != 0 {
					refs[id] = struct{}{}
				}
			}
		}
	}
	for id := range refs {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		db, table, rel, err := c.e.GetRelationById(ctx, c.proc.GetTxnOperator(), id)
		if err != nil {
			if isMissingTableByIdForFkCleanup(err) {
				continue
			}
			return nil, nil, err
		}
		if rel == nil || db == "" || table == "" || rel.GetTableID(ctx) != id || rel.GetDBID(ctx) == 0 {
			return nil, nil, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		identity := dropLifecycleIdentity{database: db, table: table, databaseID: rel.GetDBID(ctx)}
		if old, ok := identities[id]; ok {
			if old.database != db || old.table != table || old.databaseID != identity.databaseID {
				return nil, nil, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
			}
		} else {
			identities[id] = identity
		}
		names = append(names, lifecycleDatabaseName{accountID: accountID, name: db, mode: lock.LockMode_Shared})
	}
	return names, identities, nil
}

func (c *Compile) admitDropLifecycleRCWithDomain(tables []*plan.DropTable, database string) (map[uint64]dropLifecycleIdentity, bool, error) {
	names, before, err := c.loadDropLifecycleDomain(tables, database)
	if err != nil || len(names) == 0 {
		return nil, false, err
	}
	rootIDs := dropLifecycleBranchRoots(before, tables, database)
	needsExclusiveGate, err := c.dropRootsHaveAlterLineageRC(rootIDs)
	if err != nil {
		return nil, false, err
	}
	if err = c.admitLifecycleRC(names, needsExclusiveGate); err != nil {
		return nil, false, err
	}
	_, after, err := c.loadDropLifecycleDomain(tables, database)
	if err != nil {
		return nil, false, err
	}
	if !equalDropLifecycleDomain(before, after) {
		return nil, false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	// An owner can publish lineage while the first probe still sees an older
	// RC snapshot. D and the applied frontier make this probe authoritative.
	// Promote G without waiting, then retry under the refreshed frontier. A
	// competing holder causes a terminal lock conflict and whole-txn rollback;
	// queuing an upgrade after D could form a wait cycle.
	if !needsExclusiveGate {
		participates, err := c.dropRootsHaveAlterLineageRC(dropLifecycleBranchRoots(after, tables, database))
		if err != nil {
			return nil, false, err
		}
		if participates {
			if err := c.admitLifecycleRC(nil, true); err != nil {
				return nil, false, err
			}
			return nil, false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
	}
	return after, needsExclusiveGate, nil
}

func (c *Compile) dropRootsHaveAlterLineageRC(ids []uint64) (bool, error) {
	if len(ids) == 0 {
		return false, nil
	}
	hasBranch, err := c.databaseHasBranchLineageRC(ids)
	if err != nil || !hasBranch {
		return false, err
	}
	dag, err := c.loadAlterDataBranchDAG(false)
	if err != nil {
		return false, err
	}
	return dag.ComponentsHaveAlterLineage(ids), nil
}

func dropLifecycleBranchRoots(domain map[uint64]dropLifecycleIdentity, tables []*plan.DropTable, database string) []uint64 {
	ids := make([]uint64, 0, len(domain))
	if database != "" {
		for id, identity := range domain {
			if id != 0 && identity.database == database {
				ids = append(ids, id)
			}
		}
	} else {
		for _, table := range tables {
			if table != nil && !table.IsView && table.TableId != 0 {
				if _, ok := domain[table.TableId]; ok {
					ids = append(ids, table.TableId)
				}
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func createTablePublishesForeignKeys(q *plan.CreateTable) bool {
	return len(q.GetUpdateFkSqls()) != 0 || len(q.GetTableDef().GetFkeys()) != 0 ||
		len(q.GetFkDbs()) != 0 || len(q.GetFkTables()) != 0 || len(q.GetFksReferToMe()) != 0
}

func (c *Compile) admitForeignKeyCreate() error {
	borrowed, err := c.borrowedBroadLifecycle()
	if err != nil || borrowed {
		return err
	}
	op := c.proc.GetTxnOperator()
	if op == nil || !op.Txn().IsPessimistic() {
		return nil
	}
	if c.isLifecycleRC() {
		return c.admitLifecycleRC(nil, true)
	}
	return c.lockDataBranchLineageOwnerLifecyclePessimistic()
}

func (c *Compile) validateForeignKeyParentGeneration(database, table string, expectedID uint64) error {
	db, err := c.e.Database(c.proc.Ctx, database, c.proc.GetTxnOperator())
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			if expectedID == 0 {
				return nil
			}
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		return err
	}
	parent, err := db.Relation(c.proc.Ctx, table, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			if expectedID == 0 {
				return nil
			}
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		return err
	}
	if expectedID == 0 || parent.GetTableID(c.proc.Ctx) != expectedID {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return nil
}

func (c *Compile) validateCreateForeignKeyParents(q *plan.CreateTable, database string) error {
	if q.GetTemporary() {
		return nil
	}
	names, databases := q.GetFkTables(), q.GetFkDbs()
	fkeys := q.GetTableDef().GetFkeys()
	if len(names) != len(databases) || len(names) != len(fkeys) {
		return moerr.NewInternalError(c.proc.Ctx, "inconsistent CREATE foreign-key parents")
	}
	for i, name := range names {
		if databases[i] == database && name == q.GetTableDef().GetName() {
			continue
		}
		if fkeys[i] == nil {
			return moerr.NewInternalError(c.proc.Ctx, "missing CREATE foreign-key parent")
		}
		if err := c.validateForeignKeyParentGeneration(databases[i], name, fkeys[i].ForeignTbl); err != nil {
			return err
		}
	}

	for _, parent := range q.UnresolvedFkParents {
		if parent == nil {
			return moerr.NewInternalError(c.proc.Ctx, "missing unresolved CREATE foreign-key parent")
		}
		if err := c.validateForeignKeyParentGeneration(parent.DbName, parent.ObjName, 0); err != nil {
			return err
		}
	}

	return nil
}

// createNameDomain includes negative dependencies: a declared absent parent
// owns the same name boundary as a materialized parent.
func createNameDomain(q *plan.CreateTable, database string) map[plan2.FkReferKey]struct{} {
	domain := make(map[plan2.FkReferKey]struct{})
	if q == nil {
		return domain
	}
	if q.Database != "" {
		database = q.Database
	}
	domain[plan2.FkReferKey{Db: database, Tbl: q.GetTableDef().GetName()}] = struct{}{}
	for i, name := range q.FkTables {
		if i < len(q.FkDbs) {
			domain[plan2.FkReferKey{Db: q.FkDbs[i], Tbl: name}] = struct{}{}
		}
	}
	for _, parent := range q.UnresolvedFkParents {
		if parent != nil {
			domain[plan2.FkReferKey{Db: parent.DbName, Tbl: parent.ObjName}] = struct{}{}
		}
	}
	return domain
}

func createTableForNameAdmission(p *plan.Plan) *plan.CreateTable {
	ddl := p.GetDdl()
	if create := ddl.GetCreateTable(); create != nil {
		return create
	}
	return ddl.GetCloneTable().GetCreateTable().GetDdl().GetCreateTable()
}

func sameCreateNameDomain(a, b *plan.CreateTable) bool {
	return a != nil && b != nil && a.Database == b.Database && a.GetTableDef().GetName() == b.GetTableDef().GetName() &&
		maps.Equal(createNameDomain(a, a.Database), createNameDomain(b, b.Database))
}

func sameCreateNameBinding(a *lock.LockTable, b lock.LockTable) bool {
	return a != nil && a.Valid && b.Valid && a.AllocatorID != "" && a.AllocatorID == b.AllocatorID &&
		a.Group == b.Group && a.Table == b.Table && a.OriginTable == b.OriginTable &&
		a.Sharding == b.Sharding && a.ServiceID == b.ServiceID && a.Version == b.Version
}

func (c *Compile) lockCatalogName(rel engine.Relation, actor uint32, names []string,
	mode lock.LockMode, policy lock.WaitPolicy) (lock.Result, error) {
	keys, err := getLockBatch(c.proc, actor, names)
	if err != nil {
		return lock.Result{}, err
	}
	defer keys.GetVector(0).Free(c.proc.Mp())
	if !c.proc.GetTxnOperator().Txn().IsRCIsolation() {
		return lock.Result{}, lockop.LockRowsWithWaitPolicy(c.e, c.proc, rel, rel.GetTableID(c.proc.Ctx), keys, 0, *keys.GetVector(0).GetType(), mode, lock.Sharding_None, actor, policy)
	}
	return lockop.LockRowsForAdmissionResult(c.proc.Ctx, c.e, c.proc, rel.GetTableID(c.proc.Ctx),
		keys, 0, *keys.GetVector(0).GetType(), mode, actor, policy)
}

func (c *Compile) admitCreateNames(q *plan.CreateTable, database string) error {
	rebuild, err := c.pinCreateNames(q, database)
	if err != nil {
		return err
	}
	if rebuild {
		return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
	}
	return nil
}

// pinCreateNames reports a pre-effect plan refresh separately from execution retry.
func (c *Compile) pinCreateNames(q *plan.CreateTable, database string) (bool, error) {
	if c.disableLock || q.Temporary || plan2.IsFkBannedDatabase(database) || !c.isLifecycleRC() || c.createNamePinsAdmitted {
		return false, nil
	}
	// Internal replacement must borrow its outer frontier, never advance after DROP.
	if receipt, ok := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt); ok {
		actor, err := defines.GetAccountId(c.proc.Ctx)
		if err != nil {
			return false, err
		}
		if receipt.closed || receipt.owner != c.proc.GetTxnOperator() || actor != receipt.admission.accountID {
			return false, moerr.NewInternalError(c.proc.Ctx, "invalid CREATE lifecycle owner")
		}
		for name := range createNameDomain(q, database) {
			if _, ok := receipt.admission.createNames[name]; !ok {
				return false, moerr.NewInternalError(c.proc.Ctx, "CREATE exceeds admitted replacement names")
			}
		}
		c.createNamePinsAdmitted = true
		return false, nil
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return false, err
	}
	snapshot, err := c.proc.GetTxnOperator().Snapshot()
	if err != nil {
		return false, err
	}
	policy := lock.WaitPolicy_Wait
	if len(snapshot.LockTables) > 0 || createTablePublishesForeignKeys(q) {
		policy = lock.WaitPolicy_FastFail
	}
	dbNames := make(map[string]struct{})
	domain := createNameDomain(q, database)
	for name := range domain {
		dbNames[name.Db] = struct{}{}
	}
	databases := slices.Sorted(maps.Keys(dbNames))
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_DATABASE)
	if err != nil {
		return false, err
	}
	for _, dbName := range databases {
		if _, err = c.lockCatalogName(dbRel, actor, []string{dbName}, lock.LockMode_Shared, policy); err != nil {
			return false, err
		}
	}
	tableRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return false, err
	}
	names := slices.Collect(maps.Keys(domain))
	slices.SortFunc(names, func(a, b plan2.FkReferKey) int {
		if a.Db < b.Db {
			return -1
		}
		if a.Db > b.Db {
			return 1
		}
		if a.Tbl < b.Tbl {
			return -1
		}
		if a.Tbl > b.Tbl {
			return 1
		}
		return 0
	})
	planSnapshot, hasPlanSnapshot := c.PlanSnapshotTS()
	if !hasPlanSnapshot {
		planSnapshot = c.proc.GetTxnOperator().SnapshotTS()
	}
	refresh := false
	for _, name := range names {
		result, err := c.lockCatalogName(tableRel, actor, []string{name.Db, name.Tbl}, lock.LockMode_Exclusive, policy)
		if err != nil {
			return false, err
		}
		refresh = refresh || !sameCreateNameBinding(q.FkNameBinding, result.LockedOn) || result.Timestamp.IsEmpty() || !result.Timestamp.Less(planSnapshot)
	}
	if refresh {
		if err = c.advanceLifecycleAdmissionSnapshot(); err != nil {
			return false, err
		}
		c.createNamePinsAdmitted = true
		return true, nil
	}
	c.createNamePinsAdmitted = true
	return false, nil
}

// prepareCreateNamePlan uses the existing planner before a borrowed statement
// acquires execution resources. Later execution retries still belong to its caller.
func (c *Compile) prepareCreateNamePlan(p *plan.Plan) (*plan.Plan, error) {
	q := createTableForNameAdmission(p)
	if q == nil || q.Temporary || c.disableLock || !c.isLifecycleRC() {
		return p, nil
	}
	database := q.Database
	if database == "" {
		database = c.db
	}
	if plan2.IsFkBannedDatabase(database) {
		return p, nil
	}
	c.capturePlanSnapshot()
	if createTablePublishesForeignKeys(q) {
		if err := c.admitForeignKeyCreate(); err != nil {
			return nil, err
		}
	}
	rebuild, err := c.pinCreateNames(q, database)
	if err != nil {
		return nil, err
	}
	if !rebuild {
		return p, nil
	}
	rebuilt, err := c.buildPlanFunc(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	next := createTableForNameAdmission(rebuilt)
	if !sameCreateNameDomain(q, next) {
		return nil, moerr.NewLockConflict(c.proc.Ctx)
	}
	if createTablePublishesForeignKeys(next) {
		if err := c.admitForeignKeyCreate(); err != nil {
			return nil, err
		}
	}
	c.capturePlanSnapshot()
	return rebuilt, nil
}

// Pin replacement endpoints before any DROP/COPY effects. G excludes FK
// publication, while endpoint name pins also exclude index-definition writers.
func (c *Compile) admitReplacementNames(database, source, target string, rel engine.Relation, added []*plan.AlterTableAddFk) (map[plan2.FkReferKey]struct{}, error) {
	domain := map[plan2.FkReferKey]struct{}{{Db: database, Tbl: target}: {}, {Db: database, Tbl: source}: {}}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	def := rel.CopyTableDef(c.proc.Ctx)
	if def.Partition != nil {
		metadata, err := c.proc.GetPartitionService().GetPartitionMetadata(c.proc.Ctx, rel.GetTableID(c.proc.Ctx), c.proc.GetTxnOperator())
		if err != nil {
			return nil, err
		}
		for _, partition := range metadata.Partitions {
			domain[plan2.FkReferKey{Db: database, Tbl: partition.PartitionTableName}] = struct{}{}
			domain[plan2.FkReferKey{Db: database, Tbl: partitionservice.GetPartitionTableName(target, partition.Name)}] = struct{}{}
		}
	}
	ids := append([]uint64(nil), def.RefChildTbls...)
	for _, fk := range def.Fkeys {
		ids = append(ids, fk.ForeignTbl)
	}
	for _, id := range sortedFKIDs(ids) {
		if id == 0 || id == rel.GetTableID(c.proc.Ctx) {
			continue
		}
		db, table, _, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), id)
		if err != nil {
			return nil, err
		}
		domain[plan2.FkReferKey{Db: db, Tbl: table}] = struct{}{}
	}
	for _, fk := range added {
		if fk != nil {
			domain[plan2.FkReferKey{Db: fk.DbName, Tbl: fk.TableName}] = struct{}{}
		}
	}
	if !plan2.IsFkBannedDatabase(database) {
		res, err := c.runSqlWithResultAndOptions(fmt.Sprintf("select distinct refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name=%s and table_name=%s", sqlquote.String(database), sqlquote.String(source)), int32(actor), executor.StatementOption{}.WithDisableLog())
		if err != nil {
			return nil, err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				domain[plan2.FkReferKey{Db: string(cols[0].GetBytesAt(i)), Tbl: string(cols[1].GetBytesAt(i))}] = struct{}{}
			}
			return true
		})
		res.Close()
	}
	keys := slices.Collect(maps.Keys(domain))
	slices.SortFunc(keys, func(a, b plan2.FkReferKey) int {
		if a.Db != b.Db {
			if a.Db < b.Db {
				return -1
			}
			return 1
		}
		if a.Tbl < b.Tbl {
			return -1
		}
		if a.Tbl > b.Tbl {
			return 1
		}
		return 0
	})
	dbs, err := getRelFromMoCatalog(c, catalog.MO_DATABASE)
	if err != nil {
		return nil, err
	}
	tables, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if _, err := c.lockCatalogName(dbs, actor, []string{key.Db}, lock.LockMode_Shared, lock.WaitPolicy_FastFail); err != nil {
			return nil, err
		}
		if _, err := c.lockCatalogName(tables, actor, []string{key.Db, key.Tbl}, lock.LockMode_Exclusive, lock.WaitPolicy_FastFail); err != nil {
			return nil, err
		}
	}
	// The caller's existing replacement frontier follows these pins, before
	// any effects. Do not add another frontier or refresh inside nested CREATE.
	return domain, nil
}
