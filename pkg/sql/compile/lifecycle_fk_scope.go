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
	"maps"
	"slices"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type dropLifecycleIdentity struct {
	database, table   string
	databaseID        uint64
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

// Only TRUNCATE's synchronous nested DROP may borrow its already-held broad
// gate. The receipt cannot outlive the callback or authorize another owner.
type broadDropLifecycleKey struct{}
type broadDropLifecycle struct {
	owner  client.TxnOperator
	closed bool
}

func (c *Compile) withBroadDropLifecycle(run func() error) error {
	old := c.proc.Ctx
	receipt := &broadDropLifecycle{owner: c.proc.GetTxnOperator()}
	c.proc.Ctx = context.WithValue(old, broadDropLifecycleKey{}, receipt)
	defer func() { receipt.closed = true; c.proc.Ctx = old }()
	return run()
}

func (c *Compile) borrowedDropLifecycle() (bool, error) {
	r, ok := c.proc.Ctx.Value(broadDropLifecycleKey{}).(*broadDropLifecycle)
	if !ok {
		return false, nil
	}
	if r.closed || r.owner == nil || r.owner != c.proc.GetTxnOperator() {
		return false, moerr.NewInternalError(c.proc.Ctx, "expired DROP lifecycle owner")
	}
	return true, nil
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

func (c *Compile) admitDropLifecycleRC(tables []*plan.DropTable, database string) error {
	names, before, err := c.loadDropLifecycleDomain(tables, database)
	if err != nil || len(names) == 0 {
		return err
	}
	if err = c.admitLifecycleRC(names); err != nil {
		return err
	}
	_, after, err := c.loadDropLifecycleDomain(tables, database)
	if err != nil {
		return err
	}
	if !equalDropLifecycleDomain(before, after) {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return nil
}
