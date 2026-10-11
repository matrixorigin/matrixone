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
	"maps"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type forwardForeignKeyChild struct {
	relation   engine.Relation
	constraint *engine.ConstraintDef
	bindings   []*plan2.FkData
}

// The plan's incoming references only determine admission order. The catalog
// and child definitions read after admission own the actual constraints.
func (c *Compile) bindCreateForwardForeignKeys(q *plan.CreateTable, database string) ([]forwardForeignKeyChild, []string, error) {
	if q.Temporary || len(q.FksReferToMe) == 0 {
		return nil, nil, nil
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, nil, err
	}
	pinned := make(map[plan2.FkReferKey]struct{})
	for _, preview := range q.FksReferToMe {
		pinned[plan2.FkReferKey{Db: preview.Db, Tbl: preview.Table}] = struct{}{}
	}
	keys := slices.Collect(maps.Keys(pinned))
	slices.SortFunc(keys, func(a, b plan2.FkReferKey) int {
		if n := strings.Compare(a.Db, b.Db); n != 0 {
			return n
		}
		return strings.Compare(a.Tbl, b.Tbl)
	})
	if !c.disableLock && c.isLifecycleRC() {
		tables, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
		if err != nil {
			return nil, nil, err
		}
		for _, key := range keys {
			if _, err := c.lockCatalogName(tables, actor, []string{key.Db, key.Tbl}, lock.LockMode_Shared, lock.WaitPolicy_FastFail); err != nil {
				return nil, nil, err
			}
		}
		if _, borrowed := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt); !borrowed {
			if err := c.advanceLifecycleAdmissionSnapshot(); err != nil {
				return nil, nil, err
			}
		}
	}
	catalogRel, err := getRelFromMoCatalog(c, catalog.MOForeignKeys)
	if err != nil {
		return nil, nil, err
	}
	declarations, extended, err := plan2.ReadForeignKeysReferredTo(catalogRel.CopyTableDef(c.proc.Ctx), database, q.TableDef.Name,
		func(sql string) (executor.Result, error) {
			return c.runSqlWithResultAndOptions(sql, int32(actor), executor.StatementOption{}.WithDisableLog())
		})
	if err != nil {
		return nil, nil, err
	}
	delete(declarations, plan2.FkReferKey{Db: database, Tbl: q.TableDef.Name})
	for key := range declarations {
		if _, ok := pinned[key]; !ok {
			if c.executionGeneration > 0 {
				return nil, nil, moerr.NewLockConflict(c.proc.Ctx)
			}
			return nil, nil, moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}
	}
	preserved, err := c.replacementForeignKeyCatalog(database, q.TableDef.Name)
	if err != nil {
		return nil, nil, err
	}
	children := make([]forwardForeignKeyChild, 0, len(declarations))
	var backfills []string
	for _, key := range keys {
		constraints, ok := declarations[key]
		if !ok {
			continue
		}
		db, err := c.e.Database(c.proc.Ctx, key.Db, c.proc.GetTxnOperator())
		if err != nil {
			return nil, nil, err
		}
		rel, err := db.Relation(c.proc.Ctx, key.Tbl, nil)
		if err != nil {
			return nil, nil, err
		}
		current := rel.CopyTableDef(c.proc.Ctx)
		complete, err := GetConstraintDef(c.proc.Ctx, rel)
		if err != nil {
			return nil, nil, err
		}
		child := forwardForeignKeyChild{relation: rel, constraint: complete}
		for _, name := range slices.Sorted(maps.Keys(constraints)) {
			binding, err := plan2.BindForwardForeignKey(c.proc.Ctx, name, constraints[name], current, q.TableDef)
			if err != nil {
				return nil, nil, err
			}
			if preserved != nil {
				selected := constraints[name][0].ReferencedIndexName
				if selected != "" && !plan2.ForeignKeyReferencedIndexMatches(q.TableDef, binding.Def.ForeignCols, selected) {
					return nil, nil, moerr.NewInternalError(c.proc.Ctx, "replacement no longer has the incoming foreign key's selected referenced key")
				}
				// Preserve legacy empty bindings conservatively as well as named keys.
				binding.Def.ReferencedIndexName = selected
			}
			child.bindings = append(child.bindings, binding)
			if extended && constraints[name][0].ReferencedIndexName != binding.Def.ReferencedIndexName {
				backfills = append(backfills, plan2.GetSqlForUpdateFkReferencedIndex(key.Db, key.Tbl, name, binding.Def.ReferencedIndexName))
			}
		}
		children = append(children, child)
	}
	return children, backfills, nil
}

func (c *Compile) publishCreateForwardForeignKeys(parent engine.Relation, children []forwardForeignKeyChild) error {
	if len(children) == 0 {
		return nil
	}
	defs, err := parent.TableDefs(c.proc.Ctx)
	if err != nil {
		return err
	}
	columnIDs := make(map[string]uint64)
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			columnIDs[strings.ToLower(attr.Attr.Name)] = attr.Attr.ID
		}
	}
	childIDs := make([]uint64, 0, len(children))
	for _, child := range children {
		fresh := make(map[string]*plan.ForeignKeyDef, len(child.bindings))
		for _, binding := range child.bindings {
			fk := binding.Def
			fk.ForeignTbl = parent.GetTableID(c.proc.Ctx)
			for i, name := range binding.ColsReferred.Cols {
				id, ok := columnIDs[strings.ToLower(name)]
				if !ok {
					return moerr.NewInternalErrorf(c.proc.Ctx, "missing referenced column %s", name)
				}
				fk.ForeignCols[i] = id
			}
			fresh[fk.Name] = fk
		}
		// Preserve every unrelated constraint, and update each child exactly once.
		merged := &engine.ForeignKeyDef{}
		complete := &engine.ConstraintDef{Cts: make([]engine.Constraint, 0, len(child.constraint.Cts)+1)}
		for _, constraint := range child.constraint.Cts {
			if existing, ok := constraint.(*engine.ForeignKeyDef); ok {
				for _, fk := range existing.Fkeys {
					if _, replaced := fresh[fk.Name]; !replaced {
						merged.Fkeys = append(merged.Fkeys, fk)
					}
				}
			} else {
				complete.Cts = append(complete.Cts, constraint)
			}
		}
		for _, binding := range child.bindings {
			merged.Fkeys = append(merged.Fkeys, binding.Def)
		}
		complete.Cts = append(complete.Cts, merged)
		if err := child.relation.UpdateConstraint(c.proc.Ctx, complete); err != nil {
			return err
		}
		childIDs = append(childIDs, child.relation.GetTableID(c.proc.Ctx))
	}
	complete, err := GetConstraintDef(c.proc.Ctx, parent)
	if err != nil {
		return err
	}
	addRefChildTableIDs(complete, childIDs)
	return parent.UpdateConstraint(c.proc.Ctx, complete)
}

func (c *Compile) replacementForeignKeyCatalog(database, table string) (*replacementForeignKeyCatalog, error) {
	receipt, ok := c.proc.Ctx.Value(dropLifecycleKey{}).(*dropLifecycleReceipt)
	if !ok || receipt.admission.foreignKeyCatalog == nil {
		return nil, nil
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	preserved := receipt.admission.foreignKeyCatalog
	if receipt.closed || receipt.owner != c.proc.GetTxnOperator() || receipt.admission.accountID != actor ||
		preserved.target != (plan2.FkReferKey{Db: database, Tbl: table}) {
		return nil, moerr.NewInternalError(c.proc.Ctx, "invalid replacement foreign-key catalog owner")
	}
	return preserved, nil
}

func (c *Compile) applyReplacementForeignKeyHints(q *plan.CreateTable, database string) error {
	preserved, err := c.replacementForeignKeyCatalog(database, q.TableDef.Name)
	if err != nil || preserved == nil {
		return err
	}
	for _, hint := range preserved.materialized {
		for _, fk := range q.TableDef.Fkeys {
			if fk.Name != hint.Name {
				continue
			}
			if hint.ReferencedIndexName != "" {
				parent := q.TableDef
				if fk.ForeignTbl != 0 {
					_, _, rel, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fk.ForeignTbl)
					if err != nil {
						return err
					}
					parent = rel.CopyTableDef(c.proc.Ctx)
				}
				if !plan2.ForeignKeyReferencedIndexMatches(parent, fk.ForeignCols, hint.ReferencedIndexName) {
					return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
				}
			}
			fk.ReferencedIndexName = hint.ReferencedIndexName
			fk.OnDeleteOrigin = hint.OnDeleteOrigin
			fk.OnUpdateOrigin = hint.OnUpdateOrigin
		}
	}
	return nil
}
