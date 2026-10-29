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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type forwardForeignKeyChild struct {
	relation engine.Relation
	bindings []*plan2.FkData
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
		child := forwardForeignKeyChild{relation: rel}
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
		// Read the child's CURRENT constraint instead of the bind-time snapshot:
		// between binding and publication, createTable registers this child on
		// the direct-FK parents' RefChildTableDef. Merging into the stale
		// snapshot would rewrite the child with a constraint that no longer
		// carries unrelated entries added in the meantime (#30604-like loss).
		latest, err := GetConstraintDef(c.proc.Ctx, child.relation)
		if err != nil {
			return err
		}
		// Preserve every unrelated constraint, and update each child exactly once.
		merged := &engine.ForeignKeyDef{}
		complete := &engine.ConstraintDef{Cts: make([]engine.Constraint, 0, len(latest.Cts)+1)}
		for _, constraint := range latest.Cts {
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
	if receipt.closed || receipt.owner != c.proc.GetTxnOperator() || receipt.admission.accountID != actor {
		return nil, moerr.NewInternalError(c.proc.Ctx, "invalid replacement foreign-key catalog owner")
	}
	// Physical descendants borrow lifecycle admission, not their root's FK payload.
	if preserved.target != (plan2.FkReferKey{Db: database, Tbl: table}) {
		return nil, nil
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
				// Tolerate a stale recorded key: the equivalent-key transition moves
				// the catalog binding while a child's stored ForeignKeyDef keeps the
				// removed name until the table is re-materialized (TRUNCATE or
				// recreation). Heal it to the surviving covering key instead of
				// failing the recreation (review 5691776262).
				healed, changed, valid := plan2.HealedForeignKeyReferencedIndex(
					parent, fk.ForeignCols, hint.ReferencedIndexName)
				if !valid {
					return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
				}
				if changed {
					hint.ReferencedIndexName = healed
				}
			}
			fk.ReferencedIndexName = hint.ReferencedIndexName
			fk.OnDeleteOrigin = hint.OnDeleteOrigin
			fk.OnUpdateOrigin = hint.OnUpdateOrigin
		}
	}
	return nil
}

// syncDurableForeignKeyIndexBindings aligns each referencing child's stored
// ForeignKeyDef.ReferencedIndexName with the mo_foreign_keys rows this ALTER
// just updated, inside the same DDL transaction: the catalog rebinding and the
// durable constraint repair commit or roll back atomically together
// (review 5691776262). The re-materialization heals stale cached copies of
// the same defs, but the persisted post-commit state must never diverge.
func (c *Compile) syncDurableForeignKeyIndexBindings(qry *plan.AlterTable) error {
	if len(qry.GetUpdateFkSqls()) == 0 || plan2.IsFkBannedDatabase(qry.Database) {
		return nil
	}
	// The zero marker is the durable self-reference sentinel: the referencing
	// child is this ALTER's own table. Resolve it to the table's real ID so an
	// INPLACE alter aligns its own stored ForeignKeyDef with the catalog rows
	// this statement just updated (review 5695003826). A COPY still skips it
	// below: the in-flight original must not be disturbed mid-transfer, and
	// the published replacement relation is aligned after the FK state
	// restore instead.
	for _, childID := range qry.GetTableDef().GetRefChildTbls() {
		if childID == 0 && qry.GetTableDef().GetTblId() != 0 {
			childID = qry.GetTableDef().GetTblId()
		}
		if childID == 0 {
			continue
		}
		// A COPY repairs the self-referencing child's durable def through the
		// replacement relation's post-restore alignment, so the in-flight
		// original must not be disturbed mid-transfer; an INPLACE alter keeps
		// the original relation itself, and the same transaction can align its
		// durable binding directly.
		if childID == qry.GetTableDef().GetTblId() && qry.AlgorithmType == plan.AlterTable_COPY {
			continue
		}
		if err := c.syncChildDurableForeignKeyIndexBinding(childID); err != nil {
			return err
		}
	}
	return nil
}

// syncChildDurableForeignKeyIndexBinding rewrites one child's stored binding
// to the catalog's authoritative value when they diverge. The read, the
// rewrite and the mo_foreign_keys update share one transaction, so a failure
// aborts the statement and rolls both states back together.
func (c *Compile) syncChildDurableForeignKeyIndexBinding(childID uint64) error {
	childDb, childTbl, rel, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childID)
	if err != nil {
		return err
	}
	bindings, err := c.readChildForeignKeyIndexBindings(childDb, childTbl)
	if err != nil {
		return err
	}
	return alignDurableForeignKeyIndexBindings(c.proc.Ctx, rel, bindings)
}

// alignDurableForeignKeyIndexBindings rewrites one relation's stored
// ForeignKeyDef bindings to the catalog's authoritative values when they
// diverge. The durable read, the rewrite and the UpdateConstraint write all
// share the caller's transaction, so a failure aborts the statement and rolls
// the catalog rebinding and the durable repair back together. This is the
// shared durable-write consumer for cross-table children, the INPLACE self
// reference, and the COPY post-restore self alignment (review 5695003826).
func alignDurableForeignKeyIndexBindings(ctx context.Context, rel engine.Relation, bindings map[string]string) error {
	if len(bindings) == 0 {
		return nil
	}
	latest, err := GetConstraintDef(ctx, rel)
	if err != nil {
		return err
	}
	changed := false
	complete := &engine.ConstraintDef{Cts: make([]engine.Constraint, 0, len(latest.Cts))}
	for _, constraint := range latest.Cts {
		fks, ok := constraint.(*engine.ForeignKeyDef)
		if !ok {
			complete.Cts = append(complete.Cts, constraint)
			continue
		}
		for _, fk := range fks.Fkeys {
			if want, found := bindings[fk.Name]; found && want != "" && want != fk.ReferencedIndexName {
				fk.ReferencedIndexName = want
				changed = true
			}
		}
		complete.Cts = append(complete.Cts, fks)
	}
	if !changed {
		return nil
	}
	return rel.UpdateConstraint(ctx, complete)
}

func (c *Compile) readChildForeignKeyIndexBindings(db, tbl string) (map[string]string, error) {
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	res, err := c.runSqlWithResultAndOptions(
		fmt.Sprintf(
			"select constraint_name, referenced_index_name from `mo_catalog`.`mo_foreign_keys` where db_name = %s and table_name = %s",
			plan2.QuoteSQLStringLiteral(db), plan2.QuoteSQLStringLiteral(tbl)),
		int32(accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	bindings := make(map[string]string)
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		names := executor.GetStringRows(cols[0])
		referenced := executor.GetStringRows(cols[1])
		for i := 0; i < n && i < len(names) && i < len(referenced); i++ {
			bindings[names[i]] = referenced[i]
		}
		return true
	})
	return bindings, nil
}
