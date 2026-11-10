// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// A historical clone (SNAPSHOT/MO_TS, including every snapshot/PITR restore)
// copies data blocks directly into the destination relation and therefore
// bypasses the row-wise foreign-key checks of ordinary INSERT. A child-table
// restore whose historical rows reference parent keys that are absent from
// the reachable parent table would commit orphan rows under an active FK
// (#30388). validateClonedTableFKIntegrity closes that hole: after the block
// clone it checks every outgoing foreign key of the destination table and
// rejects the statement when any non-NULL child key has no match in the
// parent relation visible to this transaction.
//
// Parent tables restored earlier in the same restore pass are visible here
// (the whole restore runs in one transaction), so database/account restores
// validate against their own freshly restored parents as long as the
// FK topological order is respected. A parent that does not resolve inside
// this transaction at all (dropped outside the restore scope, or a cycle
// member restored later) is skipped: its absence keeps the pre-existing
// behavior of a dangling reference instead of inventing a new one.
//
// Rows with NULL in any foreign-key column are exempt, matching ordinary FK
// semantics; self-references are excluded.

// probeCloneFKOrphan runs one orphan-detection query and reports whether a
// violating row exists, together with a printable sample of its key values.
// It is a package variable so unit tests can observe the generated SQL.
var probeCloneFKOrphan = func(c *Compile, sql string) (bool, string, error) {
	accountId, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return false, "", err
	}
	res, err := c.runSqlWithResultAndOptions(
		sql, int32(accountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return false, "", err
	}
	defer res.Close()
	found := false
	sample := ""
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n > 0 {
			found = true
			sample = cols[0].GetStringAt(0)
		}
		return false
	})
	return found, sample, nil
}

// shouldValidateClonedTableFKIntegrity limits the check to the two paths that
// copy historical data without row-wise FK checks: an inner statement of a
// snapshot/PITR restore, and a user-issued historical CREATE TABLE ... CLONE.
// An ordinary same-time clone copies the currently committed, already checked
// rows and keeps its cost profile.
func shouldValidateClonedTableFKIntegrity(c *Compile, clonePlan *plan.CloneTable) bool {
	if clonePlan.GetScanSnapshot() != nil {
		return true
	}
	session := c.proc.GetSessionInfo()
	return session != nil && session.IsRestore
}

func fkColumnNames(tableDef *plan.TableDef, colIds []uint64) ([]string, error) {
	if tableDef == nil || len(tableDef.Cols) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("missing table definition of %s for foreign-key restore validation", tableDef.GetName())
	}
	names := make([]string, len(colIds))
	for i, colId := range colIds {
		found := false
		for _, col := range tableDef.Cols {
			if col.ColId == colId {
				names[i] = col.Name
				found = true
				break
			}
		}
		if !found {
			return nil, moerr.NewInternalErrorNoCtxf("missing column id %d of table %s for foreign-key restore validation", colId, tableDef.GetName())
		}
	}
	return names, nil
}

// buildCloneFKOrphanProbeSQL finds one child row whose every FK column is
// non-NULL while no parent row matches it. The parent probe stays inside the
// cloning transaction, so earlier restore steps and committed data are both
// visible.
func buildCloneFKOrphanProbeSQL(
	dbName, tblName string, childCols []string,
	parentDb, parentTbl string, parentCols []string,
) string {
	qualifiedChild := sqlquote.QualifiedIdent(dbName, tblName)
	qualifiedParent := sqlquote.QualifiedIdent(parentDb, parentTbl)

	var b strings.Builder
	b.WriteString("select concat_ws(',', ")
	for i, col := range childCols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s.%s", qualifiedChild, sqlquote.Ident(col))
	}
	fmt.Fprintf(&b, ") from %s where ", qualifiedChild)
	for i, col := range childCols {
		if i > 0 {
			b.WriteString(" and ")
		}
		fmt.Fprintf(&b, "%s.%s is not null", qualifiedChild, sqlquote.Ident(col))
	}
	b.WriteString(" and not exists (select 1 from ")
	b.WriteString(qualifiedParent)
	b.WriteString(" where ")
	for i := range childCols {
		if i > 0 {
			b.WriteString(" and ")
		}
		fmt.Fprintf(&b, "%s.%s = %s.%s", qualifiedParent, sqlquote.Ident(parentCols[i]), qualifiedChild, sqlquote.Ident(childCols[i]))
	}
	b.WriteString(") limit 1")
	return b.String()
}

func validateClonedTableFKIntegrity(
	c *Compile,
	clonePlan *plan.CloneTable,
	dbName string,
	tblName string,
) error {
	if !shouldValidateClonedTableFKIntegrity(c, clonePlan) {
		return nil
	}
	dstDb, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	dstRel, err := dstDb.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		return err
	}
	// Read the FK definition from the freshly created relation, not from the
	// create plan: creation refreshes column ids, so plan-side ids must never
	// be mixed into a lookup against the stored definition.
	constraint, err := GetConstraintDef(c.proc.Ctx, dstRel)
	if err != nil {
		return err
	}
	var fkeys []*plan.ForeignKeyDef
	for _, ct := range constraint.Cts {
		if fkDef, ok := ct.(*engine.ForeignKeyDef); ok {
			fkeys = append(fkeys, fkDef.Fkeys...)
		}
	}
	if len(fkeys) == 0 {
		return nil
	}
	dstTableDef := dstRel.GetTableDef(c.proc.Ctx)
	for _, fk := range fkeys {
		if fk == nil || fk.ForeignTbl == 0 {
			// self references clone their parent rows together with the
			// children from the same source and cannot orphan against it.
			continue
		}
		parentDb, parentTbl, parentRel, err := c.e.GetRelationById(
			c.proc.Ctx, c.proc.GetTxnOperator(), fk.ForeignTbl)
		if err != nil {
			if isMissingTableByIdForFkCleanup(err) {
				// The parent is invisible to this transaction (dropped outside
				// the restore scope). Keep the historical dangling-reference
				// behavior rather than misreporting orphans.
				continue
			}
			return err
		}
		childCols, err := fkColumnNames(dstTableDef, fk.Cols)
		if err != nil {
			return err
		}
		parentCols, err := fkColumnNames(parentRel.CopyTableDef(c.proc.Ctx), fk.ForeignCols)
		if err != nil {
			return err
		}
		sql := buildCloneFKOrphanProbeSQL(dbName, tblName, childCols, parentDb, parentTbl, parentCols)
		found, sample, err := probeCloneFKOrphan(c, sql)
		if err != nil {
			return err
		}
		if found {
			return moerr.NewInternalErrorf(c.proc.Ctx,
				"cannot restore table %s.%s: foreign key constraint `%s` would leave orphan row(s) (key value %s) because the referenced key is missing in %s.%s; restore the referenced table together with this one or remove the constraint first",
				dbName, tblName, fk.Name, sample, parentDb, parentTbl)
		}
	}
	return nil
}
