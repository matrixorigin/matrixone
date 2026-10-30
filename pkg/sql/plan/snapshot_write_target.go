// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// A {snapshot=...} / {timestamp=...} table qualifier selects a read-only
// historical version. The DML and DDL builders resolve write targets against
// the live table, so a qualified target used to modify (or drop) the live table
// silently. Reject such targets before planning; snapshot-qualified tables
// remain valid as read sources (INSERT ... SELECT, UPDATE/DELETE join sources).
func rejectSnapshotQualifiedWriteTarget(ctx CompilerContext, stmt tree.Statement) error {
	var (
		op      string
		targets []*tree.TableName
	)
	switch s := stmt.(type) {
	case *tree.Insert:
		op, targets = "INSERT", firstTableName(s.Table)
	case *tree.Replace:
		op, targets = "REPLACE", firstTableName(s.Table)
	case *tree.Update:
		op, targets = "UPDATE", updateWriteTargets(s)
	case *tree.Delete:
		op, targets = "DELETE", deleteWriteTargets(s)
	case *tree.AlterTable:
		op, targets = "ALTER TABLE", []*tree.TableName{s.Table}
	case *tree.TruncateTable:
		op, targets = "TRUNCATE TABLE", []*tree.TableName{s.Name}
	case *tree.DropTable:
		op, targets = "DROP TABLE", s.Names
	default:
		return nil
	}
	for _, target := range targets {
		if target != nil && target.AtTsExpr != nil {
			return moerr.NewNotSupportedf(ctx.GetContext(),
				"%s on a snapshot-qualified table '%s'", op, string(target.ObjectName))
		}
	}
	return nil
}

type snapshotTableRef struct {
	name  *tree.TableName
	alias string
}

func collectTableRefs(expr tree.TableExpr, refs []snapshotTableRef) []snapshotTableRef {
	switch e := expr.(type) {
	case *tree.TableName:
		return append(refs, snapshotTableRef{name: e})
	case *tree.AliasedTableExpr:
		if tn, ok := e.Expr.(*tree.TableName); ok {
			return append(refs, snapshotTableRef{name: tn, alias: string(e.As.Alias)})
		}
		return collectTableRefs(e.Expr, refs)
	case *tree.ParenTableExpr:
		return collectTableRefs(e.Expr, refs)
	case *tree.JoinTableExpr:
		refs = collectTableRefs(e.Left, refs)
		return collectTableRefs(e.Right, refs)
	}
	return refs
}

func firstTableName(expr tree.TableExpr) []*tree.TableName {
	refs := collectTableRefs(expr, nil)
	if len(refs) == 0 {
		return nil
	}
	return []*tree.TableName{refs[0].name}
}

func (r snapshotTableRef) matches(name string) bool {
	if r.alias != "" {
		return strings.EqualFold(r.alias, name)
	}
	return strings.EqualFold(string(r.name.ObjectName), name)
}

// updateWriteTargets returns the snapshot-qualified tables an UPDATE assigns
// to: the table of a single-table UPDATE, or the tables named by qualified
// SET columns of a multi-table UPDATE. Unqualified columns of a multi-table
// UPDATE are left to column resolution.
func updateWriteTargets(stmt *tree.Update) []*tree.TableName {
	var refs []snapshotTableRef
	for _, expr := range stmt.Tables {
		refs = collectTableRefs(expr, refs)
	}
	if len(refs) == 1 {
		return []*tree.TableName{refs[0].name}
	}
	var targets []*tree.TableName
	for _, assignment := range stmt.Exprs {
		for _, name := range assignment.Names {
			if name == nil || name.NumParts < 2 {
				continue
			}
			for _, ref := range refs {
				if ref.matches(name.TblNameOrigin()) {
					targets = append(targets, ref.name)
				}
			}
		}
	}
	return targets
}

// deleteWriteTargets returns the tables a DELETE removes rows from: the listed
// tables of a single-table DELETE, or the TableRefs entries matched by the
// multi-table DELETE target list.
func deleteWriteTargets(stmt *tree.Delete) []*tree.TableName {
	var targetRefs []snapshotTableRef
	for _, expr := range stmt.Tables {
		targetRefs = collectTableRefs(expr, targetRefs)
	}
	if len(stmt.TableRefs) == 0 {
		targets := make([]*tree.TableName, 0, len(targetRefs))
		for _, ref := range targetRefs {
			targets = append(targets, ref.name)
		}
		return targets
	}
	var sources []snapshotTableRef
	for _, expr := range stmt.TableRefs {
		sources = collectTableRefs(expr, sources)
	}
	var targets []*tree.TableName
	for _, target := range targetRefs {
		if target.name.AtTsExpr != nil {
			targets = append(targets, target.name)
			continue
		}
		for _, source := range sources {
			if source.matches(string(target.name.ObjectName)) {
				targets = append(targets, source.name)
			}
		}
	}
	return targets
}
