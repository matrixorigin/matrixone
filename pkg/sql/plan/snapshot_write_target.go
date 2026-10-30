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
		op, targets = "UPDATE", updateWriteTargets(ctx, s)
	case *tree.Delete:
		op, targets = "DELETE", deleteWriteTargets(ctx, s)
	case *tree.AlterTable:
		op, targets = "ALTER TABLE", []*tree.TableName{s.Table}
	case *tree.TruncateTable:
		op, targets = "TRUNCATE TABLE", []*tree.TableName{s.Name}
	case *tree.DropTable:
		op, targets = "DROP TABLE", s.Names
	case *tree.Load:
		op, targets = "LOAD DATA", []*tree.TableName{s.Table}
	case *tree.MultiInsert:
		op, targets = "INSERT ALL", multiInsertTargets(s)
	case *tree.CreateIndex:
		op, targets = "CREATE INDEX", []*tree.TableName{s.Table}
	case *tree.DropIndex:
		op, targets = "DROP INDEX", []*tree.TableName{s.TableName}
	case *tree.RenameTable:
		op = "RENAME TABLE"
		for _, alter := range s.AlterTables {
			if alter != nil {
				targets = append(targets, alter.Table)
			}
		}
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

// matches reports whether a qualifier names this table reference. Like table
// names, aliases compare case-sensitively under lower_case_table_names=0.
func (r snapshotTableRef) matches(name string, caseSensitive bool) bool {
	refName := r.alias
	if refName == "" {
		refName = string(r.name.ObjectName)
	}
	if caseSensitive {
		return refName == name
	}
	return strings.EqualFold(refName, name)
}

func multiInsertTargets(stmt *tree.MultiInsert) []*tree.TableName {
	var targets []*tree.TableName
	add := func(list []*tree.MultiInsertTarget) {
		for _, target := range list {
			if target != nil {
				targets = append(targets, target.Table)
			}
		}
	}
	add(stmt.Targets)
	for _, when := range stmt.Whens {
		if when != nil {
			add(when.Targets)
		}
	}
	add(stmt.Else)
	return targets
}

// tableHasColumn reports whether a table reference's table has the column,
// resolving it against the catalog. Unknown tables report false; the planner
// reports them later.
func tableHasColumn(ctx CompilerContext, ref snapshotTableRef, column string) bool {
	schema := string(ref.name.SchemaName)
	if schema == "" {
		schema = ctx.DefaultDatabase()
	}
	_, def, err := ctx.Resolve(schema, string(ref.name.ObjectName), nil)
	if err != nil || def == nil {
		return false
	}
	for _, col := range def.Cols {
		if strings.EqualFold(col.Name, column) || strings.EqualFold(col.OriginName, column) {
			return true
		}
	}
	return false
}

// updateWriteTargets returns the snapshot-qualified tables an UPDATE assigns
// to: the table of a single-table UPDATE, or the tables named by qualified
// SET columns of a multi-table UPDATE. An unqualified SET column assigns to
// the snapshot-qualified table that has that column.
func updateWriteTargets(ctx CompilerContext, stmt *tree.Update) []*tree.TableName {
	var refs []snapshotTableRef
	for _, expr := range stmt.Tables {
		refs = collectTableRefs(expr, refs)
	}
	if len(refs) == 1 {
		return []*tree.TableName{refs[0].name}
	}
	caseSensitive := ctx.GetLowerCaseTableNames() == 0
	var targets []*tree.TableName
	for _, assignment := range stmt.Exprs {
		for _, name := range assignment.Names {
			if name == nil {
				continue
			}
			for _, ref := range refs {
				if name.NumParts < 2 {
					if ref.name.AtTsExpr != nil && tableHasColumn(ctx, ref, name.ColNameOrigin()) {
						targets = append(targets, ref.name)
					}
				} else if ref.matches(name.TblNameOrigin(), caseSensitive) {
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
func deleteWriteTargets(ctx CompilerContext, stmt *tree.Delete) []*tree.TableName {
	caseSensitive := ctx.GetLowerCaseTableNames() == 0
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
			if source.matches(string(target.name.ObjectName), caseSensitive) {
				targets = append(targets, source.name)
			}
		}
	}
	return targets
}
