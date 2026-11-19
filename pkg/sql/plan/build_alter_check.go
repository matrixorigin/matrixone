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

package plan

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Resolve generic drops without conflating CHECK with FOREIGN KEY. Keep an
// ordered name view so later clauses can see constraints added/dropped earlier.
func resolveAlterConstraintOptions(ctx CompilerContext, source *TableDef, options tree.AlterTableOptions, bindings alterColumnBindings) (tree.AlterTableOptions, error) {
	needsChecks := false
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.AlterOptionAdd:
			_, isCheck := opt.Def.(*tree.CheckIndex)
			needsChecks = needsChecks || isCheck
		case *tree.AlterOptionDrop:
			needsChecks = needsChecks || opt.Typ == tree.AlterTableDropCheck || opt.Typ == tree.AlterTableDropConstraint
		}
	}
	if !needsChecks {
		return options, nil
	}
	current := DeepCopyTableDef(source, true)
	if err := recoverLegacyChecks(ctx, current); err != nil {
		return nil, err
	}
	checks := make(map[string]bool, len(current.Checks))
	for _, check := range current.Checks {
		checks[check.Name] = true
	}
	// MySQL frees the name of a CHECK that follows a dropped column before
	// admitting new constraints (prepare_check_constraints_for_alter consults
	// dropped_cc_names), so a same-statement replacement succeeds in either
	// clause order. The replay resolves each DROP against the column identity
	// present at that clause, matching execution. The checks map itself stays
	// untouched: a generic DROP CONSTRAINT below must still resolve the
	// original CHECK identity in either clause order.
	releasedChecks, err := releasedCheckNamesForDroppedColumns(ctx.GetContext(), current.Checks, options)
	if err != nil {
		return nil, err
	}
	checkNameTaken := func(name string) bool {
		return checks[name] && !releasedChecks[name]
	}
	// Generic DROP does not yet carry its algorithm intent. CHECK/PRIMARY
	// require COPY; the other generic drops have the index/FK INPLACE intent.
	namingOptions := slices.Clone(options)
	for i, option := range options {
		if drop, ok := option.(*tree.AlterOptionDrop); ok && drop.Typ == tree.AlterTableDropConstraint {
			copy := *drop
			copy.Typ = tree.AlterTableDropForeignKey
			if checks[string(drop.Name)] {
				copy.Typ = tree.AlterTableDropCheck
			} else if current.Pkey != nil && !catalog.IsFakePkName(current.Pkey.PkeyColName) &&
				indexNameKey(string(drop.Name)) == indexNameKey("PRIMARY") {
				copy.Typ = tree.AlterTableDropPrimaryKey
			} else if slices.ContainsFunc(source.Indexes, func(index *planpb.IndexDef) bool {
				return index.Unique && IndexNamesEqual(index.IndexName, string(drop.Name))
			}) {
				copy.Typ = tree.AlterTableDropIndex
			}
			namingOptions[i] = &copy
		}
	}
	namingAlgorithm, err := ResolveAlterTableAlgorithm(ctx.GetContext(), namingOptions, source)
	if err != nil {
		return nil, err
	}
	copyNames := namingAlgorithm == planpb.AlterTable_COPY
	inlineIndexes := make(map[tree.AlterTableOption][]*IndexDef)
	inlinePrimary := make(map[tree.AlterTableOption]string)
	droppedUnique := make(map[tree.AlterTableOption][]string)
	if copyNames {
		// Plugin dependencies share the same index-reference identity as Parts.
		// Reuse their rename owner, including legacy INCLUDE parameter recovery;
		// catalog SQL returned by these planning hooks is not executed here.
		for oldName, origin := range bindings.sourceOrigins {
			if _, err := handleAlterRenameColumnWithPluginHooks(current, oldName, origin); err != nil {
				return nil, err
			}
		}
		for _, index := range current.Indexes {
			for i, part := range index.Parts {
				if origin, ok := bindings.sourceOrigins[catalog.ResolveAlias(part)]; ok {
					if catalog.IsAlias(part) {
						origin = catalog.CreateAlias(origin)
					}
					index.Parts[i] = origin
				}
			}
		}
		// COPY builds inline column constraints immediately and table-level
		// indexes after column changes. Retain inline producers separately so
		// ordered DROP resolution sees them only after their column clause.
		for ordinal, option := range options {
			var column *tree.ColumnTableDef
			switch opt := option.(type) {
			case *tree.AlterAddCol:
				column = opt.Column
			case *tree.AlterTableModifyColumnClause:
				column = opt.NewColumn
			case *tree.AlterTableChangeColumnClause:
				column = opt.NewColumn
			case *tree.AlterOptionDrop:
				name := string(opt.Name)
				if opt.Typ == tree.AlterTableDropIndex || opt.Typ == tree.AlterTableDropKey {
					for i, index := range current.Indexes {
						if IndexNamesEqual(index.IndexName, name) {
							cols := functionalIndexColumns(current, index)
							current.Indexes = slices.Delete(current.Indexes, i, i+1)
							for _, col := range cols {
								if err := handleDropColumnPosition(ctx.GetContext(), current, col); err != nil {
									return nil, err
								}
							}
							break
						}
					}
				}
				if opt.Typ == tree.AlterTableDropColumn {
					if original := bindings.clauses[ordinal].origin; original != "" && !bindings.clauses[ordinal].preserveIndexes {
						before := slices.Clone(current.Indexes)
						if err := handleDropColumnWithIndex(ctx.GetContext(), original, current); err != nil {
							return nil, err
						}
						// Reuse COPY's removal decision, but expose it to
						// generic DROP only when this column clause is reached.
						for _, index := range before {
							if index.Unique && !slices.Contains(current.Indexes, index) {
								droppedUnique[option] = append(droppedUnique[option], indexNameKey(index.IndexName))
							}
						}
					}
				}
			}
			if column == nil {
				continue
			}
			origin := bindings.clauses[ordinal].origin
			for _, attr := range column.Attributes {
				switch attr.(type) {
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					inlinePrimary[option] = column.Name.ColName()
				case *tree.AttributeUnique, *tree.AttributeUniqueKey:
					definition := newAlterColumnUniqueIndex(current, column)
					index := &IndexDef{IndexName: definition.Name, Unique: true, Parts: []string{origin}}
					current.Indexes = append(current.Indexes, index)
					inlineIndexes[option] = append(inlineIndexes[option], index)
				}
			}
		}
	}
	inlineUniqueNames := make(map[string]bool)
	for _, indexes := range inlineIndexes {
		for _, index := range indexes {
			if slices.Contains(current.Indexes, index) {
				inlineUniqueNames[indexNameKey(index.IndexName)] = true
			}
		}
	}
	fkeys := make(map[string]bool, len(current.Fkeys))
	for _, fk := range current.Fkeys {
		fkeys[fk.Name] = true
	}
	unique := make(map[string]bool)
	for _, index := range source.Indexes {
		if index.Unique {
			unique[indexNameKey(index.IndexName)] = true
		}
	}
	indexNames := make(map[string]bool, len(current.Indexes))
	for _, index := range current.Indexes {
		indexNames[indexNameKey(index.IndexName)] = true
	}
	if copyNames {
		for _, option := range options {
			if drop, ok := option.(*tree.AlterOptionDrop); ok {
				name := string(drop.Name)
				key := indexNameKey(name)
				if drop.Typ == tree.AlterTableDropIndex || drop.Typ == tree.AlterTableDropKey ||
					(drop.Typ == tree.AlterTableDropConstraint && unique[key] && !checks[name]) {
					if !inlineUniqueNames[key] {
						delete(indexNames, key)
					}
				}
			}
		}
	}
	pendingIndexNames := make(map[string]bool)
	pendingUniqueNames := make(map[string]bool)
	// Primary keys are applied immediately by COPY. Track their columns in
	// clause order; a later producer must not hide an earlier primary key.
	primaryColumns := make(map[string]bool)
	if source.Pkey != nil && !catalog.IsFakePkName(source.Pkey.PkeyColName) {
		for _, name := range source.Pkey.Names {
			primaryColumns[name] = true
		}
		if len(primaryColumns) == 0 {
			primaryColumns[source.Pkey.PkeyColName] = true
		}
	}
	renamePrimaryColumn := func(oldName, newName string) {
		if primaryColumns[oldName] {
			delete(primaryColumns, oldName)
			primaryColumns[newName] = true
		}
	}
	applyInlineConstraints := func(option tree.AlterTableOption) {
		for _, index := range inlineIndexes[option] {
			unique[indexNameKey(index.IndexName)] = true
		}
		if name := inlinePrimary[option]; name != "" {
			clear(primaryColumns)
			primaryColumns[name] = true
		}
	}
	resolved := slices.Clone(options)
	for i, option := range options {
		switch opt := option.(type) {
		case *tree.AlterAddCol, *tree.AlterTableModifyColumnClause:
			applyInlineConstraints(option)
		case *tree.AlterTableChangeColumnClause:
			renamePrimaryColumn(opt.OldColumnName.ColName(), opt.NewColumn.Name.ColName())
			applyInlineConstraints(option)
		case *tree.AlterTableRenameColumnClause:
			renamePrimaryColumn(opt.OldColumnName.ColName(), opt.NewColumnName.ColName())
		case *tree.AlterOptionAdd:
			switch def := opt.Def.(type) {
			case *tree.CheckIndex:
				copy := *def
				if copy.ConstraintSymbol == "" {
					for ordinal := len(checks) + 1; ; ordinal++ {
						name := fmt.Sprintf("%s_chk_%d", source.Name, ordinal)
						if !checkNameTaken(name) {
							copy.ConstraintSymbol = name
							break
						}
					}
				}
				if checkNameTaken(copy.ConstraintSymbol) {
					return nil, moerr.NewInvalidInputf(ctx.GetContext(), "duplicate check constraint name '%s'", copy.ConstraintSymbol)
				}
				// The first replacement consumes the released name; further
				// adds collide with that replacement as usual.
				delete(releasedChecks, copy.ConstraintSymbol)
				checks[copy.ConstraintSymbol] = true
				add := *opt
				add.Def = &copy
				resolved[i] = &add
			case *tree.ForeignKey:
				fkeys[def.ConstraintSymbol] = true
			case *tree.UniqueIndex:
				copy := *def
				if copy.GetIndexName() == "" {
					if err := checkIndexKeypartSupportability(ctx.GetContext(), copy.KeyParts); err != nil {
						return nil, err
					}
					setEmptyUniqueIndexName(indexNames, &copy)
				}
				name := indexNameKey(copy.GetIndexName())
				unique[name] = true
				indexNames[name] = true
				pendingIndexNames[name] = true
				pendingUniqueNames[name] = true
				// Only infer the name for generic DROP resolution. COPY names
				// pending indexes after all column changes; keep its decision.
				copy.Name = def.Name
				add := *opt
				add.Def = &copy
				resolved[i] = &add
			case *tree.Index:
				copy := *def
				if copy.Name == "" && !hasFunctionalKey(copy.KeyParts) {
					setEmptyIndexName(indexNames, &copy)
				}
				indexNames[indexNameKey(copy.Name)] = true
				pendingIndexNames[indexNameKey(copy.Name)] = true
				copy.Name = def.Name
				add := *opt
				add.Def = &copy
				resolved[i] = &add
			case *tree.FullTextIndex:
				copy := *def
				if copy.Name == "" {
					if err := checkIndexKeypartSupportability(ctx.GetContext(), copy.KeyParts); err != nil {
						return nil, err
					}
					setEmptyFullTextIndexName(indexNames, &copy)
				}
				indexNames[indexNameKey(copy.Name)] = true
				pendingIndexNames[indexNameKey(copy.Name)] = true
				copy.Name = def.Name
				add := *opt
				add.Def = &copy
				resolved[i] = &add
			case *tree.PrimaryKeyIndex:
				clear(primaryColumns)
				for _, part := range def.KeyParts {
					primaryColumns[part.ColName.ColName()] = true
				}
			}
		case *tree.AlterOptionDrop:
			name := string(opt.Name)
			drop := *opt
			if drop.Typ == tree.AlterTableDropConstraint {
				matches := 0
				for _, candidate := range []struct {
					present bool
					typ     tree.AlterTableDropType
				}{
					{checks[name], tree.AlterTableDropCheck},
					{fkeys[name], tree.AlterTableDropForeignKey},
					{unique[indexNameKey(name)], tree.AlterTableDropIndex},
					{len(primaryColumns) > 0 && indexNameKey(name) == indexNameKey("PRIMARY"), tree.AlterTableDropPrimaryKey},
				} {
					if candidate.present {
						matches++
						drop.Typ = candidate.typ
					}
				}
				if matches == 0 {
					return nil, moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), name)
				}
				if matches > 1 {
					return nil, moerr.NewInvalidInputf(ctx.GetContext(), "ambiguous constraint name '%s'; use constraint-specific DROP syntax", name)
				}
				resolved[i] = &drop
			}
			switch drop.Typ {
			case tree.AlterTableDropCheck:
				delete(checks, name)
			case tree.AlterTableDropForeignKey:
				delete(fkeys, name)
			case tree.AlterTableDropIndex, tree.AlterTableDropKey:
				key := indexNameKey(name)
				if !copyNames || !pendingUniqueNames[key] {
					delete(unique, key)
				}
				if !copyNames || !pendingIndexNames[key] {
					delete(indexNames, key)
				}
			case tree.AlterTableDropPrimaryKey:
				clear(primaryColumns)
			case tree.AlterTableDropColumn:
				delete(primaryColumns, name)
				for _, key := range droppedUnique[option] {
					if !pendingUniqueNames[key] {
						delete(unique, key)
					}
				}
			}
		}
	}
	return resolved, nil
}

// Apply CHECK names/drops before column mutations, then bind new expressions
// against the final schema. COPY validates the rows before publishing the DDL.
func prepareAlterCopyChecks(ctx CompilerContext, target *TableDef, options tree.AlterTableOptions) ([]*tree.CheckIndex, error) {
	var pending []*tree.CheckIndex
	changed := false
	for _, option := range options {
		if add, ok := option.(*tree.AlterOptionAdd); ok {
			if _, ok := add.Def.(*tree.CheckIndex); ok {
				changed = true
			}
		}
		if drop, ok := option.(*tree.AlterOptionDrop); ok && drop.Typ == tree.AlterTableDropCheck {
			changed = true
		}
	}
	if !changed {
		return nil, nil
	}
	if target.TableType == catalog.SystemExternalRel {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "CHECK constraints on external tables")
	}
	if err := requireCheckConstraintProtocol(ctx.GetContext(), ctx.GetProcess()); err != nil {
		return nil, err
	}
	if err := recoverLegacyChecks(ctx, target); err != nil {
		return nil, err
	}
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.AlterOptionAdd:
			if check, ok := opt.Def.(*tree.CheckIndex); ok {
				if !check.Enforced {
					return nil, moerr.NewNotSupported(ctx.GetContext(), "NOT ENFORCED CHECK constraints")
				}
				pending = append(pending, check)
			}
		case *tree.AlterOptionDrop:
			if opt.Typ != tree.AlterTableDropCheck {
				continue
			}
			name := string(opt.Name)
			index := slices.IndexFunc(target.Checks, func(check *planpb.CheckDef) bool { return check.Name == name })
			if index >= 0 {
				target.Checks = slices.Delete(target.Checks, index, index+1)
				continue
			}
			index = slices.IndexFunc(pending, func(check *tree.CheckIndex) bool { return check.ConstraintSymbol == name })
			if index < 0 {
				return nil, moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), name)
			}
			pending = slices.Delete(pending, index, index+1)
		}
	}
	// The legacy SHOW/rename fallback must not resurrect a deliberately removed
	// CHECK when the structured list becomes empty. All checks are now decoded.
	target.Createsql = ""
	return pending, nil
}

// checkColumnReferenceVisitor collects distinct unqualified column references,
// the only spelling the persisted CHECK binder admits.
type checkColumnReferenceVisitor struct {
	references map[string]bool
}

func (v *checkColumnReferenceVisitor) Enter(expr tree.Expr) (tree.Expr, bool) {
	name, ok := expr.(*tree.UnresolvedName)
	if !ok {
		return expr, false
	}
	if name.NumParts == 1 {
		v.references[strings.ToLower(name.ColName())] = true
	}
	return expr, true
}

func (v *checkColumnReferenceVisitor) Exit(expr tree.Expr) (tree.Expr, bool) {
	return expr, true
}

// checkConstraintReferencedColumns returns the distinct case-folded column
// names a persisted CHECK expression references. OriginSql is the authoritative
// text the temporary COPY CREATE replans against the final schema; the stored
// positional expression can alias column mutations made by earlier clauses of
// the same ALTER.
func checkConstraintReferencedColumns(ctx context.Context, check *planpb.CheckDef) (map[string]bool, error) {
	if check == nil || check.OriginSql == "" {
		return nil, nil
	}
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+check.OriginSql, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid CHECK constraint expression")
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(clause.Exprs) != 1 {
		return nil, moerr.NewInternalError(ctx, "invalid CHECK constraint expression")
	}
	visitor := &checkColumnReferenceVisitor{references: make(map[string]bool)}
	if _, ok := clause.Exprs[0].Expr.Accept(visitor); !ok {
		return nil, moerr.NewInternalError(ctx, "failed to inspect CHECK constraint expression")
	}
	return visitor.references, nil
}

// checkFollowsDroppedColumn mirrors the MySQL
// check_constraint_expr_refers_to_only_column predicate: a CHECK is
// auto-dropped with the column only when its expression references exactly one
// distinct column and that column is being dropped.
func checkFollowsDroppedColumn(references map[string]bool, dropped string) bool {
	return len(references) == 1 && references[dropped]
}

// handleDropColumnWithCheck applies the MySQL DROP COLUMN dependency rule at
// the moment the column clause executes: a CHECK is removed with the column
// only when its expression references that column alone, and the ALTER is
// rejected when any other surviving CHECK still references it
// (ER_DEPENDENT_BY_CHECK_CONSTRAINT). Earlier rename/change clauses have
// already rewritten the stored expressions, so the decision resolves the
// column identity this clause actually drops. Pending ADDs are intentionally
// not inspected; a same-statement replacement keeps its name while the old
// constraint follows the column.
func handleDropColumnWithCheck(ctx CompilerContext, colName string, tableDef *TableDef) error {
	if len(tableDef.Checks) == 0 && !strings.Contains(strings.ToUpper(tableDef.Createsql), "CHECK") {
		return nil
	}
	if err := recoverLegacyChecks(ctx, tableDef); err != nil {
		return err
	}
	dropped := strings.ToLower(colName)
	references := make(map[*planpb.CheckDef]map[string]bool, len(tableDef.Checks))
	remaining := make([]*planpb.CheckDef, 0, len(tableDef.Checks))
	removed := false
	for _, check := range tableDef.Checks {
		refs, err := checkConstraintReferencedColumns(ctx.GetContext(), check)
		if err != nil {
			return err
		}
		if checkFollowsDroppedColumn(refs, dropped) {
			removed = true
			continue
		}
		references[check] = refs
		remaining = append(remaining, check)
	}
	for _, check := range remaining {
		if references[check][dropped] {
			return moerr.NewErrDependentByCheckConstraint(ctx.GetContext(), check.Name, colName)
		}
	}
	if !removed {
		return nil
	}
	tableDef.Checks = remaining
	// The legacy SHOW/rename fallback must not resurrect a removed CHECK when
	// the structured list becomes empty, matching prepareAlterCopyChecks.
	if len(tableDef.Checks) == 0 {
		tableDef.Createsql = ""
	}
	return nil
}

// releasedCheckNamesForDroppedColumns replays the rename/change/drop-column
// clause sequence against the recovered CHECK definitions and returns the
// names execution will auto-drop. The replay calls the same helpers the
// option loop does, so each DROP resolves against the column identity present
// at that clause: a CHECK redirected by an earlier rename is judged by its
// rewritten expression. Only auto-drops release a name; constraints the
// replay would reject with ER_DEPENDENT_BY_CHECK_CONSTRAINT abort the ALTER
// during execution and never reach admission.
func releasedCheckNamesForDroppedColumns(ctx context.Context, source []*planpb.CheckDef, options tree.AlterTableOptions) (map[string]bool, error) {
	released := make(map[string]bool)
	if len(source) == 0 {
		return released, nil
	}
	// Clone the definitions: the replay rewrites stored expressions, and those
	// edits must not leak into the admission view of the source constraints.
	simulated := make([]*planpb.CheckDef, len(source))
	for i, check := range source {
		dup := *check
		simulated[i] = &dup
	}
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.AlterTableRenameColumnClause:
			if err := renameColumnInCheckConstraints(ctx, simulated, opt.OldColumnName.ColName(), opt.NewColumnName.ColNameOrigin()); err != nil {
				return nil, err
			}
		case *tree.AlterTableChangeColumnClause:
			if opt.OldColumnName.ColName() != opt.NewColumn.Name.ColName() {
				if err := renameColumnInCheckConstraints(ctx, simulated, opt.OldColumnName.ColName(), opt.NewColumn.Name.ColNameOrigin()); err != nil {
					return nil, err
				}
			}
		case *tree.AlterOptionDrop:
			if opt.Typ != tree.AlterTableDropColumn {
				continue
			}
			dropped := strings.ToLower(string(opt.Name))
			kept := make([]*planpb.CheckDef, 0, len(simulated))
			for _, check := range simulated {
				refs, err := checkConstraintReferencedColumns(ctx, check)
				if err != nil {
					return nil, err
				}
				if checkFollowsDroppedColumn(refs, dropped) {
					released[check.Name] = true
					continue
				}
				kept = append(kept, check)
			}
			simulated = kept
		}
	}
	return released, nil
}
