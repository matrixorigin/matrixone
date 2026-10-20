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
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
						if !checks[name] {
							copy.ConstraintSymbol = name
							break
						}
					}
				}
				if checks[copy.ConstraintSymbol] {
					return nil, moerr.NewInvalidInputf(ctx.GetContext(), "duplicate check constraint name '%s'", copy.ConstraintSymbol)
				}
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
