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
func resolveAlterConstraintOptions(ctx CompilerContext, source *TableDef, options tree.AlterTableOptions) (tree.AlterTableOptions, error) {
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
	fkeys := make(map[string]bool, len(current.Fkeys))
	for _, fk := range current.Fkeys {
		fkeys[fk.Name] = true
	}
	unique := make(map[string]bool)
	indexNames := make(map[string]bool, len(current.Indexes))
	for _, index := range current.Indexes {
		indexNames[indexNameKey(index.IndexName)] = true
		if index.Unique {
			unique[indexNameKey(index.IndexName)] = true
		}
	}
	primary := current.Pkey != nil && !catalog.IsFakePkName(current.Pkey.PkeyColName)
	resolved := slices.Clone(options)
	for i, option := range options {
		switch opt := option.(type) {
		case *tree.AlterOptionAdd:
			switch def := opt.Def.(type) {
			case *tree.CheckIndex:
				copy := *def
				if copy.ConstraintSymbol == "" {
					for ordinal := len(checks) + 1; ; ordinal++ {
						name := fmt.Sprintf("__mo_chk_%d", ordinal)
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
				copy.Name = def.Name
				add := *opt
				add.Def = &copy
				resolved[i] = &add
			case *tree.PrimaryKeyIndex:
				primary = true
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
					{primary && indexNameKey(name) == indexNameKey("PRIMARY"), tree.AlterTableDropPrimaryKey},
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
				delete(unique, indexNameKey(name))
				delete(indexNames, indexNameKey(name))
			case tree.AlterTableDropPrimaryKey:
				primary = false
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
