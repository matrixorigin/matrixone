// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// AddPrimaryKey will add a new column to the table.
func AddPrimaryKey(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.PrimaryKeyIndex, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		return moerr.NewErrMultiplePriKey(ctx.GetContext())
	}
	if tableDef.ClusterBy != nil && tableDef.ClusterBy.Name != "" {
		return moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
	}

	primaryKeys := make([]string, 0)
	pksMap := map[string]bool{}
	for _, key := range spec.KeyParts {
		colName := key.ColName.ColName() // name of primary key column
		col := FindColumn(tableDef.Cols, colName)
		if col == nil {
			return moerr.NewErrKeyColumnDoesNotExist(ctx.GetContext(), colName)
		}
		if err := checkColumnNameValid(ctx.GetContext(), colName); err != nil {
			return err
		}
		if err := checkPrimaryKeyPartType(ctx.GetContext(), col.Typ, colName); err != nil {
			return err
		}

		if _, ok := pksMap[colName]; ok {
			return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name '%s' in primary key", colName)
		}

		primaryKeys = append(primaryKeys, colName)
		pksMap[colName] = true
	}

	pkeyName := ""
	if len(primaryKeys) == 1 {
		pkeyName = primaryKeys[0]
		for _, col := range tableDef.Cols {
			if col.Name == pkeyName {
				col.Primary = true
				col.NotNull = true
				col.Default.NullAbility = false
				tableDef.Pkey = &PrimaryKeyDef{
					Names:       primaryKeys,
					PkeyColName: pkeyName,
				}
				break
			}
		}
	} else {
		for _, coldef := range tableDef.Cols {
			if coldef.Hidden {
				continue
			}

			for _, primaryKey := range primaryKeys {
				if coldef.Name == primaryKey {
					coldef.NotNull = true
					coldef.Default.NullAbility = true
				}
			}
		}
		pkeyName = catalog.CPrimaryKeyColName
		colDef := MakeHiddenColDefByName(pkeyName)
		colDef.Primary = true
		tableDef.Cols = append(tableDef.Cols, colDef)

		pkeyDef := &PrimaryKeyDef{
			Names:       primaryKeys,
			PkeyColName: pkeyName,
			CompPkeyCol: colDef,
		}
		tableDef.Pkey = pkeyDef
	}
	return nil
}

func DropPrimaryKey(ctx CompilerContext, alterPlan *plan.AlterTable, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	// MySQL equivalent-key transition: dropping the primary key recorded by a
	// child foreign key is allowed when another key covering the same columns
	// survives; the binding moves there through the ALTER's FK catalog
	// updates. The rebind is deferred until the owning ALTER assembles the
	// COPY's final key set (applyAlterCopyPrimaryRebind), so a same-statement
	// ADD UNIQUE on the same columns can supply the surviving key regardless
	// of SQL clause order (review 5691776262). Legacy bindings and keyless
	// survivors keep the 1553 rejection at that point.
	alterCtx.pendingPrimaryRebind = append([]string(nil), tableDef.Pkey.Names...)
	if tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), "PRIMARY")
	}
	pkey := tableDef.Pkey
	if len(pkey.Names) == 1 {
		for _, coldef := range tableDef.Cols {
			if pkey.PkeyColName == coldef.Name {
				coldef.Primary = false
			}
		}
	} else {
		tableDef.Cols = RemoveIf[*ColDef](tableDef.Cols, func(coldef *ColDef) bool {
			return coldef.Hidden && pkey.PkeyColName == coldef.Name
		})
	}
	tableDef.Pkey = nil
	return nil
}

// applyAlterCopyPrimaryRebind resolves a DROP PRIMARY KEY's child rebinding
// after the COPY's final key set is assembled (including same-statement ADD
// UNIQUE indexes), then emits the catalog updates this statement carries.
func applyAlterCopyPrimaryRebind(
	cctx CompilerContext,
	copyTableDef *TableDef,
	alterCtx *AlterTableContext,
) error {
	if alterCtx == nil || len(alterCtx.pendingPrimaryRebind) == 0 {
		return nil
	}
	if len(copyTableDef.RefChildTbls) == 0 {
		return nil
	}
	rebinds, err := resolveForeignKeyBoundIndexDrops(
		cctx, copyTableDef, "PRIMARY", alterCtx.pendingPrimaryRebind, true, copyTableDef, nil)
	if err != nil {
		return err
	}
	for _, rebind := range rebinds {
		// Self-referencing children are recorded against the COPY's temporary
		// name, but the rows that survive the relation transfer live under the
		// original table's name: readdress those before emitting the update.
		if alterCtx.copyTableName != "" && rebind.childTable == alterCtx.copyTableName {
			if alterCtx.originTableName == "" {
				return moerr.NewInternalError(cctx.GetContext(), "missing original table name for foreign key rebinding")
			}
			rebind.childTable = alterCtx.originTableName
			// A self-reference persists its constraint through this statement's
			// COPY: rewrite the copy's durable binding too, so the catalog
			// update and the stored ForeignKeyDef commit together
			// (review 5691776262).
			for _, fk := range copyTableDef.Fkeys {
				if fk.Name == rebind.constraintName {
					fk.ReferencedIndexName = rebind.newIndexName
				}
			}
		}
		alterCtx.UpdateSqls = append(alterCtx.UpdateSqls, GetSqlForUpdateFkReferencedIndex(
			rebind.childDatabase, rebind.childTable, rebind.constraintName, rebind.newIndexName))
	}
	return nil
}
