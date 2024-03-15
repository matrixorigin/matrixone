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
		colName := key.ColName.Parts[0] // name of primary key column
		col := FindColumn(tableDef.Cols, colName)
		if col == nil {
			return moerr.NewErrKeyColumnDoesNotExist(ctx.GetContext(), colName)
		}
		if err := CheckColumnNameValid(ctx.GetContext(), colName); err != nil {
			return err
		}
		if err := checkPrimaryKeyPartType(ctx.GetContext(), &col.Typ, colName); err != nil {
			return err
		}

		if _, ok := pksMap[colName]; ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "duplicate column name '%s' in primary key", colName)
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
	if tableDef.Pkey == nil || tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
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
		for idx, coldef := range tableDef.Cols {
			if coldef.Hidden && pkey.PkeyColName == coldef.Name {
				tableDef.Cols = append(tableDef.Cols[:idx], tableDef.Cols[idx+1:]...)
				break
			}
		}
	}
	tableDef.Pkey = nil
	return nil
}
