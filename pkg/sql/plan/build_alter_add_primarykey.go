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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	hasNative0900Part := false
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
		hasNative0900Part = hasNative0900Part || isNative0900Type(col.Typ)
	}

	pkeyName := ""
	// A native 0900 string primary key keeps the user-visible column and
	// stores its comparison identity in the hidden physical primary-key
	// column, just like CREATE TABLE.  Treating it as an ordinary single
	// column primary key would make the table's physical key use the original
	// bytes and would disagree with inserts, lookups, and index backfill.
	native0900Primary := len(primaryKeys) == 1 && isNative0900Type(FindColumn(tableDef.Cols, primaryKeys[0]).Typ)
	if !native0900Primary && len(primaryKeys) == 1 {
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
					if coldef.Default != nil {
						// Preserve the existing composite-key behavior.  The
						// native single-column case follows CREATE TABLE and
						// must be explicitly non-null.
						coldef.Default.NullAbility = !native0900Primary
					}
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
		if hasNative0900Part {
			tableDef.KeyFormat = uint32(types.PADSpaceKeyV1)
		}
	}
	return recomputeTableCollationMetadata(ctx.GetContext(), tableDef)
}

func DropPrimaryKey(ctx CompilerContext, alterPlan *plan.AlterTable, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	if err := checkDropReferencedKeyForeignKeyDependency(ctx, tableDef, "PRIMARY", nil); err != nil {
		return err
	}
	if tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), "PRIMARY")
	}
	pkey := tableDef.Pkey
	if len(pkey.Names) == 1 {
		if pkey.CompPkeyCol != nil && pkey.CompPkeyCol.Hidden {
			compName := pkey.CompPkeyCol.Name
			tableDef.Cols = RemoveIf[*ColDef](tableDef.Cols, func(coldef *ColDef) bool {
				return coldef != nil && coldef.Name == compName
			})
		} else {
			for _, coldef := range tableDef.Cols {
				if pkey.PkeyColName == coldef.Name {
					coldef.Primary = false
				}
			}
		}
	} else {
		tableDef.Cols = RemoveIf[*ColDef](tableDef.Cols, func(coldef *ColDef) bool {
			return coldef.Hidden && pkey.PkeyColName == coldef.Name
		})
	}
	tableDef.Pkey = nil
	return recomputeTableCollationMetadata(ctx.GetContext(), tableDef)
}
