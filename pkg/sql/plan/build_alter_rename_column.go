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
	"strings"
)

// RenameColumn Can change a column name but not its definition.
// More convenient than CHANGE to rename a column without changing its definition.
func RenameColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableRenameColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	// get the original column name
	originalColName := spec.OldColumnName.Parts[0]

	// get the new column name
	newColName := spec.NewColumnName.Parts[0]

	// Check whether original column has existed.
	originalCol := FindColumn(tableDef.Cols, originalColName)
	if originalCol == nil || originalCol.Hidden {
		return moerr.NewBadFieldError(ctx.GetContext(), originalColName, alterPlan.TableDef.Name)
	}

	if originalColName == newColName {
		return nil
	}

	// Check if the new column name is valid and conflicts with internal hidden columns
	if err := CheckColumnNameValid(ctx.GetContext(), newColName); err != nil {
		return err
	}

	if isColumnWithPartition(originalCol.Name, tableDef.Partition) {
		return moerr.NewNotSupported(ctx.GetContext(), "unsupport alter partition part column currently")
	}

	// If you want to rename the original column name to new name, you need to first check if the new name already exists.
	if newColName != originalColName {
		newcol := FindColumn(tableDef.Cols, newColName)
		if newcol != nil {
			return moerr.NewErrDupFieldName(ctx.GetContext(), newColName)
		}

		// If the column name of the table changes, it is necessary to check if it is associated
		// with the index key. If it is an index key column, column name replacement is required.
		for _, indexInfo := range alterPlan.CopyTableDef.Indexes {
			for j, partCol := range indexInfo.Parts {
				partCol = catalog.ResolveAlias(partCol)
				if partCol == originalCol.Name {
					indexInfo.Parts[j] = newColName
					break
				}
			}
		}

		primaryKeyDef := alterPlan.CopyTableDef.Pkey
		for j, partCol := range primaryKeyDef.Names {
			if partCol == originalCol.Name {
				primaryKeyDef.Names[j] = newColName
				break
			}
		}
		// handle cluster by key in modify column
		handleClusterByKey(ctx.GetContext(), alterPlan, newColName, originalCol.Name)
	}

	for i, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, originalCol.Name) {
			colDef := DeepCopyColDef(col)
			colDef.Name = newColName
			tableDef.Cols[i] = colDef
			break
		}
	}

	delete(alterCtx.alterColMap, originalCol.Name)
	alterCtx.alterColMap[newColName] = selectExpr{
		sexprType: columnName,
		sexprStr:  originalCol.Name,
	}

	if tmpCol, ok := alterCtx.changColDefMap[originalCol.ColId]; ok {
		tmpCol.Name = newColName
	}

	alterCtx.UpdateSqls = append(alterCtx.UpdateSqls,
		getSqlForRenameColumn(alterPlan.Database,
			alterPlan.TableDef.Name,
			originalColName,
			newColName)...)

	return nil
}

// AlterColumn ALTER ... SET DEFAULT or ALTER ... DROP DEFAULT specify a new default value for a column or remove the old default value, respectively.
// If the old default is removed and the column can be NULL, the new default is NULL. If the column cannot be NULL, MySQL assigns a default value
func AlterColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableAlterColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	// get the original column name
	originalColName := spec.ColumnName.Parts[0]

	// Check whether original column has existed.
	originalCol := FindColumn(tableDef.Cols, originalColName)
	if originalCol == nil || originalCol.Hidden {
		return moerr.NewBadFieldError(ctx.GetContext(), originalColName, alterPlan.TableDef.Name)
	}

	for i, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, originalCol.Name) {
			colDef := DeepCopyColDef(col)
			if spec.OptionType == tree.AlterColumnOptionSetDefault {
				tmpColumnDef := tree.NewColumnTableDef(spec.ColumnName, nil, []tree.ColumnAttribute{spec.DefalutExpr})
				defer func() {
					tmpColumnDef.Free()
				}()
				defaultValue, err := buildDefaultExpr(tmpColumnDef, &colDef.Typ, ctx.GetProcess())
				if err != nil {
					return err
				}
				defaultValue.NullAbility = colDef.Default.NullAbility
				colDef.Default = defaultValue
			} else if spec.OptionType == tree.AlterColumnOptionDropDefault {
				colDef.Default.Expr = nil
				colDef.Default.OriginString = ""
			}
			tableDef.Cols[i] = colDef
			break
		}
	}
	return nil
}

// OrderByColumn Currently, Mo only performs semantic checks on alter table order by
// and does not implement the function of changing the physical storage order of data in the table
func OrderByColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableOrderByColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	for _, order := range spec.AlterOrderByList {
		// get the original column name
		originalColName := order.Column.Parts[0]
		// Check whether original column has existed.
		originalCol := FindColumn(tableDef.Cols, originalColName)
		if originalCol == nil || originalCol.Hidden {
			return moerr.NewBadFieldError(ctx.GetContext(), originalColName, alterPlan.TableDef.Name)
		}
	}
	return nil
}
