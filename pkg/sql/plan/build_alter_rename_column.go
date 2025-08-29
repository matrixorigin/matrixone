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
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// RenameColumn Can change a column name but not its definition.
// More convenient than CHANGE to rename a column without changing its definition.
func RenameColumn(
	ctx CompilerContext,
	alterPlan *plan.AlterTable,
	spec *tree.AlterTableRenameColumnClause,
	alterCtx *AlterTableContext,
) error {

	oldColName := spec.OldColumnName.ColName()
	oldCol := FindColumn(alterPlan.CopyTableDef.Cols, oldColName)
	if oldCol == nil || oldCol.Hidden {
		return moerr.NewBadFieldError(
			ctx.GetContext(),
			spec.OldColumnName.ColNameOrigin(),
			alterPlan.TableDef.Name,
		)
	}

	if err := addRenameContextToAlterCtx(
		ctx.GetContext(),
		alterCtx,
		oldCol,
		alterPlan.TableDef.Name,
		alterPlan.CopyTableDef,
		spec,
	); err != nil {
		return err
	}

	if _, err := updateRenameColumnInTableDef(
		ctx,
		oldCol,
		alterPlan.CopyTableDef,
		spec,
	); err != nil {
		return err
	}

	return nil
}

func updateRenameColumnInTableDef(
	ctx CompilerContext,
	oldCol *ColDef,
	tableDef *plan.TableDef,
	spec *tree.AlterTableRenameColumnClause,
) (sqls []string, err error) {

	// get the old column name
	oldColName := oldCol.Name
	oldColNameOrigin := oldCol.OriginName

	// get the new column name
	newColName := spec.NewColumnName.ColName()
	newColNameOrigin := spec.NewColumnName.ColNameOrigin()

	if oldColNameOrigin == newColNameOrigin {
		return nil, nil
	}

	// Check if the new column name is valid and conflicts with internal hidden columns
	if err := checkColumnNameValid(ctx.GetContext(), newColName); err != nil {
		return nil, err
	}

	// check new name duplicate
	if FindColumn(tableDef.Cols, newColName) != nil &&
		!strings.EqualFold(newColName, oldColName) {
		return nil, moerr.NewErrDupFieldName(ctx.GetContext(), newColNameOrigin)
	}

	// update index key
	indexAffected := false
	for _, indexInfo := range tableDef.Indexes {
		for j, partCol := range indexInfo.Parts {
			partCol = catalog.ResolveAlias(partCol)
			if partCol == oldColName {
				indexInfo.Parts[j] = newColName
				indexAffected = true
				break
			}
		}
	}

	indexFmt := "update `mo_catalog`.`mo_indexes` set column_name = '%s' " +
		"where table_id = %d and column_name = '%s' ; "

	if indexAffected {
		sqls = append(sqls, fmt.Sprintf(
			indexFmt,
			newColNameOrigin,
			tableDef.TblId,
			oldColNameOrigin,
		))
	}

	// update primary key
	primaryKeyDef := tableDef.Pkey
	primaryKeyAffected := false
	for j, partCol := range primaryKeyDef.Names {
		if partCol == oldColName {
			primaryKeyDef.Names[j] = newColName
			if len(primaryKeyDef.Names) == 1 {
				primaryKeyAffected = true
				primaryKeyDef.PkeyColName = newColName
			}
			break
		}
	}

	if primaryKeyAffected {
		sqls = append(sqls, fmt.Sprintf(
			indexFmt,
			catalog.CreateAlias(newColName),
			tableDef.TblId,
			catalog.CreateAlias(oldColName),
		))
	}

	// update clusterby key
	updateClusterByInTableDef(ctx.GetContext(), tableDef, newColName, oldColName)

	// update column name itself
	for i, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, oldColName) {
			colDef := DeepCopyColDef(col)
			colDef.Name = newColName
			colDef.OriginName = newColNameOrigin
			tableDef.Cols[i] = colDef
			break
		}
	}

	return
}

func addRenameContextToAlterCtx(
	_ context.Context,
	alterCtx *AlterTableContext,
	oldCol *ColDef,
	originTblName string,
	tableDef *plan.TableDef,
	spec *tree.AlterTableRenameColumnClause) error {

	oldColName := oldCol.Name
	newColName := spec.NewColumnName.ColName()

	oldColNameOrigin := oldCol.OriginName
	newColNameOrigin := spec.NewColumnName.ColNameOrigin()

	if oldColNameOrigin == newColNameOrigin {
		return nil
	}

	// map the new column name to the old column as its data source
	delete(alterCtx.alterColMap, oldColName)
	alterCtx.alterColMap[newColName] = selectExpr{
		sexprType: exprColumnName,
		sexprStr:  oldColName,
	}

	if tmpCol, ok := alterCtx.changColDefMap[oldCol.ColId]; ok {
		tmpCol.Name = newColName
		tmpCol.OriginName = newColNameOrigin
	}

	// update fk relationships in mo_foreign_keys
	alterCtx.UpdateSqls = append(alterCtx.UpdateSqls,
		getSqlForRenameColumn(tableDef.DbName,
			originTblName,
			oldColNameOrigin,
			newColNameOrigin)...)

	return nil
}

// AlterColumn ALTER ... SET DEFAULT or ALTER ... DROP DEFAULT specify a new default value for a column or remove the old default value, respectively.
// If the old default is removed and the column can be NULL, the new default is NULL. If the column cannot be NULL, MySQL assigns a default value
func AlterColumn(
	ctx CompilerContext,
	alterPlan *plan.AlterTable,
	spec *tree.AlterTableAlterColumnClause,
	alterCtx *AlterTableContext,
) (bool, error) {

	tableDef := alterPlan.CopyTableDef

	// get the original column name
	originalColName := spec.ColumnName.ColName()

	// Check whether original column has existed.
	originalCol := FindColumn(tableDef.Cols, originalColName)
	if originalCol == nil || originalCol.Hidden {
		return false, moerr.NewBadFieldError(ctx.GetContext(), spec.ColumnName.ColNameOrigin(), alterPlan.TableDef.Name)
	}

	for i, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, originalCol.Name) {
			colDef := DeepCopyColDef(col)
			if spec.OptionType == tree.AlterColumnOptionSetDefault {
				tmpColumnDef := tree.NewColumnTableDef(spec.ColumnName, nil, []tree.ColumnAttribute{spec.DefaultExpr})
				defer func() {
					tmpColumnDef.Free()
				}()
				defaultValue, err := buildDefaultExpr(tmpColumnDef, colDef.Typ, ctx.GetProcess())
				if err != nil {
					return false, err
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
	return originalCol.Primary, nil
}

// OrderByColumn Currently, Mo only performs semantic checks on alter table order by
// and does not implement the function of changing the physical storage order of data in the table
func OrderByColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableOrderByColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	for _, order := range spec.AlterOrderByList {
		// get the original column name
		originalColName := order.Column.ColName()
		// Check whether original column has existed.
		originalCol := FindColumn(tableDef.Cols, originalColName)
		if originalCol == nil || originalCol.Hidden {
			return moerr.NewBadFieldError(ctx.GetContext(), order.Column.ColNameOrigin(), alterPlan.TableDef.Name)
		}
	}
	return nil
}
