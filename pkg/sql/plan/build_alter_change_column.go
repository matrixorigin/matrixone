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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// ChangeColumn Can rename a column and change its definition, or both. Has more capability than MODIFY or RENAME COLUMN,
// but at the expense of convenience for some operations. CHANGE requires naming the column twice if not renaming it, and
// requires respecifying the column definition if only renaming it. With FIRST or AFTER, can reorder columns.
func ChangeColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableChangeColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	// get the original column name
	oldColName := spec.OldColumnName.ColName()
	oldColNameOrigin := spec.OldColumnName.ColNameOrigin()

	specNewColumn := spec.NewColumn
	// get the new column name
	newColName := specNewColumn.Name.ColName()
	newColNameOrigin := specNewColumn.Name.ColNameOrigin()

	// Check whether original column has existed.
	oldCol := FindColumn(tableDef.Cols, oldColName)
	if oldCol == nil || oldCol.Hidden {
		return moerr.NewBadFieldError(ctx.GetContext(), oldColNameOrigin, alterPlan.TableDef.Name)
	}

	if isColumnWithPartition(oldColName, tableDef.Partition) {
		return moerr.NewNotSupported(ctx.GetContext(), "unsupport alter partition part column currently")
	}

	// If you want to rename the original column name to new name, you need to first check if the new name already exists.
	if newColName != oldColName && FindColumn(tableDef.Cols, newColName) != nil {
		return moerr.NewErrDupFieldName(ctx.GetContext(), newColName)
	}

	//change the name of the column in the foreign key constraint
	if newColNameOrigin != oldColNameOrigin {
		alterCtx.UpdateSqls = append(alterCtx.UpdateSqls,
			getSqlForRenameColumn(alterPlan.Database,
				alterPlan.TableDef.Name,
				oldColNameOrigin,
				newColNameOrigin)...)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return err
	}

	// check if the newly added column type is valid
	if err = checkAddColumnType(ctx.GetContext(), &colType, newColName); err != nil {
		return err
	}

	newCol, err := buildChangeColumnAndConstraint(ctx, alterPlan, oldCol, specNewColumn, colType)
	if err != nil {
		return err
	}

	// check new column foreign key constraints
	if err = CheckModifyColumnForeignkeyConstraint(ctx, tableDef, oldCol, newCol); err != nil {
		return err
	}

	if err = checkChangeTypeCompatible(ctx.GetContext(), &oldCol.Typ, &newCol.Typ); err != nil {
		return err
	}

	if err = checkModifyNewColumn(ctx.GetContext(), tableDef, oldCol, newCol, spec.Position); err != nil {
		return err
	}

	handleClusterByKey(ctx.GetContext(), alterPlan, newColName, oldColName)

	delete(alterCtx.alterColMap, oldColName)
	alterCtx.alterColMap[newColName] = selectExpr{
		sexprType: columnName,
		sexprStr:  oldColName,
	}

	if tmpCol, ok := alterCtx.changColDefMap[oldCol.ColId]; ok {
		tmpCol.Name = newColName
		tmpCol.OriginName = newColNameOrigin
	}
	return nil
}

// buildChangeColumnAndConstraint Build the changed new column definition, and check its column level integrity constraints,
// and check other table level constraints, such as primary keys, indexes, etc
func buildChangeColumnAndConstraint(ctx CompilerContext, alterPlan *plan.AlterTable, oldCol *ColDef, specNewColumn *tree.ColumnTableDef, colType plan.Type) (*ColDef, error) {
	newColName := specNewColumn.Name.ColName()
	newColNameOrigin := specNewColumn.Name.ColNameOrigin()
	// Check if the new column name is valid and conflicts with internal hidden columns
	err := CheckColumnNameValid(ctx.GetContext(), newColName)
	if err != nil {
		return nil, err
	}

	newCol := &ColDef{
		ColId:      oldCol.ColId,
		Primary:    oldCol.Primary,
		ClusterBy:  oldCol.ClusterBy,
		Name:       newColName,
		OriginName: newColNameOrigin,
		Typ:        colType,
		Alg:        plan.CompressType_Lz4,
	}

	hasDefaultValue := false
	hasNullFlag := false
	auto_incr := false
	for _, attr := range specNewColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			err = checkPrimaryKeyPartType(ctx.GetContext(), colType, newColName)
			if err != nil {
				return nil, err
			}
			// If the table already contains a primary key, an `ErrMultiplePriKey` error is reported
			if alterPlan.CopyTableDef.Pkey != nil && alterPlan.CopyTableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				return nil, moerr.NewErrMultiplePriKey(ctx.GetContext())
			} else if alterPlan.CopyTableDef.ClusterBy != nil && alterPlan.CopyTableDef.ClusterBy.Name != "" {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
			} else {
				alterPlan.CopyTableDef.Pkey = &PrimaryKeyDef{
					Names:       []string{newColName},
					PkeyColName: newColName,
				}
				newCol.Primary = true
			}
		case *tree.AttributeComment:
			comment := attribute.CMT.String()
			if getNumOfCharacters(comment) > maxLengthOfColumnComment {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "comment for column '%s' is too long", newColNameOrigin)
			}
			newCol.Comment = comment
		case *tree.AttributeAutoIncrement:
			auto_incr = true
			if !types.T(colType.GetId()).IsInteger() {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
			}
			newCol.Typ.AutoIncr = true
		case *tree.AttributeUnique, *tree.AttributeUniqueKey:
			err = checkUniqueKeyPartType(ctx.GetContext(), colType, newColName)
			if err != nil {
				return nil, err
			}
			uniqueIndex := &tree.UniqueIndex{
				KeyParts: []*tree.KeyPart{
					{
						ColName: specNewColumn.Name,
					},
				},
			}

			constrNames := map[string]bool{}
			// Check not empty constraint name whether is duplicated.
			for _, idx := range alterPlan.CopyTableDef.Indexes {
				nameLower := strings.ToLower(idx.IndexName)
				constrNames[nameLower] = true
			}
			// set empty constraint names(index and unique index)
			setEmptyUniqueIndexName(constrNames, uniqueIndex)

			indexDef, err := checkAddColumWithUniqueKey(ctx.GetContext(), alterPlan.CopyTableDef, uniqueIndex)
			if err != nil {
				return nil, err
			}
			alterPlan.CopyTableDef.Indexes = append(alterPlan.CopyTableDef.Indexes, indexDef)
		case *tree.AttributeDefault:
			defaultValue, err := buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			newCol.Default = defaultValue
			hasDefaultValue = true
		case *tree.AttributeNull:
			defaultValue, err := buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			newCol.Default = defaultValue
			hasNullFlag = true
		case *tree.AttributeOnUpdate:
			onUpdateExpr, err := buildOnUpdate(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			newCol.OnUpdate = onUpdateExpr
		default:
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "unsupport column definition %v", attribute)
		}
	}
	if auto_incr && hasDefaultValue {
		return nil, moerr.NewErrInvalidDefault(ctx.GetContext(), newColNameOrigin)
	}
	if !hasDefaultValue {
		defaultValue, err := buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
		if err != nil {
			return nil, err
		}
		newCol.Default = defaultValue
	}

	// If the column name of the table changes, it is necessary to check if it is associated
	// with the index key. If it is an index key column, column name replacement is required.
	if newColName != oldCol.Name {
		for _, indexInfo := range alterPlan.CopyTableDef.Indexes {
			for j, partCol := range indexInfo.Parts {
				partCol = catalog.ResolveAlias(partCol)
				if partCol == oldCol.Name {
					indexInfo.Parts[j] = newColName
				}
			}
		}

		primaryKeyDef := alterPlan.CopyTableDef.Pkey
		for j, partCol := range primaryKeyDef.Names {
			if partCol == oldCol.Name {
				primaryKeyDef.Names[j] = newColName
				break
			}
		}
	}

	if alterPlan.CopyTableDef.Pkey != nil {
		for _, partCol := range alterPlan.CopyTableDef.Pkey.Names {
			if partCol == newColName {
				newCol.Default.NullAbility = false
				newCol.NotNull = true
				break
			}
		}
	}

	if err = checkPriKeyConstraint(ctx.GetContext(), newCol, hasDefaultValue, hasNullFlag, alterPlan.CopyTableDef.Pkey); err != nil {
		return nil, err
	}

	return newCol, nil
}

// Check if the column name is valid and conflicts with internal hidden columns
func CheckColumnNameValid(ctx context.Context, colName string) error {
	if _, ok := catalog.InternalColumns[colName]; ok {
		return moerr.NewErrWrongColumnName(ctx, colName)
	}
	return nil
}

// handleClusterByKey Process the cluster by table. If the cluster by key name is modified, proceed with the process
func handleClusterByKey(ctx context.Context, alterPlan *plan.AlterTable, newColName string, originalColName string) error {
	clusterBy := alterPlan.CopyTableDef.ClusterBy
	if clusterBy != nil && clusterBy.Name != "" {
		var clNames []string
		if util.JudgeIsCompositeClusterByColumn(clusterBy.Name) {
			clNames = util.SplitCompositeClusterByColumnName(clusterBy.Name)
		} else {
			clNames = []string{clusterBy.Name}
		}
		for j, part := range clNames {
			if part == originalColName {
				clNames[j] = newColName
				break
			}
		}

		if len(clNames) == 1 {
			alterPlan.CopyTableDef.ClusterBy = &plan.ClusterByDef{
				Name: clNames[0],
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clNames)
			alterPlan.CopyTableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		}
	}
	return nil
}
