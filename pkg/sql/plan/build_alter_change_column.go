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
func ChangeColumn(
	cctx CompilerContext,
	alterPlan *plan.AlterTable,
	spec *tree.AlterTableChangeColumnClause,
	alterCtx *AlterTableContext,
) (bool, error) {
	tableDef := alterPlan.CopyTableDef

	ctx := cctx.GetContext()
	// get the original column name
	oldColName := spec.OldColumnName.ColName()
	oldColNameOrigin := spec.OldColumnName.ColNameOrigin()

	specNewColumn := spec.NewColumn
	// get the new column name
	newColName := specNewColumn.Name.ColName()
	newColNameOrigin := specNewColumn.Name.ColNameOrigin()

	// Check whether original column has existed.
	oCol := FindColumn(tableDef.Cols, oldColName)
	if oCol == nil || oCol.Hidden {
		return false, moerr.NewBadFieldError(
			ctx,
			oldColNameOrigin,
			alterPlan.TableDef.Name,
		)
	}

	// If you want to rename the original column name to new name,
	// you need to first check if the new name already exists.
	if newColName != oldColName &&
		FindColumn(tableDef.Cols, newColName) != nil {
		return false, moerr.NewErrDupFieldName(ctx, newColName)
	}

	//change the name of the column in the foreign key constraint
	if newColNameOrigin != oldColNameOrigin {
		alterCtx.UpdateSqls = append(alterCtx.UpdateSqls,
			getSqlForRenameColumn(alterPlan.Database,
				alterPlan.TableDef.Name,
				oldColNameOrigin,
				newColNameOrigin)...)
	}

	pkAffected, err := updateNewColumnInTableDef(
		cctx, tableDef, oCol, specNewColumn, spec.Position,
	)
	if err != nil {
		return false, err
	}

	updateClusterByInTableDef(ctx, tableDef, newColName, oldColName)

	delete(alterCtx.alterColMap, oldColName)
	alterCtx.alterColMap[newColName] = selectExpr{
		sexprType: exprColumnName,
		sexprStr:  oldColName,
	}

	if tmpCol, ok := alterCtx.changColDefMap[oCol.ColId]; ok {
		tmpCol.Name = newColName
		tmpCol.OriginName = newColNameOrigin
	}
	return pkAffected, nil
}

// buildColumnAndConstraint Build the changed new column definition, and check its column level integrity constraints,
// and check other table level constraints, such as primary keys, indexes, etc
func buildColumnAndConstraint(
	ctx CompilerContext,
	targetTableDef *plan.TableDef,
	oldCol *ColDef,
	specNewColumn *tree.ColumnTableDef,
	colType plan.Type,
) (*ColDef, error) {
	newColName := specNewColumn.Name.ColName()
	newColNameOrigin := specNewColumn.Name.ColNameOrigin()
	// Check if the new column name is valid and conflicts with internal hidden columns
	err := checkColumnNameValid(ctx.GetContext(), newColName)
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

	// If the column null property is not specified, it defaults to allowing null
	hasNullFlag := false
	auto_incr := false
	hasDefaultValue := false
	for _, attr := range specNewColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			err = checkPrimaryKeyPartType(ctx.GetContext(), colType, newColName)
			if err != nil {
				return nil, err
			}
			// If the table already contains a primary key, an `ErrMultiplePriKey` error is reported
			if targetTableDef.Pkey != nil && targetTableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				return nil, moerr.NewErrMultiplePriKey(ctx.GetContext())
			} else if targetTableDef.ClusterBy != nil && targetTableDef.ClusterBy.Name != "" {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
			} else {
				targetTableDef.Pkey = &PrimaryKeyDef{
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
			for _, idx := range targetTableDef.Indexes {
				nameLower := strings.ToLower(idx.IndexName)
				constrNames[nameLower] = true
			}
			// set empty constraint names(index and unique index)
			setEmptyUniqueIndexName(constrNames, uniqueIndex)

			indexDef, err := checkAddColumWithUniqueKey(ctx.GetContext(), targetTableDef, uniqueIndex)
			if err != nil {
				return nil, err
			}
			targetTableDef.Indexes = append(targetTableDef.Indexes, indexDef)
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
			hasNullFlag = defaultValue.NullAbility
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
		for _, indexInfo := range targetTableDef.Indexes {
			for j, partCol := range indexInfo.Parts {
				partCol = catalog.ResolveAlias(partCol)
				if partCol == oldCol.Name {
					indexInfo.Parts[j] = newColName
				}
			}
		}

		primaryKeyDef := targetTableDef.Pkey
		for j, partCol := range primaryKeyDef.Names {
			if partCol == oldCol.Name {
				primaryKeyDef.Names[j] = newColName
				break
			}
		}
	}

	if targetTableDef.Pkey != nil {
		for _, partCol := range targetTableDef.Pkey.Names {
			if partCol == newColName {
				newCol.Default.NullAbility = false
				newCol.NotNull = true
				break
			}
		}
	}

	if err = checkPriKeyConstraint(ctx.GetContext(), newCol, hasDefaultValue, hasNullFlag, targetTableDef.Pkey); err != nil {
		return nil, err
	}

	return newCol, nil
}

// Check if the column name is valid and conflicts with internal hidden columns
func checkColumnNameValid(ctx context.Context, colName string) error {
	if _, ok := catalog.InternalColumns[colName]; ok {
		return moerr.NewErrWrongColumnName(ctx, colName)
	}
	return nil
}

// updateClusterByInTableDef Process the cluster by table. If the cluster by key name is modified, proceed with the process
func updateClusterByInTableDef(
	ctx context.Context,
	tableDef *TableDef,
	newColName string,
	originalColName string,
) {
	clusterBy := tableDef.ClusterBy
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
			tableDef.ClusterBy = &plan.ClusterByDef{
				Name: clNames[0],
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clNames)
			tableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		}
	}
}
