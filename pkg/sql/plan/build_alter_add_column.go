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
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// AddColumn will add a new column to the table.
func AddColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterAddCol, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	if len(tableDef.Cols) == TableColumnCountLimit {
		return moerr.NewErrTooManyFields(ctx.GetContext())
	}

	specNewColumn := spec.Column
	// Check whether added column has existed.
	newColName := specNewColumn.Name.Parts[0]
	if col := FindColumn(tableDef.Cols, newColName); col != nil {
		return moerr.NewErrDupFieldName(ctx.GetContext(), newColName)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return err
	}
	if err = checkAddColumnType(ctx.GetContext(), &colType, newColName); err != nil {
		return err
	}
	newCol, err := buildAddColumnAndConstraint(ctx, alterPlan, specNewColumn, colType)
	if err != nil {
		return err
	}
	if err = handleAddColumnPosition(ctx.GetContext(), tableDef, newCol, spec.Position); err != nil {
		return err
	}

	if !newCol.Default.NullAbility && len(newCol.Default.OriginString) == 0 {
		alterCtx.alterColMap[newCol.Name] = selectExpr{
			sexprType: constValue,
			sexprStr:  buildNotNullColumnVal(newCol),
		}
	}

	return nil
}

// checkModifyNewColumn Check the position information of the newly formed column and place the new column in the target location
func handleAddColumnPosition(ctx context.Context, tableDef *TableDef, newCol *ColDef, pos *tree.ColumnPosition) error {
	if pos != nil && pos.Typ != tree.ColumnPositionNone {
		targetPos, err := findPositionRelativeColumn(ctx, tableDef.Cols, pos)
		if err != nil {
			return err
		}
		tableDef.Cols = append(tableDef.Cols[:targetPos], append([]*ColDef{newCol}, tableDef.Cols[targetPos:]...)...)
	} else {
		tableDef.Cols = append(tableDef.Cols, newCol)
	}
	return nil
}

func buildAddColumnAndConstraint(ctx CompilerContext, alterPlan *plan.AlterTable, specNewColumn *tree.ColumnTableDef, colType plan.Type) (*ColDef, error) {
	newColName := specNewColumn.Name.Parts[0]
	// Check if the new column name is valid and conflicts with internal hidden columns
	err := CheckColumnNameValid(ctx.GetContext(), newColName)
	if err != nil {
		return nil, err
	}

	newCol := &ColDef{
		ColId: math.MaxUint64,
		//Primary: originalCol.Primary,
		//NotNull:  originalCol.NotNull,
		//Default:  originalCol.Default,
		//Comment:  originalCol.Comment,
		//OnUpdate: originalCol.OnUpdate,
		Name: newColName,
		Typ:  colType,
		Alg:  plan.CompressType_Lz4,
	}

	hasDefaultValue := false
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
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "comment for column '%s' is too long", specNewColumn.Name.Parts[0])
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
		case *tree.AttributeDefault, *tree.AttributeNull:
			defaultValue, err := buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			newCol.Default = defaultValue
			hasDefaultValue = true
		case *tree.AttributeOnUpdate:
			onUpdateExpr, err := buildOnUpdate(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			newCol.OnUpdate = onUpdateExpr
		default:
			return nil, moerr.NewNotSupported(ctx.GetContext(), "unsupport column definition %v", attribute)
		}
	}
	if auto_incr && hasDefaultValue {
		return nil, moerr.NewErrInvalidDefault(ctx.GetContext(), specNewColumn.Name.Parts[0])
	}
	if !hasDefaultValue {
		defaultValue, err := buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
		if err != nil {
			return nil, err
		}
		newCol.Default = defaultValue
	}
	return newCol, nil
}

// checkAddColumnType check type for add single column.
func checkAddColumnType(ctx context.Context, colType *plan.Type, columnName string) error {
	if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
		colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
		if colType.GetWidth() > types.MaxStringSize {
			return moerr.NewInvalidInput(ctx, "string width (%d) is too long", colType.GetWidth())
		}
	}

	if colType.Id == int32(types.T_array_float32) || colType.Id == int32(types.T_array_float64) {
		if colType.GetWidth() > types.MaxArrayDimension {
			return moerr.NewInvalidInput(ctx, "vector width (%d) is too long", colType.GetWidth())
		}
	}
	return nil
}

func checkPrimaryKeyPartType(ctx context.Context, colType plan.Type, columnName string) error {
	if colType.GetId() == int32(types.T_blob) {
		return moerr.NewNotSupported(ctx, "blob type in primary key")
	}
	if colType.GetId() == int32(types.T_text) {
		return moerr.NewNotSupported(ctx, "text type in primary key")
	}
	if colType.GetId() == int32(types.T_json) {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("JSON column '%s' cannot be in primary key", columnName))
	}
	if colType.GetId() == int32(types.T_enum) {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("ENUM column '%s' cannot be in primary key", columnName))
	}
	return nil
}

func checkUniqueKeyPartType(ctx context.Context, colType plan.Type, columnName string) error {
	if colType.GetId() == int32(types.T_blob) {
		return moerr.NewNotSupported(ctx, "blob type in primary key")
	}
	if colType.GetId() == int32(types.T_text) {
		return moerr.NewNotSupported(ctx, "text type in primary key")
	}
	if colType.GetId() == int32(types.T_json) {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("JSON column '%s' cannot be in primary key", columnName))
	}
	return nil
}

func checkAddColumWithUniqueKey(ctx context.Context, tableDef *TableDef, uniKey *tree.UniqueIndex) (*plan.IndexDef, error) {
	indexName := uniKey.GetIndexName()
	if strings.EqualFold(indexName, PrimaryKeyName) {
		return nil, moerr.NewErrWrongNameForIndex(ctx, uniKey.GetIndexName())
	}

	indexTableName, err := util.BuildIndexTableName(ctx, true)
	if err != nil {
		return nil, err
	}

	indexParts := make([]string, 0)
	for _, keyPart := range uniKey.KeyParts {
		name := keyPart.ColName.Parts[0]
		indexParts = append(indexParts, name)
	}
	if len(indexParts) > MaxKeyParts {
		return nil, moerr.NewErrTooManyKeyParts(ctx, MaxKeyParts)
	}

	indexDef := &plan.IndexDef{
		IndexName:      indexName,
		Unique:         true,
		Parts:          indexParts,
		IndexTableName: indexTableName,
		TableExist:     true,
		Comment:        "",
	}

	if uniKey.IndexOption != nil {
		indexDef.Comment = uniKey.IndexOption.Comment
	}
	return indexDef, nil
}

// findPositionRelativeColumn returns a position relative to the position of the add/modify/change column.
func findPositionRelativeColumn(ctx context.Context, cols []*ColDef, pos *tree.ColumnPosition) (int, error) {
	position := len(cols)
	// gets the position of the column, which defaults to the length of the column indicating appending.
	if pos == nil {
		return position, nil
	}
	if pos.Typ == tree.ColumnPositionFirst {
		position = 0
	} else if pos.Typ == tree.ColumnPositionAfter {
		relcolIndex := -1
		for i, col := range cols {
			if col.Name == pos.RelativeColumn.Parts[0] {
				relcolIndex = i
				break
			}
		}
		if relcolIndex == -1 {
			return -1, moerr.NewBadFieldError(ctx, pos.RelativeColumn.Parts[0], "Columns Set")
		}
		// the insertion position is after the above column.
		position = int(relcolIndex + 1)
	}
	return position, nil
}

// AddColumn will add a new column to the table.
func DropColumn(ctx CompilerContext, alterPlan *plan.AlterTable, colName string, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef
	// Check whether original column has existed.
	col := FindColumn(tableDef.Cols, colName)
	if col == nil || col.Hidden {
		return moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), colName)
	}

	// We only support dropping column with single-value none Primary Key index covered now.
	if err := handleDropColumnWithIndex(ctx.GetContext(), colName, tableDef); err != nil {
		return err
	}
	if err := handleDropColumnWithPrimaryKey(ctx.GetContext(), colName, tableDef); err != nil {
		return err
	}
	if err := checkDropColumnWithPartition(ctx.GetContext(), tableDef, colName); err != nil {
		return err
	}
	// Check the column with foreign key.
	if err := checkDropColumnWithForeignKey(ctx, tableDef, col); err != nil {
		return err
	}
	if err := checkVisibleColumnCnt(ctx.GetContext(), tableDef, 0, 1); err != nil {
		return err
	}
	if isColumnWithPartition(col.Name, tableDef.Partition) {
		return moerr.NewNotSupported(ctx.GetContext(), "unsupport alter partition part column currently")
	}

	if err := handleDropColumnPosition(ctx.GetContext(), tableDef, col); err != nil {
		return err
	}

	if err := handleDropColumnWithClusterBy(ctx.GetContext(), tableDef, col); err != nil {
		return err
	}

	delete(alterCtx.alterColMap, colName)
	return nil
}

func checkVisibleColumnCnt(ctx context.Context, tblInfo *TableDef, addCount, dropCount int) error {
	visibleColumCnt := 0
	for _, column := range tblInfo.Cols {
		if !column.Hidden {
			visibleColumCnt++
		}
	}
	if visibleColumCnt+addCount > dropCount {
		return nil
	}
	if len(tblInfo.Cols)-visibleColumCnt > 0 {
		// There are only invisible columns.
		return moerr.NewErrTableMustHaveColumns(ctx)
	}
	return moerr.NewErrCantRemoveAllFields(ctx)
}

func handleDropColumnWithIndex(ctx context.Context, colName string, tbInfo *TableDef) error {
	for i := 0; i < len(tbInfo.Indexes); i++ {
		indexInfo := tbInfo.Indexes[i]
		for j := 0; j < len(indexInfo.Parts); j++ {
			if catalog.ResolveAlias(indexInfo.Parts[j]) == colName {
				indexInfo.Parts = append(indexInfo.Parts[:j], indexInfo.Parts[j+1:]...)
				break
			}
		}
		if indexInfo.Unique {
			// handle unique index
			if len(indexInfo.Parts) == 0 {
				tbInfo.Indexes = append(tbInfo.Indexes[:i], tbInfo.Indexes[i+1:]...)
			}
		} else if !indexInfo.Unique {
			// handle secondary index
			switch catalog.ToLower(indexInfo.IndexAlgo) {
			case catalog.MoIndexDefaultAlgo.ToString(), catalog.MoIndexBTreeAlgo.ToString():
				// regular secondary index
				if len(indexInfo.Parts) == 1 &&
					(catalog.IsAlias(indexInfo.Parts[0]) ||
						indexInfo.Parts[0] == catalog.FakePrimaryKeyColName ||
						indexInfo.Parts[0] == catalog.CPrimaryKeyColName) {
					// Handles deleting the secondary index when there is no more user defined secondary keys.

					//NOTE: if the last SK column is an __mo_alias or __mo_fake or __mo_cp, then the index will be deleted.
					// There is no way that user can add __mo_alias or __mo_fake or __mo_cp as the SK column.
					tbInfo.Indexes = append(tbInfo.Indexes[:i], tbInfo.Indexes[i+1:]...)
				} else if len(indexInfo.Parts) == 0 {
					tbInfo.Indexes = append(tbInfo.Indexes[:i], tbInfo.Indexes[i+1:]...)
				}
			case catalog.MoIndexIvfFlatAlgo.ToString():
				// ivf index
				if len(indexInfo.Parts) == 0 {
					// remove 3 index records: metadata, centroids, entries
					tbInfo.Indexes = append(tbInfo.Indexes[:i], tbInfo.Indexes[i+3:]...)
				}
			case catalog.MOIndexMasterAlgo.ToString():
				if len(indexInfo.Parts) == 0 {
					// TODO: verify this
					tbInfo.Indexes = append(tbInfo.Indexes[:i], tbInfo.Indexes[i+1:]...)
				}
			}
		}
	}
	return nil
}

func handleDropColumnWithPrimaryKey(ctx context.Context, colName string, tbInfo *TableDef) error {
	if tbInfo.Pkey != nil && tbInfo.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil
	} else {
		for i := 0; i < len(tbInfo.Pkey.Names); i++ {
			if tbInfo.Pkey.Names[i] == colName {
				tbInfo.Pkey.Names = append(tbInfo.Pkey.Names[:i], tbInfo.Pkey.Names[i+1:]...)
				break
			}
		}

		if len(tbInfo.Pkey.Names) == 0 {
			tbInfo.Pkey = nil
		} else if len(tbInfo.Pkey.Names) == 1 {
			tbInfo.Pkey.PkeyColName = tbInfo.Pkey.Names[0]
			for _, coldef := range tbInfo.Cols {
				if coldef.Name == tbInfo.Pkey.PkeyColName {
					coldef.Primary = true
					break
				}
			}
		}

		return nil
	}
}

func checkDropColumnWithForeignKey(ctx CompilerContext, tbInfo *TableDef, targetCol *ColDef) error {
	colName := targetCol.Name
	for _, fkInfo := range tbInfo.Fkeys {
		for _, colId := range fkInfo.Cols {
			referCol := FindColumnByColId(tbInfo.Cols, colId)
			if referCol == nil {
				continue
			}
			if referCol.Name == colName {
				return moerr.NewErrFkColumnCannotDrop(ctx.GetContext(), colName, fkInfo.Name)
			}
		}
	}

	for _, referredTblId := range tbInfo.RefChildTbls {
		_, refTableDef := ctx.ResolveById(referredTblId)
		if refTableDef == nil {
			return moerr.NewInternalError(ctx.GetContext(), "The reference foreign key table %d does not exist", referredTblId)
		}
		for _, referredFK := range refTableDef.Fkeys {
			if referredFK.ForeignTbl == tbInfo.TblId {
				for i := 0; i < len(referredFK.Cols); i++ {
					if referredFK.ForeignCols[i] == targetCol.ColId {
						return moerr.NewErrFkColumnCannotDropChild(ctx.GetContext(), colName, referredFK.Name, refTableDef.Name)
					}
				}
			}
		}
	}
	return nil
}

// checkDropColumnWithPartition is used to check the partition key of the drop column.
func checkDropColumnWithPartition(ctx context.Context, tbInfo *TableDef, colName string) error {
	if tbInfo.Partition != nil {
		partition := tbInfo.Partition
		// TODO Implement this method in the future to obtain the partition column in the partition expression
		// func (m *PartitionByDef) GetPartitionColumnNames() []string
		for _, name := range partition.GetPartitionColumns().PartitionColumns {
			if strings.EqualFold(name, colName) {
				return moerr.NewErrDependentByPartitionFunction(ctx, colName)
			}
		}
	}
	return nil
}

// checkModifyNewColumn Check the position information of the newly formed column and place the new column in the target location
func handleDropColumnPosition(ctx context.Context, tableDef *TableDef, col *ColDef) error {
	targetPos := -1
	for i := 0; i < len(tableDef.Cols); i++ {
		if tableDef.Cols[i].Name == col.Name {
			targetPos = i
			break
		}
	}
	tableDef.Cols = append(tableDef.Cols[:targetPos], tableDef.Cols[targetPos+1:]...)
	return nil
}

// handleDropColumnWithClusterBy Process the cluster by table. If the cluster by key name is deleted, proceed with the process
func handleDropColumnWithClusterBy(ctx context.Context, copyTableDef *TableDef, originCol *ColDef) error {
	if copyTableDef.ClusterBy != nil && copyTableDef.ClusterBy.Name != "" {
		clusterBy := copyTableDef.ClusterBy
		var clNames []string
		if util.JudgeIsCompositeClusterByColumn(clusterBy.Name) {
			clNames = util.SplitCompositeClusterByColumnName(clusterBy.Name)
		} else {
			clNames = []string{clusterBy.Name}
		}
		deleteIndex := -1
		for j, part := range clNames {
			if part == originCol.Name {
				deleteIndex = j
				break
			}
		}

		if deleteIndex != -1 {
			clNames = append(clNames[:deleteIndex], clNames[deleteIndex+1:]...)
		}

		if len(clNames) == 0 {
			copyTableDef.ClusterBy = nil
		} else if len(clNames) == 1 {
			copyTableDef.ClusterBy = &plan.ClusterByDef{
				Name: clNames[0],
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clNames)
			copyTableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		}
	}
	return nil
}
