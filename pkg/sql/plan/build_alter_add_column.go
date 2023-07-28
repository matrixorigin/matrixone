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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"math"
	"strings"
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
	if err = checkAddColumnType(ctx.GetContext(), colType, newColName); err != nil {
		return err
	}
	newCol, err := buildAddColumnAndConstraint(ctx, alterPlan, specNewColumn, colType)
	if err != nil {
		return err
	}
	if err = handleAddColumnPosition(ctx.GetContext(), tableDef, newCol, spec.Position); err != nil {
		return err
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

func buildAddColumnAndConstraint(ctx CompilerContext, alterPlan *plan.AlterTable, specNewColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
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
	return nil
}

func checkPrimaryKeyPartType(ctx context.Context, colType *plan.Type, columnName string) error {
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

func checkUniqueKeyPartType(ctx context.Context, colType *plan.Type, columnName string) error {
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
