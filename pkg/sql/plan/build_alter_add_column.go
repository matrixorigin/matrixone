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
	"strings"
)

// AddColumn will add a new column to the table.
func AddColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterAddCol) error {
	newColumn := spec.Column
	if err := checkAddColumnTooManyColumns(ctx.GetContext(), len(alterPlan.CopyTableDef.Cols)+1); err != nil {
		return err
	}
	_, err := checkAndCreateNewColumn(ctx, spec, alterPlan, newColumn)
	if err != nil {
		return err
	}
	return nil
}

func checkAndCreateNewColumn(ctx CompilerContext, spec *tree.AlterAddCol, alterPlan *plan.AlterTable, specNewColumn *tree.ColumnTableDef) (*ColDef, error) {
	tableDef := alterPlan.CopyTableDef
	err := checkUnsupportedColumnConstraint(ctx.GetContext(), specNewColumn, tableDef.Name)
	if err != nil {
		return nil, err
	}

	// Check whether added column has existed.
	colName := specNewColumn.Name.Parts[0]
	col := FindColumn(tableDef.Cols, colName)
	if col != nil {
		return nil, moerr.NewErrDupFieldName(ctx.GetContext(), colName)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return nil, err
	}

	if err = checkAddColumnType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0]); err != nil {
		return nil, err
	}

	return CreateNewColumn(ctx, alterPlan, spec, specNewColumn, colType)
}

// CreateNewColumn creates a new column according to the column information.
func CreateNewColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterAddCol, specNewColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	isPrimaryKey := false
	hasNullFlag := false
	hasDefaultValue := false
	auto_incr := false
	var err error
	for _, attr := range specNewColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			pkeyColName := specNewColumn.Name.Parts[0]
			err = checkPrimaryKeyPartType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0])
			if err != nil {
				return nil, err
			}
			// If the table already contains a primary key, an `ErrMultiplePriKey` error is reported
			if alterPlan.CopyTableDef.Pkey != nil && alterPlan.CopyTableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				return nil, moerr.NewErrMultiplePriKey(ctx.GetContext())
			}
			alterPlan.CopyTableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{pkeyColName},
				PkeyColName: pkeyColName,
			}
			isPrimaryKey = true
			fmt.Printf("isPrimaryKey: %v \n", isPrimaryKey)
		case *tree.AttributeComment:
			comment := attribute.CMT.String()
			if getNumOfCharacters(comment) > maxLengthOfColumnComment {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "comment for column '%s' is too long", specNewColumn.Name.Parts[0])
			}
		case *tree.AttributeAutoIncrement:
			auto_incr = true
			if !types.T(colType.GetId()).IsInteger() {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
			}
		case *tree.AttributeUnique, *tree.AttributeUniqueKey:
			err = checkUniqueKeyPartType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0])
			if err != nil {
				return nil, err
			}
			uniqueIndex := &tree.UniqueIndex{
				KeyParts: []*tree.KeyPart{
					{
						ColName: specNewColumn.Name,
					},
				},
				Name: specNewColumn.Name.Parts[0],
			}
			indexDef, err2 := checkAddColumWithUniqueKey(ctx.GetContext(), alterPlan.CopyTableDef, uniqueIndex)
			if err2 != nil {
				return nil, err2
			}
			alterPlan.CopyTableDef.Indexes = append(alterPlan.CopyTableDef.Indexes, indexDef)
		case *tree.AttributeDefault:
			_, err = buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			hasDefaultValue = true
		case *tree.AttributeNull:
			hasNullFlag = true
			fmt.Printf("hasNullFlag: %v \n", hasNullFlag)
		default:
			return nil, moerr.NewNotSupported(ctx.GetContext(), "unsupport column definition %v", attribute)
		}
	}

	if auto_incr && hasDefaultValue {
		return nil, moerr.NewErrInvalidDefault(ctx.GetContext(), specNewColumn.Name.Parts[0])
	}

	colDef, err := buildColumnAndConstraint(ctx, 0, specNewColumn, colType)
	if err != nil {
		return nil, err
	}
	alterPlan.CopyTableDef.Cols = append(alterPlan.CopyTableDef.Cols, colDef)

	return colDef, nil
}

func buildColumnAndConstraint(ctx CompilerContext, offset int, newColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	colName := newColumn.Name.Parts[0]
	if _, ok := catalog.InternalColumns[colName]; ok {
		return nil, moerr.NewErrWrongColumnName(ctx.GetContext(), colName)
	}

	colDef, err := columnDefToCol(ctx, offset, newColumn, colType)
	if err != nil {
		return nil, err
	}
	return colDef, nil
}

func columnDefToCol(ctx CompilerContext, offset int, newColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	colDef := &ColDef{
		Name: newColumn.Name.Parts[0],
		Alg:  plan.CompressType_Lz4,
		Typ:  colType,
	}

	//isPrimaryKey := false
	hasNullFlag := false
	//auto_incr := false
	var defaultValue *plan.Default
	var onUpdateExpr *plan.OnUpdate
	var err error
	for _, attr := range newColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			colDef.Primary = true
			//isPrimaryKey = true
		case *tree.AttributeComment:
			colDef.Comment = attribute.CMT.String()
		case *tree.AttributeAutoIncrement:
			//auto_incr = true
			colDef.Typ.AutoIncr = true
		case *tree.AttributeDefault, *tree.AttributeNull:
			defaultValue, err = buildDefaultExpr(newColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			colDef.Default = defaultValue
			hasNullFlag = true
			fmt.Printf("hasNullFlag: %v\n", hasNullFlag)
		case *tree.AttributeOnUpdate:
			onUpdateExpr, err = buildOnUpdate(newColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			colDef.OnUpdate = onUpdateExpr
		}
	}

	if defaultValue == nil {
		defaultValue, err = buildDefaultExpr(newColumn, colType, ctx.GetProcess())
		if err != nil {
			return nil, err
		}
		colDef.Default = defaultValue
	}

	return colDef, nil
}

func checkAddColumnTooManyColumns(ctx context.Context, colNum int) error {
	if uint32(colNum) > TableColumnCountLimit {
		return moerr.NewErrTooManyFields(ctx)
	}
	return nil
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

func checkUnsupportedColumnConstraint(ctx context.Context, column *tree.ColumnTableDef, tableName string) error {
	for _, constraint := range column.Attributes {
		switch constraint.(type) {
		case *tree.AttributeAutoIncrement:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint AUTO_INCREMENT when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributeKey:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' index when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributeUniqueKey, *tree.AttributeUnique:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint UNIQUE KEY when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributePrimaryKey:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint PRIMARY KEY when alter table '%s'", column.Name.Parts[0], tableName)
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
