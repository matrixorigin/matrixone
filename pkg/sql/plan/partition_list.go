// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

type listPartitionBuilder struct {
	addPartitions            []*plan.PartitionItem
	addPartitionSubTableDefs []*TableDef
	newPartitionDef          *PartitionByDef
}

// buildListPartitiion handle List Partitioning and List columns partitioning
func (lpb *listPartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionSyntaxDef := stmt.PartitionOption
	partitionCount, err := getValidPartitionCount(ctx, true, partitionSyntaxDef)
	if err != nil {
		return err
	}
	partitionType := partitionSyntaxDef.PartBy.PType.(*tree.ListType)

	partitionDef := &plan.PartitionByDef{
		IsSubPartition: partitionSyntaxDef.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionCount),
		PartitionNum:   partitionCount,
	}

	if len(partitionType.ColumnList) == 0 {
		//PARTITION BY LIST(expr)
		partitionDef.Type = plan.PartitionType_LIST
		err := buildPartitionExpr(ctx, tableDef, partitionBinder, partitionDef, partitionType.Expr)
		if err != nil {
			return err
		}
	} else {
		//PARTITION BY LIST COLUMNS(col1,col2,...)
		partitionDef.Type = plan.PartitionType_LIST_COLUMNS
		err = buildPartitionColumns(ctx, tableDef, partitionBinder, partitionDef, partitionType.ColumnList)
		if err != nil {
			return err
		}
	}

	err = lpb.buildPartitionDefs(ctx, partitionBinder, partitionDef, partitionSyntaxDef.Partitions)
	if err != nil {
		return err
	}

	err = lpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionDef)
	if err != nil {
		return err
	}

	err = lpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt.PartitionOption, partitionDef)
	if err != nil {
		return err
	}

	//partitionDef.PartitionMsg = tree.String(partitionSyntaxDef, dialect.MYSQL)
	partitionDef.PartitionMsg = tree.StringWithOpts(partitionSyntaxDef, dialect.MYSQL, tree.WithSingleQuoteString())
	tableDef.Partition = partitionDef
	return nil
}

func (lpb *listPartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error) {
	for i, partition := range defs {
		name := string(partition.Name)
		partitionItem := &plan.PartitionItem{
			PartitionName:   name,
			OrdinalPosition: uint32(i + 1),
		}

		if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
			valueList := valuesIn.ValueList
			if len(valueList) == 0 {
				return moerr.NewInvalidInput(ctx, "there is no value in value list")
			}
			binder := NewPartitionBinder(nil, nil)
			inValues := make([]*plan.Expr, len(valueList))

			for j, value := range valueList {
				tuple, err := binder.BindExpr(value, 0, false)
				if err != nil {
					return err
				}
				inValues[j] = tuple
			}
			partitionItem.InValues = inValues
			partitionItem.Description = tree.String(valuesIn, dialect.MYSQL)
		} else {
			return moerr.NewInternalError(ctx, "LIST PARTITIONING can only use VALUES IN definition")
		}
		for _, tableOption := range partition.Options {
			if opComment, ok := tableOption.(*tree.TableOptionComment); ok {
				partitionItem.Comment = opComment.Comment
			}
		}
		partitionDef.Partitions[i] = partitionItem
	}
	return buildListPartitionItem(partitionBinder, partitionDef, defs)
}

func (lpb *listPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	if err := checkPartitionExprType(ctx, partitionBinder, tableDef, partitionDef); err != nil {
		return err
	}
	if err := checkPartitionDefines(ctx, partitionBinder, partitionDef, tableDef); err != nil {
		return err
	}
	if err := checkPartitionKeys(ctx, partitionBinder.builder.nameByColRef, tableDef, partitionDef); err != nil {
		return err
	}
	if partitionDef.Type == plan.PartitionType_KEY || partitionDef.Type == plan.PartitionType_LINEAR_KEY {
		if len(partitionDef.PartitionColumns.Columns) == 0 {
			return handleEmptyKeyPartition(partitionBinder, tableDef, partitionDef)
		}
	}
	return nil
}

func (lpb *listPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.PartitionOption, partitionDef *plan.PartitionByDef) error {
	partitionType := stmt.PartBy.PType.(*tree.ListType)
	// For the List partition, convert the partition information into the expression, such as:
	// case when expr in (1, 5, 9, 13, 17) then 0 when expr in (2, 6, 10, 14, 18) then 1 else -1 end
	if partitionType.ColumnList == nil {
		listExpr := partitionType.Expr
		partitionExprAst, err := buildListCaseWhenExpr(listExpr, stmt.Partitions)
		if err != nil {
			return err
		}
		tempExpr, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
		if err != nil {
			return err
		}
		partitionExpression, err := appendCastBeforeExpr(ctx, tempExpr, &plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		})
		if err != nil {
			return err
		}
		partitionDef.PartitionExpression = partitionExpression
	} else {
		// For the List Columns partition, convert the partition information into the expression, such as:
		// case when col1 = 0 and col2 = 0 or col1 = null and col2 = null then 0
		// when col1 = 0 and col2 = 1 or col1 = 0 and col2 = 2 or col1 = 0 and col2 = 3 then 1
		// when col1 = 1 and col2 = 0 or col1 = 2 and col2 = 0 or col1 = 2 and col2 = 1 then 2
		// else -1 end
		columnsExpr := partitionType.ColumnList
		partitionExprAst, err := buildListColumnsCaseWhenExpr(columnsExpr, stmt.Partitions)
		if err != nil {
			return err
		}
		tempExpr, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
		if err != nil {
			return err
		}
		partitionExpression, err := appendCastBeforeExpr(ctx, tempExpr, &plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		})
		if err != nil {
			return err
		}
		partitionDef.PartitionExpression = partitionExpression
	}
	return nil
}

// buildAddPartition Perform `ADD PARTITION` semantic check on the list partition table
func (lpb *listPartitionBuilder) buildAddPartition(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.AlterPartitionAddPartitionClause, tableDef *TableDef) error {
	partitionInfo := DeepCopyPartitionByDef(tableDef.Partition)

	err := lpb.buildAddPartitionDefinitions(ctx, partitionBinder, partitionInfo, stmt.Partitions)
	if err != nil {
		return err
	}

	err = lpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	//------------------------------------------------------------------------------------------------------------------
	// Regenerate the syntax tree for the partition by clause
	ast, err := mysql.ParseOne(ctx, "create table t1() "+partitionInfo.PartitionMsg, 1, 0)
	if err != nil {
		return err
	}

	createTable, ok := ast.(*tree.CreateTable)
	if !ok {
		panic("Partition definition statement Parser restore failed")
	}

	// Regenerate partition calculation expression
	partitionBy := createTable.PartitionOption
	partitionBy.Partitions = append(partitionBy.Partitions, stmt.Partitions...)
	err = lpb.buildEvalPartitionExpression(ctx, partitionBinder, partitionBy, partitionInfo)
	if err != nil {
		return err
	}

	// Generate a new partition by clause's restore string
	partitionInfo.PartitionMsg = tree.StringWithOpts(partitionBy, dialect.MYSQL, tree.WithSingleQuoteString())

	// Construct partition subtable `TableDef` for newly added partitions
	err = lpb.makePartitionSubTableDef(ctx, tableDef, partitionInfo, lpb.addPartitions)
	if err != nil {
		return err
	}
	return nil
}

// Perform semantic check for newly added partition definitions
func (lpb *listPartitionBuilder) buildAddPartitionDefinitions(ctx context.Context, partitionBinder *PartitionBinder, newPartitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error) {
	srcLen := len(newPartitionDef.Partitions)
	addPartitions := make([]*plan.PartitionItem, len(defs))

	for i, partition := range defs {
		name := string(partition.Name)
		partitionItem := &plan.PartitionItem{
			PartitionName:   name,
			OrdinalPosition: uint32(srcLen + i + 1),
		}

		if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
			valueList := valuesIn.ValueList
			if len(valueList) == 0 {
				return moerr.NewInvalidInput(ctx, "there is no value in value list")
			}
			binder := NewPartitionBinder(nil, nil)
			inValues := make([]*plan.Expr, len(valueList))

			for j, value := range valueList {
				tuple, err := binder.BindExpr(value, 0, false)
				if err != nil {
					return err
				}
				inValues[j] = tuple
			}
			partitionItem.InValues = inValues
			partitionItem.Description = tree.String(valuesIn, dialect.MYSQL)
		} else {
			return moerr.NewInternalError(ctx, "LIST PARTITIONING can only use VALUES IN definition")
		}
		for _, tableOption := range partition.Options {
			if opComment, ok := tableOption.(*tree.TableOptionComment); ok {
				partitionItem.Comment = opComment.Comment
			}
		}
		addPartitions[i] = partitionItem
		newPartitionDef.Partitions = append(newPartitionDef.Partitions, partitionItem)
	}
	lpb.addPartitions = addPartitions
	return buildListPartitionItem(partitionBinder, newPartitionDef, defs)
}

// makePartitionSubTableDef Construct List partition subtable `TableDef` for newly added partitions
func (lpb *listPartitionBuilder) makePartitionSubTableDef(ctx context.Context, tableDef *TableDef, newPartitionDef *plan.PartitionByDef, alterAddPartition []*plan.PartitionItem) error {
	//add partition table
	//there is no index for the partition table
	//there is no foreign key for the partition table
	mainTableName := tableDef.Name
	if !util.IsValidNameForPartitionTable(mainTableName) {
		return moerr.NewInvalidInput(ctx, "invalid main table name %s", mainTableName)
	}

	// common properties
	partitionProps := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemPartitionRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: "",
		},
	}
	partitionPropsDef := &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: partitionProps,
			},
		}}

	partitionTableDefs := make([]*TableDef, len(alterAddPartition))
	partitionTableNames := make([]string, len(alterAddPartition))

	for i := 0; i < len(alterAddPartition); i++ {
		part := alterAddPartition[i]
		ok, partitionTableName := util.MakeNameOfPartitionTable(part.GetPartitionName(), mainTableName)
		if !ok {
			return moerr.NewInvalidInput(ctx, "invalid partition table name %s", partitionTableName)
		}

		// save the table name for a partition
		part.PartitionTableName = partitionTableName
		partitionTableNames[i] = partitionTableName

		partitionTableDefs[i] = &TableDef{
			Name: partitionTableName,
			Cols: deepCopyTableCols(tableDef.Cols, true), //same as the main table's column defs
		}

		partitionTableDefs[i].Pkey = tableDef.GetPkey()
		partitionTableDefs[i].Defs = append(partitionTableDefs[i].Defs, partitionPropsDef)
	}
	newPartitionDef.PartitionTableNames = append(newPartitionDef.PartitionTableNames, partitionTableNames...)
	newPartitionDef.PartitionNum = newPartitionDef.PartitionNum + uint64(len(alterAddPartition))
	lpb.newPartitionDef = newPartitionDef
	lpb.addPartitionSubTableDefs = partitionTableDefs
	return nil
}
