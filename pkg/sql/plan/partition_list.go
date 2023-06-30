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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type listPartitionBuilder struct {
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
		err = buildPartitionColumns(ctx, partitionBinder, partitionDef, partitionType.ColumnList)
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

	err = lpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionDef)
	if err != nil {
		return err
	}

	partitionDef.PartitionMsg = tree.String(partitionSyntaxDef, dialect.MYSQL)
	tableDef.Partition = partitionDef
	return nil
}

func (lpb *listPartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, syntaxDefs []*tree.Partition) (err error) {
	dedup := make(map[string]int)
	for i, partition := range syntaxDefs {
		name := string(partition.Name)
		if _, ok := dedup[name]; ok {
			return moerr.NewInvalidInput(ctx, "duplicate partition name %s", name)
		}
		dedup[name] = 0
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
		partitionDef.Partitions[i] = partitionItem
	}
	return buildListPartitionItem(partitionBinder, partitionDef, syntaxDefs)
}

func (lpb *listPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	if err := checkPartitionExprType(ctx, partitionBinder, tableDef, partitionDef); err != nil {
		return err
	}
	if err := checkPartitionDefs(ctx, partitionBinder, partitionDef, tableDef); err != nil {
		return err
	}
	if err := checkPartitionKeys(ctx, partitionBinder.builder.nameByColRef, tableDef, partitionDef); err != nil {
		return err
	}
	if partitionDef.Type == plan.PartitionType_KEY || partitionDef.Type == plan.PartitionType_LINEAR_KEY {
		//if len(partitionInfo.Columns) == 0 {
		if len(partitionDef.PartitionColumns.Columns) == 0 {
			return handleEmptyKeyPartition(partitionBinder, tableDef, partitionDef)
		}
	}
	return nil
}

func (lpb *listPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.ListType)
	// For the List partition, convert the partition information into the expression, such as:
	// case when expr in (1, 5, 9, 13, 17) then 0 when expr in (2, 6, 10, 14, 18) then 1 else -1 end
	if partitionType.ColumnList == nil {
		listExpr := partitionType.Expr
		partitionExprAst, err := buildListCaseWhenExpr(listExpr, partitionOp.Partitions)
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
		partitionExprAst, err := buildListColumnsCaseWhenExpr(columnsExpr, partitionOp.Partitions)
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
