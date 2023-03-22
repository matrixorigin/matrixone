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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type listPartitionBuilder struct {
}

// buildListPartitiion handle List Partitioning and List columns partitioning
func (lpb *listPartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.ListType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError(partitionBinder.GetContext(), "build list partition")
	}

	partitionInfo := &plan.PartitionByDef{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
		PartitionNum:   uint64(partitionNum),
	}

	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_LIST
		planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.PartitionExpr = &plan.PartitionExpr{
			Expr:    planExpr,
			ExprStr: tree.String(partitionType.Expr, dialect.MYSQL),
		}
	} else {
		partitionInfo.Type = plan.PartitionType_LIST_COLUMNS
		err := buildPartitionColumns(partitionBinder, partitionInfo, partitionType.ColumnList)
		if err != nil {
			return err
		}
	}

	err := lpb.buildPartitionDefs(ctx, partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}

	err = lpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	err = lpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Partition = partitionInfo
	return nil
}

func (lpb *listPartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error) {
	for i, partition := range defs {
		partitionItem := &plan.PartitionItem{
			PartitionName:   string(partition.Name),
			OrdinalPosition: uint32(i + 1),
		}

		if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
			binder := NewPartitionBinder(nil, nil)
			inValues := make([]*plan.Expr, len(valuesIn.ValueList))

			for j, value := range valuesIn.ValueList {
				tuple, err := binder.BindExpr(value, 0, false)
				if err != nil {
					return err
				}
				inValues[j] = tuple
			}
			partitionItem.InValues = inValues
			partitionItem.Description = tree.String(valuesIn, dialect.MYSQL)
		} else {
			return moerr.NewInternalError(partitionBinder.GetContext(), "LIST PARTITIONING can only use VALUES IN definition")
		}
		partitionDef.Partitions[i] = partitionItem
	}
	return buildListPartitionItem(partitionBinder, partitionDef, defs)
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
		partitionExpression, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
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
		partitionExpression, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
		if err != nil {
			return err
		}
		partitionDef.PartitionExpression = partitionExpression
	}
	return nil
}
