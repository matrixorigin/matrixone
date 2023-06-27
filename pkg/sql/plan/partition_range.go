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

type rangePartitionBuilder struct {
}

// buildRangePartition handle Range Partitioning and Range columns partitioning
func (rpb *rangePartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.RangeType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError(partitionBinder.GetContext(), "build range partition")
	}

	partitionInfo := &plan.PartitionByDef{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
		PartitionNum:   uint64(partitionNum),
	}

	// RANGE Partitioning
	//if len(partitionType.ColumnList) == 0 {
	//	partitionInfo.Type = plan.PartitionType_RANGE
	//	planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
	//	if err != nil {
	//		return err
	//	}
	//	partitionInfo.PartitionExpr = &plan.PartitionExpr{
	//		Expr:    planExpr,
	//		ExprStr: tree.String(partitionType.Expr, dialect.MYSQL),
	//	}
	//} else {
	//	// RANGE COLUMNS partitioning
	//	partitionInfo.Type = plan.PartitionType_RANGE_COLUMNS
	//	err := buildPartitionColumns(ctx, partitionBinder, partitionInfo, partitionType.ColumnList)
	//	if err != nil {
	//		return err
	//	}
	//}
	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_RANGE
		err := buildRangePartitionExpr(ctx, partitionBinder, partitionType.Expr, partitionInfo)
		if err != nil {
			return err
		}
	} else {
		// RANGE COLUMNS partitioning
		partitionInfo.Type = plan.PartitionType_RANGE_COLUMNS
		err := buildRangePartitionColumns(ctx, partitionBinder, partitionInfo, partitionType.ColumnList)
		if err != nil {
			return err
		}
	}

	err := rpb.buildPartitionDefs(ctx, partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}

	err = rpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	err = rpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Partition = partitionInfo
	return nil
}

func (rpb *rangePartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error) {
	// VALUES LESS THAN value must be strictly increasing for each partition
	for i, partition := range defs {
		partitionItem := &plan.PartitionItem{
			PartitionName:   string(partition.Name),
			OrdinalPosition: uint32(i + 1),
		}

		if valuesLessThan, ok := partition.Values.(*tree.ValuesLessThan); ok {
			planExprs := make([]*plan.Expr, len(valuesLessThan.ValueList))
			binder := NewPartitionBinder(nil, nil)

			for j, valueExpr := range valuesLessThan.ValueList {
				// value must be able to evaluate the expression's return value
				planExpr, err := binder.BindExpr(valueExpr, 0, false)
				if err != nil {
					return err
				}
				planExprs[j] = planExpr
			}

			partitionItem.LessThan = planExprs
			partitionItem.Description = tree.String(valuesLessThan.ValueList, dialect.MYSQL)
		} else {
			return moerr.NewInternalError(partitionBinder.GetContext(), "RANGE PARTITIONING can only use VALUES LESS THAN definition")
		}

		for _, tableOption := range partition.Options {
			if opComment, ok := tableOption.(*tree.TableOptionComment); ok {
				partitionItem.Comment = opComment.Comment
			}
		}
		partitionDef.Partitions[i] = partitionItem
	}
	return buildRangePartitionItem(partitionBinder, partitionDef, defs)
}

func (rpb *rangePartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
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

func (rpb *rangePartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.RangeType)
	// For the Range partition, convert the partition information into the expression,such as:
	// case when expr < 6 then 0 when expr < 11 then 1 when true then 3 else -1 end
	if partitionType.ColumnList == nil {
		rangeExpr := partitionType.Expr
		partitionExprAst, err := buildRangeCaseWhenExpr(rangeExpr, partitionOp.Partitions)
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
		// For the Range Columns partition, convert the partition information into the expression, such as:
		// (a, b, c) < (x0, x1, x2) -->  a < x0 || (a = x0 && (b < x1 || b = x1 && c < x2))
		columnsExpr := partitionType.ColumnList
		partitionExprAst, err := buildRangeColumnsCaseWhenExpr(columnsExpr, partitionOp.Partitions)
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

func buildRangePartitionExpr(ctx context.Context, partitionBinder *PartitionBinder, pExpr tree.Expr, partitionDef *plan.PartitionByDef) error {
	//PARTITION BY LIST(expr)
	planExpr, err := partitionBinder.BindExpr(pExpr, 0, true)
	if err != nil {
		return err
	}
	fmtCtx := tree.NewFmtCtx2(dialect.MYSQL, tree.RestoreNameBackQuotes)
	pExpr.Format(fmtCtx)
	exprStr := fmtCtx.ToString()

	partitionDef.PartitionExpr = &plan.PartitionExpr{
		Expr:    planExpr,
		ExprStr: exprStr,
	}
	return nil
}

func buildRangePartitionColumns(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, columnList []*tree.UnresolvedName) error {
	var err error
	columnsExpr := make([]*plan.Expr, len(columnList))
	partitionColumns := make([]string, len(columnList))
	partitionFmtColumns := make([]string, len(columnList))

	fmtCtx := tree.NewFmtCtx2(dialect.MYSQL, tree.RestoreNameBackQuotes)
	// partition COLUMNS does not accept expressions, only names of columns.
	for i, column := range columnList {
		columnsExpr[i], err = partitionBinder.BindColRef(column, 0, true)
		if err != nil {
			return err
		}
		// The permitted data types are shown in the following list:
		// All integer types
		// DATE and DATETIME
		// CHAR, VARCHAR, BINARY, and VARBINARY
		// See https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns.html
		t := types.T(columnsExpr[i].Typ.Id)
		if !t.IsInteger() && !t.IsMySQLString() && !t.IsDateRelate() {
			return moerr.NewSyntaxError(ctx, "column %s type %s is not allowed in partition clause", tree.String(column, dialect.MYSQL), t.String())
		}

		partitionColumns[i] = tree.String(column, dialect.MYSQL)

		column.Format(fmtCtx)
		partitionFmtColumns[i] = fmtCtx.ToString()
		fmtCtx.Reset()
	}
	partitionDef.PartitionColumns = &plan.PartitionColumns{
		Columns:             columnsExpr,
		PartitionColumns:    partitionColumns,
		PartitionFmtColumns: partitionFmtColumns,
	}
	return err
}
