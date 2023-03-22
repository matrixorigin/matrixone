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
	"strconv"
)

// keyPartitionBuilder processes key partition
type keyPartitionBuilder struct {
}

func (kpb *keyPartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	if partitionOp.SubPartBy != nil {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "no subpartition")
	}

	// if you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	partitionsNum := partitionOp.PartBy.Num
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkPartitionCount(ctx, partitionBinder, int(partitionsNum)); err != nil {
		return err
	}

	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "the 'ALGORITHM' option has too many values")
	}

	partitionInfo := &plan.PartitionByDef{
		Partitions:     make([]*plan.PartitionItem, partitionsNum),
		PartitionNum:   partitionsNum,
		Algorithm:      partitionType.Algorithm,
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
	}

	if partitionType.Linear {
		partitionInfo.Type = plan.PartitionType_LINEAR_KEY
	} else {
		partitionInfo.Type = plan.PartitionType_KEY
	}
	err := buildPartitionColumns(partitionBinder, partitionInfo, partitionType.ColumnList)
	if err != nil {
		return err
	}

	err = kpb.buildPartitionDefs(ctx, partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}
	err = kpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}
	err = kpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Partition = partitionInfo
	return nil
}

func (kpb *keyPartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error) {
	for i := uint64(0); i < partitionDef.PartitionNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:   "p" + strconv.FormatUint(i, 10),
			OrdinalPosition: uint32(i + 1),
		}
		partitionDef.Partitions[i] = partition
	}
	return nil
}

func (kpb *keyPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
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

func (kpb *keyPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	// For the Key partition, convert the partition information into the expression,such as : abs (hash_value (expr)) % partitionNum
	var astExprs []tree.Expr
	if partitionDef.PartitionColumns == nil {
		// Any columns used as the partitioning key must comprise part or all of the table's primary key, if the table has one.
		// Where no column name is specified as the partitioning key, the table's primary key is used, if there is one.
		// If there is no primary key but there is a unique key, then the unique key is used for the partitioning key
		primaryKeys, uniqueIndexs := getPrimaryKeyAndUniqueKey(stmt.Defs)
		if len(primaryKeys) != 0 {
			astExprs = make([]tree.Expr, len(primaryKeys))
			for i, kexpr := range primaryKeys {
				astExprs[i] = kexpr
			}
		} else if len(uniqueIndexs) != 0 {
			uniqueKey := uniqueIndexs[0]
			astExprs = make([]tree.Expr, len(uniqueKey.KeyParts))
			for i, keyPart := range uniqueKey.KeyParts {
				astExprs[i] = keyPart.ColName
			}
		} else {
			return moerr.NewInvalidInput(partitionBinder.GetContext(), "Field in list of fields for partition function not found in table")
		}
	} else {
		keyList := partitionType.ColumnList
		astExprs = make([]tree.Expr, len(keyList))
		for i, expr := range keyList {
			astExprs[i] = expr
		}
	}

	partitionAst := genPartitionAst(astExprs, int64(partitionDef.PartitionNum))
	partitionExpression, err := partitionBinder.baseBindExpr(partitionAst, 0, true)
	if err != nil {
		return err
	}
	partitionDef.PartitionExpression = partitionExpression
	return nil
}
