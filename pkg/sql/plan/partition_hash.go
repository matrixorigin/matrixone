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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// hashPartitionBuilder processes Hash Partition
type hashPartitionBuilder struct {
}

// buildHashPartition handle Hash Partitioning
func (hpb *hashPartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionSyntaxDef := stmt.PartitionOption
	partitionCount, err := getValidPartitionCount(ctx, false, partitionSyntaxDef)
	if err != nil {
		return err
	}
	partitionType := partitionSyntaxDef.PartBy.PType.(*tree.HashType)
	partitionDef := &plan.PartitionByDef{
		PartitionNum:   partitionCount,
		IsSubPartition: partitionSyntaxDef.PartBy.IsSubPartition,
	}

	partitionDef.Type = plan.PartitionType_HASH
	if partitionType.Linear {
		partitionDef.Type = plan.PartitionType_LINEAR_HASH
	}

	//step 3: build partition expr
	planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
	if err != nil {
		return err
	}

	partitionDef.PartitionExpr = &plan.PartitionExpr{
		Expr:    planExpr,
		ExprStr: tree.String(partitionType.Expr, dialect.MYSQL),
	}

	err = hpb.buildPartitionDefs(ctx, partitionBinder, partitionDef, partitionSyntaxDef.Partitions)
	if err != nil {
		return err
	}

	err = hpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionDef)
	if err != nil {
		return err
	}

	err = hpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionDef)
	if err != nil {
		return err
	}

	partitionDef.PartitionMsg = tree.String(partitionSyntaxDef, dialect.MYSQL)
	tableDef.Partition = partitionDef
	return nil
}

// buildPartitionDefs decides the partition defs
func (hpb *hashPartitionBuilder) buildPartitionDefs(ctx context.Context, _ *PartitionBinder,
	partitionDef *plan.PartitionByDef, syntaxDefs []*tree.Partition) error {
	return buildPartitionDefs(ctx, partitionDef, syntaxDefs)
}

/*
checkTableDefPartition
check partition keys
check partition expr type
check partition defs constraints
*/
func (hpb *hashPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	return checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionDef)
}

func (hpb *hashPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder,
	stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.HashType)
	// For the Hash partition, convert the partition information into the expression,such as: abs (hash_value (expr)) % partitionNum
	hashExpr := partitionType.Expr
	partitionAst := genPartitionAst(tree.Exprs{hashExpr}, int64(partitionDef.PartitionNum))

	tempExpr, err := partitionBinder.baseBindExpr(partitionAst, 0, true)
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
	return nil
}
