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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	//step 1 : reject subpartition
	if partitionSyntaxDef.SubPartBy != nil {
		return moerr.NewInvalidInput(ctx, "no subpartition")
	}
	//step 2: verify the partition number [1,1024]
	partitionsNum := partitionSyntaxDef.PartBy.Num
	// If you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkPartitionCount(ctx, partitionBinder, int(partitionsNum)); err != nil {
		return err
	}

	partitionType := partitionSyntaxDef.PartBy.PType.(*tree.HashType)
	partitionDef := &plan.PartitionByDef{
		Partitions:     make([]*plan.PartitionItem, partitionsNum),
		PartitionNum:   partitionsNum,
		IsSubPartition: partitionSyntaxDef.PartBy.IsSubPartition,
	}

	if partitionType.Linear {
		partitionDef.Type = plan.PartitionType_LINEAR_HASH
	} else {
		partitionDef.Type = plan.PartitionType_HASH
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
	//step 1: verify the partition defs valid or not
	if partitionDef.PartitionNum < uint64(len(syntaxDefs)) {
		return moerr.NewInvalidInput(ctx, "wrong number of partition definitions")
	}

	dedup := make(map[string]bool)
	//step 2: process defs in syntax
	i := 0
	for ; i < len(syntaxDefs); i++ {
		name := string(syntaxDefs[i].Name)
		if _, ok := dedup[name]; ok {
			return moerr.NewInvalidInput(ctx, "duplicate partition name %s", name)
		}
		dedup[name] = true

		//get COMMENT option only
		comment := ""
		for _, option := range syntaxDefs[i].Options {
			if commentOpt, ok := option.(*tree.TableOptionComment); ok {
				comment = commentOpt.Comment
			}
		}

		pi := &plan.PartitionItem{
			PartitionName:   name,
			OrdinalPosition: uint32(i + 1),
			Comment:         comment,
		}
		if i < len(partitionDef.Partitions) {
			partitionDef.Partitions[i] = pi
		} else {
			partitionDef.Partitions = append(partitionDef.Partitions, pi)
		}
	}

	//step 3: complement partition defs missing in syntax
	for ; i < int(partitionDef.PartitionNum); i++ {
		name := fmt.Sprintf("p%d", i)
		if _, ok := dedup[name]; ok {
			return moerr.NewInvalidInput(ctx, "duplicate partition name %s", name)
		}
		dedup[name] = true
		pi := &plan.PartitionItem{
			PartitionName:   name,
			OrdinalPosition: uint32(i + 1),
		}
		if i < len(partitionDef.Partitions) {
			partitionDef.Partitions[i] = pi
		} else {
			partitionDef.Partitions = append(partitionDef.Partitions, pi)
		}
	}

	return nil
}

/*
checkTableDefPartition
check partition keys
check partition expr type
check partition defs constraints
*/
func (hpb *hashPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	if err := checkPartitionKeys(ctx, partitionBinder.builder.nameByColRef, tableDef, partitionDef); err != nil {
		return err
	}

	if err := checkPartitionExprType(ctx, partitionBinder, tableDef, partitionDef); err != nil {
		return err
	}
	if err := checkPartitionDefs(ctx, partitionBinder, partitionDef, tableDef); err != nil {
		return err
	}

	if partitionDef.Type == plan.PartitionType_KEY || partitionDef.Type == plan.PartitionType_LINEAR_KEY {
		//if len(partitionDef.Columns) == 0 {
		if len(partitionDef.PartitionColumns.Columns) == 0 {
			return handleEmptyKeyPartition(partitionBinder, tableDef, partitionDef)
		}
	}
	return nil
}

func (hpb *hashPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder,
	stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.HashType)
	// For the Hash partition, convert the partition information into the expression,such as: abs (hash_value (expr)) % partitionNum
	hashExpr := partitionType.Expr
	partitionAst := genPartitionAst(tree.Exprs{hashExpr}, int64(partitionDef.PartitionNum))

	partitionExpression, err := partitionBinder.baseBindExpr(partitionAst, 0, true)
	if err != nil {
		return err
	}
	partitionDef.PartitionExpression = partitionExpression
	return nil
}
