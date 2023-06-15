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
	"strings"
)

// keyPartitionBuilder processes key partition
type keyPartitionBuilder struct {
}

func (kpb *keyPartitionBuilder) build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionSyntaxDef := stmt.PartitionOption
	partitionCount, err := getValidPartitionCount(ctx, false, partitionSyntaxDef)
	if err != nil {
		return err
	}
	partitionType := partitionSyntaxDef.PartBy.PType.(*tree.KeyType)
	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return moerr.NewInvalidInput(ctx, "the 'ALGORITHM' option is unsupported")
	}

	partitionDef := &plan.PartitionByDef{
		PartitionNum:   partitionCount,
		Algorithm:      partitionType.Algorithm,
		IsSubPartition: partitionSyntaxDef.PartBy.IsSubPartition,
	}

	partitionDef.Type = plan.PartitionType_KEY
	if partitionType.Linear {
		partitionDef.Type = plan.PartitionType_LINEAR_KEY
	}

	// complement the column list if there is no one in the syntax
	if len(partitionType.ColumnList) == 0 {
		// Any columns used as the partitioning key must comprise part or all of the table's primary key, if the table has one.
		// Where no column name is specified as the partitioning key, the table's primary key is used, if there is one.
		// If there is no primary key but there is a unique key, then the unique key is used for the partitioning key
		primaryKeys, uniqueIndices := getPrimaryKeyAndUniqueKey(stmt.Defs)
		if len(primaryKeys) != 0 {
			partitionType.ColumnList = primaryKeys
		} else if len(uniqueIndices) != 0 {
			isLastUniqueIndex := false

		label1:
			for i, _ := range uniqueIndices {
				isLastUniqueIndex = len(uniqueIndices) == i
				uniqueKey := uniqueIndices[i]
				names := make([]*tree.UnresolvedName, len(uniqueKey.KeyParts))
				for i, keyPart := range uniqueKey.KeyParts {
					// if the unique key column were not defined as NOT NULL, then the previous statement would fail.
					// See: https://dev.mysql.com/doc/refman/8.0/en/partitioning-key.html
					if ok := checkTableColumnsNotNull(tableDef, keyPart.ColName.Parts[0]); !ok {
						if isLastUniqueIndex {
							return moerr.NewInvalidInput(ctx, "Field in list of fields for partition function not found in table")
						} else {
							continue label1
						}
					}
					names[i] = keyPart.ColName
				}
				partitionType.ColumnList = names
			}
		}
		if len(partitionType.ColumnList) == 0 {
			return moerr.NewInvalidInput(ctx, "Field in list of fields for partition function not found in table")
		}
	}

	err = buildPartitionColumns(ctx, partitionBinder, partitionDef, partitionType.ColumnList)
	if err != nil {
		return err
	}

	err = kpb.buildPartitionDefs(ctx, partitionBinder, partitionDef, partitionSyntaxDef.Partitions)
	if err != nil {
		return err
	}
	err = kpb.checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionDef)
	if err != nil {
		return err
	}
	err = kpb.buildEvalPartitionExpression(ctx, partitionBinder, stmt, partitionDef)
	if err != nil {
		return err
	}

	partitionDef.PartitionMsg = tree.String(partitionSyntaxDef, dialect.MYSQL)
	tableDef.Partition = partitionDef
	return nil
}

func (kpb *keyPartitionBuilder) buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, syntaxDefs []*tree.Partition) (err error) {
	return buildPartitionDefs(ctx, partitionDef, syntaxDefs)
}

func (kpb *keyPartitionBuilder) checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	return checkPartitionIntegrity(ctx, partitionBinder, tableDef, partitionDef)
}

func (kpb *keyPartitionBuilder) buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, partitionDef *plan.PartitionByDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	//For the Key partition, convert the partition information into the expression,
	//such as : abs (hash_value (expr)) % partitionNum
	var astExprs []tree.Expr
	keyList := partitionType.ColumnList
	astExprs = make([]tree.Expr, len(keyList))
	for i, expr := range keyList {
		astExprs[i] = expr
	}

	partitionAst := genPartitionAst(astExprs, int64(partitionDef.PartitionNum))
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

// checkTableColumnsNotNull check unique column is `NOT NULL`
func checkTableColumnsNotNull(tableDef *TableDef, columnName string) bool {
	for _, coldef := range tableDef.Cols {
		if strings.EqualFold(coldef.Name, columnName) {
			return !coldef.Default.NullAbility
		}
	}
	return true
}
