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
	"fmt"
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	// Reference link https://dev.mysql.com/doc/mysql-reslimits-excerpt/5.6/en/partitioning-limitations.html
	PartitionNumberLimit = 8192
)

// buildHashPartition handle Hash Partitioning
func buildHashPartition(partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	if partitionOp.SubPartBy != nil {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "no subpartition")
	}
	partitionsNum := partitionOp.PartBy.Num
	// If you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkPartitionsNumber(partitionBinder, partitionsNum); err != nil {
		return err
	}

	partitionType := partitionOp.PartBy.PType.(*tree.HashType)
	partitionInfo := &plan.PartitionInfo{
		Partitions:     make([]*plan.PartitionItem, partitionsNum),
		PartitionNum:   partitionsNum,
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
	}

	if partitionType.Linear {
		partitionInfo.Type = plan.PartitionType_LINEAR_HASH
	} else {
		partitionInfo.Type = plan.PartitionType_HASH
	}

	planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
	if err != nil {
		return err
	}
	partitionInfo.Expr = planExpr

	err = buildPartitionDefinitionsInfo(partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}
	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}
	err = buildEvalPartitionExpression(partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})

	return nil
}

// buildKeyPartition handle KEY Partitioning
func buildKeyPartition(partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
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
	if err := checkPartitionsNumber(partitionBinder, partitionsNum); err != nil {
		return err
	}

	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "the 'ALGORITHM' option has too many values")
	}

	partitionInfo := &plan.PartitionInfo{
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

	err = buildPartitionDefinitionsInfo(partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}
	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}
	err = buildEvalPartitionExpression(partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})

	return nil
}

// buildRangePartition handle Range Partitioning and Range columns partitioning
func buildRangePartition(partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.RangeType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError(partitionBinder.GetContext(), "build range partition")
	}

	partitionInfo := &plan.PartitionInfo{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
		PartitionNum:   uint64(partitionNum),
	}

	// RANGE Partitioning
	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_RANGE
		planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.Expr = planExpr
	} else {
		// RANGE COLUMNS partitioning
		partitionInfo.Type = plan.PartitionType_RANGE_COLUMNS
		err := buildPartitionColumns(partitionBinder, partitionInfo, partitionType.ColumnList)
		if err != nil {
			return err
		}
	}

	err := buildPartitionDefinitionsInfo(partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}

	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	err = buildEvalPartitionExpression(partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})

	return nil
}

// buildListPartitiion handle List Partitioning and List columns partitioning
func buildListPartitiion(partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error {
	partitionOp := stmt.PartitionOption
	partitionType := partitionOp.PartBy.PType.(*tree.ListType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError(partitionBinder.GetContext(), "build list partition")
	}

	partitionInfo := &plan.PartitionInfo{
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
		partitionInfo.Expr = planExpr
	} else {
		partitionInfo.Type = plan.PartitionType_LIST_COLUMNS
		err := buildPartitionColumns(partitionBinder, partitionInfo, partitionType.ColumnList)
		if err != nil {
			return err
		}
	}

	err := buildPartitionDefinitionsInfo(partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}

	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	err = buildEvalPartitionExpression(partitionBinder, stmt, partitionInfo)
	if err != nil {
		return err
	}

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

// buildPartitionColumns COLUMNS partitioning enables the use of multiple columns in partitioning keys
func buildPartitionColumns(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, columnList []*tree.UnresolvedName) error {
	columnsExpr := make([]*plan.Expr, len(columnList))
	partitionColumns := make([]string, len(columnList))

	// partition COLUMNS does not accept expressions, only names of columns.
	for i, column := range columnList {
		colExpr, err := partitionBinder.BindColRef(column, 0, true)
		if err != nil {
			return moerr.NewParseError(partitionBinder.GetContext(), "build partition columns")
		}
		columnsExpr[i] = colExpr
		partitionColumns[i] = tree.String(column, dialect.MYSQL)
	}
	// check whether the columns partitioning type is legal
	if err := checkColumnsPartitionType(partitionBinder, partitionInfo, columnsExpr); err != nil {
		return err
	}
	partitionInfo.PartitionColumns = partitionColumns
	partitionInfo.Columns = columnsExpr

	return nil
}

func getPrimaryKeyAndUniqueKey(defs tree.TableDefs) (primaryKeys []*tree.UnresolvedName, uniqueIndexs []*tree.UniqueIndex) {
	for _, item := range defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			for _, attr := range def.Attributes {
				if _, ok := attr.(*tree.AttributePrimaryKey); ok {
					primaryKeys = append(primaryKeys, def.Name)
				}

				if _, ok := attr.(*tree.AttributeUniqueKey); ok {
					part := &tree.KeyPart{
						ColName: def.Name,
					}
					uniqueKey := &tree.UniqueIndex{
						KeyParts: []*tree.KeyPart{part},
						Name:     "",
						Empty:    true,
					}
					uniqueIndexs = append(uniqueIndexs, uniqueKey)
				}
			}
		case *tree.PrimaryKeyIndex:
			for _, key := range def.KeyParts {
				primaryKeys = append(primaryKeys, key.ColName)
			}
		case *tree.UniqueIndex:
			uniqueIndexs = append(uniqueIndexs, def)
		}
	}
	return
}

// This method is used to generate partition ast for key partition and hash partition
// For example: abs (hash_value (col3))% 4
func genPartitionAst(exprs tree.Exprs, partNum int64) tree.Expr {
	hashFuncName := tree.SetUnresolvedName(strings.ToLower("hash_value"))
	hashfuncExpr := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(hashFuncName),
		Exprs: exprs,
	}

	absFuncName := tree.SetUnresolvedName(strings.ToLower("abs"))
	absFuncExpr := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(absFuncName),
		Exprs: tree.Exprs{hashfuncExpr},
	}

	numstr := fmt.Sprintf("%v", partNum)
	divExpr := tree.NewNumValWithType(constant.MakeInt64(partNum), numstr, false, tree.P_int64)
	modOpExpr := tree.NewBinaryExpr(tree.MOD, absFuncExpr, divExpr)
	return modOpExpr
}

// This method is used to convert different types of partition structures into plan.Expr
func buildEvalPartitionExpression(partitionBinder *PartitionBinder, stmt *tree.CreateTable, partitionInfo *plan.PartitionInfo) error {
	partitionOp := stmt.PartitionOption
	switch partitionType := partitionOp.PartBy.PType.(type) {
	case *tree.KeyType:
		// For the Key partition, convert the partition information into the expression,such as : abs (hash_value (expr)) % partitionNum
		var astExprs []tree.Expr
		if len(partitionInfo.Columns) == 0 {
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

		partitionAst := genPartitionAst(astExprs, int64(partitionInfo.PartitionNum))
		partitionExpression, err := partitionBinder.baseBindExpr(partitionAst, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.PartitionExpression = partitionExpression
	case *tree.HashType:
		// For the Hash partition, convert the partition information into the expression,such as: abs (hash_value (expr)) % partitionNum
		hashExpr := partitionType.Expr
		partitionAst := genPartitionAst(tree.Exprs{hashExpr}, int64(partitionInfo.PartitionNum))

		partitionExpression, err := partitionBinder.baseBindExpr(partitionAst, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.PartitionExpression = partitionExpression
	case *tree.RangeType:
		// For the Range partition, convert the partition information into the expression,such as:
		// case when expr < 6 then 0 when expr < 11 then 1 when true then 3 else -1 end
		if partitionType.ColumnList == nil {
			rangeExpr := partitionType.Expr
			partitionExprAst, err := buildRangeCaseWhenExpr(rangeExpr, partitionOp.Partitions)
			if err != nil {
				return err
			}
			partitionExpression, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
			if err != nil {
				return err
			}
			partitionInfo.PartitionExpression = partitionExpression
		} else {
			// For the Range Columns partition, convert the partition information into the expression, such as:
			// (a, b, c) < (x0, x1, x2) -->  a < x0 || (a = x0 && (b < x1 || b = x1 && c < x2))
			columnsExpr := partitionType.ColumnList
			partitionExprAst, err := buildRangeColumnsCaseWhenExpr(columnsExpr, partitionOp.Partitions)
			if err != nil {
				return err
			}
			partitionExpression, err := partitionBinder.baseBindExpr(partitionExprAst, 0, true)
			if err != nil {
				return err
			}
			partitionInfo.PartitionExpression = partitionExpression
		}

	case *tree.ListType:
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
			partitionInfo.PartitionExpression = partitionExpression
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
			partitionInfo.PartitionExpression = partitionExpression
		}
	}
	return nil
}

// This method is used to convert the list columns partition into case when expression,such as:
// PARTITION BY LIST COLUMNS(a,b) (
// PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
// PARTITION p1 VALUES IN( (0,1), (0,2) ),
// PARTITION p2 VALUES IN( (1,0), (2,0) )
// );-->
// case
// when a = 0 and b = 0 or a = null and b = null then 0
// when a = 0 and b = 1 or a = 0 and b = 2 then 1
// when a = 1 and b = 0 or a = 2 and b = 0 then 2
// else -1
// end
func buildListColumnsCaseWhenExpr(columnsExpr []*tree.UnresolvedName, defs []*tree.Partition) (*tree.CaseExpr, error) {
	whens := make([]*tree.When, len(defs))

	for i, partition := range defs {
		valuesIn := partition.Values.(*tree.ValuesIn)

		elements := make([]tree.Expr, len(valuesIn.ValueList))
		for j, value := range valuesIn.ValueList {
			if tuple, ok := value.(*tree.Tuple); ok {
				exprs := tuple.Exprs
				if len(exprs) != len(columnsExpr) {
					panic("the number of IN expression parameters does not match")
				}

				if len(columnsExpr) == 1 {
					newExpr := tree.NewComparisonExpr(tree.EQUAL, columnsExpr[0], exprs[0])
					elements[j] = newExpr
					continue
				}

				if len(columnsExpr) >= 2 {
					var andExpr tree.Expr

					first := true
					for k, lexpr := range columnsExpr {
						if first {
							andExpr = tree.NewComparisonExpr(tree.EQUAL, lexpr, exprs[k])
							first = false
							continue
						}
						newExpr := tree.NewComparisonExpr(tree.EQUAL, lexpr, exprs[k])
						andExpr = tree.NewAndExpr(andExpr, newExpr)
					}
					elements[j] = andExpr
					continue
				}
			} else {
				if len(columnsExpr) != 1 {
					panic("the number of IN expression parameters does not match")
				}
				newExpr := tree.NewComparisonExpr(tree.EQUAL, columnsExpr[0], value)
				elements[j] = newExpr
				continue
			}
		}

		var conditionExpr tree.Expr
		if len(valuesIn.ValueList) == 1 {
			conditionExpr = elements[0]
		}

		if len(valuesIn.ValueList) > 1 {
			for m := 1; m < len(elements); m++ {
				if m == 1 {
					conditionExpr = tree.NewOrExpr(elements[m-1], elements[m])
				} else {
					conditionExpr = tree.NewOrExpr(conditionExpr, elements[m])
				}
			}
		}

		when := &tree.When{
			Cond: conditionExpr,
			Val:  tree.NewNumValWithType(constant.MakeInt64(int64(i)), fmt.Sprintf("%v", i), false, tree.P_int64),
		}
		whens[i] = when
	}
	caseWhenExpr := &tree.CaseExpr{
		Expr:  nil,
		Whens: whens,
		Else:  tree.NewNumValWithType(constant.MakeInt64(int64(-1)), fmt.Sprintf("%v", -1), false, tree.P_int64),
	}
	return caseWhenExpr, nil
}

// This method is used to convert the range partition into case when expression,such as:
// PARTITION BY RANGE (code + 5) (
// PARTITION p0 VALUES LESS THAN (6),
// PARTITION p1 VALUES LESS THAN (11),
// PARTITION p2 VALUES LESS THAN (MAXVALUE),
// ); -->
// case when (code + 5) < 6 then 0 when (code + 5) < 11 then 1 when true then 3 else -1 end
func buildRangeCaseWhenExpr(pexpr tree.Expr, defs []*tree.Partition) (*tree.CaseExpr, error) {
	whens := make([]*tree.When, len(defs))
	for i, partition := range defs {
		valuesLessThan := partition.Values.(*tree.ValuesLessThan)
		if len(valuesLessThan.ValueList) != 1 {
			panic("range partition less than expression should have one element")
		}
		valueExpr := valuesLessThan.ValueList[0]

		var conditionExpr tree.Expr
		if _, ok := valueExpr.(*tree.MaxValue); ok {
			conditionExpr = tree.NewNumValWithType(constant.MakeBool(true), "true", false, tree.P_bool)
		} else {
			LessThanExpr := tree.NewComparisonExpr(tree.LESS_THAN, pexpr, valueExpr)
			conditionExpr = LessThanExpr
		}

		when := &tree.When{
			Cond: conditionExpr,
			Val:  tree.NewNumValWithType(constant.MakeInt64(int64(i)), fmt.Sprintf("%v", i), false, tree.P_int64),
		}
		whens[i] = when
	}

	caseWhenExpr := &tree.CaseExpr{
		Expr:  nil,
		Whens: whens,
		Else:  tree.NewNumValWithType(constant.MakeInt64(int64(-1)), fmt.Sprintf("%v", -1), false, tree.P_int64),
	}
	return caseWhenExpr, nil
}

// This method is used to optimize the row constructor expression in range columns partition item into a common logical operation expression,
// such as: (a, b, c) < (x0, x1, x2) ->  a < x0 || (a = x0 && (b < x1 || b = x1 && c < x2))
func buildRangeColumnsCaseWhenExpr(columnsExpr []*tree.UnresolvedName, defs []*tree.Partition) (*tree.CaseExpr, error) {
	whens := make([]*tree.When, len(defs))
	for i, partition := range defs {
		valuesLessThan := partition.Values.(*tree.ValuesLessThan)

		if len(valuesLessThan.ValueList) != len(columnsExpr) {
			panic("the number of less value expression parameters does not match")
		}

		var tempExpr tree.Expr
		for j := len(valuesLessThan.ValueList) - 1; j >= 0; j-- {
			valueExpr := valuesLessThan.ValueList[j]
			if j == len(valuesLessThan.ValueList)-1 {
				if _, ok := valueExpr.(*tree.MaxValue); ok {
					trueExpr := tree.NewNumValWithType(constant.MakeBool(true), "true", false, tree.P_bool)
					tempExpr = trueExpr
				} else {
					lessThanExpr := tree.NewComparisonExpr(tree.LESS_THAN, columnsExpr[j], valueExpr)
					tempExpr = lessThanExpr
				}
				continue
			} else {
				var firstExpr tree.Expr
				if _, ok := valueExpr.(*tree.MaxValue); ok {
					trueExpr := tree.NewNumValWithType(constant.MakeBool(true), "true", false, tree.P_bool)
					firstExpr = trueExpr
				} else {
					lessThanExpr := tree.NewComparisonExpr(tree.LESS_THAN, columnsExpr[j], valueExpr)
					firstExpr = lessThanExpr
				}

				var middleExpr tree.Expr
				if _, ok := valueExpr.(*tree.MaxValue); ok {
					trueExpr := tree.NewNumValWithType(constant.MakeBool(true), "true", false, tree.P_bool)
					middleExpr = trueExpr
				} else {
					equalExpr := tree.NewComparisonExpr(tree.EQUAL, columnsExpr[j], valueExpr)
					middleExpr = equalExpr
				}
				secondExpr := tree.NewAndExpr(middleExpr, tempExpr)
				tempExpr = tree.NewOrExpr(firstExpr, secondExpr)
			}
		}

		when := &tree.When{
			Cond: tempExpr,
			Val:  tree.NewNumValWithType(constant.MakeInt64(int64(i)), fmt.Sprintf("%v", i), false, tree.P_int64),
		}
		whens[i] = when
	}
	caseWhenExpr := &tree.CaseExpr{
		Expr:  nil,
		Whens: whens,
		Else:  tree.NewNumValWithType(constant.MakeInt64(int64(-1)), fmt.Sprintf("%v", -1), false, tree.P_int64),
	}
	return caseWhenExpr, nil
}

// This method is used to convert the list columns partition into an case when expression,such as:
// PARTITION BY LIST (expr) (
// PARTITION p0 VALUES IN(1, 5, 9, 13, 17),
// PARTITION p1 VALUES IN (2, 6, 10, 14, 18)
// );-->
// case when expr in (1, 5, 9, 13, 17) then 0 when expr in (2, 6, 10, 14, 18) then 1 else -1 end
func buildListCaseWhenExpr(listExpr tree.Expr, defs []*tree.Partition) (*tree.CaseExpr, error) {
	whens := make([]*tree.When, len(defs))
	for i, partition := range defs {
		valuesIn := partition.Values.(*tree.ValuesIn)

		tuple := tree.NewTuple(valuesIn.ValueList)
		inExpr := tree.NewComparisonExpr(tree.IN, listExpr, tuple)

		when := &tree.When{
			Cond: inExpr,
			Val:  tree.NewNumValWithType(constant.MakeInt64(int64(i)), fmt.Sprintf("%v", i), false, tree.P_int64),
		}
		whens[i] = when
	}
	caseWhenExpr := &tree.CaseExpr{
		Expr:  nil,
		Whens: whens,
		Else:  tree.NewNumValWithType(constant.MakeInt64(int64(-1)), fmt.Sprintf("%v", -1), false, tree.P_int64),
	}
	return caseWhenExpr, nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, defs []*tree.Partition) (err error) {
	switch partitionInfo.Type {
	case plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
		err = buildHashPartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY:
		err = buildKeyPartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_RANGE, plan.PartitionType_RANGE_COLUMNS:
		err = buildRangePartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_LIST, plan.PartitionType_LIST_COLUMNS:
		err = buildListPartitionDefinitions(partitionBinder, defs, partitionInfo)
	}
	return err
}

func buildRangePartitionDefinitions(partitionBinder *PartitionBinder, defs []*tree.Partition, partitionInfo *plan.PartitionInfo) error {
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
			panic("syntax error")
		}

		for _, tableOption := range partition.Options {
			if opComment, ok := tableOption.(*tree.TableOptionComment); ok {
				partitionItem.Comment = opComment.Comment
			}
		}
		partitionInfo.Partitions[i] = partitionItem
	}
	return nil
}

func buildListPartitionDefinitions(partitionBinder *PartitionBinder, defs []*tree.Partition, partitionInfo *plan.PartitionInfo) error {
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
			panic("syntax error")
		}
		partitionInfo.Partitions[i] = partitionItem
	}
	return nil
}

func buildHashPartitionDefinitions(partitionBinder *PartitionBinder, defs []*tree.Partition, partitionInfo *plan.PartitionInfo) error {
	for i := uint64(0); i < partitionInfo.PartitionNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:   "p" + strconv.FormatUint(i, 10),
			OrdinalPosition: uint32(i + 1),
		}
		partitionInfo.Partitions[i] = partition
	}
	return nil
}

func buildKeyPartitionDefinitions(partitionBinder *PartitionBinder, defs []*tree.Partition, partitionInfo *plan.PartitionInfo) error {
	for i := uint64(0); i < partitionInfo.PartitionNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:   "p" + strconv.FormatUint(i, 10),
			OrdinalPosition: uint32(i + 1),
		}
		partitionInfo.Partitions[i] = partition
	}
	return nil
}

// The permitted data types are shown in the following list:
// All integer types
// DATE and DATETIME
// CHAR, VARCHAR, BINARY, and VARBINARY
// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
func checkColumnsPartitionType(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, columnPlanExprs []*plan.Expr) error {
	columnNames := partitionInfo.PartitionColumns
	for i, planexpr := range columnPlanExprs {
		t := types.T(planexpr.Typ.Id)
		if !types.IsInteger(t) && !types.IsString(t) && !types.IsDateRelate(t) {
			return moerr.NewSyntaxError(partitionBinder.GetContext(), "type %s of column %s not allowd in partition clause", t.String(), columnNames[i])
		}
	}
	return nil
}

// checkTableDefPartition Perform integrity constraint check on partitions of create table statement
func checkTableDefPartition(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	if err := checkPartitionFuncType(partitionBinder, tableDef, partitionInfo); err != nil {
		return err
	}
	if err := checkPartitionDefinitionConstraints(partitionBinder, partitionInfo); err != nil {
		return err
	}
	if err := checkPartitionKeysConstraints(partitionBinder, tableDef, partitionInfo); err != nil {
		return err
	}
	if partitionInfo.Type == plan.PartitionType_KEY || partitionInfo.Type == plan.PartitionType_LINEAR_KEY {
		if len(partitionInfo.Columns) == 0 {
			return handleEmptyKeyPartition(partitionBinder, tableDef, partitionInfo)
		}
	}
	return nil
}

// check partition expression type
func checkPartitionFuncType(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	if partitionInfo.Expr == nil {
		return nil
	} else {
		expr := partitionInfo.Expr
		// expr must return a nonconstant, nonrandom integer value (in other words, it should be varying but deterministic)
		// XXX Why?   I may want to use a const to force partition into one dn.
		if rule.IsConstant(expr) {
			return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition functin is not const")
		}

		t := types.T(expr.Typ.Id)
		if !types.IsInteger(t) {
			return moerr.NewSyntaxError(partitionBinder.GetContext(), "type %s not allowed in partition clause", t.String())
		}
	}
	return nil
}

// checkPartitionKeysConstraints checks the partitioning key is included in the table constraint.
func checkPartitionKeysConstraints(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	defs := tableDef.Defs

	hasPrimaryKey := false
	var primaryKey *plan.PrimaryKeyDef
	for _, def := range defs {
		if pkdef, ok := def.Def.(*plan.TableDef_DefType_Pk); ok {
			hasPrimaryKey = true
			primaryKey = pkdef.Pk
			break
		}
	}

	hasUniqueKey := false
	var uniqueKey *plan.UniqueIndexDef
	for _, def := range defs {
		if ukdef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			uniqueKey = ukdef.UIdx
			if len(uniqueKey.IndexNames) != 0 {
				hasUniqueKey = true
				break
			}
		}
	}

	if hasPrimaryKey {
		var pkcols []string
		if len(primaryKey.Names) > 0 && util.JudgeIsCompositePrimaryKeyColumn(primaryKey.Names[0]) {
			pkcols = util.SplitCompositePrimaryKeyColumnName(primaryKey.Names[0])
		} else {
			pkcols = primaryKey.Names
		}

		if partitionInfo.PartitionColumns != nil {
			if !checkUniqueKeyIncludePartKey(partitionInfo.PartitionColumns, pkcols) {
				return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
			}
		} else {
			extractCols := extractColFromExpr(partitionBinder, partitionInfo.Expr)
			if !checkUniqueKeyIncludePartKey(extractCols, pkcols) {
				return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
			}
		}
	}

	if hasUniqueKey {
		for _, field := range uniqueKey.Fields {
			uniqueKeyCols := field.Parts
			if partitionInfo.PartitionColumns != nil {
				if !checkUniqueKeyIncludePartKey(partitionInfo.PartitionColumns, uniqueKeyCols) {
					return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
				}
			} else {
				extractCols := extractColFromExpr(partitionBinder, partitionInfo.Expr)
				if !checkUniqueKeyIncludePartKey(extractCols, uniqueKeyCols) {
					return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
				}
			}
		}
	}

	if partitionInfo.Type == plan.PartitionType_KEY {
		if len(partitionInfo.Columns) == 0 && !hasUniqueKey && !hasPrimaryKey {
			return moerr.NewInvalidInput(partitionBinder.GetContext(), "Field in list of fields for partition function not found in table")
		}
	}

	return nil
}

// checkUniqueKeyIncludePartKey checks the partitioning key is included in the constraint(primary key and unique key).
func checkUniqueKeyIncludePartKey(partitionKeys []string, uqkeys []string) bool {
	for i := 0; i < len(partitionKeys); i++ {
		partitionKey := partitionKeys[i]
		if !findColumnInIndexCols(partitionKey, uqkeys) {
			return false
		}
	}
	return true
}

func findColumnInIndexCols(c string, pkcols []string) bool {
	for _, c1 := range pkcols {
		if strings.EqualFold(c, c1) {
			return true
		}
	}
	return false
}

func checkPartitionDefinitionConstraints(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	var err error
	if err = checkPartitionNameUnique(partitionBinder, partitionInfo); err != nil {
		return err
	}
	if err = checkPartitionsNumber(partitionBinder, uint64(len(partitionInfo.Partitions))); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(partitionBinder, partitionInfo); err != nil {
		return err
	}

	if len(partitionInfo.Partitions) == 0 {
		if partitionInfo.Type == plan.PartitionType_RANGE || partitionInfo.Type == plan.PartitionType_RANGE_COLUMNS {
			return moerr.NewInvalidInput(partitionBinder.GetContext(), "range partition cannot be empty")
		} else if partitionInfo.Type == plan.PartitionType_LIST || partitionInfo.Type == plan.PartitionType_LIST_COLUMNS {
			return moerr.NewInvalidInput(partitionBinder.GetContext(), "list partition cannot be empty")
		}
	}
	return err
}

// checkPartitionColumnsUnique check partition columns for duplicate columns
func checkPartitionColumnsUnique(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.PartitionColumns) <= 1 {
		return nil
	}
	var columnsMap = make(map[string]byte)
	for _, column := range partitionInfo.PartitionColumns {
		if _, ok := columnsMap[column]; ok {
			return moerr.NewSyntaxError(partitionBinder.GetContext(), "duplicate partition column %s", column)
		}
		columnsMap[column] = 1
	}
	return nil
}

// checkPartitionsNumber: check whether check partition number exceeds the limit
func checkPartitionsNumber(partitionBinder *PartitionBinder, partNum uint64) error {
	if partNum > uint64(PartitionNumberLimit) {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "too many (%d) partitions", partNum)
	}
	return nil
}

// Check whether the partition name is duplicate
func checkPartitionNameUnique(partitionBinder *PartitionBinder, pd *plan.PartitionInfo) error {
	partitions := pd.Partitions

	partNames := make(map[string]byte, len(partitions))
	for _, par := range partitions {
		if _, ok := partNames[par.PartitionName]; ok {
			return moerr.NewSyntaxError(partitionBinder.GetContext(), "duplicate partition name %s", par.PartitionName)
		}
		partNames[par.PartitionName] = 1
	}
	return nil
}

// extractColFromExpr: extract column names from partition expression
func extractColFromExpr(partitionBinder *PartitionBinder, expr *Expr) []string {
	result := make([]string, 0)
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		builder := partitionBinder.builder
		tableColName := builder.nameByColRef[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]
		split := strings.Split(tableColName, ".")
		colName := split[len(split)-1]
		result = append(result, colName)
	case *plan.Expr_F:
		tmpcols := extractColFromFunc(partitionBinder, exprImpl)
		result = append(result, tmpcols...)
	}
	return result
}

// extractColFromFunc extract column names from function expression
func extractColFromFunc(partitionBinder *PartitionBinder, funcExpr *plan.Expr_F) []string {
	result := make([]string, 0)
	for _, arg := range funcExpr.F.Args {
		tmpcols := extractColFromExpr(partitionBinder, arg)
		result = append(result, tmpcols...)
	}
	return result
}

func handleEmptyKeyPartition(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	defs := tableDef.Defs
	hasPrimaryKey := false
	hasUniqueKey := false
	var primaryKey *plan.PrimaryKeyDef
	var uniqueKey *plan.UniqueIndexDef

	for _, def := range defs {
		if pkdef, ok := def.Def.(*plan.TableDef_DefType_Pk); ok {
			hasPrimaryKey = true
			primaryKey = pkdef.Pk
			break
		}
	}

	for _, def := range defs {
		if ukdef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			uniqueKey = ukdef.UIdx
			if len(uniqueKey.IndexNames) != 0 {
				hasUniqueKey = true
				break
			}
		}
	}

	if hasPrimaryKey {
		//  Any columns used as the partitioning key must comprise part or all of the table's primary key, if the table has one.
		// Where no column name is specified as the partitioning key, the table's primary key is used, if there is one.
		var pkcols []string
		if len(primaryKey.Names) > 0 && util.JudgeIsCompositePrimaryKeyColumn(primaryKey.Names[0]) {
			pkcols = util.SplitCompositePrimaryKeyColumnName(primaryKey.Names[0])
		}

		if hasUniqueKey {
			for _, field := range uniqueKey.Fields {
				// A UNIQUE INDEX must include all columns in the table's partitioning function
				if !checkUniqueKeyIncludePartKey(pkcols, field.Parts) {
					return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
				}
			}
		}
	} else if hasUniqueKey {
		// If there is no primary key but there is a unique key, then the unique key is used for the partitioning key
		if len(uniqueKey.IndexNames) >= 2 {
			var firstUniqueKeyCols []string
			for _, field := range uniqueKey.Fields {
				firstUniqueKeyCols = field.Parts
				break
			}

			for _, field := range uniqueKey.Fields {
				if !checkUniqueKeyIncludePartKey(firstUniqueKeyCols, field.Parts) {
					return moerr.NewInvalidInput(partitionBinder.GetContext(), "partition key is not part of primary key")
				}
			}
		}
	} else {
		return moerr.NewInvalidInput(partitionBinder.GetContext(), "Field in list of fields for partition function not found in table")
	}
	return nil
}
