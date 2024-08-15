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
	"go/constant"
	"strings"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	/*
		https://dev.mysql.com/doc/refman/8.0/en/create-table.html
		PARTITION BY
			If used, a partition_options clause begins with PARTITION BY. This clause contains the function that is used
			to determine the partition; the function returns an integer value ranging from 1 to num, where num is
			the number of partitions. (The maximum number of user-defined partitions which a table may contain is 1024;
			the number of subpartitions—discussed later in this section—is included in this maximum.)
	*/
	PartitionCountLimit = 1024

	/*
		    https://dev.mysql.com/doc/refman/8.0/en/create-table.html
			1. KEY(column_list): the column_list argument is simply a list of 1 or more table columns (maximum: 16).
			2. RANGE COLUMNS(column_list): The maximum number of columns that can be referenced in the column_list
				and value_list is 16.
			3. LIST COLUMNS(column_list): The maximum number of columns that can be used in the column_list and
				in the elements making up the value_list is 16.
	*/
	PartitionColumnsLimit = 16
)

type partitionBuilder interface {
	build(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) error

	// buildPartitionDefinitionsInfo build partition definitions info without assign partition id.
	buildPartitionDefs(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) (err error)

	// checkTableDefPartition Perform integrity constraint check on partitions of create table statement
	checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error

	// This method is used to convert different types of partition structures into plan.Expr
	buildEvalPartitionExpression(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.PartitionOption, partitionDef *plan.PartitionByDef) error
}

var _ partitionBuilder = &hashPartitionBuilder{}
var _ partitionBuilder = &keyPartitionBuilder{}
var _ partitionBuilder = &rangePartitionBuilder{}
var _ partitionBuilder = &listPartitionBuilder{}

// visit partition expression and check the correctness of partition expression
type partitionExprProcessor func(ctx context.Context, tbInfo *plan.TableDef, expr tree.Expr) error

// partitionExprChecker used to check partition expression legitimacy
type partitionExprChecker struct {
	ctx        context.Context
	processors []partitionExprProcessor
	tableInfo  *plan.TableDef
	err        error     // Error occurred during checking partition expression
	columns    []*ColDef // Columns used in partition expressions
}

func newPartitionExprChecker(ctx context.Context, tbInfo *plan.TableDef, processor ...partitionExprProcessor) *partitionExprChecker {
	p := &partitionExprChecker{
		ctx:        ctx,
		processors: processor,
		tableInfo:  tbInfo,
	}
	p.processors = append(p.processors, p.extractColumns)
	return p
}

func (p *partitionExprChecker) Enter(n tree.Expr) (node tree.Expr, skipChildren bool) {
	for _, processor := range p.processors {
		if err := processor(p.ctx, p.tableInfo, n); err != nil {
			p.err = err
			return n, true
		}
	}
	return n, false
}

func (p *partitionExprChecker) Exit(n tree.Expr) (node tree.Expr, ok bool) {
	return n, p.err == nil
}

func (p *partitionExprChecker) extractColumns(ctx context.Context, _ *plan.TableDef, expr tree.Expr) error {
	columnNameExpr, ok := expr.(*tree.UnresolvedName)
	if !ok {
		return nil
	}

	colInfo := findColumnByName(columnNameExpr.ColName(), p.tableInfo)
	if colInfo == nil {
		return moerr.NewBadFieldError(ctx, columnNameExpr.ColNameOrigin(), "partition function")
	}

	p.columns = append(p.columns, colInfo)
	return nil
}

// checkPartitionExprValid checks partition expression function validly.
func checkPartitionExprValid(ctx context.Context, tblInfo *plan.TableDef, expr tree.Expr) error {
	if expr == nil {
		return nil
	}
	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed)
	expr.Accept(exprChecker)
	if exprChecker.err != nil {
		return exprChecker.err
	}
	if len(exprChecker.columns) == 0 {
		return moerr.NewWrongExprInPartitionFunc(ctx)
	}
	return nil
}

// buildPartitionExpr enables the use of multiple columns in partitioning keys
func buildPartitionExpr(ctx context.Context, tblInfo *plan.TableDef, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, pExpr tree.Expr) error {
	if err := checkPartitionExprValid(ctx, tblInfo, pExpr); err != nil {
		return err
	}
	planExpr, err := partitionBinder.BindExpr(pExpr, 0, true)
	if err != nil {
		return err
	}
	//TODO: format partition expression
	//fmtCtx := tree.NewFmtCtxWithFlag(dialect.MYSQL, tree.RestoreNameBackQuotes)
	//pExpr.Format(fmtCtx)
	//exprFmtStr := fmtCtx.ToString()

	// Temporary operation
	partitionDef.PartitionExpr = &plan.PartitionExpr{
		Expr:    planExpr,
		ExprStr: tree.String(pExpr, dialect.MYSQL),
		//ExprFmtStr: exprFmtStr,
	}
	return nil
}

// getValidPartitionCount checks the subpartition and adjust the number of the partition
func getValidPartitionCount(ctx context.Context, needPartitionDefs bool, partitionSyntaxDef *tree.PartitionOption) (uint64, error) {
	var err error
	//step 1 : reject subpartition
	if partitionSyntaxDef.SubPartBy != nil {
		return 0, moerr.NewInvalidInput(ctx, "subpartition is unsupported")
	}

	if needPartitionDefs && len(partitionSyntaxDef.Partitions) == 0 {
		if _, ok := partitionSyntaxDef.PartBy.PType.(*tree.ListType); ok {
			return 0, moerr.NewPartitionsMustBeDefined(ctx, "LIST")
		} else {
			return 0, moerr.NewInvalidInput(ctx, "each partition must be defined")
		}
	}

	//step 2: verify the partition number [1,1024]
	partitionCount := partitionSyntaxDef.PartBy.Num
	/*
		"partitionCount = 0" only occurs when the PARTITIONS clause is missed.
	*/
	if partitionCount <= 0 {
		if len(partitionSyntaxDef.Partitions) == 0 {
			//if there is no partition definition, the default number for the partitionCount is 1.
			partitionCount = 1
		} else {
			//if there are at lease one partition definitions, the default number for the partitionsNums
			//is designated as the number of the partition definitions.
			partitionCount = uint64(len(partitionSyntaxDef.Partitions))
		}
	} else if len(partitionSyntaxDef.Partitions) != 0 && partitionCount != uint64(len(partitionSyntaxDef.Partitions)) {
		//if partition definitions exists in the syntax, but the count of it is different from
		//the one in PARTITIONS clause, it is wrong.
		return 0, moerr.NewInvalidInput(ctx, "Wrong number of partitions defined")
	}
	// check partition number
	if err = checkPartitionCount(ctx, int(partitionCount)); err != nil {
		return 0, err
	}
	return partitionCount, err
}

// buildPartitionColumns enables the use of multiple columns in partitioning keys
func buildPartitionColumns(ctx context.Context, tableDef *TableDef, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, columnList []*tree.UnresolvedName) error {
	var err error
	partitionDef.PartitionColumns = &plan.PartitionColumns{
		Columns:          make([]*plan.Expr, len(columnList)),
		PartitionColumns: make([]string, len(columnList)),
	}

	// 1. First, check if the partition column is legal,
	for i, column := range columnList {
		partitionDef.PartitionColumns.PartitionColumns[i] = tree.String(column, dialect.MYSQL)
	}
	if err = checkColumnsPartitionType(ctx, tableDef, partitionDef); err != nil {
		return err
	}

	// 2. then construct the expression for the partition column
	for i, column := range columnList {
		partitionDef.PartitionColumns.Columns[i], err = partitionBinder.BindColRef(column, 0, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkColumnsPartitionType Check if the type of the partition column is correct
func checkColumnsPartitionType(ctx context.Context, tbInfo *TableDef, pi *plan.PartitionByDef) error {
	for _, colName := range pi.PartitionColumns.PartitionColumns {
		colInfo := findColumnByName(colName, tbInfo)
		if colInfo == nil {
			return moerr.NewFieldNotFoundPart(ctx)
		}

		t := types.T(colInfo.Typ.Id)
		if pi.Type == plan.PartitionType_KEY || pi.Type == plan.PartitionType_LINEAR_KEY {
			// When partitioning by [LINEAR] KEY, it is possible to use columns of any valid MySQL data type other than TEXT or BLOB
			// as partitioning keys, because MySQL's internal key-hashing functions produce the correct data type from these types.
			// See https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
			if t == types.T_blob || t == types.T_text || t == types.T_json || t == types.T_datalink {
				return moerr.NewBlobFieldInPartFunc(ctx)
			}
		} else {
			// Both `RANGE COLUMNS` partitioning and `LIST COLUMNS` partitioning support the use of non-integer columns for defining value ranges or list members. The permitted data types are shown in the following list:
			//1. All integer types: TINYINT, SMALLINT, MEDIUMINT, INT (INTEGER), and BIGINT. (This is the same as with partitioning by RANGE and LIST.)
			//  Other numeric data types (such as DECIMAL or FLOAT) are not supported as partitioning columns.
			//2. DATE and DATETIME.
			//	Columns using other data types relating to dates or times are not supported as partitioning columns.
			//3. The following string types: CHAR, VARCHAR, BINARY, and VARBINARY.
			//	TEXT and BLOB columns are not supported as partitioning columns.
			// See https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns.html
			if t == types.T_float32 || t == types.T_float64 || t == types.T_decimal64 || t == types.T_decimal128 ||
				t == types.T_timestamp || t == types.T_blob || t == types.T_text || t == types.T_json || t == types.T_enum || t == types.T_datalink {
				return moerr.NewFieldTypeNotAllowedAsPartitionField(ctx, colName)
			}
		}
	}
	return nil
}

// buildPartitionDefs constructs the partitions
func buildPartitionDefs(ctx context.Context,
	partitionDef *plan.PartitionByDef, syntaxDefs []*tree.Partition) error {
	if len(syntaxDefs) != 0 && len(syntaxDefs) != int(partitionDef.PartitionNum) {
		return moerr.NewInvalidInput(ctx, "Wrong number of partitions defined")
	}
	dedup := make(map[string]bool)
	if len(syntaxDefs) == 0 {
		//complement partition defs missing in syntax
		for i := 0; i < int(partitionDef.PartitionNum); i++ {
			name := fmt.Sprintf("p%d", i)
			if _, ok := dedup[name]; ok {
				//return moerr.NewInvalidInput(ctx, "duplicate partition name %s", name)
				return moerr.NewSameNamePartition(ctx, name)
			}
			dedup[name] = true
			pi := &plan.PartitionItem{
				PartitionName:   name,
				OrdinalPosition: uint32(i + 1),
			}
			partitionDef.Partitions = append(partitionDef.Partitions, pi)
		}
	} else {
		//process defs in syntax
		for i := 0; i < len(syntaxDefs); i++ {
			name := string(syntaxDefs[i].Name)
			if _, ok := dedup[name]; ok {
				//return moerr.NewInvalidInput(ctx, "duplicate partition name %s", name)
				return moerr.NewSameNamePartition(ctx, name)
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
			partitionDef.Partitions = append(partitionDef.Partitions, pi)
		}
	}

	return nil
}

func checkPartitionIntegrity(ctx context.Context, partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	if err := checkPartitionKeys(ctx, partitionBinder.builder.nameByColRef, tableDef, partitionDef); err != nil {
		return err
	}
	if err := checkPartitionExprType(ctx, partitionBinder, tableDef, partitionDef); err != nil {
		return err
	}
	if err := checkPartitionDefines(ctx, partitionBinder, partitionDef, tableDef); err != nil {
		return err
	}
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
	hashFuncName := tree.NewUnresolvedColName("hash_value")
	hashfuncExpr := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(hashFuncName),
		Exprs: exprs,
	}

	absFuncName := tree.NewUnresolvedColName("abs")
	absFuncExpr := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(absFuncName),
		Exprs: tree.Exprs{hashfuncExpr},
	}

	numstr := fmt.Sprintf("%v", partNum)
	divExpr := tree.NewNumValWithType(constant.MakeInt64(partNum), numstr, false, tree.P_int64)
	modOpExpr := tree.NewBinaryExpr(tree.MOD, absFuncExpr, divExpr)
	return modOpExpr
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

// checkPartitionExprType checks partition function return type.
func checkPartitionExprType(ctx context.Context, _ *PartitionBinder, _ *TableDef, partitionDef *plan.PartitionByDef) error {
	if partitionDef.PartitionExpr != nil && partitionDef.PartitionExpr.Expr != nil {
		expr := partitionDef.PartitionExpr.Expr
		t := types.T(expr.Typ.Id)
		if partitionDef.Type == plan.PartitionType_HASH || partitionDef.Type == plan.PartitionType_LINEAR_HASH {
			if !t.IsInteger() {
				if _, ok := expr.Expr.(*plan.Expr_Col); ok {
					return moerr.NewFieldTypeNotAllowedAsPartitionField(ctx, partitionDef.PartitionExpr.ExprStr)
				} else {
					return moerr.NewPartitionFuncNotAllowed(ctx, "PARTITION")
				}
			}
		}

		if partitionDef.Type == plan.PartitionType_RANGE && !t.IsInteger() {
			return moerr.NewFieldTypeNotAllowedAsPartitionField(ctx, partitionDef.PartitionExpr.ExprStr)
		}

		if partitionDef.Type == plan.PartitionType_LIST {
			if !t.IsInteger() {
				if _, ok := expr.Expr.(*plan.Expr_Col); ok {
					return moerr.NewFieldTypeNotAllowedAsPartitionField(ctx, partitionDef.PartitionExpr.ExprStr)
				} else {
					return moerr.NewPartitionFuncNotAllowed(ctx, "PARTITION")
				}
			}
		}
	}
	return nil
}

// stringSliceToMap converts the string slice to the string map
// return true -- has duplicate names
func stringSliceToMap(stringSlice []string, stringMap map[string]int) (bool, string) {
	for _, s := range stringSlice {
		if _, ok := stringMap[s]; ok {
			return true, s
		}
		stringMap[s] = 0
	}
	return false, ""
}

func stringSliceToMapForIndexParts(stringSlice []string, stringMap map[string]int) (bool, string) {
	for _, s := range stringSlice {
		s = catalog2.ResolveAlias(s)
		if _, ok := stringMap[s]; ok {
			return true, s
		}
		stringMap[s] = 0
	}
	return false, ""
}

// checkPartitionKeys checks the partitioning key is included in the table constraint.
func checkPartitionKeys(ctx context.Context, nameByColRef map[[2]int32]string,
	tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	partitionKeys := make(map[string]int)
	if partitionDef.PartitionColumns != nil {
		if dup, dupName := stringSliceToMap(partitionDef.PartitionColumns.PartitionColumns, partitionKeys); dup {
			//return moerr.NewInvalidInput(ctx, "duplicate name %s", dupName)
			return moerr.NewSameNamePartition(ctx, dupName)
		}
	} else if partitionDef.PartitionExpr.Expr != nil {
		extractColFromExpr(nameByColRef, partitionDef.PartitionExpr.Expr, partitionKeys)
	} else {
		return moerr.NewInvalidInput(ctx, "both COLUMNS and EXPR in PARTITION BY are invalid")
	}

	//do nothing
	if len(partitionKeys) == 0 {
		return nil
	}

	if tableDef.Pkey != nil && !onlyHasHiddenPrimaryKey(tableDef) {
		pKeys := make(map[string]int)
		if dup, dupName := stringSliceToMap(tableDef.Pkey.Names, pKeys); dup {
			//return moerr.NewInvalidInput(ctx, "duplicate name %s", dupName)
			return moerr.NewSameNamePartition(ctx, dupName)
		}
		if !checkUniqueKeyIncludePartKey(partitionKeys, pKeys) {
			//return moerr.NewInvalidInput(ctx, "partition key is not part of primary key")
			return moerr.NewUniqueKeyNeedAllFieldsInPf(ctx, "PRIMARY KEY")
		}
	}

	if tableDef.Indexes != nil {
		for _, indexDef := range tableDef.Indexes {
			if indexDef.Unique {
				uniqueKeys := make(map[string]int)
				if dup, dupName := stringSliceToMapForIndexParts(indexDef.Parts, uniqueKeys); dup {
					//return moerr.NewInvalidInput(ctx, "duplicate name %s", dupName)
					return moerr.NewSameNamePartition(ctx, dupName)
				}
				if !checkUniqueKeyIncludePartKey(partitionKeys, uniqueKeys) {
					//return moerr.NewInvalidInput(ctx, "partition key is not part of unique key")
					return moerr.NewUniqueKeyNeedAllFieldsInPf(ctx, "PRIMARY KEY")
				}
			}
		}
	}

	return nil
}

// checkUniqueKeyIncludePartKey checks the partitioning key is included in the constraint(primary key and unique key).
func checkUniqueKeyIncludePartKey(partitionKeys map[string]int, uqkeys map[string]int) bool {
	for key := range partitionKeys {
		if !findColumnInIndexCols(key, uqkeys) {
			return false
		}
	}
	return true
}

func findColumnInIndexCols(c string, pkcols map[string]int) bool {
	for c1 := range pkcols {
		if strings.EqualFold(c, c1) {
			return true
		}
	}
	return false
}

/*
checkPartitionDefines Check partition definition

	1.check partition name unique or not
	2.check partition count limitation
	3.check partition columns count limitation
	4.check partition column name uinque
*/
func checkPartitionDefines(ctx context.Context, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tableDef *TableDef) error {
	var err error
	if err = checkPartitionNameUnique(ctx, partitionDef); err != nil {
		return err
	}

	if err = checkPartitionCount(ctx, len(partitionDef.Partitions)); err != nil {
		return err
	}

	if err = checkPartitionColumnsCount(ctx, partitionDef); err != nil {
		return err
	}

	if err = checkPartitionColumnsUnique(ctx, partitionDef); err != nil {
		return err
	}

	if len(partitionDef.Partitions) == 0 {
		if partitionDef.Type == plan.PartitionType_RANGE || partitionDef.Type == plan.PartitionType_RANGE_COLUMNS {
			return moerr.NewPartitionsMustBeDefined(ctx, "RANGE")
		} else if partitionDef.Type == plan.PartitionType_LIST || partitionDef.Type == plan.PartitionType_LIST_COLUMNS {
			return moerr.NewPartitionsMustBeDefined(ctx, "LIST")
		}
	}

	switch partitionDef.Type {
	case plan.PartitionType_RANGE, plan.PartitionType_RANGE_COLUMNS:
		err = checkPartitionByRange(partitionBinder, partitionDef, tableDef)
	case plan.PartitionType_LIST, plan.PartitionType_LIST_COLUMNS:
		err = checkPartitionByList(partitionBinder, partitionDef, tableDef)
	}
	return err
}

// checkPartitionColumnsUnique check duplicate partition columns
func checkPartitionColumnsUnique(ctx context.Context, pi *plan.PartitionByDef) error {
	if pi.PartitionColumns != nil {
		if len(pi.PartitionColumns.PartitionColumns) <= 1 {
			return nil
		}
		var columnsMap = make(map[string]struct{})
		for _, column := range pi.PartitionColumns.PartitionColumns {
			if _, ok := columnsMap[column]; ok {
				return moerr.NewSameNamePartitionField(ctx, column)
			}
			columnsMap[column] = struct{}{}
		}
	}
	return nil
}

// checkPartitionColumns Check if the number of partition columns exceeds the limit
func checkPartitionColumnsCount(ctx context.Context, pi *plan.PartitionByDef) error {
	if pi.PartitionColumns != nil && len(pi.PartitionColumns.PartitionColumns) > PartitionColumnsLimit {
		return moerr.NewErrTooManyPartitionFuncFields(ctx, "list of partition fields")
	}
	return nil
}

// checkPartitionCount: check whether check partition number exceeds the limit
func checkPartitionCount(ctx context.Context, partNum int) error {
	if partNum > PartitionCountLimit {
		return moerr.NewErrTooManyPartitions(ctx)
	}
	return nil
}

// Check whether the partition name is duplicate
func checkPartitionNameUnique(ctx context.Context, pi *plan.PartitionByDef) error {
	partitions := pi.Partitions
	partNames := make(map[string]struct{}, len(partitions))
	for _, partitionItem := range partitions {
		if _, ok := partNames[partitionItem.PartitionName]; ok {
			return moerr.NewSameNamePartition(ctx, partitionItem.PartitionName)
		}
		partNames[partitionItem.PartitionName] = struct{}{}
	}
	return nil
}

// extractColFromExpr extracts column names from partition expression
func extractColFromExpr(nameByColRef map[[2]int32]string, expr *Expr, result map[string]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRef := nameByColRef[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]
		split := strings.Split(colRef, ".")
		colName := split[len(split)-1]
		result[colName] = 0
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			extractColFromExpr(nameByColRef, arg, result)
		}
	}
}

func handleEmptyKeyPartition(partitionBinder *PartitionBinder, tableDef *TableDef, partitionDef *plan.PartitionByDef) error {
	hasValidPrimaryKey := false
	hasUniqueKey := false
	var primaryKey *plan.PrimaryKeyDef

	if tableDef.Pkey != nil && !onlyHasHiddenPrimaryKey(tableDef) {
		hasValidPrimaryKey = true
		primaryKey = tableDef.Pkey
	}

	uniqueIndexCount := 0
	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			if indexdef.Unique {
				hasUniqueKey = true
				uniqueIndexCount++
			}
		}
	}

	if hasValidPrimaryKey {
		//  Any columns used as the partitioning key must comprise part or all of the table's primary key, if the table has one.
		// Where no column name is specified as the partitioning key, the table's primary key is used, if there is one.
		pkcols := make(map[string]int)
		stringSliceToMap(primaryKey.Names, pkcols)
		if hasUniqueKey {
			for _, indexdef := range tableDef.Indexes {
				if indexdef.Unique {
					// A UNIQUE INDEX must include all columns in the table's partitioning function
					uniqueKeys := make(map[string]int)
					stringSliceToMapForIndexParts(indexdef.Parts, uniqueKeys)
					if !checkUniqueKeyIncludePartKey(pkcols, uniqueKeys) {
						return moerr.NewUniqueKeyNeedAllFieldsInPf(partitionBinder.GetContext(), "PRIMARY KEY")
					}
				} else {
					continue
				}
			}
		}
	} else if hasUniqueKey {
		// If there is no primary key but there is a unique key, then the unique key is used for the partitioning key
		if uniqueIndexCount >= 2 {
			firstUniqueKeyCols := make(map[string]int)
			for _, indexdef := range tableDef.Indexes {
				stringSliceToMapForIndexParts(indexdef.Parts, firstUniqueKeyCols)
				break
			}

			for _, indexdef := range tableDef.Indexes {
				uniqueKeys := make(map[string]int)
				stringSliceToMapForIndexParts(indexdef.Parts, uniqueKeys)
				if !checkUniqueKeyIncludePartKey(firstUniqueKeyCols, uniqueKeys) {
					return moerr.NewUniqueKeyNeedAllFieldsInPf(partitionBinder.GetContext(), "PRIMARY KEY")
				}
			}
		}
	} else {
		return moerr.NewFieldNotFoundPart(partitionBinder.GetContext())
	}
	return nil
}
