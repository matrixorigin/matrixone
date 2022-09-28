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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strconv"
	"strings"

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
func buildHashPartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOp.SubPartBy != nil {
		return moerr.NewInvalidInput("no subpartition")
	}
	partitionsNum := partitionOp.PartBy.Num
	// If you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkPartitionsNumber(partitionsNum); err != nil {
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

	err := buildPartitionExpr(partitionBinder, partitionInfo, partitionType.Expr)
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

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

// buildKeyPartition handle KEY Partitioning
func buildKeyPartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOp.SubPartBy != nil {
		return moerr.NewInvalidInput("no subpartition")
	}

	// if you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	partitionsNum := partitionOp.PartBy.Num
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkPartitionsNumber(partitionsNum); err != nil {
		return err
	}

	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return moerr.NewInvalidInput("the 'ALGORITHM' option has too many values")
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

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

// buildRangePartition handle Range Partitioning and Range columns partitioning
func buildRangePartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	partitionType := partitionOp.PartBy.PType.(*tree.RangeType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError("build range partition")
	}

	partitionInfo := &plan.PartitionInfo{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
		PartitionNum:   uint64(partitionNum),
	}

	// RANGE Partitioning
	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_RANGE
		err := buildPartitionExpr(partitionBinder, partitionInfo, partitionType.Expr)
		if err != nil {
			return err
		}
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

	partitionInfo.PartitionMsg = tree.String(partitionOp, dialect.MYSQL)
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

// buildListPartitiion handle List Partitioning and List columns partitioning
func buildListPartitiion(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	partitionType := partitionOp.PartBy.PType.(*tree.ListType)

	partitionNum := len(partitionOp.Partitions)
	if partitionOp.PartBy.Num != 0 && uint64(partitionNum) != partitionOp.PartBy.Num {
		return moerr.NewParseError("build list partition")
	}

	partitionInfo := &plan.PartitionInfo{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
		PartitionNum:   uint64(partitionNum),
	}

	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_LIST
		err := buildPartitionExpr(partitionBinder, partitionInfo, partitionType.Expr)
		if err != nil {
			return err
		}
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
			return moerr.NewParseError("build partition columns")
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

// buildPartitionExpr expr partitioning is an expression using one or more table columns.
func buildPartitionExpr(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, expr tree.Expr) error {
	planExpr, err := partitionBinder.BindExpr(expr, 0, true)
	if err != nil {
		return err
	}
	partitionInfo.Expr = planExpr

	partitionExpr := tree.String(expr, dialect.MYSQL)
	partitionInfo.PartitionExpression = partitionExpr
	return nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, defs []*tree.Partition) (err error) {
	switch partitionInfo.Type {
	case plan.PartitionType_HASH:
		fallthrough
	case plan.PartitionType_LINEAR_HASH:
		err = buildHashPartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_KEY:
		fallthrough
	case plan.PartitionType_LINEAR_KEY:
		err = buildKeyPartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_RANGE:
		fallthrough
	case plan.PartitionType_RANGE_COLUMNS:
		err = buildRangePartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_LIST:
		fallthrough
	case plan.PartitionType_LIST_COLUMNS:
		err = buildListPartitionDefinitions(partitionBinder, defs, partitionInfo)
	}

	if err != nil {
		return err
	}
	return nil
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
			return moerr.NewSyntaxError("type %s of column %s not allowd in partition clause", t.String(), columnNames[i])
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
		if isConstant(expr) {
			return moerr.NewInvalidInput("partition functin is not const")
		}

		t := types.T(expr.Typ.Id)
		if !types.IsInteger(t) {
			return moerr.NewSyntaxError("type %s not allowed in partition clause", t.String())
		}
	}
	return nil
}

// checkPartitionKeysConstraints checks the partitioning key is included in the table constraint.
func checkPartitionKeysConstraints(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	defs := tableDef.Defs
	hasPrimaryKey := false
	var pkNames []string
	for _, def := range defs {
		if pkdef, ok := def.Def.(*plan.TableDef_DefType_Pk); ok {
			hasPrimaryKey = true
			pkNames = pkdef.Pk.Names
		}
	}

	if hasPrimaryKey {
		if partitionInfo.PartitionColumns != nil {
			if !checkUniqueKeyIncludePartKey(partitionInfo.PartitionColumns, pkNames) {
				return moerr.NewInvalidInput("partition key is not part of primary key")
			}
		} else {
			extractCols := extractColFromExpr(partitionBinder, partitionInfo.Expr)
			if !checkUniqueKeyIncludePartKey(extractCols, pkNames) {
				return moerr.NewInvalidInput("partition key is not part of primary key")
			}
		}
	}
	return nil
}

// checkUniqueKeyIncludePartKey checks the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partCols []string, pkcols []string) bool {
	if len(pkcols) > 0 && util.JudgeIsCompositePrimaryKeyColumn(pkcols[0]) {
		pkcols = util.SplitCompositePrimaryKeyColumnName(pkcols[0])
	}
	for i := 0; i < len(partCols); i++ {
		partCol := partCols[i]
		if !findColumnInIndexCols(partCol, pkcols) {
			// Partition column is not found in the index columns.
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
	if err = checkPartitionNameUnique(partitionInfo); err != nil {
		return err
	}
	if err = checkPartitionsNumber(uint64(len(partitionInfo.Partitions))); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(partitionInfo); err != nil {
		return err
	}

	if len(partitionInfo.Partitions) == 0 {
		if partitionInfo.Type == plan.PartitionType_RANGE || partitionInfo.Type == plan.PartitionType_RANGE_COLUMNS {
			return moerr.NewInvalidInput("range partition cannot be empty")
		} else if partitionInfo.Type == plan.PartitionType_LIST || partitionInfo.Type == plan.PartitionType_LIST_COLUMNS {
			return moerr.NewInvalidInput("list partition cannot be empty")
		}
	}
	return err
}

// checkPartitionColumnsUnique check partition columns for duplicate columns
func checkPartitionColumnsUnique(partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.PartitionColumns) <= 1 {
		return nil
	}
	var columnsMap = make(map[string]byte)
	for _, column := range partitionInfo.PartitionColumns {
		if _, ok := columnsMap[column]; ok {
			return moerr.NewSyntaxError("duplicate partition column %s", column)
		}
		columnsMap[column] = 1
	}
	return nil
}

// checkPartitionsNumber: check whether check partition number exceeds the limit
func checkPartitionsNumber(partNum uint64) error {
	if partNum > uint64(PartitionNumberLimit) {
		return moerr.NewInvalidInput("too many (%d) partitions", partNum)
	}
	return nil
}

// Check whether the partition name is duplicate
func checkPartitionNameUnique(pd *plan.PartitionInfo) error {
	partitions := pd.Partitions

	partNames := make(map[string]byte, len(partitions))
	for _, par := range partitions {
		if _, ok := partNames[par.PartitionName]; ok {
			return moerr.NewSyntaxError("duplicate partition name %s", par.PartitionName)
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
