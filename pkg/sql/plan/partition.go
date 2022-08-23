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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"math"
	"strconv"
	"strings"
)

const (
	// Reference link https://dev.mysql.com/doc/mysql-reslimits-excerpt/5.6/en/partitioning-limitations.html
	PartitionNumberLimit = 8192
)

func buildHashPartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOp.SubPartBy != nil {
		return moerr.New(moerr.ErrPartitionSubpartition)
	}

	partitionType := partitionOp.PartBy.PType.(*tree.HashType)
	expr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
	if err != nil {
		return err
	}
	partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)

	// expr must return a nonconstant, nonrandom integer value (in other words, it should be varying but deterministic)
	if isConstant(expr) {
		return moerr.New(moerr.ErrWrongExprInPartitionFunc)
	}
	if !IsIntegerType(expr) {
		return moerr.New(moerr.ErrFieldTypeNotAllowedAsPartitionField, partitionExpr)
	}

	partitionsNum := partitionOp.PartBy.Num
	// If you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err = checkAddPartitionTooManyPartitions(partitionsNum); err != nil {
		return err
	}

	partitionInfo := &plan.PartitionInfo{
		IsSubPartition:      partitionOp.PartBy.IsSubPartition,
		Partitions:          make([]*plan.PartitionItem, partitionsNum),
		PartitionExpression: partitionExpr,
		Expr:                expr,
	}

	if partitionType.Linear {
		partitionInfo.Type = plan.PartitionType_LINEAR_HASH
	} else {
		partitionInfo.Type = plan.PartitionType_HASH
	}

	for i := uint64(0); i < partitionsNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:   "p" + strconv.FormatUint(i, 10),
			OrdinalPosition: uint32(i + 1),
		}
		partitionInfo.Partitions[i] = partition
	}

	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

func buildKeyPartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOp.SubPartBy != nil {
		return moerr.New(moerr.ErrPartitionSubpartition)
	}

	partitionType := partitionOp.PartBy.PType.(*tree.KeyType)
	partitionExpr, err := semanticCheckKeyPartition(partitionBinder, partitionType.ColumnList)
	if err != nil {
		return err
	}

	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return errors.New(errno.InvalidOptionValue, "the 'ALGORITHM' option only supports 1 or 2 values")
	}

	// if you do not include a PARTITIONS clause, the number of partitions defaults to 1.
	partitionsNum := partitionOp.PartBy.Num
	if partitionsNum <= 0 {
		partitionsNum = 1
	}

	// check partition number
	if err = checkAddPartitionTooManyPartitions(partitionsNum); err != nil {
		return err
	}

	partitionInfo := &plan.PartitionInfo{
		IsSubPartition:   partitionOp.PartBy.IsSubPartition,
		Partitions:       make([]*plan.PartitionItem, partitionsNum),
		PartitionColumns: partitionExpr,
		Algorithm:        partitionType.Algorithm,
	}

	if partitionType.Linear {
		partitionInfo.Type = plan.PartitionType_LINEAR_KEY
	} else {
		partitionInfo.Type = plan.PartitionType_KEY
	}

	for i := uint64(0); i < partitionsNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:   "p" + strconv.FormatUint(i, 10),
			OrdinalPosition: uint32(i + 1),
		}
		partitionInfo.Partitions[i] = partition
	}

	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

func buildRangePartition(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	partitionType := partitionOp.PartBy.PType.(*tree.RangeType)

	partitionNum := len(partitionOp.Partitions)
	partitionInfo := &plan.PartitionInfo{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
	}

	// RANGE Partitioning
	if partitionType.ColumnList == nil {
		partitionInfo.Type = plan.PartitionType_RANGE

		planExpr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.Expr = planExpr

		partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)
		partitionInfo.PartitionExpression = partitionExpr
	} else {
		// RANGE COLUMNS partitioning
		partitionInfo.Type = plan.PartitionType_RANGE_COLUMNS

		columnsExprs := make([]*plan.Expr, len(partitionType.ColumnList))
		partitionColumns := make([]string, len(partitionType.ColumnList))

		// RANGE COLUMNS does not accept expressions, only names of columns.
		for i, column := range partitionType.ColumnList {
			colExpr, err := partitionBinder.BindColRef(column, 0, true)
			if err != nil {
				return err
			}
			columnsExprs[i] = colExpr
			partitionColumns[i] = tree.String(column, dialect.MYSQL)
		}
		// check whether the columns partitioning type is legal
		if err := checkColumnsPartitionType(partitionBinder, partitionInfo, columnsExprs); err != nil {
			return err
		}
		partitionInfo.PartitionColumns = partitionColumns
		partitionInfo.Columns = columnsExprs
	}

	err := buildPartitionDefinitionsInfo(partitionBinder, partitionInfo, partitionOp.Partitions)
	if err != nil {
		return err
	}

	err = checkTableDefPartition(partitionBinder, tableDef, partitionInfo)
	if err != nil {
		return err
	}

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

func buildListPartitiion(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) error {
	partitionType := partitionOp.PartBy.PType.(*tree.ListType)

	partitionNum := len(partitionOp.Partitions)
	partitionInfo := &plan.PartitionInfo{
		IsSubPartition: partitionOp.PartBy.IsSubPartition,
		Partitions:     make([]*plan.PartitionItem, partitionNum),
	}

	if len(partitionType.ColumnList) == 0 {
		partitionInfo.Type = plan.PartitionType_LIST
		expr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
		if err != nil {
			return err
		}
		partitionInfo.Expr = expr

		partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)
		partitionInfo.PartitionExpression = partitionExpr
	} else {
		partitionInfo.Type = plan.PartitionType_LIST_COLUMNS

		columnsExpr := make([]*plan.Expr, len(partitionType.ColumnList))
		partitionColumns := make([]string, len(partitionType.ColumnList))
		for i, column := range partitionType.ColumnList {
			colExpr, err := partitionBinder.baseBindColRef(column, 0, true)
			if err != nil {
				return err
			}
			columnsExpr[i] = colExpr
			partitionColumns[i] = tree.String(column, dialect.MYSQL)
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

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionInfo,
		},
	})
	return nil
}

//----------------------------------------------------------------------------------------------------------------------
// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, defs []*tree.Partition) (err error) {
	switch partitionInfo.Type {
	case plan.PartitionType_RANGE:
		fallthrough
	case plan.PartitionType_RANGE_COLUMNS:
		err = buildRangePartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_LIST:
		fallthrough
	case plan.PartitionType_LIST_COLUMNS:
		err = buildListPartitionDefinitions(partitionBinder, defs, partitionInfo)
	case plan.PartitionType_HASH:
		fallthrough
	case plan.PartitionType_LINEAR_HASH:
		err = buildHashPartitionDefinitions(partitionBinder, defs, partitionInfo)
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
			inValues := make([]*plan.InValue, len(valuesIn.ValueList))

			for j, invalue := range valuesIn.ValueList {
				values := make([]*plan.Expr, len(invalue))
				for k, value := range invalue {
					expr, err := binder.BindExpr(value, 0, false)
					if err != nil {
						return err
					}
					values[k] = expr
				}
				inValues[j] = &plan.InValue{
					Value: values,
				}
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
	return nil
}

//---------------------------------------check -----------------------------------------------------------------------

func checkColumnsPartitionType(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo, columnPlanExprs []*plan.Expr) error {
	columnNames := partitionInfo.PartitionColumns
	for i, planexpr := range columnPlanExprs {
		// The permitted data types are shown in the following list:
		// All integer types
		// DATE and DATETIME
		// CHAR, VARCHAR, BINARY, and VARBINARY
		// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
		if !IsIntegerType(planexpr) && !isStringType(planexpr) && !isDateRelateType(planexpr) {
			return moerr.New(moerr.ErrFieldTypeNotAllowedAsPartitionField, columnNames[i])
		}
	}
	return nil
}

func checkTableDefPartition(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
	if err := checkPartitionDefinitionConstraints(partitionBinder, partitionInfo); err != nil {
		return err
	}
	if err := checkPartitioningKeysConstraints(partitionBinder, tableDef, partitionInfo); err != nil {
		return err
	}
	return nil
}

func checkPartitioningKeysConstraints(partitionBinder *PartitionBinder, tableDef *TableDef, partitionInfo *plan.PartitionInfo) error {
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
				return moerr.New(moerr.ErrUniqueKeyNeedAllFieldsInPf, "PRIMARY KEY")
			}
		} else {
			extractCols := extractColFromExpr(partitionBinder, partitionInfo.Expr)
			if !checkUniqueKeyIncludePartKey(extractCols, pkNames) {
				return moerr.New(moerr.ErrUniqueKeyNeedAllFieldsInPf, "PRIMARY KEY")
			}
		}
	}
	return nil
}

// checkUniqueKeyIncludePartKey checks that the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partCols []string, pkcols []string) bool {
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
	if err = checkAddPartitionTooManyPartitions(uint64(len(partitionInfo.Partitions))); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(partitionInfo); err != nil {
		return err
	}

	switch partitionInfo.Type {
	case plan.PartitionType_RANGE:
		fallthrough
	case plan.PartitionType_RANGE_COLUMNS:
		err = checkPartitionByRange(partitionBinder, partitionInfo)
	case plan.PartitionType_LIST:
		fallthrough
	case plan.PartitionType_LIST_COLUMNS:
		err = checkPartitionByList(partitionBinder, partitionInfo)
		//case plan.PartitionType_HASH:
		//	err = checkPartitionByHash(ctx, tbInfo)
	}
	return err
}

// checkPartitionByList checks validity of a "BY LIST" partition.
func checkPartitionByList(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.Partitions) == 0 {
		return moerr.New(moerr.ErrPartitionsMustBeDefined, "LIST")
	}
	// Multiple definition of same constant in list partitioning -> ErrMultipleDefConstInListPart
	return nil
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.PartitionColumns) == 0 {
		return checkRangePartitionValue(partitionBinder, partitionInfo)
	}

	return checkRangeColumnsPartitionValue(partitionBinder, partitionInfo)
}

// checkRangePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkRangePartitionValue(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.Partitions) == 0 {
		return nil
	}
	defs := partitionInfo.Partitions

	lastValue := defs[len(defs)-1].LessThan[0]
	if lastValue.Expr.(*plan.Expr_C).C.IsMaxValue {
		defs = defs[:len(defs)-1]
	}

	unsigned := isUnsignedType(partitionInfo.Expr)
	var prevRangeValue interface{}

	for i, def := range defs {
		tmpValue := def.LessThan[0]
		constValue := tmpValue.Expr.(*plan.Expr_C)
		if constValue.C.IsMaxValue {
			return moerr.New(moerr.ErrPartitionMaxvalue)
		}

		var currentRangeValue interface{}

		switch rv := constValue.C.Value.(type) {
		case *plan.Const_Ival:
			if unsigned {
				if rv.Ival < 0 {
					return moerr.New(moerr.ErrPartitionConstDomain)
				}
			}
			currentRangeValue = rv.Ival
		case *plan.Const_Uval:
			if !unsigned {
				if rv.Uval > math.MaxInt64 {
					return moerr.New(moerr.ErrPartitionConstDomain)
				}
			}
			currentRangeValue = rv.Uval
		default:
			return moerr.New(moerr.ErrValuesIsNotIntType, def.PartitionName)
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if unsigned {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return moerr.New(moerr.ErrRangeNotIncreasing)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return moerr.New(moerr.ErrRangeNotIncreasing)
			}
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

func checkRangeColumnsPartitionValue(partitionBinder *PartitionBinder, partitionInfo *plan.PartitionInfo) error {
	defs := partitionInfo.Partitions
	if len(defs) < 1 {
		return moerr.New(moerr.ErrPartitionsMustBeDefined, "RANGE")
	}

	if len(defs[0].LessThan) != len(partitionInfo.PartitionColumns) {
		return moerr.New(moerr.ErrPartitionColumnList)
	}

	var prev, curr *plan.PartitionItem
	curr = defs[0]
	for i := 0; i < len(defs); i++ {
		prev, curr = curr, defs[i]
		succ, err := checkTwoRangeColumn(partitionBinder, curr, prev, partitionInfo)
		if err != nil {
			return err
		}
		if !succ {
			return moerr.New(moerr.ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeColumn(partitionBinder *PartitionBinder, curr *plan.PartitionItem, prev *plan.PartitionItem, partitionInfo *plan.PartitionInfo) (bool, error) {
	if len(curr.LessThan) != len(partitionInfo.PartitionColumns) {
		return false, moerr.New(moerr.ErrPartitionColumnList)
	}
	for i := 0; i < len(partitionInfo.PartitionColumns); i++ {
		// handling current for MAXVALUE.
		if curr.LessThan[i].Expr.(*plan.Expr_C).C.IsMaxValue {
			return true, nil
		}

		// current is not maxvalue, and previous is maxvalue.
		if prev.LessThan[i].Expr.(*plan.Expr_C).C.IsMaxValue {
			return true, nil
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		columnType := partitionInfo.Columns[i].Typ
		err := checkLessThanExprMonotony(partitionBinder, curr.LessThan[i], prev.LessThan[i], columnType)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Check whether the column value tuple used to define the partition is strictly incremented
func checkLessThanExprMonotony(artitionBinder *PartitionBinder, curr *plan.Expr, prev *plan.Expr, typ *plan.Type) error {
	t := types.T(typ.Id)
	if t == types.T_int8 || t == types.T_int16 || t == types.T_int32 || t == types.T_int64 {
		if IsIntegerType(curr) {
			if isUnsignedType(curr) {
				constValue := curr.Expr.(*plan.Expr_C)
				rv := constValue.C.Value.(*plan.Const_Uval)
				if rv.Uval > math.MaxInt64 {
					return moerr.New(moerr.ErrPartitionConstDomain)
				}
			}
			// strictly incremented

		} else {
			return moerr.New(moerr.ErrWrongTypeColumnValue)
		}
	} else if t == types.T_uint8 || t == types.T_uint16 || t == types.T_uint32 || t == types.T_uint64 {
		if IsIntegerType(curr) {
			if isSignedType(curr) {
				constValue := curr.Expr.(*plan.Expr_C)
				rv := constValue.C.Value.(*plan.Const_Ival)
				if rv.Ival < 0 {
					return moerr.New(moerr.ErrPartitionConstDomain)
				}
			}
			// strictly incremented

		} else {
			return moerr.New(moerr.ErrWrongTypeColumnValue)
		}
	} else if t == types.T_varchar || t == types.T_char {

	} else if t == types.T_date || t == types.T_datetime {

	} else {
		panic("Columns partitioning does not support other types temporarily")
	}
	return nil
}

func checkPartitionColumnsUnique(partitionInfo *plan.PartitionInfo) error {
	if len(partitionInfo.PartitionColumns) <= 1 {
		return nil
	}
	var columnsMap = make(map[string]byte)
	for _, column := range partitionInfo.PartitionColumns {
		if _, ok := columnsMap[column]; ok {
			return moerr.New(moerr.ErrSameNamePartitionField, column)
		}
		columnsMap[column] = 1
	}
	return nil
}

func checkAddPartitionTooManyPartitions(partNum uint64) error {
	if partNum > uint64(PartitionNumberLimit) {
		return moerr.New(moerr.ErrTooManyPartitions)
	}
	return nil
}

// Check whether the partition name is duplicate
func checkPartitionNameUnique(pd *plan.PartitionInfo) error {
	partitions := pd.Partitions

	partNames := make(map[string]byte, len(partitions))
	for _, par := range partitions {
		if _, ok := partNames[par.PartitionName]; ok {
			return moerr.New(moerr.ErrSameNamePartition, par.PartitionName)
		}
		partNames[par.PartitionName] = 1
	}
	return nil
}

func semanticCheckKeyPartition(partitionBinder *PartitionBinder, columnList []*tree.UnresolvedName) ([]string, error) {
	columnsList := make([]string, len(columnList))
	for i, column := range columnList {
		partitionBinder.baseBindColRef(column, 0, true)
		_, err := partitionBinder.baseBindColRef(column, 0, true)
		if err != nil {
			return nil, moerr.New(moerr.ErrFieldNotFoundPart)
		}
		columnsList[i] = tree.String(column, dialect.MYSQL)
	}
	return columnsList, nil
}

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

func extractColFromFunc(partitionBinder *PartitionBinder, funcExpr *plan.Expr_F) []string {
	result := make([]string, 0)
	for _, arg := range funcExpr.F.Args {
		tmpcols := extractColFromExpr(partitionBinder, arg)
		result = append(result, tmpcols...)
	}
	return result
}
