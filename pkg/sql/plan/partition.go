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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
)

const (
	// Reference link https://dev.mysql.com/doc/mysql-reslimits-excerpt/5.6/en/partitioning-limitations.html
	PartitionNumberLimit = 8192
)

func buildHashPartition(partitionBinder *PartitionBinder, partitionOption *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOption.SubPartBy != nil {
		return moerr.NewError(moerr.ERROR_SUBPARTITION, "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")
	}

	partitionType := partitionOption.PartBy.PType.(*tree.HashType)

	partitionBinder.BindExpr(partitionType.Expr, 0, true)
	partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)

	partitionsNum := partitionOption.PartBy.Num
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkAddPartitionTooManyPartitions(partitionsNum); err != nil {
		return err
	}

	partitionDef := &plan.PartitionDef{
		IsSubPartition:      partitionOption.PartBy.IsSubPartition,
		Partitions:          make([]*plan.PartitionItem, partitionsNum),
		PartitionExpression: partitionExpr,
	}

	if partitionType.Linear {
		partitionDef.Typ = plan.PartitionDef_LINEAR_HASH
	} else {
		partitionDef.Typ = plan.PartitionDef_HASH
	}

	for i := uint64(0); i < partitionsNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:       "p" + strconv.FormatUint(i, 10),
			PartitionExpression: partitionExpr,
			OrdinalPosition:     uint32(i + 1),
		}
		partitionDef.Partitions[i] = partition
	}

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionDef,
		},
	})

	return nil
}

func buildKeyPartition(partitionBinder *PartitionBinder, partitionOption *tree.PartitionOption, tableDef *TableDef) error {
	if partitionOption.SubPartBy != nil {
		return moerr.NewError(moerr.ERROR_SUBPARTITION, "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")
	}

	partitionType := partitionOption.PartBy.PType.(*tree.KeyType)

	partitionExpr, err := semanticCheckKeyPartition(partitionBinder, partitionType.ColumnList)
	if err != nil {
		return err
	}

	// check the algorithm option
	if partitionType.Algorithm != 1 && partitionType.Algorithm != 2 {
		return errors.New(errno.InvalidOptionValue, "the 'ALGORITHM' option only supports 1 or 2 values")
	}

	partitionsNum := partitionOption.PartBy.Num
	if partitionsNum <= 0 {
		partitionsNum = 1
	}
	// check partition number
	if err := checkAddPartitionTooManyPartitions(partitionsNum); err != nil {
		return err
	}

	partitionDef := &plan.PartitionDef{
		IsSubPartition:   partitionOption.PartBy.IsSubPartition,
		Partitions:       make([]*plan.PartitionItem, partitionsNum),
		PartitionColumns: partitionExpr,
		Algorithm:        partitionType.Algorithm,
	}

	if partitionType.Linear {
		partitionDef.Typ = plan.PartitionDef_LINEAR_KEY
	} else {
		partitionDef.Typ = plan.PartitionDef_KEY
	}

	for i := uint64(0); i < partitionsNum; i++ {
		partition := &plan.PartitionItem{
			PartitionName:    "p" + strconv.FormatUint(i, 10),
			OrdinalPosition:  uint32(i + 1),
			PartitionColumns: partitionExpr,
			Algorithm:        partitionType.Algorithm,
		}
		partitionDef.Partitions[i] = partition
	}

	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Partition{
			Partition: partitionDef,
		},
	})
	return nil
}

func buildRangePartition(partitionBinder *PartitionBinder, partitionOption *tree.PartitionOption, tableDef *TableDef) error {
	/*
		partitionType := partitionOption.PartBy.PType.(*tree.RangeType)

		partitionNum := len(partitionOption.Partitions)
		partitionDef := &plan.PartitionDef{
			IsSubPartition: partitionOption.PartBy.IsSubPartition,
			Partitions:     make([]*plan.PartitionItem, partitionNum),
		}

		if partitionType.ColumnList == nil {
			partitionDef.Typ = plan.PartitionDef_RANGE

			expr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
			if err != nil {
				return err
			}
			fmt.Println(expr)
			partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)

			// VALUES LESS THAN value must be strictly increasing for each partition
			for i, partition := range partitionOption.Partitions {
				partitionItem := &plan.PartitionItem{
					PartitionName:       string(partition.Name),
					OrdinalPosition:     uint32(i + 1),
					PartitionExpression: partitionExpr,
				}

				if valuesLessThan, ok := partition.Values.(*tree.ValuesLessThan); ok {
					if len(valuesLessThan.ValueList) != 1 {
						panic("syntax error")
					}
					partitionItem.Description = tree.String(valuesLessThan, dialect.MYSQL)
				} else {
					panic("syntax error")
				}
				partitionDef.Partitions[i] = partitionItem
			}
			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Partition{
					Partition: partitionDef,
				},
			})
		} else {
			partitionDef.Typ = plan.PartitionDef_RANGE_COLUMNS
			colListExprs := make([]*plan.Expr, len(partitionType.ColumnList))
			var partitionExpr string
			for i, column := range partitionType.ColumnList {
				colExpr, err := partitionBinder.BindColRef(column, 0, true)
				if err != nil {
					return err
				}
				colListExprs[i] = colExpr
				partitionExpr += tree.String(column, dialect.MYSQL)
			}

			// VALUES LESS THAN value must be strictly increasing for each partition
			for i, partition := range partitionOption.Partitions {
				partitionItem := &plan.PartitionItem{
					PartitionName:       string(partition.Name),
					OrdinalPosition:     uint32(i + 1),
					PartitionExpression: partitionExpr,
				}

				if valuesLessThan, ok := partition.Values.(*tree.ValuesLessThan); ok {
					if len(valuesLessThan.ValueList) != len(colListExprs) {
						panic("syntax error")
					}

					for _, value := range valuesLessThan.ValueList {
						partitionItem.Description += tree.String(value, dialect.MYSQL)
					}
				} else {
					panic("syntax error")
				}
				partitionDef.Partitions[i] = partitionItem
			}

			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Partition{
					Partition: partitionDef,
				},
			})
		}
	*/
	return nil
}

func buildListPartitiion(partitionBinder *PartitionBinder, partitionOption *tree.PartitionOption, tableDef *TableDef) error {
	/*
		partitionType := partitionOption.PartBy.PType.(*tree.ListType)

		partitionNum := len(partitionOption.Partitions)
		partitionDef := &plan.PartitionDef{
			IsSubPartition: partitionOption.PartBy.IsSubPartition,
			Partitions:     make([]*plan.PartitionItem, partitionNum),
		}

		if partitionType.ColumnList != nil {
			partitionDef.Typ = plan.PartitionDef_LIST

			expr, err := partitionBinder.BindExpr(partitionType.Expr, 0, true)
			if err != nil {
				return err
			}
			fmt.Println(expr)
			partitionExpr := tree.String(partitionType.Expr, dialect.MYSQL)

			// VALUES LESS THAN value must be strictly increasing for each partition
			for i, partition := range partitionOption.Partitions {
				partitionItem := &plan.PartitionItem{
					PartitionName:       string(partition.Name),
					OrdinalPosition:     uint32(i + 1),
					PartitionExpression: partitionExpr,
				}

				if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
					partitionItem.Description = tree.String(valuesIn, dialect.MYSQL)
				} else {
					panic("syntax error")
				}
				partitionDef.Partitions[i] = partitionItem
			}
			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Partition{
					Partition: partitionDef,
				},
			})
		} else {
			partitionDef.Typ = plan.PartitionDef_LIST_COLUMNS
			colListExprs := make([]*plan.Expr, len(partitionType.ColumnList))
			var partitionExpr string
			for i, column := range partitionType.ColumnList {
				colExpr, err := partitionBinder.baseBindColRef(column, 0, true)
				if err != nil {
					return err
				}
				colListExprs[i] = colExpr
				partitionExpr += tree.String(column, dialect.MYSQL)
			}

			// VALUES LESS THAN value must be strictly increasing for each partition
			for i, partition := range partitionOption.Partitions {
				partitionItem := &plan.PartitionItem{
					PartitionName:       string(partition.Name),
					OrdinalPosition:     uint32(i + 1),
					PartitionExpression: partitionExpr,
				}
				if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
					if len(valuesIn.ValueList) != len(colListExprs) {
						panic("syntax error")
					}

					for _, value := range valuesIn.ValueList {
						partitionItem.Description += tree.String(value, dialect.MYSQL)
					}
				} else {
					panic("syntax error")
				}
				partitionDef.Partitions[i] = partitionItem
			}
			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Partition{
					Partition: partitionDef,
				},
			})
		}*/
	return nil
}

func semanticCheckKeyPartition(partitionBinder *PartitionBinder, columnList []*tree.UnresolvedName) ([]string, error) {
	columnsList := make([]string, len(columnList))
	for i, column := range columnList {
		partitionBinder.baseBindColRef(column, 0, true)
		_, err := partitionBinder.baseBindColRef(column, 0, true)
		if err != nil {
			return nil, moerr.NewError(moerr.ERROR_FIELD_NOT_FOUND_PART, "Field in list of fields for partition function not found in table")
		}
		columnsList[i] = tree.String(column, dialect.MYSQL)
	}
	return columnsList, nil
}

func checkAddPartitionTooManyPartitions(num uint64) error {
	if num > uint64(PartitionNumberLimit) {
		return errors.New(errno.InvalidOptionValue, "too many partitions were defined")
	}
	return nil
}

func checkPartitionValuesIsInt() error {
	return nil
}

func checkPartitionNameUnique() error {
	return nil
}

func checkColumnsPartitionType() error {
	return nil
}
