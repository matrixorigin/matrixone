// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func checkDropColumnWithPrimaryKey(colName string, pkey *plan.PrimaryKeyDef, ctx CompilerContext) error {
	for _, column := range pkey.Names {
		if column == colName {
			return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column %s with primary key covered now", colName)
		}
	}
	return nil
}

func checkDropColumnWithIndex(colName string, indexes []*plan.IndexDef, ctx CompilerContext) error {
	for _, indexInfo := range indexes {
		if indexInfo.Unique {
			for _, column := range indexInfo.Parts {
				if column == colName {
					return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column %s with unique index covered now", colName)
				}
			}
		}
	}
	return nil
}

func checkAlterColumnWithForeignKey(colName string, RefChildTbls []uint64, Fkeys []*ForeignKeyDef, ctx CompilerContext) error {
	if len(RefChildTbls) > 0 || len(Fkeys) > 0 {
		// We do not support drop column that dependent foreign keys constraints
		return moerr.NewInvalidInput(ctx.GetContext(), "can't add/drop column for foreign key table now")
	}
	return nil
}

func checkAlterColumnWithPartitionKeys(colName string, tblInfo *TableDef, ctx CompilerContext) error {
	if tblInfo.Partition != nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't add/drop column for partition table now")
	}
	return nil
}

func checkDropColumnWithCluster(colName string, tblInfo *TableDef, ctx CompilerContext) error {
	//if tblInfo.ClusterBy != nil {
	//	// We do not support drop column that dependent foreign keys constraints
	//	return moerr.NewInvalidInput(ctx.GetContext(), "can't add/drop column for cluster table now")
	//}
	return nil
}

func checkIsDroppableColumn(tableDef *TableDef, colName string, ctx CompilerContext) error {
	// Check whether dropped column has existed.
	col := FindColumn(tableDef.Cols, colName)
	if col == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't DROP '%-.192s'; check that column/key exists", colName)
	}

	var colCnt int
	for _, col := range tableDef.Cols {
		if !col.Hidden {
			colCnt++
		}
	}
	if colCnt == 1 {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop only column %s in table %s", colName, tableDef.Name)
	}

	// We don not support drop auto_incr col
	if col.Typ.AutoIncr {
		return moerr.NewInvalidInput(ctx.GetContext(), "can' t drop auto_incr col")
	}

	// We do not support drop column that contain primary key columns now.
	err := checkDropColumnWithPrimaryKey(colName, tableDef.Pkey, ctx)
	if err != nil {
		return err
	}
	// We do not support drop column that contain index columns now.
	err = checkDropColumnWithIndex(colName, tableDef.Indexes, ctx)
	if err != nil {
		return err
	}
	// We do not support drop column that contain foreign key columns now.
	err = checkAlterColumnWithForeignKey(colName, tableDef.RefChildTbls, tableDef.Fkeys, ctx)
	if err != nil {
		return err
	}

	// We do not support drop column for partitioned table now
	err = checkAlterColumnWithPartitionKeys(colName, tableDef, ctx)
	if err != nil {
		return err
	}

	// We do not support drop column for cluster table now
	err = checkDropColumnWithCluster(colName, tableDef, ctx)
	if err != nil {
		return err
	}
	return nil
}

func checkIsAddableColumn(tableDef *TableDef, colName string, colType *plan.Type, ctx CompilerContext) error {
	// Check whether added column has existed.
	col := FindColumn(tableDef.Cols, colName)
	if col != nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't add '%-.192s'; check that column/key exists", colName)
	}

	// We don not support add auto_incr col
	if colType.AutoIncr {
		return moerr.NewInvalidInput(ctx.GetContext(), "can' t add auto_incr col")
	}

	// We do not support add column that contain foreign key columns now.
	err := checkAlterColumnWithForeignKey(colName, tableDef.RefChildTbls, tableDef.Fkeys, ctx)
	if err != nil {
		return err
	}

	// We do not support add column for partitioned table now
	err = checkAlterColumnWithPartitionKeys(colName, tableDef, ctx)
	if err != nil {
		return err
	}

	// We do not support add column for cluster table now
	err = checkDropColumnWithCluster(colName, tableDef, ctx)
	if err != nil {
		return err
	}
	return nil
}

// FindColumn finds column in cols by name.
func FindColumn(cols []*ColDef, name string) *ColDef {
	for _, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return col
		}
	}
	return nil
}

// FindColumn finds column in cols by colId
func FindColumnByColId(cols []*ColDef, colId uint64) *ColDef {
	for _, col := range cols {
		if col.ColId == colId {
			return col
		}
	}
	return nil
}
