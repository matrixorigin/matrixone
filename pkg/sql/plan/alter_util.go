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

func checkDropColumnWithForeignKey(colName string, fkeys []*ForeignKeyDef, ctx CompilerContext) error {
	if len(fkeys) > 0 {
		// We do not support drop column that dependent foreign keys constraints
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column for partition table now")
	}
	return nil
}

func checkDropColumnWithPartitionKeys(colName string, tblInfo *TableDef, ctx CompilerContext) error {
	if tblInfo.Partition != nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column for partition table now")
	}
	return nil
}

func checkIsDroppableColumn(tableDef *TableDef, colName string, ctx CompilerContext) error {
	// Check whether dropped column has existed.
	col := FindColumn(tableDef.Cols, colName)
	if col == nil {
		//err = dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
		return moerr.NewInvalidInput(ctx.GetContext(), "Can't DROP '%-.192s'; check that column/key exists", colName)
	}

	if len(tableDef.Cols) == 1 {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop only column %s in table %s", colName, tableDef.Name)
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
	err = checkDropColumnWithForeignKey(colName, tableDef.Fkeys, ctx)
	if err != nil {
		return err
	}

	// We do not support drop column for partitioned table now
	err = checkDropColumnWithPartitionKeys(colName, tableDef, ctx)
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