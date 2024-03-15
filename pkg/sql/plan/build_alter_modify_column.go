// Copyright 2023 Matrix Origin
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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// ModifyColumn Can change a column definition but not its name.
// More convenient than CHANGE to change a column definition without renaming it.
// With FIRST or AFTER, can reorder columns.
func ModifyColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableModifyColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	specNewColumn := spec.NewColumn
	originalColName := specNewColumn.Name.Parts[0]

	// Check whether added column has existed.
	colName := specNewColumn.Name.Parts[0]
	col := FindColumn(tableDef.Cols, originalColName)
	if col == nil || col.Hidden {
		return moerr.NewBadFieldError(ctx.GetContext(), colName, alterPlan.TableDef.Name)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return err
	}
	if err = checkAddColumnType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0]); err != nil {
		return err
	}

	newCol, err := buildChangeColumnAndConstraint(ctx, alterPlan, col, specNewColumn, colType)
	if err != nil {
		return err
	}

	// Check new column foreign key constraints
	if err = CheckModifyColumnForeignkeyConstraint(ctx, tableDef, col, newCol); err != nil {
		return err
	}

	if isColumnWithPartition(col.Name, tableDef.Partition) {
		return moerr.NewNotSupported(ctx.GetContext(), "unsupport alter partition part column currently")
	}

	if err = checkChangeTypeCompatible(ctx.GetContext(), &col.Typ, &newCol.Typ); err != nil {
		return err
	}

	if err = checkModifyNewColumn(ctx.GetContext(), tableDef, col, newCol, spec.Position); err != nil {
		return err
	}

	alterCtx.alterColMap[newCol.Name] = selectExpr{
		sexprType: columnName,
		sexprStr:  col.Name,
	}

	return nil
}

// checkModifyNewColumn Check the position information of the newly formed column and place the new column in the target location
func checkModifyNewColumn(ctx context.Context, tableDef *TableDef, oldCol, newCol *ColDef, pos *tree.ColumnPosition) error {
	if pos != nil && pos.Typ != tree.ColumnPositionNone {
		// detete old column
		originIndex := -1
		for i, col := range tableDef.Cols {
			if strings.EqualFold(col.Name, oldCol.Name) {
				originIndex = i
				break
			}
		}
		tableDef.Cols = append(tableDef.Cols[:originIndex], tableDef.Cols[originIndex+1:]...)

		targetPos, err := findPositionRelativeColumn(ctx, tableDef.Cols, pos)
		if err != nil {
			return err
		}
		tableDef.Cols = append(tableDef.Cols[:targetPos], append([]*ColDef{newCol}, tableDef.Cols[targetPos:]...)...)
	} else {
		for i, col := range tableDef.Cols {
			if strings.EqualFold(col.Name, oldCol.Name) {
				tableDef.Cols[i] = newCol
				break
			}
		}
	}
	return nil
}

// Check if the modify column is associated with the partition key
func isColumnWithPartition(colName string, partitionDef *PartitionByDef) bool {
	if partitionDef != nil {
		if partitionDef.PartitionColumns != nil {
			for _, column := range partitionDef.PartitionColumns.PartitionColumns {
				if column == colName {
					return true
				}
			}
		} else {
			if strings.EqualFold(partitionDef.PartitionExpr.ExprStr, colName) {
				return true
			}
		}
	}
	return false
}

// checkChangeTypeCompatible checks whether changes column type to another is compatible and can be changed.
func checkChangeTypeCompatible(ctx context.Context, origin *plan.Type, to *plan.Type) error {
	// Deal with the same type.
	if origin.Id == to.Id {
		return nil
	} else {
		if (origin.Id == int32(types.T_time) || origin.Id == int32(types.T_timestamp) || origin.Id == int32(types.T_date) || origin.Id == int32(types.T_datetime) || origin.Id == int32(types.T_char) || origin.Id == int32(types.T_varchar) || origin.Id == int32(types.T_json) || origin.Id == int32(types.T_uuid)) &&
			to.Id == int32(types.T_binary) {
			return moerr.NewNotSupported(ctx, "currently unsupport change from original type %v to %v ", origin.Id, to.Id)
		}

		if (origin.Id == int32(types.T_binary) || origin.Id == int32(types.T_decimal64) || origin.Id == int32(types.T_decimal128) || origin.Id == int32(types.T_float32) || origin.Id == int32(types.T_float64)) &&
			(to.Id == int32(types.T_time) || to.Id == int32(types.T_timestamp) || to.Id == int32(types.T_date) || to.Id == int32(types.T_datetime)) {
			return moerr.NewNotSupported(ctx, "currently unsupport change from original type %v to %v ", origin.Id, to.Id)
		}
	}
	return nil
}

// CheckModifyColumnForeignkeyConstraint check for table column foreign key dependencies, including
// the foreign keys of the table itself and being dependent on foreign keys of other tables
func CheckModifyColumnForeignkeyConstraint(ctx CompilerContext, tbInfo *TableDef, originalCol, newCol *ColDef) error {
	if newCol.Typ.GetId() == originalCol.Typ.GetId() &&
		newCol.Typ.GetWidth() == originalCol.Typ.GetWidth() &&
		newCol.Typ.GetAutoIncr() == originalCol.Typ.GetAutoIncr() {
		return nil
	}

	for _, fkInfo := range tbInfo.Fkeys {
		for i, colId := range fkInfo.Cols {
			if colId == originalCol.ColId {
				// Check if the parent table of the foreign key exists
				_, referTableDef := ctx.ResolveById(fkInfo.ForeignTbl)
				if referTableDef == nil {
					continue
				}

				referCol := FindColumnByColId(referTableDef.Cols, fkInfo.ForeignCols[i])
				if referCol == nil {
					continue
				}
				if newCol.Typ.GetId() != referCol.Typ.GetId() {
					return moerr.NewErrForeignKeyColumnCannotChange(ctx.GetContext(), originalCol.Name, fkInfo.Name)
				}

				if newCol.Typ.GetWidth() < referCol.Typ.GetWidth() ||
					newCol.Typ.GetWidth() < originalCol.Typ.GetWidth() {
					return moerr.NewErrForeignKeyColumnCannotChange(ctx.GetContext(), originalCol.Name, fkInfo.Name)
				}
			}
		}
	}

	for _, referredTblId := range tbInfo.RefChildTbls {
		refObjRef, refTableDef := ctx.ResolveById(referredTblId)
		if refTableDef == nil {
			return moerr.NewInternalError(ctx.GetContext(), "The reference foreign key table %d does not exist", referredTblId)
		}
		var referredFK *ForeignKeyDef
		for _, fkInfo := range refTableDef.Fkeys {
			if fkInfo.ForeignTbl == tbInfo.TblId {
				referredFK = fkInfo
				break
			}
		}

		for i := range referredFK.Cols {
			if referredFK.ForeignCols[i] == originalCol.ColId {
				if originalCol.Name != newCol.Name {
					return moerr.NewErrAlterOperationNotSupportedReasonFkRename(ctx.GetContext())
				} else {
					return moerr.NewErrForeignKeyColumnCannotChangeChild(ctx.GetContext(), originalCol.Name, referredFK.Name, refObjRef.SchemaName+"."+refTableDef.Name)
				}

				//childCol := FindColumnByColId(refTableDef.Cols, colId)
				//if childCol == nil {
				//	continue
				//}
				//
				//if newCol.Typ.GetId() != childCol.Typ.GetId() {
				//	return moerr.NewErrFKIncompatibleColumns(ctx.GetContext(), childCol.Name, originalCol.Name, referredFK.Name)
				//}
				//
				//if newCol.Typ.GetWidth() < childCol.Typ.GetWidth() ||
				//	newCol.Typ.GetWidth() < originalCol.Typ.GetWidth() {
				//	return moerr.NewErrForeignKeyColumnCannotChangeChild(ctx.GetContext(), originalCol.Name, referredFK.Name, refObjRef.SchemaName+"."+refTableDef.Name)
				//}
			}
		}
	}
	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(ctx context.Context, col *ColDef, hasDefaultValue, hasNullFlag bool, priKeyDef *plan.PrimaryKeyDef) error {
	if hasDefaultValue {
		hasNullFlag = DefaultValueIsNull(col.Default) || hasNullFlag
	}
	// Primary key should not be null.
	if col.Primary && hasDefaultValue && DefaultValueIsNull(col.Default) {
		return moerr.NewErrInvalidDefault(ctx, col.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create table t1 (id int ,name varchar(20), age int, primary key(id, name))
	if !col.Primary && priKeyDef != nil {
		for _, key := range priKeyDef.Names {
			if key == col.Name {
				// Primary key should not be null.
				if hasNullFlag {
					return moerr.NewErrPrimaryCantHaveNull(ctx)
				}
				break
			} else {
				continue
			}
		}
	}
	return nil
}

func DefaultValueIsNull(Default *plan.Default) bool {
	if Default != nil {
		if constExpr, ok := Default.GetExpr().Expr.(*plan.Expr_Lit); ok {
			return constExpr.Lit.Isnull
		}
		return false
	}
	return false
}
