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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// ModifyColumn Can change a column definition but not its name.
// More convenient than CHANGE to change a column definition without renaming it.
// With FIRST or AFTER, can reorder columns.
func ModifyColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterTableModifyColumnClause, alterCtx *AlterTableContext) error {
	tableDef := alterPlan.CopyTableDef

	specNewColumn := spec.NewColumn
	colName := specNewColumn.Name.ColName()

	// Check whether added column has existed.
	//col is the old column?
	col := FindColumn(tableDef.Cols, colName)
	if col == nil || col.Hidden {
		return moerr.NewBadFieldError(ctx.GetContext(), specNewColumn.Name.ColNameOrigin(), alterPlan.TableDef.Name)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return err
	}
	if err = checkAddColumnType(ctx.GetContext(), &colType, colName); err != nil {
		return err
	}

	//colType is the type of the new column
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
		tableDef.Cols = RemoveIf[*ColDef](tableDef.Cols, func(col *ColDef) bool {
			return strings.EqualFold(col.Name, oldCol.Name)
		})

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
		// The enumeration type has an independent cast function to handle it
		if origin.Id == int32(types.T_enum) || to.Id == int32(types.T_enum) {
			return nil
		}

		if supported := function.IfTypeCastSupported(types.T(origin.GetId()), types.T(to.GetId())); !supported {
			return moerr.NewNotSupportedf(ctx, "currently unsupport change from original type %v to %v ", types.T(origin.Id).String(), types.T(to.Id).String())
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
				_, referTableDef := ctx.ResolveById(fkInfo.ForeignTbl, nil)
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
		refObjRef, refTableDef := ctx.ResolveById(referredTblId, nil)
		if refTableDef == nil {
			return moerr.NewInternalErrorf(ctx.GetContext(), "The reference foreign key table %d does not exist", referredTblId)
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
	hasPriKeyFlag := false
	if col.Primary {
		hasPriKeyFlag = true
	} else if priKeyDef != nil {
		for _, key := range priKeyDef.Names {
			if key == col.Name {
				hasPriKeyFlag = true
				break
			} else {
				continue
			}
		}
	}

	// Primary key should not be null.
	if hasPriKeyFlag && hasDefaultValue {
		if DefaultValueIsNull(col.Default) {
			//return moerr.NewErrInvalidDefault(ctx, col.Name)
			return moerr.NewErrPrimaryCantHaveNull(ctx)
		}
	}

	// Primary key should not be null.
	if hasPriKeyFlag && hasNullFlag {
		return moerr.NewErrPrimaryCantHaveNull(ctx)
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
