// Copyright 2024 Matrix Origin
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

package versions

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type TableType int8

const (
	BASE_TABLE TableType = iota
	SYSTEM_VIEW
	VIEW
)

type UpgradeType int8

const (
	// alter table structure
	ADD_COLUMN UpgradeType = iota
	DROP_COLUMN
	CHANGE_COLUMN
	MODIFY_COLUMN
	RENAME_COLUMN
	ALTER_COLUMN_DEFAULT
	ADD_INDEX
	DROP_INDEX
	ALTER_INDEX_VISIBLE
	ADD_CONSTRAINT_UNIQUE_INDEX
	ADD_CONSTRAINT_PRIMARY_KEY
	ADD_CONSTRAINT_FOREIGN_KEY
	DROP_CONSTRAINT
	DROP_PRIMARY_KEY
	DROP_FOREIGN_KEY
	CREATE_NEW_TABLE

	// alter view definition
	MODIFY_VIEW
	CREATE_VIEW
	DROP_VIEW
)

// ----------------------------------------------------------------------------------------------------------------------
type UpgradeResource interface {
	getTableSchema() (string, string)
	getAddColumn() *plan.ColDef
	getDropColumn() *plan.ColDef
	getModifyColumn() (*plan.ColDef, *plan.ColDef)
	getRelCreateSQL() string
}

type TableChangeResource struct {
	Schema     string
	TableName  string
	TableType  TableType
	HopeColumn *plan.ColDef
	CurColumn  *plan.ColDef
	createSQL  string
}

func (t *TableChangeResource) getTableSchema() (string, string) {
	return t.Schema, t.TableName
}

func (t *TableChangeResource) getAddColumn() *plan.ColDef {
	return t.HopeColumn
}

func (t *TableChangeResource) getDropColumn() *plan.ColDef {
	return t.CurColumn
}

func (t *TableChangeResource) getModifyColumn() (*plan.ColDef, *plan.ColDef) {
	return t.CurColumn, t.HopeColumn
}

func (t *TableChangeResource) getRelCreateSQL() string {
	return t.createSQL
}

type ViewChangeResource struct {
	Schema        string
	TableName     string
	TableType     TableType
	NewViewDefine string
}

func (t *ViewChangeResource) getTableSchema() (string, string) {
	return t.Schema, t.TableName
}

func (t *ViewChangeResource) getAddColumn() *plan.ColDef {
	return nil
}

func (t *ViewChangeResource) getDropColumn() *plan.ColDef {
	return nil
}

func (t *ViewChangeResource) getModifyColumn() (*plan.ColDef, *plan.ColDef) {
	return nil, nil
}

func (t *ViewChangeResource) getRelCreateSQL() string {
	return t.NewViewDefine
}

type UpgradeCluster struct {
	UpgType     UpgradeType
	UpgResource UpgradeResource
	Comment     string
}

func (u *UpgradeCluster) Upgrade(txn executor.TxnExecutor, upgradeType UpgradeType, resource UpgradeResource) error {
	schema, tableName := resource.getTableSchema()
	var err error = nil
	switch upgradeType {
	case ADD_COLUMN:
		targetCol := resource.getAddColumn()
		_, err = AddColumn(txn, targetCol, schema, tableName)
	case DROP_COLUMN:
		targetCol := resource.getDropColumn()
		_, err = DropColumn(txn, targetCol, schema, tableName)
	case MODIFY_COLUMN:
		_, targetCol := resource.getModifyColumn()
		_, err = ModifyColumn(txn, targetCol, schema, tableName)
	case CHANGE_COLUMN:
		oldCol, newCol := resource.getModifyColumn()
		_, err = ChangeColumn(txn, oldCol, newCol, schema, tableName)
	case CREATE_NEW_TABLE:
		_, err = AddNewTable(txn, resource.getRelCreateSQL())
	case MODIFY_VIEW, CREATE_VIEW:
		viewDefine := resource.getRelCreateSQL()
		_, err = UpdateViewDef(txn, viewDefine, schema, tableName)
	case DROP_VIEW:
		_, err = DropViewDef(txn, schema, tableName)
	}
	return err
}

func AddColumn(txn executor.TxnExecutor, newCol *plan.ColDef, schema string, tableName string) (bool, error) {
	// 1.Check the table/view  status in current tenant
	colState, err := checkTableColumn(schema, tableName, txn, *newCol)
	if err != nil {
		return false, err
	}

	// 检查是否具有状态
	if (colState & NameSame) != 0 {
		return true, nil
	} else {
		typeMsg := plan2.MakeTypeByPlan2Type(newCol.Typ).DescString()
		// ADD [COLUMN] col_name column_definition
		sql := fmt.Sprintf("alter table `%s`.`%s` add column `%s` %s", schema, tableName, newCol.Name, typeMsg)
		fmt.Printf("------------wuxiliang4---------->sql:%s\n", sql)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			fmt.Printf("------------wuxiliang4---------->sql:%s, err:%s \n", sql, err.Error())
			return false, err
		}
		res.Close()
	}
	return true, nil
}

func DropColumn(txn executor.TxnExecutor, targetCol *plan.ColDef, schema string, tableName string) (bool, error) {
	// 1.Check the table/view  status in current tenant
	colState, err := checkTableColumn(schema, tableName, txn, *targetCol)
	if err != nil {
		return false, err
	}

	// 检查是否具有状态
	if (colState & NameSame) != 0 {
		// ADD [COLUMN] col_name column_definition
		sql := fmt.Sprintf("alter table `%s`.`%s` drop column `%s`", schema, tableName, targetCol.Name)
		fmt.Printf("------------wuxiliang5---------->sql:%s\n", sql)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			fmt.Printf("------------wuxiliang5---------->sql:%s, err:%s \n", sql, err.Error())
			return false, err
		}
		res.Close()
	} else {
		return true, nil
	}
	return true, nil
}

func ModifyColumn(txn executor.TxnExecutor, newCol *plan.ColDef, schema string, tableName string) (bool, error) {
	// 1.Check the table/view  status in current tenant
	colState, err := checkTableColumn(schema, tableName, txn, *newCol)
	if err != nil {
		return false, err
	}

	// 检查是否具有状态
	if (colState & NameSame) != 0 {
		if (colState&TypeSame) != 0 && (colState&PrecisionSame) != 0 {
			return true, nil
		} else {
			typeMsg := plan2.MakeTypeByPlan2Type(newCol.Typ).DescString()
			sql := fmt.Sprintf("alter table `%s`.`%s` modify column `%s` %s", schema, tableName, newCol.Name, typeMsg)
			fmt.Printf("------------wuxiliang6---------->sql:%s\n", sql)
			res, err := txn.Exec(sql, executor.StatementOption{})
			if err != nil {
				fmt.Printf("------------wuxiliang6---------->sql:%s, err:%s \n", sql, err.Error())
				return false, err
			}
			res.Close()
		}
	} else {
		typeMsg := plan2.MakeTypeByPlan2Type(newCol.Typ).DescString()
		// ADD [COLUMN] col_name column_definition
		sql := fmt.Sprintf("alter table `%s`.`%s` add column `%s` %s", schema, tableName, newCol.Name, typeMsg)
		fmt.Printf("------------wuxiliang7---------->sql:%s\n", sql)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			fmt.Printf("------------wuxiliang7---------->sql:%s, err:%s \n", sql, err.Error())
			return false, err
		}
		res.Close()
	}
	return true, nil
}

func ChangeColumn(txn executor.TxnExecutor, oldCol *plan.ColDef, newCol *plan.ColDef, schema string, tableName string) (bool, error) {
	// 1.Check the table/view  status in current tenant
	colState, err := checkTableColumn(schema, tableName, txn, *oldCol)
	if err != nil {
		return false, err
	}

	// 检查是否具有状态
	if (colState & NameSame) != 0 {
		if (colState&TypeSame) != 0 && (colState&PrecisionSame) != 0 {
			return true, nil
		} else {
			typeMsg := plan2.MakeTypeByPlan2Type(newCol.Typ).DescString()
			sql := fmt.Sprintf("alter table `%s`.`%s` change column `%s` `%s` %s", schema, tableName, oldCol.Name, newCol.Name, typeMsg)
			fmt.Printf("------------wuxiliang8---------->sql:%s\n", sql)
			res, err := txn.Exec(sql, executor.StatementOption{})
			if err != nil {
				fmt.Printf("------------wuxiliang8---------->sql:%s, err:%s \n", sql, err.Error())
				return false, err
			}
			res.Close()
		}
	} else {
		panic("No columns found for the target table")
	}
	return true, nil
}

func UpdateViewDef(txn executor.TxnExecutor, newViewDef string, schema string, viewName string) (bool, error) {
	isExisted, curViewDef, err := checkViewDefinition(schema, viewName, txn)
	if err != nil {
		return false, err
	}

	if isExisted {
		// If the current view definition is different from the new view definition, execute the update view definition operation
		if curViewDef != newViewDef {
			// Delete the current view
			sql := fmt.Sprintf("drop view `%s`.`%s`", schema, viewName)
			if _, err := txn.Exec(sql, executor.StatementOption{}); err != nil {
				return false, err
			}

			// Create a new view
			if _, err := txn.Exec(newViewDef, executor.StatementOption{}); err != nil {
				return false, err
			}
		}
	} else {
		// If the view does not exist, you can choose to execute the operation of creating the view
		if _, err := txn.Exec(newViewDef, executor.StatementOption{}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func DropViewDef(txn executor.TxnExecutor, schema string, viewName string) (bool, error) {
	isExisted, _, err := checkViewDefinition(schema, viewName, txn)
	if err != nil {
		return false, err
	}

	if isExisted {
		// Delete the current view
		sql := fmt.Sprintf("drop view `%s`.`%s`", schema, viewName)
		if _, err = txn.Exec(sql, executor.StatementOption{}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func AddNewTable(txn executor.TxnExecutor, tabledef string) (bool, error) {
	if _, err := txn.Exec(tabledef, executor.StatementOption{}); err != nil {
		return false, err
	}
	return true, nil
}

//----------------------------------------------------------------------------------------------------------------------

type ColumnMatchResult int8

const (
	NameSame ColumnMatchResult = 1 << iota
	TypeSame
	PrecisionSame
	NullableSame
)

func checkTableColumn(
	schema string,
	tableName string,
	txn executor.TxnExecutor,
	column plan.ColDef) (ColumnMatchResult, error) {
	var objectState ColumnMatchResult = 0
	sql := fmt.Sprintf(`select data_type, 
    			character_maximum_length, 
       			numeric_precision, 
       			numeric_scale, 
       			datetime_precision, 
       			ordinal_position 
				from information_schema.columns 
                where table_schema = '%s' and table_name = '%s' and column_name = '%s'`,
		schema, tableName, column.Name)
	fmt.Printf("------------wuxiliang3------------>sql:%s\n", sql)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return objectState, err
	}
	defer res.Close()

	loaded := false
	n := 0
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		data_type := cols[0].GetStringAt(0)
		character_length := vector.GetFixedAt[int64](cols[1], 0)
		numeric_precision := vector.GetFixedAt[int64](cols[2], 0)
		numeric_scale := vector.GetFixedAt[int64](cols[3], 0)
		datetime_precision := vector.GetFixedAt[int64](cols[4], 0)

		objectState |= NameSame
		if plan2.MakeTypeByPlan2Type(column.Typ).String() == data_type {
			objectState |= TypeSame
		}

		if column.Typ.Id == int32(types.T_varchar) || column.Typ.Id == int32(types.T_char) {
			if character_length == int64(column.Typ.Width) {
				objectState |= PrecisionSame
			}
		} else if column.Typ.Id == int32(types.T_decimal64) || column.Typ.Id == int32(types.T_decimal128) || column.Typ.Id == int32(types.T_decimal256) {
			if numeric_precision == int64(column.Typ.Width) && numeric_scale == int64(column.Typ.Scale) {
				objectState |= PrecisionSame
			}
		} else if column.Typ.Id == int32(types.T_datetime) || column.Typ.Id == int32(types.T_timestamp) {
			if datetime_precision == int64(column.Typ.Width) {
				objectState |= PrecisionSame
			}
		}
		n++
		loaded = true
		return false
	})

	if loaded && n > 1 {
		panic("BUG: Duplicate column names in table")
	}
	return objectState, nil
}

func checkViewDefinition(schema string, viewName string, txn executor.TxnExecutor) (bool, string, error) {
	sql := fmt.Sprintf(`select view_definition from information_schema.views 
                       where table_schema = '%s' and table_name = '%s'`, schema, viewName)
	fmt.Printf("------------wuxiliang3------------>sql:%s\n", sql)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return false, "", err
	}
	defer res.Close()

	view_def := ""
	loaded := false
	n := 0
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		view_def = cols[0].GetStringAt(0)
		n++
		loaded = true
		return false
	})

	if loaded && n > 1 {
		panic("BUG: Duplicate column names in table")
	}
	return loaded, view_def, nil
}
