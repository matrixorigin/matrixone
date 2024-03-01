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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	T_any           = "ANY"
	T_bool          = "BOOL"
	T_bit           = "BIT"
	T_int8          = "TINYINT"
	T_int16         = "SMALLINT"
	T_int32         = "INT"
	T_int64         = "BIGINT"
	T_uint8         = "TINYINT UNSIGNED"
	T_uint16        = "SMALLINT UNSIGNED"
	T_uint32        = "INT UNSIGNED"
	T_uint64        = "BIGINT UNSIGNED"
	T_float32       = "FLOAT"
	T_float64       = "DOUBLE"
	T_date          = "DATE"
	T_datetime      = "DATETIME"
	T_time          = "TIME"
	T_timestamp     = "TIMESTAMP"
	T_char          = "CHAR"
	T_varchar       = "VARCHAR"
	T_binary        = "BINARY"
	T_varbinary     = "VARBINARY"
	T_json          = "JSON"
	T_tuple         = "TUPLE"
	T_decimal64     = "DECIMAL64"
	T_decimal128    = "DECIMAL128"
	T_decimal256    = "DECIMAL256"
	T_blob          = "BLOB"
	T_text          = "TEXT"
	T_TS            = "TRANSACTION TIMESTAMP"
	T_Rowid         = "ROWID"
	T_uuid          = "UUID"
	T_Blockid       = "BLOCKID"
	T_interval      = "INTERVAL"
	T_array_float32 = "VECF32"
	T_array_float64 = "VECF64"
	T_enum          = "ENUM"
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

// Describe the detailed information of the table column
type ColumnInfo struct {
	IsExits           bool
	Name              string
	Nullable          bool
	ColType           string
	ChatLength        int64
	Precision         int64
	Scale             int64
	datetimePrecision int64
	Position          int32
	Default           string
	Extra             string
	Comment           string
}

// UpgradeEntry is used to designate a specific upgrade entity.
// Users must provide `UpgSql` and `CheckFunc` implementations
type UpgradeEntry struct {
	Schema    string
	TableName string
	UpgType   UpgradeType
	TableType TableType
	UpgSql    string
	CheckFunc func(txn executor.TxnExecutor, accountId uint32) (bool, error)
}

// Upgrade entity execution upgrade entrance
func (u *UpgradeEntry) Upgrade(txn executor.TxnExecutor, accountId uint32) error {
	exist, err := u.CheckFunc(txn, accountId)
	if err != nil {
		return err
	}

	if exist {
		return nil
	} else {
		res, err := txn.Exec(u.UpgSql, executor.StatementOption{}.WithAccountID(accountId))
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

// CheckTableColumn Check if the columns in the table exist, and if so,
// return the detailed information of the columns
func CheckTableColumn(txn executor.TxnExecutor,
	accountId uint32,
	schema string,
	tableName string,
	columnName string) (ColumnInfo, error) {

	xyz := ColumnInfo{
		IsExits: false,
		Name:    columnName,
	}

	sql := fmt.Sprintf(`select data_type,
       			is_nullable,
    			character_maximum_length, 
       			numeric_precision, 
       			numeric_scale, 
       			datetime_precision, 
       			ordinal_position,
       			column_default,
       			extra,
				column_comment
				from information_schema.columns 
                where table_schema = '%s' and table_name = '%s' and column_name = '%s'`,
		schema, tableName, columnName)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
	if err != nil {
		return xyz, err
	}
	defer res.Close()

	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		data_type := cols[0].GetStringAt(0)
		is_nullable := cols[1].GetStringAt(0)
		character_length := vector.GetFixedAt[int64](cols[2], 0)
		numeric_precision := vector.GetFixedAt[int64](cols[3], 0)
		numeric_scale := vector.GetFixedAt[int64](cols[4], 0)
		datetime_precision := vector.GetFixedAt[int64](cols[5], 0)
		ordinal_position := vector.GetFixedAt[int32](cols[6], 0)
		column_default := cols[7].GetStringAt(0)
		extra := cols[8].GetStringAt(0)
		column_comment := cols[9].GetStringAt(0)

		xyz.IsExits = true
		xyz.ColType = data_type
		xyz.Nullable = checkInput(is_nullable)
		xyz.ChatLength = character_length
		xyz.Precision = numeric_precision
		xyz.Scale = numeric_scale
		xyz.datetimePrecision = datetime_precision
		xyz.Position = ordinal_position
		xyz.Default = column_default
		xyz.Extra = extra
		xyz.Comment = column_comment

		return false
	})
	return xyz, nil
}

// CheckViewDefinition Check if the view exists, if so, return true and return the view definition
func CheckViewDefinition(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
	sql := fmt.Sprintf(`select view_definition from information_schema.views
                       where table_schema = '%s' and table_name = '%s'`, schema, viewName)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
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

// CheckTableDefinition Check if the table exists, return true if it exists
func CheckTableDefinition(txn executor.TxnExecutor, accountId uint32, schema string, tableName string) (bool, error) {
	sql := fmt.Sprintf(`select * from information_schema.tables where table_schema = '%s' and table_name = '%s'`, schema, tableName)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
	if err != nil {
		return false, err
	}
	defer res.Close()

	loaded := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		loaded = true
		return false
	})

	return loaded, nil
}

// DropView Execute the delete view operation
func DropView(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) error {
	// Delete the current view
	sql := fmt.Sprintf("drop view `%s`.`%s`", schema, viewName)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
	if err != nil {
		return err
	}
	defer res.Close()
	return nil
}

func checkInput(input string) bool {
	switch input {
	case "YES", "yes":
		return true
	case "NO", "no":
		return false
	default:
		return false
	}
}
