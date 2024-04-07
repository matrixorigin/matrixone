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
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	CREATE_DATABASE
	MODIFY_METADATA
)

// ColumnInfo Describe the detailed information of the table column
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
	// UpgType declare the type of upgrade
	UpgType UpgradeType
	// UpgSql is used to perform upgrade operations
	UpgSql string
	// CheckFunc was used to check whether an upgrade is required
	// return true if the system is already in the final state and does not need to be upgraded,
	// otherwise return false
	CheckFunc func(txn executor.TxnExecutor, accountId uint32) (bool, error)
	PreSql    string
	PostSql   string
}

// Upgrade entity execution upgrade entrance
func (u *UpgradeEntry) Upgrade(txn executor.TxnExecutor, accountId uint32) error {
	exist, err := u.CheckFunc(txn, accountId)
	if err != nil {
		getLogger().Error("execute upgrade entry check error", zap.Error(err), zap.String("upgrade entry", u.String()))
		return err
	}

	if exist {
		return nil
	} else {
		// 1. First, judge whether there is prefix sql
		if u.PreSql != "" {
			res, err := txn.Exec(u.PreSql, executor.StatementOption{}.WithAccountID(accountId))
			if err != nil {
				return err
			}
			res.Close()
		}

		// 2. Second, Execute upgrade sql
		res, err := txn.Exec(u.UpgSql, executor.StatementOption{}.WithAccountID(accountId))
		if err != nil {
			getLogger().Error("execute upgrade entry sql error", zap.Error(err), zap.String("upgrade entry", u.String()))
			return err
		}
		res.Close()

		// 2. Third, after the upgrade is completed, judge whether there is post-sql
		if u.PostSql != "" {
			res, err = txn.Exec(u.PostSql, executor.StatementOption{}.WithAccountID(accountId))
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	return nil
}

func (u *UpgradeEntry) String() string {
	return fmt.Sprintf("UpgradeEntry type:%v schema: %s.%s, upgrade sql: %s",
		u.UpgType,
		u.Schema,
		u.TableName,
		u.UpgSql)
}

// CheckTableColumn Check if the columns in the table exist, and if so,
// return the detailed information of the column
func CheckTableColumn(txn executor.TxnExecutor,
	accountId uint32,
	schema string,
	tableName string,
	columnName string) (ColumnInfo, error) {

	checkInput := func(input string) bool {
		switch input {
		case "YES", "yes":
			return true
		case "NO", "no":
			return false
		default:
			return false
		}
	}

	colInfo := ColumnInfo{
		IsExits: false,
		Name:    columnName,
	}

	sql := fmt.Sprintf(`SELECT mo_show_visible_bin(atttyp, 2) AS DATA_TYPE, 
       CASE WHEN attnotnull != 0 THEN 'NO' ELSE 'YES' END AS IS_NULLABLE, 
       internal_char_length(atttyp) AS CHARACTER_MAXIMUM_LENGTH, 
       internal_numeric_precision(atttyp) AS NUMERIC_PRECISION, 
       internal_numeric_scale(atttyp) AS NUMERIC_SCALE, 
       internal_datetime_scale(atttyp) AS DATETIME_PRECISION, 
       attnum AS ORDINAL_POSITION, 
       mo_show_visible_bin(att_default, 1) AS COLUMN_DEFAULT, 
       CASE WHEN att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS EXTRA, 
       att_comment AS COLUMN_COMMENT FROM mo_catalog.mo_columns 
            WHERE att_relname != 'mo_increment_columns' AND att_relname NOT LIKE '__mo_cpkey_%%' 
            AND attname != '__mo_rowid' 
            AND att_database = '%s' and att_relname = '%s' and attname = '%s';`, schema, tableName, columnName)

	if accountId == catalog.System_Account {
		sql = fmt.Sprintf(`SELECT mo_show_visible_bin(atttyp, 2) AS DATA_TYPE, 
       CASE WHEN attnotnull != 0 THEN 'NO' ELSE 'YES' END AS IS_NULLABLE, 
       internal_char_length(atttyp) AS CHARACTER_MAXIMUM_LENGTH, 
       internal_numeric_precision(atttyp) AS NUMERIC_PRECISION, 
       internal_numeric_scale(atttyp) AS NUMERIC_SCALE, 
       internal_datetime_scale(atttyp) AS DATETIME_PRECISION, 
       attnum AS ORDINAL_POSITION, 
       mo_show_visible_bin(att_default, 1) AS COLUMN_DEFAULT, 
       CASE WHEN att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS EXTRA, 
       att_comment AS COLUMN_COMMENT FROM mo_catalog.mo_columns 
            WHERE att_relname != 'mo_increment_columns' AND att_relname NOT LIKE '__mo_cpkey_%%' 
            AND attname != '__mo_rowid' AND account_id = 0 
            AND att_database = '%s' and att_relname = '%s' and attname = '%s';`, schema, tableName, columnName)
	}

	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
	if err != nil {
		return colInfo, err
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

		colInfo.IsExits = true
		colInfo.ColType = data_type
		colInfo.Nullable = checkInput(is_nullable)
		colInfo.ChatLength = character_length
		colInfo.Precision = numeric_precision
		colInfo.Scale = numeric_scale
		colInfo.datetimePrecision = datetime_precision
		colInfo.Position = ordinal_position
		colInfo.Default = column_default
		colInfo.Extra = extra
		colInfo.Comment = column_comment

		return false
	})
	return colInfo, nil
}

// CheckViewDefinition Check if the view exists, if so, return true and return the view definition
func CheckViewDefinition(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
	sql := fmt.Sprintf("SELECT tbl.rel_createsql AS `VIEW_DEFINITION` FROM mo_catalog.mo_tables tbl LEFT JOIN mo_catalog.mo_user usr ON tbl.creator = usr.user_id WHERE tbl.relkind = 'v' AND tbl.reldatabase = '%s'  AND  tbl.relname = '%s'", schema, viewName)
	if accountId == catalog.System_Account {
		sql = fmt.Sprintf("SELECT tbl.rel_createsql AS `VIEW_DEFINITION` FROM mo_catalog.mo_tables tbl LEFT JOIN mo_catalog.mo_user usr ON tbl.creator = usr.user_id WHERE tbl.relkind = 'v' AND account_id = 0 AND tbl.reldatabase = '%s'  AND  tbl.relname = '%s'", schema, viewName)
	}

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

// CheckTableDefinition is used to check if the specified table definition exists in the specified database. If it exists,
// return true; otherwise, return false.
func CheckTableDefinition(txn executor.TxnExecutor, accountId uint32, schema string, tableName string) (bool, error) {
	if schema == "" || tableName == "" {
		return false, moerr.NewInternalErrorNoCtx("schema name or table name is empty")
	}

	sql := fmt.Sprintf(`SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables tbl 
                              WHERE tbl.relname NOT LIKE '__mo_index_%%' AND tbl.relkind != 'partition' 
                              AND reldatabase = '%s' AND relname = '%s'`, schema, tableName)
	if accountId == catalog.System_Account {
		sql = fmt.Sprintf(`SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables tbl 
                                  WHERE tbl.relname NOT LIKE '__mo_index_%%' AND tbl.relkind != 'partition' 
                                  AND account_id = 0 AND reldatabase = '%s' AND relname = '%s'`, schema, tableName)
	}

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

// CheckDatabaseDefinition This function is used to check if the database definition exists.
// If it exists, return true; otherwise, return false.
func CheckDatabaseDefinition(txn executor.TxnExecutor, accountId uint32, schema string) (bool, error) {
	if schema == "" {
		return false, moerr.NewInternalErrorNoCtx("schema name is empty")
	}

	sql := fmt.Sprintf(`select datname from mo_catalog.mo_database where datname = '%s'`, schema)
	if accountId == catalog.System_Account {
		sql = fmt.Sprintf(`select datname from mo_catalog.mo_database where account_id = 0 and datname = '%s'`, schema)
	}

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

// CheckTableDataExist Used to checks whether a table contains specific data
// This function executes the given SQL query, returns true if the result set is not empty, otherwise returns false.
func CheckTableDataExist(txn executor.TxnExecutor, accountId uint32, sql string) (bool, error) {
	if sql == "" {
		return false, moerr.NewInternalErrorNoCtx("check table data sql is empty")
	}

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

// CheckIndexDefinition Used to check if a certain index is defined in the table
// This function executes the given SQL query, returns true if the result set is not empty, otherwise returns false.
func CheckIndexDefinition(txn executor.TxnExecutor, accountId uint32, schema string, tableName string, indexName string) (bool, error) {
	if schema == "" || tableName == "" || indexName == "" {
		return false, moerr.NewInternalErrorNoCtx("schema name or table name or indexName is empty")
	}

	sql := fmt.Sprintf("select distinct `idx`.`name` from `mo_catalog`.`mo_indexes` `idx` "+
		"left join `mo_catalog`.`mo_tables` `tbl` on `idx`.`table_id` = `tbl`.`rel_id` "+
		"where `tbl`.`reldatabase` = '%s' AND `tbl`.`relname` = '%s' AND `idx`.`name` = '%s'",
		schema, tableName, indexName)
	if accountId == catalog.System_Account {
		sql = fmt.Sprintf("select distinct `idx`.`name` from `mo_catalog`.`mo_indexes` `idx` "+
			"left join `mo_catalog`.`mo_tables` `tbl` on `idx`.`table_id` = `tbl`.`rel_id` "+
			"where `tbl`.`account_id` = %d AND `tbl`.`reldatabase` = '%s' AND `tbl`.`relname` = '%s' AND `idx`.`name` = '%s'",
			accountId, schema, tableName, indexName)
	}
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
