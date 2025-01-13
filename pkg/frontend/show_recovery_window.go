// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	getTablePitrRecordsFormat = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = %d and account_name = '%s' and database_name = '%s' and table_name = '%s' and pitr_name != '%s' and level = 'table'"

	getTableSnapshotRecordsFormat = "select sname, ts from MO_CATALOG.MO_SNAPSHOTS where account_name = '%s' and database_name = '%s' and table_name = '%s' and level = 'table'"

	getDbPitrRecordsFormat = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = %d and account_name = '%s' and database_name = '%s' and pitr_name != '%s' and level = 'database'"

	getDbSnapshotRecordsFormat = "select sname, ts from MO_CATALOG.MO_SNAPSHOTS where account_name = '%s' and database_name = '%s' and level = 'database'"

	getAccountPitrRecordsFormat = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = %d and account_name = '%s' and pitr_name != '%s' and level = 'account'"

	getAccountSnapshotRecordsFormat = "select sname, ts from MO_CATALOG.MO_SNAPSHOTS where account_name = '%s' and level = 'account'"
)

func doShowRecoveryWindow(ctx context.Context, ses *Session, srw *tree.ShowRecoveryWindow) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check privilege
	err = checkShowRecoveryWindowPrivilege(ctx, ses, srw)
	if err != nil {
		return err
	}

	// build result columns
	// recovery window level
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Recovery_Window_Level")

	// account name
	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Account_Name")

	// database name
	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("Database_Name")

	// table name
	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("Table_Name")

	// recovery windows
	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_JSON)
	col5.SetName("Recovery_Windows")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)

	rows := make([][]interface{}, 0)
	// recovey level
	level := srw.Level
	switch level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
		var accountName string
		if len(srw.AccountName) > 0 {
			accountName = srw.AccountName.String()
		} else {
			accountName = ses.GetTenantInfo().GetTenant()
		}

		var windowRows [][]interface{}
		windowRows, err = getAccountRecoveryWindowRows(ctx, ses, bh, accountName)
		if err != nil {
			return err
		}
		rows = append(rows, windowRows...)
	case tree.RECOVERYWINDOWLEVELDATABASE:
		var accountName, dbName string
		accountName = ses.GetTenantInfo().GetTenant()
		if len(srw.DatabaseName.String()) > 0 {
			dbName = srw.DatabaseName.String()
		}
		var windowRows [][]interface{}
		windowRows, err = getDbRecoveryWindowRows(ctx, ses, bh, accountName, dbName)
		if err != nil {
			return err
		}
		rows = append(rows, windowRows...)

	case tree.RECOVERYWINDOWLEVELTABLE:
		// get table recovery window
		var accountName, dbName, tblName string
		accountName = ses.GetTenantInfo().GetTenant()
		if len(srw.DatabaseName.String()) > 0 {
			dbName = srw.DatabaseName.String()
		}
		if len(srw.TableName.String()) > 0 {
			tblName = srw.TableName.String()
		}

		var windowRows [][]interface{}
		windowRows, err = getTableRecoveryWindowRows(ctx, ses, bh, accountName, dbName, tblName)
		if err != nil {
			return err
		}
		rows = append(rows, windowRows...)
	}

	for _, row := range rows {
		mrs.AddRow(row)
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func getAccountRecoveryWindowRows(ctx context.Context, ses *Session, bh BackgroundExec, accountName string) ([][]interface{}, error) {
	bh.ClearExecResultSet()
	bh.Exec(ctx, "show databases")
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	dbs := make([]string, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			dbName, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			if !needSkipDb(dbName) {
				dbs = append(dbs, dbName)
			}
		}
	}

	var rows [][]interface{}

	// get account recovery window for pitr
	pitrRecords, err := getAccountPitrRecords(ctx, ses, bh, accountName)
	if err != nil {
		return nil, err
	}
	// get account recovery window for snapshot
	snapshotRecords, err := getAccountSnapshotRecords(ctx, ses, bh, accountName)
	if err != nil {
		return nil, err
	}

	for _, dbName := range dbs {
		// get db recovery window separately
		var windowRows [][]interface{}
		windowRows, err = getAccountRecoveryWindow(ctx, ses, bh, accountName, dbName, &pitrRecords, &snapshotRecords)
		if err != nil {
			return nil, err
		}
		rows = append(rows, windowRows...)
	}
	return rows, nil
}

func getAccountRecoveryWindow(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName string, accountPitrs *[]tableRecoveryWindow, accountSnapshot *[]tableRecoveryWindowForSnapshot) ([][]interface{}, error) {
	// get tables
	var err error
	var erArray []ExecResult
	bh.ClearExecResultSet()
	bh.Exec(ctx, "show tables from "+dbName)
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			tableName, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			tables = append(tables, tableName)

		}
	}

	var rows [][]interface{}

	// get db recovery window for pitr
	pitrRecords, err := getDbPitrRecords(ctx, ses, bh, accountName, dbName)
	if err != nil {
		return nil, err
	}
	pitrRecords = append(pitrRecords, *accountPitrs...)

	// get db recovery window for snapshot
	snapshotRecords, err := getDbSnapshotsRecords(ctx, ses, bh, accountName, dbName)
	if err != nil {
		return nil, err
	}
	snapshotRecords = append(snapshotRecords, *accountSnapshot...)

	// get table recovery window separately
	for _, tblName := range tables {
		var windowRows [][]interface{}
		windowRows, err = getTableRecoveryWindowRowsForDb(ctx, ses, bh, accountName, dbName, tblName, &pitrRecords, &snapshotRecords)
		if err != nil {
			return nil, err
		}
		rows = append(rows, windowRows...)
	}
	return rows, nil
}

func getDbRecoveryWindowRows(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName string) ([][]interface{}, error) {
	// get tables
	var err error
	var erArray []ExecResult
	bh.ClearExecResultSet()
	bh.Exec(ctx, "show tables from "+dbName)
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			tableName, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			tables = append(tables, tableName)

		}
	}

	var rows [][]interface{}

	// get db recovery window for pitr
	pitrRecords, err := getDbPitrRecords(ctx, ses, bh, accountName, dbName)
	if err != nil {
		return nil, err
	}

	// get db recovery window for snapshot
	snapshotRecords, err := getDbSnapshotsRecords(ctx, ses, bh, accountName, dbName)
	if err != nil {
		return nil, err
	}

	// get table recovery window separately
	for _, tblName := range tables {
		var windowRows [][]interface{}
		windowRows, err = getTableRecoveryWindowRowsForDb(ctx, ses, bh, accountName, dbName, tblName, &pitrRecords, &snapshotRecords)
		if err != nil {
			return nil, err
		}
		rows = append(rows, windowRows...)
	}
	return rows, nil
}

func getTableRecoveryWindowRowsForDb(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName, tblName string, dbPitrRecords *[]tableRecoveryWindow, dbSnapshotRecords *[]tableRecoveryWindowForSnapshot) ([][]interface{}, error) {
	var rows [][]interface{}
	var err error
	marshelStrs := make([]string, 0)
	// get table recovery window
	var pitrRecords []tableRecoveryWindow
	pitrRecords, err = getTablePitrRecords(ctx, ses, bh, accountName, dbName, tblName)
	if err != nil {
		return nil, err
	}
	pitrRecords = append(pitrRecords, *dbPitrRecords...)
	for _, record := range pitrRecords {
		var str string
		str, err = marshalRcoveryWindowToJson(record)
		if err != nil {
			return nil, err
		}
		if len(str) > 0 {
			// skip invalid pitr
			marshelStrs = append(marshelStrs, str)
		}
	}

	// get table snapshot
	var snapshotRecords []tableRecoveryWindowForSnapshot
	snapshotRecords, err = getTableSnapshotRecords(ctx, ses, bh, accountName, dbName, tblName)
	if err != nil {
		return nil, err
	}
	snapshotRecords = append(snapshotRecords, *dbSnapshotRecords...)
	for _, record := range snapshotRecords {
		str := marshalRcoveryWindowToJsonForSnapshot(record)
		marshelStrs = append(marshelStrs, str)
	}

	if len(marshelStrs) > 0 {
		str := ""
		for i, s := range marshelStrs {
			if i == 0 {
				str = s
			} else {
				str = str + ","
				str += "\n"
				str = str + s
			}
		}
		row := make([]interface{}, 5)
		row[0] = "database"
		row[1] = accountName
		row[2] = dbName
		row[3] = tblName
		row[4] = []byte(str)
		rows = append(rows, row)
	}
	return rows, nil
}

func getTableRecoveryWindowRows(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName, tblName string) ([][]interface{}, error) {
	var rows [][]interface{}
	var err error
	marshelStrs := make([]string, 0)
	// get table recovery window
	var pitrRecords []tableRecoveryWindow
	pitrRecords, err = getTablePitrRecords(ctx, ses, bh, accountName, dbName, tblName)
	if err != nil {
		return nil, err
	}
	for _, record := range pitrRecords {
		var str string
		str, err = marshalRcoveryWindowToJson(record)
		if err != nil {
			return nil, err
		}
		if len(str) > 0 {
			// skip invalid pitr
			marshelStrs = append(marshelStrs, str)
		}
	}

	// get table snapshot
	var snapshotRecords []tableRecoveryWindowForSnapshot
	snapshotRecords, err = getTableSnapshotRecords(ctx, ses, bh, accountName, dbName, tblName)
	if err != nil {
		return nil, err
	}
	for _, record := range snapshotRecords {
		str := marshalRcoveryWindowToJsonForSnapshot(record)
		marshelStrs = append(marshelStrs, str)
	}

	if len(marshelStrs) > 0 {
		str := ""
		for i, s := range marshelStrs {
			if i == 0 {
				str = s
			} else {
				str = str + ","
				str += "\n"
				str = str + s
			}
		}
		row := make([]interface{}, 5)
		row[0] = "table"
		row[1] = accountName
		row[2] = dbName
		row[3] = tblName
		row[4] = []byte(str)
		rows = append(rows, row)
	}
	return rows, nil
}

type tableRecoveryWindow struct {
	pitrName              string
	modifiedTime          string
	pitrValue             uint64
	pitrUnit              string
	pitrStatus            uint64 // 1:active 0:inactive
	pitrStatusChangedTime string
}

/*
  marshal recovery window to json format
 {
    "start_time": "2024-11-20 00:00:00",
    "end_time": "2024-11-29 23:59:59",
    "source": "pitr",
    "source_name": "pitr_001"
  },
*/

func marshalRcoveryWindowToJson(records tableRecoveryWindow) (string, error) {
	var startTime string
	var endTime string
	var err error

	startTime, err = getStartTimeOfRecoveryWindow(records)
	if err != nil {
		return "", err
	}
	if len(startTime) == 0 {
		// invalid pitr
		return "", nil
	}

	endTime = getEndTimeOfRecoveryWindow(records)

	// print json string for every field
	jsonStr := fmt.Sprintf(`{"start_time": "%s", "end_time": "%s", "source": "pitr", "source_name": "%s"}`, startTime, endTime, records.pitrName)
	return jsonStr, nil
}

/*
  marshal recovery window to json format
  {
    "timestamp": "2024-11-25 12:00:00",
    "source": "snapshot",
    "source_name": "snapshot_002"
  },
*/

func marshalRcoveryWindowToJsonForSnapshot(records tableRecoveryWindowForSnapshot) string {
	// parser utc timestamp to local time
	ts := records.ts
	t := time.Unix(0, ts)
	tsFormatLocal := t.Local().Format("2006-01-02 15:04:05")
	var jsonStr = fmt.Sprintf(`{"timestamp": "%s", "source": "snapshot", "source_name": "%s"}`, tsFormatLocal, records.snapshotName)
	return jsonStr
}

func getStartTimeOfRecoveryWindow(records tableRecoveryWindow) (string, error) {
	var err error
	var t time.Time
	modifyTime := records.modifiedTime
	// parse createTimeStr to utc time
	t, err = time.ParseInLocation("2006-01-02 15:04:05", modifyTime, time.UTC)
	if err != nil {
		return "", err
	}
	pitrValidMinTs := t.UTC().UnixNano()

	pitrValue := records.pitrValue
	pitrUnit := records.pitrUnit

	var startTs time.Time
	startTs, err = addTimeSpan(int(pitrValue), pitrUnit)
	if err != nil {
		return "", err
	}

	if records.pitrStatus == 0 {
		pitrChangedtime := records.pitrStatusChangedTime
		// parse pitrChangedtime to utc time
		t, err = time.ParseInLocation("2006-01-02 15:04:05", pitrChangedtime, time.UTC)
		if err != nil {
			return "", err
		}
		pitrValidMinTs = t.UTC().UnixNano()
		if startTs.UnixNano() > pitrValidMinTs {
			// invalid pitr
			return "", nil
		} else {
			// return startTs in local time
			return startTs.Local().Format("2006-01-02 15:04:05"), nil
		}
	} else {
		if startTs.UnixNano() > pitrValidMinTs {
			// return startTs in local time
			return startTs.Local().Format("2006-01-02 15:04:05"), nil
		} else {
			return t.Local().Format("2006-01-02 15:04:05"), nil
		}
	}
}

func getEndTimeOfRecoveryWindow(records tableRecoveryWindow) string {
	if records.pitrStatus == 0 {
		return records.pitrStatusChangedTime
	} else {
		// return local time now
		return time.Now().Format("2006-01-02 15:04:05")
	}
}

func getTablePitrRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName, tblName string) ([]tableRecoveryWindow, error) {
	var newCtx = ctx
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if curAccountId != sysAccountID {
		newCtx = defines.AttachAccountId(newCtx, sysAccountID)
	}

	sql := fmt.Sprintf(getTablePitrRecordsFormat, curAccountId, accountName, dbName, tblName, SYSMOCATALOGPITR)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getTablePitrRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(newCtx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err := getResultSet(newCtx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindow, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindow
			if record.pitrName, err = erArray[0].GetString(newCtx, i, 0); err != nil {
				return nil, err
			}
			if record.modifiedTime, err = erArray[0].GetString(newCtx, i, 1); err != nil {
				return nil, err
			}
			if record.pitrValue, err = erArray[0].GetUint64(newCtx, i, 2); err != nil {
				return nil, err
			}
			if record.pitrUnit, err = erArray[0].GetString(newCtx, i, 3); err != nil {
				return nil, err
			}
			if record.pitrStatus, err = erArray[0].GetUint64(newCtx, i, 4); err != nil {
				return nil, err
			}
			if record.pitrStatusChangedTime, err = erArray[0].GetString(newCtx, i, 5); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

type tableRecoveryWindowForSnapshot struct {
	snapshotName string
	ts           int64 // utc timestamp
}

func getTableSnapshotRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName, tblName string) ([]tableRecoveryWindowForSnapshot, error) {
	var erArray []ExecResult
	var err error

	sql := fmt.Sprintf(getTableSnapshotRecordsFormat, accountName, dbName, tblName)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getTableSnapshotRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindowForSnapshot, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindowForSnapshot
			if record.snapshotName, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if record.ts, err = erArray[0].GetInt64(ctx, i, 1); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

func getDbPitrRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName string) ([]tableRecoveryWindow, error) {
	var newCtx = ctx
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if curAccountId != sysAccountID {
		newCtx = defines.AttachAccountId(newCtx, sysAccountID)
	}

	sql := fmt.Sprintf(getDbPitrRecordsFormat, curAccountId, accountName, dbName, SYSMOCATALOGPITR)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getDbPitrRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(newCtx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err := getResultSet(newCtx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindow, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindow
			if record.pitrName, err = erArray[0].GetString(newCtx, i, 0); err != nil {
				return nil, err
			}
			if record.modifiedTime, err = erArray[0].GetString(newCtx, i, 1); err != nil {
				return nil, err
			}
			if record.pitrValue, err = erArray[0].GetUint64(newCtx, i, 2); err != nil {
				return nil, err
			}
			if record.pitrUnit, err = erArray[0].GetString(newCtx, i, 3); err != nil {
				return nil, err
			}
			if record.pitrStatus, err = erArray[0].GetUint64(newCtx, i, 4); err != nil {
				return nil, err
			}
			if record.pitrStatusChangedTime, err = erArray[0].GetString(newCtx, i, 5); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

func getDbSnapshotsRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName, dbName string) ([]tableRecoveryWindowForSnapshot, error) {
	var erArray []ExecResult
	var err error

	sql := fmt.Sprintf(getDbSnapshotRecordsFormat, accountName, dbName)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getDbSnapshotRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindowForSnapshot, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindowForSnapshot
			if record.snapshotName, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if record.ts, err = erArray[0].GetInt64(ctx, i, 1); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

func getAccountPitrRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName string) ([]tableRecoveryWindow, error) {
	var newCtx = ctx
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if curAccountId != sysAccountID {
		newCtx = defines.AttachAccountId(newCtx, sysAccountID)
	}

	sql := fmt.Sprintf(getAccountPitrRecordsFormat, curAccountId, accountName, SYSMOCATALOGPITR)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getAccountPitrRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(newCtx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err := getResultSet(newCtx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindow, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindow
			if record.pitrName, err = erArray[0].GetString(newCtx, i, 0); err != nil {
				return nil, err
			}
			if record.modifiedTime, err = erArray[0].GetString(newCtx, i, 1); err != nil {
				return nil, err
			}
			if record.pitrValue, err = erArray[0].GetUint64(newCtx, i, 2); err != nil {
				return nil, err
			}
			if record.pitrUnit, err = erArray[0].GetString(newCtx, i, 3); err != nil {
				return nil, err
			}
			if record.pitrStatus, err = erArray[0].GetUint64(newCtx, i, 4); err != nil {
				return nil, err
			}
			if record.pitrStatusChangedTime, err = erArray[0].GetString(newCtx, i, 5); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

func getAccountSnapshotRecords(ctx context.Context, ses *Session, bh BackgroundExec, accountName string) ([]tableRecoveryWindowForSnapshot, error) {
	var erArray []ExecResult
	var err error

	sql := fmt.Sprintf(getAccountSnapshotRecordsFormat, accountName)
	getLogger(ses.GetService()).Info(fmt.Sprintf("getAccountSnapshotRecords sql: %s", sql))

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	records := make([]tableRecoveryWindowForSnapshot, 0)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var record tableRecoveryWindowForSnapshot
			if record.snapshotName, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if record.ts, err = erArray[0].GetInt64(ctx, i, 1); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}
	return records, nil
}

func checkShowRecoveryWindowPrivilege(ctx context.Context, ses *Session, srw *tree.ShowRecoveryWindow) error {
	switch srw.Level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
		if len(srw.AccountName) > 0 && !ses.GetTenantInfo().IsSysTenant() {
			return moerr.NewInternalError(ctx, "only sys account can show other account's recovery window")
		}
	case tree.RECOVERYWINDOWLEVELDATABASE:
		dbName := srw.DatabaseName.String()
		if len(dbName) > 0 && needSkipDb(dbName) {
			return moerr.NewInternalError(ctx, "can not show recovery window for system database")
		}
	case tree.RECOVERYWINDOWLEVELTABLE:
		dbName := srw.DatabaseName.String()
		if len(dbName) > 0 && needSkipDb(dbName) {
			return moerr.NewInternalError(ctx, "can not show recovery window for system table")
		}
	}
	return nil
}
