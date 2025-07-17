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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	nsTimeFormat = "2006-01-02 15:04:05.999999999"
)

type pitrItem struct {
	start string
	end   string
	name  string
}

type snapItem struct {
	ts   string
	name string
}

func doShowRecoveryWindow(
	ctx context.Context,
	ses *Session,
	srw *tree.ShowRecoveryWindow,
) (err error) {

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	var (
		opAccount uint32

		level tree.RecoveryWindowLevel

		tableName    string
		databaseName string
		accountName  string

		rows = make([][]interface{}, 0)
	)

	level = srw.Level
	tableName = srw.TableName.String()
	accountName = srw.AccountName.String()
	databaseName = srw.DatabaseName.String()

	if accountName == "" {
		accountName = ses.GetTenantName()
	}

	if opAccount, err = defines.GetAccountId(ctx); err != nil {
		return err
	}

	// check privilege
	err = checkShowRecoveryWindowPrivilege(ctx, ses, srw, opAccount)
	if err != nil {
		return err
	}

	// build result columns
	// recovery window level
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("RecoveryWindowLevel")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("AccountName")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("DatabaseName")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("TableName")

	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_JSON)
	col5.SetName("RecoveryWindows")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)

	if rows, err = constructRecoveryWindow(
		ctx, ses, bh, level,
		tableName, databaseName, accountName,
		opAccount,
	); err != nil {
		return err
	}

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return trySaveQueryResult(ctx, ses, mrs)
}

func constructRecoveryWindow(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	level tree.RecoveryWindowLevel,
	tableName string,
	databaseName string,
	accountName string,
	opAccount uint32,
) (windowRows [][]interface{}, err error) {

	var (
		levelStr string

		snapSearchSQL string
		pitrSearchSQL string

		snapPitrSearchCond string
	)

	switch level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
		levelStr = "account"

		snapPitrSearchCond = fmt.Sprintf(
			"account_name = '%s'",
			accountName,
		)
	case tree.RECOVERYWINDOWLEVELDATABASE:
		levelStr = "database"

		snapPitrSearchCond = fmt.Sprintf(
			"(account_name = '%s' AND database_name = '') OR "+
				"(account_name = '%s' AND database_name = '%s')",
			accountName,
			accountName, databaseName,
		)
	case tree.RECOVERYWINDOWLEVELTABLE:
		levelStr = "table"

		snapPitrSearchCond = fmt.Sprintf(
			"(account_name = '%s' AND database_name = '') OR "+
				"(account_name = '%s' AND database_name = '%s' AND table_name = '') OR "+
				"(account_name = '%s' AND database_name = '%s' AND table_name = '%s')",
			accountName,
			accountName, databaseName,
			accountName, databaseName, tableName,
		)

	default:
		return nil, moerr.NewInternalErrorNoCtxf("show recovery window level does not exist")
	}

	snapSearchSQL = fmt.Sprintf(
		"select sname, level, account_name, database_name, table_name, ts from `%s`.`%s` where %s",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, snapPitrSearchCond,
	)

	pitrSearchSQL = fmt.Sprintf(
		"select "+
			"	pitr_name, level, account_name, database_name, table_name, "+
			"	modified_time, pitr_length, pitr_unit, pitr_status, pitr_status_changed_time "+
			"from "+
			"`%s`.`%s` "+
			"	where %s AND pitr_name != '%s'",
		catalog.MO_CATALOG, catalog.MO_PITR, snapPitrSearchCond, SYSMOCATALOGPITR,
	)

	var (
		pitrRecords []tableRecoveryWindow
		snapRecords []tableRecoveryWindowForSnapshot

		rows [][]interface{}

		tableToSnaps map[[3]string][]snapItem
		tableToPitrs map[[3]string][]pitrItem
	)

	if snapRecords, err = execSnapSearchSQL(ctx, ses, bh, snapSearchSQL); err != nil {
		return nil, err
	}

	newCtx := defines.AttachAccountId(ctx, sysAccountID)
	if pitrRecords, err = execPitrSearchSQL(newCtx, ses, bh, pitrSearchSQL); err != nil {
		return nil, err
	}

	if tableToSnaps, tableToPitrs, err = searchTables(
		ctx, ses, bh, level,
		accountName, databaseName, tableName,
		pitrRecords, snapRecords,
		opAccount,
	); err != nil {
		return nil, err
	}

	var (
		row  []interface{}
		keys = make(map[[3]string]struct{})

		jsonBuf bytes.Buffer
	)

	for key := range tableToSnaps {
		keys[key] = struct{}{}
	}

	for key := range tableToPitrs {
		keys[key] = struct{}{}
	}

	for val := range keys {
		row = make([]interface{}, 5)

		row[0] = levelStr
		row[1] = val[0]
		row[2] = val[1]
		row[3] = val[2]

		jsonBuf.Reset()
		jsonBuf.WriteString("[")

		for _, snap := range tableToSnaps[val] {
			jsonBuf.WriteString(
				fmt.Sprintf(
					"{'timestamp':'%s', 'source':'snapshot', 'source_name':'%s'}, ",
					snap.ts, snap.name,
				))
		}

		for _, pitr := range tableToPitrs[val] {
			jsonBuf.WriteString(
				fmt.Sprintf(
					"{'start_time':'%s', 'end_time':'%s', 'source':'pitr', 'source_name':'%s'}, ",
					pitr.start, pitr.end, pitr.name,
				))
		}

		jsonBuf.WriteString("]")

		row[4] = []byte(jsonBuf.String())

		rows = append(rows, row)
	}

	return rows, nil
}

func searchTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	level tree.RecoveryWindowLevel,
	accountName string,
	databaseName string,
	tableName string,
	pitrsRecords []tableRecoveryWindow,
	snapRecords []tableRecoveryWindowForSnapshot,
	opAccount uint32,
) (
	tableToSnaps map[[3]string][]snapItem,
	tableToPitrs map[[3]string][]pitrItem,
	err error,
) {

	var (
		newCtx context.Context

		sqlRet []ExecResult

		searchSQL string

		searchCondA string
		searchCondB string
		searchCondC string

		fromAccount    uint32
		needUpdateFrom = true
	)

	skipCond := ""
	for i, db := range skipDbs {
		skipCond += fmt.Sprintf("'%s'", db)
		if i != len(skipDbs)-1 {
			skipCond += ","
		}
	}

	searchCondA = fmt.Sprintf(" reldatabase not in (%s)", skipCond)

	// if the account name doesn't equal to the login acc,
	// this query must be `show recovery_window for acc` in the sys account.
	// if so, we cannot ensure that the account we want to show keep unchanged all the time.(delete, recreate...),
	//  that's why we need to query the account id again and again when attaching different TS.
	if accountName == ses.GetTenantName() {
		needUpdateFrom = false
		fromAccount = opAccount
	}

	makeCondB := func(ts int64) error {
		if needUpdateFrom {
			if fromAccount, err = getAccountIdByTS(ctx, bh, accountName, ts); err != nil {
				return err
			}
		}

		switch level {
		case tree.RECOVERYWINDOWLEVELACCOUNT:
			searchCondB = fmt.Sprintf(
				" AND account_id = %d",
				fromAccount,
			)
		case tree.RECOVERYWINDOWLEVELDATABASE:
			searchCondB = fmt.Sprintf(
				" AND account_id = %d AND reldatabase = '%s'",
				fromAccount, databaseName,
			)
		case tree.RECOVERYWINDOWLEVELTABLE:
			searchCondB = fmt.Sprintf(
				" AND account_id = %d AND reldatabase = '%s' AND relname = '%s'",
				fromAccount, databaseName, tableName,
			)
		default:
			return moerr.NewInternalErrorNoCtxf("show recovery window level does not exist")
		}

		return nil
	}

	var (
		endStr   string
		startStr string
		startTS  int64
	)

	tableToSnaps = make(map[[3]string][]snapItem)
	tableToPitrs = make(map[[3]string][]pitrItem)

	newCtx = defines.AttachAccountId(ctx, opAccount)
	for _, snap := range snapRecords {
		ts := timestamp.Timestamp{PhysicalTime: snap.ts}

		if err = makeCondB(snap.ts); err != nil {
			return
		}

		// already table level, no need to query mo_tables.
		if snap.level == "table" {
			key := [3]string{accountName, snap.databaseName, snap.tableName}
			tableToSnaps[key] = append(tableToSnaps[key], snapItem{
				name: snap.snapshotName,
				ts:   ts.ToStdTime().String(),
			})
			continue

		} else if snap.level == "database" {
			searchCondC = fmt.Sprintf(
				" AND reldatabase = '%s'",
				snap.databaseName,
			)

		} else if snap.level == "account" {
			searchCondC = fmt.Sprintf(
				" AND account_id = %d",
				fromAccount,
			)

		} else {
			return nil, nil, moerr.NewInternalErrorNoCtxf("unknow snapshot level")
		}

		searchSQL = fmt.Sprintf(
			"select reldatabase, relname from `%s`.`%s` {SNAPSHOT = '%s'} where %s",
			catalog.MO_CATALOG, catalog.MO_TABLES, snap.snapshotName,
			searchCondA+searchCondB+searchCondC,
		)

		bh.ClearExecResultSet()
		if err = bh.Exec(newCtx, searchSQL); err != nil {
			return
		}

		if sqlRet, err = getResultSet(newCtx, bh); err != nil {
			return
		}

		if execResultArrayHasData(sqlRet) {
			for i := uint64(0); i < sqlRet[0].GetRowCount(); i++ {
				if databaseName, err = sqlRet[0].GetString(newCtx, i, 0); err != nil {
					return
				}

				if tableName, err = sqlRet[0].GetString(newCtx, i, 1); err != nil {
					return
				}

				key := [3]string{accountName, databaseName, tableName}
				tableToSnaps[key] = append(tableToSnaps[key], snapItem{
					name: snap.snapshotName,
					ts:   ts.ToStdTime().String(),
				})
			}
		}
	}

	for _, pitr := range pitrsRecords {
		if startStr, err = getStartTimeOfRecoveryWindowInLoc(pitr); err != nil {
			return
		}

		endStr = getEndTimeOfRecoveryWindowInLoc(pitr)

		if startTS, err = doResolveTimeStamp(startStr); err != nil {
			return
		}

		if err = makeCondB(startTS); err != nil {
			return
		}

		if pitr.level == "table" {
			key := [3]string{accountName, pitr.databaseName, pitr.tableName}
			tableToPitrs[key] = append(tableToPitrs[key], pitrItem{
				name:  pitr.pitrName,
				end:   endStr,
				start: startStr,
			})

			continue

		} else if pitr.level == "database" {
			searchCondC = fmt.Sprintf(
				" AND reldatabase = '%s'",
				pitr.databaseName,
			)

		} else if pitr.level == "account" {
			searchCondC = fmt.Sprintf(
				" AND account_id = %d",
				fromAccount,
			)

		} else {
			return nil, nil, moerr.NewInternalErrorNoCtxf("unknow snapshot level")
		}

		searchSQL = fmt.Sprintf(
			"select reldatabase, relname from `%s`.`%s` {MO_TS = %d} where %s",
			catalog.MO_CATALOG, catalog.MO_TABLES, startTS,
			searchCondA+searchCondB+searchCondC,
		)

		bh.ClearExecResultSet()
		if err = bh.Exec(newCtx, searchSQL); err != nil {
			return
		}

		if sqlRet, err = getResultSet(newCtx, bh); err != nil {
			return
		}

		if execResultArrayHasData(sqlRet) {
			for i := uint64(0); i < sqlRet[0].GetRowCount(); i++ {
				if databaseName, err = sqlRet[0].GetString(newCtx, i, 0); err != nil {
					return
				}

				if tableName, err = sqlRet[0].GetString(newCtx, i, 1); err != nil {
					return
				}

				key := [3]string{accountName, databaseName, tableName}
				tableToPitrs[key] = append(tableToPitrs[key], pitrItem{
					name:  pitr.pitrName,
					end:   endStr,
					start: startStr,
				})
			}
		}
	}

	return
}

func execPitrSearchSQL(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	searchSQL string,
) (records []tableRecoveryWindow, err error) {

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, searchSQL); err != nil {
		return nil, err
	}

	var (
		record tableRecoveryWindow
		sqlRet []ExecResult
	)

	if sqlRet, err = getResultSet(ctx, bh); err != nil {
		return nil, err
	}

	records = make([]tableRecoveryWindow, 0)
	if execResultArrayHasData(sqlRet) {
		for i := uint64(0); i < sqlRet[0].GetRowCount(); i++ {
			if record.pitrName, err = sqlRet[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}

			if record.level, err = sqlRet[0].GetString(ctx, i, 1); err != nil {
				return nil, err
			}

			if record.accountName, err = sqlRet[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}

			if record.databaseName, err = sqlRet[0].GetString(ctx, i, 3); err != nil {
				return nil, err
			}

			if record.tableName, err = sqlRet[0].GetString(ctx, i, 4); err != nil {
				return nil, err
			}

			if record.modifiedTime, err = sqlRet[0].GetInt64(ctx, i, 5); err != nil {
				return nil, err
			}

			if record.pitrValue, err = sqlRet[0].GetUint64(ctx, i, 6); err != nil {
				return nil, err
			}

			if record.pitrUnit, err = sqlRet[0].GetString(ctx, i, 7); err != nil {
				return nil, err
			}

			if record.pitrStatus, err = sqlRet[0].GetUint64(ctx, i, 8); err != nil {
				return nil, err
			}

			if record.pitrStatusChangedTime, err = sqlRet[0].GetInt64(ctx, i, 9); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}

	return records, nil
}

func execSnapSearchSQL(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	searchSQL string,
) (records []tableRecoveryWindowForSnapshot, err error) {

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, searchSQL); err != nil {
		return nil, err
	}

	var (
		sqlRet []ExecResult
		record tableRecoveryWindowForSnapshot
	)

	if sqlRet, err = getResultSet(ctx, bh); err != nil {
		return nil, err
	}

	records = make([]tableRecoveryWindowForSnapshot, 0)

	if execResultArrayHasData(sqlRet) {
		for i := uint64(0); i < sqlRet[0].GetRowCount(); i++ {
			if record.snapshotName, err = sqlRet[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}

			if record.level, err = sqlRet[0].GetString(ctx, i, 1); err != nil {
				return nil, err
			}

			if record.accountName, err = sqlRet[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}

			if record.databaseName, err = sqlRet[0].GetString(ctx, i, 3); err != nil {
				return nil, err
			}

			if record.tableName, err = sqlRet[0].GetString(ctx, i, 4); err != nil {
				return nil, err
			}

			if record.ts, err = sqlRet[0].GetInt64(ctx, i, 5); err != nil {
				return nil, err
			}

			records = append(records, record)
		}
	}

	return records, nil
}

func checkShowRecoveryWindowPrivilege(
	ctx context.Context,
	ses *Session,
	srw *tree.ShowRecoveryWindow,
	opAccount uint32,
) (err error) {

	switch srw.Level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
		if len(srw.AccountName) > 0 && opAccount != sysAccountID {
			loginAcc := ses.GetTenantName()
			if srw.AccountName.String() != loginAcc {
				return moerr.NewInternalError(ctx, "only sys account can show other account's recovery window")
			}
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
	default:
		return moerr.NewInternalError(ctx, "unknown recovery window level")
	}
	return nil
}

type tableRecoveryWindow struct {
	pitrName              string
	level                 string
	modifiedTime          int64
	pitrValue             uint64
	pitrUnit              string
	pitrStatus            uint64 // 1:active 0:inactive
	pitrStatusChangedTime int64
	accountName           string
	databaseName          string
	tableName             string
}

type tableRecoveryWindowForSnapshot struct {
	snapshotName string
	level        string
	ts           int64 // utc timestamp
	accountName  string
	databaseName string
	tableName    string
}

func getStartTimeOfRecoveryWindowInLoc(record tableRecoveryWindow) (string, error) {
	var (
		err error

		objChangeInUTC time.Time

		validMinTS int64
		validMaxTS int64

		rangeStartFromNow    time.Time
		rangeStartFromChange time.Time
	)

	//      valid minTS				 valid maxTS
	//           |--------valid---------|
	// |---------|----------------------|------------------------|---
	//      pitr create/modify 		drop db/table (obj change)
	//															now
	//	                                          <---------------
	//											     pitr range

	validMinTS = record.modifiedTime
	validMaxTS = record.pitrStatusChangedTime

	objChangeInUTC = time.Unix(0, record.pitrStatusChangedTime).UTC()

	if rangeStartFromNow, err = addTimeSpan(
		time.Time{}, int(record.pitrValue), record.pitrUnit,
	); err != nil {
		return "", err
	}

	startTS := rangeStartFromNow

	// the account/database/table may be dropped
	if record.pitrStatus == 0 {
		if rangeStartFromNow.UnixNano() > validMaxTS {
			// exceeds the recovery range
			return "", nil
		}

		// the range start from the changed TS is valid only when the status has changed
		if rangeStartFromChange, err = addTimeSpan(
			objChangeInUTC, int(record.pitrValue), record.pitrUnit,
		); err != nil {
			return "", err
		}

		startTS = rangeStartFromChange
	}

	if startTS.UnixNano() < validMinTS {
		return time.Unix(0, validMinTS).Local().Format(nsTimeFormat), nil
	}

	return startTS.Local().Format(nsTimeFormat), nil
}

func getEndTimeOfRecoveryWindowInLoc(record tableRecoveryWindow) string {
	if record.pitrStatus == 0 {
		return time.Unix(0, record.pitrStatusChangedTime).Local().Format(nsTimeFormat)
	} else {
		// return local time now
		return time.Now().Local().Format(nsTimeFormat)
	}
}
