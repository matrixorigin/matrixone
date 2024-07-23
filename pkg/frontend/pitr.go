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

package frontend

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	insertIntoMoPitr = `insert into mo_catalog.mo_pitr(
		pitr_id,
		pitr_name,
		create_account,
		create_time,
		modified_time,
		level,
		account_id,
		account_name,
		database_name,
		table_name,
		obj_id,
		pitr_length,
		pitr_unit ) values ('%s', '%s', %d, '%s', '%s', '%s', %d, '%s', '%s', '%s', %d, %d, '%s');`

	checkPitrFormat = `select pitr_id from mo_catalog.mo_pitr where pitr_name = "%s" and create_account = %d order by pitr_id;`

	dropPitrFormat = `delete from mo_catalog.mo_pitr where pitr_name = '%s' and create_account = %d order by pitr_id;`

	alterPitrFormat = `update mo_catalog.mo_pitr set modified_time = '%s', pitr_length = %d, pitr_unit = '%s' where pitr_name = '%s' and create_account = %d;`
)

func getSqlForCreatePitr(ctx context.Context, pitrId, pitrName string, createAcc uint64, createTime, modifitedTime string, level string, accountId uint64, accountName, databaseName, tableName string, objectId uint64, pitrLength uint8, pitrValue string) (string, error) {
	err := inputNameIsInvalid(ctx, pitrName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(insertIntoMoPitr, pitrId, pitrName, createAcc, createTime, modifitedTime, level, accountId, accountName, databaseName, tableName, objectId, pitrLength, pitrValue), nil
}

func getSqlForCheckPitr(ctx context.Context, pitr string, accountId uint64) (string, error) {
	err := inputNameIsInvalid(ctx, pitr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(checkPitrFormat, pitr, accountId), nil
}

func getSqlForDropPitr(pitrName string, accountId uint64) string {
	return fmt.Sprintf(dropPitrFormat, pitrName, accountId)
}

func getSqlForAlterPitr(modifiedTime string, pitrLength uint8, pitrUnit string, pitrName string, accountId uint64) string {
	return fmt.Sprintf(alterPitrFormat, modifiedTime, pitrLength, pitrUnit, pitrName, accountId)
}

func checkPitrExistOrNot(ctx context.Context, bh BackgroundExec, pitrName string, accountId uint64) (bool, error) {
	var sql string
	var erArray []ExecResult
	var err error
	sql, err = getSqlForCheckPitr(ctx, pitrName, accountId)
	if err != nil {
		return false, err
	}
	var newCtx = ctx
	if accountId != sysAccountID {
		newCtx = defines.AttachAccountId(newCtx, sysAccountID)
	}
	bh.ClearExecResultSet()
	err = bh.Exec(newCtx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(newCtx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

func doCreatePitr(ctx context.Context, ses *Session, stmt *tree.CreatePitr) error {
	var err error
	var pitrLevel tree.PitrLevel
	var pitrForAccount string
	var pitrName string
	var pitrExist bool
	var accountName string
	var databaseName string
	var tableName string
	var sql string

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create pitr priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	// 2.only sys can create cluster level pitr
	tenantInfo := ses.GetTenantInfo()
	currentAccount := tenantInfo.GetTenant()
	pitrLevel = stmt.Level
	if pitrLevel == tree.PITRLEVELCLUSTER && currentAccount != sysAccountName {
		return moerr.NewInternalError(ctx, "only sys tenant can create cluster level pitr")
	}

	// 3.only sys can create tenant level pitr for other tenant
	if pitrLevel == tree.PITRLEVELACCOUNT {
		if len(stmt.AccountName) > 0 && currentAccount != sysAccountName {
			return moerr.NewInternalError(ctx, "only sys tenant can create tenant level pitr for other tenant")
		}
	}

	// 4.check pitr value [0, 100]
	pitrVal := stmt.PitrValue
	if pitrVal <= 0 || pitrVal > 100 {
		return moerr.NewInternalError(ctx, "invalid pitr value %d", pitrVal)
	}

	// 5.check pitr unit
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return moerr.NewInternalError(ctx, "invalid pitr unit %s", pitrUnit)
	}

	// 6.check pitr exists or not
	pitrName = string(stmt.Name)
	pitrExist, err = checkPitrExistOrNot(ctx, bh, pitrName, uint64(tenantInfo.GetTenantID()))
	if err != nil {
		return err
	}
	if pitrExist {
		if !stmt.IfNotExists {
			return moerr.NewInternalError(ctx, "pitr %s already exists", pitrName)
		} else {
			return nil
		}
	}

	// insert record to the system table
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// 1. get pitr id
	newUUid, err := uuid.NewV7()
	if err != nil {
		return err
	}

	// 2. get create account
	createAcc := tenantInfo.GetTenantID()

	switch pitrLevel {
	case tree.PITRLEVELCLUSTER:
		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
			pitrLevel.String(),
			0,
			accountName,
			databaseName,
			tableName,
			0,
			uint8(pitrVal),
			pitrUnit)
		if err != nil {
			return err
		}
	case tree.PITRLEVELACCOUNT:
		// sys create pitr for other account
		if len(stmt.AccountName) > 0 {
			pitrForAccount = string(stmt.AccountName)
			// check account exists or not and get accountId
			getAccountIdFunc := func(accountName string) (accountId uint64, rtnErr error) {
				var erArray []ExecResult
				sql, rtnErr = getSqlForCheckTenant(ctx, accountName)
				if rtnErr != nil {
					return 0, rtnErr
				}
				bh.ClearExecResultSet()
				rtnErr = bh.Exec(ctx, sql)
				if rtnErr != nil {
					return 0, rtnErr
				}

				erArray, rtnErr = getResultSet(ctx, bh)
				if rtnErr != nil {
					return 0, rtnErr
				}

				if execResultArrayHasData(erArray) {
					for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
						accountId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
						if rtnErr != nil {
							return 0, rtnErr
						}
					}
				} else {
					return 0, moerr.NewInternalError(ctx, "account %s does not exist", accountName)
				}
				return accountId, rtnErr
			}

			accountId, rntErr := getAccountIdFunc(pitrForAccount)
			if rntErr != nil {
				return rntErr
			}

			sql, err = getSqlForCreatePitr(
				ctx,
				newUUid.String(),
				pitrName,
				uint64(createAcc),
				types.CurrentTimestamp().String2(time.UTC, 0),
				types.CurrentTimestamp().String2(time.UTC, 0),
				pitrLevel.String(),
				accountId,
				pitrForAccount,
				databaseName,
				tableName,
				accountId,
				uint8(pitrVal),
				pitrUnit)
			if err != nil {
				return err
			}
		} else {
			// create pitr for current account
			sql, err = getSqlForCreatePitr(
				ctx,
				newUUid.String(),
				pitrName,
				uint64(createAcc),
				types.CurrentTimestamp().String2(time.UTC, 0),
				types.CurrentTimestamp().String2(time.UTC, 0),
				pitrLevel.String(),
				uint64(createAcc),
				currentAccount,
				databaseName,
				tableName,
				uint64(createAcc),
				uint8(pitrVal),
				pitrUnit)
			if err != nil {
				return err
			}
		}

	case tree.PITRLEVELDATABASE:
		// create database level pitr for current account
		databaseName = string(stmt.DatabaseName)
		getDatabaseIdFunc := func(dbName string) (dbId uint64, rtnErr error) {
			var erArray []ExecResult
			sql, rtnErr = getSqlForCheckDatabase(ctx, dbName)
			if rtnErr != nil {
				return 0, rtnErr
			}
			bh.ClearExecResultSet()
			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return 0, rtnErr
			}

			erArray, rtnErr = getResultSet(ctx, bh)
			if rtnErr != nil {
				return 0, rtnErr
			}

			if execResultArrayHasData(erArray) {
				for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
					dbId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
					if rtnErr != nil {
						return 0, rtnErr
					}
				}
			} else {
				return 0, moerr.NewInternalError(ctx, "database %s does not exist", dbName)
			}
			return dbId, rtnErr
		}
		dbId, rtnErr := getDatabaseIdFunc(databaseName)
		if rtnErr != nil {
			return rtnErr
		}

		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
			pitrLevel.String(),
			uint64(createAcc),
			currentAccount,
			databaseName,
			tableName,
			dbId,
			uint8(pitrVal),
			pitrUnit)

		if err != nil {
			return err
		}

	case tree.PITRLEVELTABLE:
		// create table level pitr for current account
		databaseName = string(stmt.DatabaseName)
		tableName = string(stmt.TableName)
		getTableIdFunc := func(dbName, tblName string) (tblId uint64, rtnErr error) {
			var erArray []ExecResult
			sql, rtnErr = getSqlForCheckDatabaseTable(ctx, dbName, tblName)
			if rtnErr != nil {
				return 0, rtnErr
			}
			bh.ClearExecResultSet()
			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return 0, rtnErr
			}

			erArray, rtnErr = getResultSet(ctx, bh)
			if rtnErr != nil {
				return 0, rtnErr
			}

			if execResultArrayHasData(erArray) {
				for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
					tblId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
					if rtnErr != nil {
						return 0, rtnErr
					}
				}
			} else {
				return 0, moerr.NewInternalError(ctx, "table %s does not exist", tblName)
			}
			return tblId, rtnErr
		}
		tblId, rtnErr := getTableIdFunc(databaseName, tableName)
		if rtnErr != nil {
			return rtnErr
		}

		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
			pitrLevel.String(),
			uint64(createAcc),
			currentAccount,
			databaseName,
			tableName,
			tblId,
			uint8(pitrVal),
			pitrUnit)

		if err != nil {
			return err
		}
	}

	// execute sql
	// chenge to sys tenant
	if currentAccount != sysAccountName {
		ctx = defines.AttachAccountId(ctx, sysAccountID)
		defer func() {
			ctx = defines.AttachAccountId(ctx, tenantInfo.GetTenantID())
		}()
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return err
}

func doDropPitr(ctx context.Context, ses *Session, stmt *tree.DropPitr) (err error) {
	var sql string
	var pitrExist bool
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check drop pitr priv
	// only admin can drop pitr for himself
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check pitr exists or not
	tenantInfo := ses.GetTenantInfo()
	pitrExist, err = checkPitrExistOrNot(ctx, bh, string(stmt.Name), uint64(tenantInfo.GetTenantID()))
	if err != nil {
		return err
	}

	if !pitrExist {
		if !stmt.IfExists {
			return moerr.NewInternalError(ctx, "pitr %s does not exist", string(stmt.Name))
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForDropPitr(
			string(stmt.Name),
			uint64(tenantInfo.GetTenantID()))

		if tenantInfo.GetTenant() != sysAccountName {
			ctx = defines.AttachAccountId(ctx, sysAccountID)
			defer func() {
				ctx = defines.AttachAccountId(ctx, tenantInfo.GetTenantID())
			}()
		}
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

func doAlterPitr(ctx context.Context, ses *Session, stmt *tree.AlterPitr) (err error) {
	var sql string
	var pitrExist bool
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check alter pitr priv
	// only admin can alter pitr for himself
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	// check pitr value
	if stmt.PitrValue < 0 || stmt.PitrValue > 100 {
		return moerr.NewInternalError(ctx, "invalid pitr value %d", stmt.PitrValue)
	}

	// check pitr unit
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return moerr.NewInternalError(ctx, "invalid pitr unit %s", pitrUnit)
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	if err != nil {
		return err
	}

	// check pitr exists or not
	tenantInfo := ses.GetTenantInfo()
	pitrExist, err = checkPitrExistOrNot(ctx, bh, string(stmt.Name), uint64(tenantInfo.GetTenantID()))
	if err != nil {
		return err
	}

	if !pitrExist {
		if !stmt.IfExists {
			return moerr.NewInternalError(ctx, "pitr %s does not exist", string(stmt.Name))
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForAlterPitr(types.CurrentTimestamp().String2(time.UTC, 0),
			uint8(stmt.PitrValue),
			pitrUnit,
			string(stmt.Name),
			uint64(tenantInfo.GetTenantID()))

		if tenantInfo.GetTenant() != sysAccountName {
			ctx = defines.AttachAccountId(ctx, sysAccountID)
			defer func() {
				ctx = defines.AttachAccountId(ctx, tenantInfo.GetTenantID())
			}()
		}

		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

func getPitrRecords(ctx context.Context, bh BackgroundExec, sql string) ([]*pitrRecord, error) {
	var erArray []ExecResult
	var err error

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	if erArray, err = getResultSet(ctx, bh); err != nil {
		return nil, err
	}

	var records []*pitrRecord
	if execResultArrayHasData(erArray) {
		for _, er := range erArray {
			var record pitrRecord
			for row := uint64(0); row < er.GetRowCount(); row++ {
				if record.pitrId, err = er.GetString(ctx, row, 0); err != nil {
					return nil, err
				}
				if record.pitrName, err = er.GetString(ctx, row, 1); err != nil {
					return nil, err
				}
				if record.createAccount, err = er.GetUint64(ctx, row, 2); err != nil {
					return nil, err
				}
				if record.createTime, err = er.GetString(ctx, row, 3); err != nil {
					return nil, err
				}
				if record.modifiedTime, err = er.GetString(ctx, row, 4); err != nil {
					return nil, err
				}
				if record.level, err = er.GetString(ctx, row, 5); err != nil {
					return nil, err
				}
				if record.accountId, err = er.GetUint64(ctx, row, 6); err != nil {
					return nil, err
				}
				if record.accountName, err = er.GetString(ctx, row, 7); err != nil {
					return nil, err
				}
				if record.databaseName, err = er.GetString(ctx, row, 8); err != nil {
					return nil, err
				}
				if record.tableName, err = er.GetString(ctx, row, 9); err != nil {
					return nil, err
				}
				if record.objId, err = er.GetUint64(ctx, row, 10); err != nil {
					return nil, err
				}
				if record.pitrValue, err = er.GetUint64(ctx, row, 11); err != nil {
					return nil, err
				}
				if record.pitrUnit, err = er.GetString(ctx, row, 12); err != nil {
					return nil, err
				}
			}
			records = append(records, &record)
		}
		return records, nil
	}
	return nil, err
}

func getPitrByName(ctx context.Context, bh BackgroundExec, pitrName string, accountId uint64) (*pitrRecord, error) {
	if err := inputNameIsInvalid(ctx, pitrName); err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("%s where pitr_name = '%s' and create_account = %d", getPitrFormat, pitrName, accountId)
	if records, err := getPitrRecords(ctx, bh, sql); err != nil {
		return nil, err
	} else if len(records) != 1 {
		return nil, moerr.NewInternalError(ctx, "find %v pitr records by name(%v), expect only 1", len(records), pitrName)
	} else {
		return records[0], nil
	}
}

// change string timeStamp which is local time to utc timeStamp
func doResolveTimeStamp(timeStamp string) (ts int64, err error) {
	loc, err := time.LoadLocation("Local")
	if err != nil {
		return
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05", timeStamp, loc)
	if err != nil {
		return
	}
	ts = t.UTC().UnixNano()
	return
}

// check the ts is valid or not
// @param ts: the timestamp
// @param pitrRecord: the pitr record
// if the ts less than the duration of the pitr
// then return an error
func checkPitrInValidDurtion(ts int64, pitrRecord *pitrRecord) (err error) {
	// use utc time now sub pitr during time get the minest time
	// is ts time less than the minest time, then return error
	pitrValue := pitrRecord.pitrValue
	pitrUnit := pitrRecord.pitrUnit

	minTs, err := addTimeSpan(int(pitrValue), pitrUnit)
	if err != nil {
		return err
	}

	if ts < minTs.UnixNano() {
		return fmt.Errorf("ts %v is less than the minest time %v", ts, minTs.Unix())
	}

	return
}

// get pitr time span
func addTimeSpan(length int, unit string) (time.Time, error) {
	now := time.Now().UTC()

	switch unit {
	case "h":
		return now.Add(time.Duration(-length) * time.Hour), nil
	case "d":
		return now.Add(time.Duration(-length) * 24 * time.Hour), nil
	case "mo":
		return now.AddDate(0, -length, 0), nil
	case "y":
		return now.AddDate(-length, 0, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unknown unit '%s'", unit)
	}
}

/// addtion

func deleteCurFkTableInPitrRestore(ctx context.Context, sid string, bh BackgroundExec, dbName string, tblName string) (err error) {
	getLogger(sid).Info("[pitr restore] start to drop cur fk tables")

	// get topo sorted tables with foreign key
	sortedFkTbls, err := fkTablesTopoSort(ctx, sid, bh, "", dbName, tblName)
	if err != nil {
		return
	}
	// collect table infos which need to be dropped in current state; snapshotName must set to empty
	curFkTableMap, err := getTableInfoMap(ctx, sid, bh, "", dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// drop tables as anti-topo order
	for i := len(sortedFkTbls) - 1; i >= 0; i-- {
		key := sortedFkTbls[i]
		if tblInfo := curFkTableMap[key]; tblInfo != nil {
			getLogger(sid).Info(fmt.Sprintf("start to drop table: %v", tblInfo.tblName))
			if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s.%s", tblInfo.dbName, tblInfo.tblName)); err != nil {
				return
			}
		}
	}
	return
}

func fkTablesTopoSortInPitrRestore(
	ctx context.Context,
	bh BackgroundExec,
	ts int64,
	dbName string,
	tblName string) (sortedTbls []string, err error) {
	// get foreign key deps from mo_catalog.mo_foreign_keys
	fkDeps, err := getFkDepsInPitrRestore(ctx, bh, ts, dbName, tblName)
	if err != nil {
		return
	}

	g := topsort{next: make(map[string][]string)}
	for key, deps := range fkDeps {
		g.addVertex(key)
		for _, depTbl := range deps {
			// exclude self dep
			if key != depTbl {
				g.addEdge(depTbl, key)
			}
		}
	}
	sortedTbls, err = g.sort()
	return
}

func getFkDepsInPitrRestore(
	ctx context.Context,
	bh BackgroundExec,
	ts int64,
	dbName string,
	tblName string) (ans map[string][]string, err error) {
	sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}
	if len(dbName) > 0 {
		sql += fmt.Sprintf(" where db_name = '%s'", dbName)
		if len(tblName) > 0 {
			sql += fmt.Sprintf(" and table_name = '%s'", tblName)
		}
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	ans = make(map[string][]string)
	var referDbName, referTblName string

	for _, rs := range resultSet {
		for row := uint64(0); row < rs.GetRowCount(); row++ {
			if dbName, err = rs.GetString(ctx, row, 0); err != nil {
				return
			}
			if tblName, err = rs.GetString(ctx, row, 1); err != nil {
				return
			}
			if referDbName, err = rs.GetString(ctx, row, 2); err != nil {
				return
			}
			if referTblName, err = rs.GetString(ctx, row, 3); err != nil {
				return
			}

			u := genKey(dbName, tblName)
			v := genKey(referDbName, referTblName)
			ans[u] = append(ans[u], v)
		}
	}
	return
}

func getTableInfoMapInPitrRestore(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string,
	tblKeys []string) (tblInfoMap map[string]*tableInfo, err error) {
	tblInfoMap = make(map[string]*tableInfo)
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	for _, key := range tblKeys {
		if _, ok := tblInfoMap[key]; ok {
			return
		}

		// filter by dbName and tblName
		d, t := splitKey(key)
		if needSkipDb(d) || needSkipTable(curAccountId, d, t) {
			continue
		}
		if dbName != "" && dbName != d {
			continue
		}
		if tblName != "" && tblName != t {
			continue
		}

		if tblInfoMap[key], err = getTableInfoInPitrRestore(ctx, sid, bh, pitrName, ts, d, t); err != nil {
			return
		}
	}
	return
}

func getTableInfoInPitrRestore(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName,
	tblName string) (*tableInfo, error) {
	tableInfos, err := getTableInfoWithPitr(ctx,
		sid,
		bh,
		pitrName,
		ts,
		dbName,
		tblName)
	if err != nil {
		return nil, err
	}

	// if table doesn't exist, return nil
	if len(tableInfos) == 0 {
		return nil, nil
	}
	return tableInfos[0], nil
}
