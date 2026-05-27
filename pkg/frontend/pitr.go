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
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
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
		pitr_unit,
        pitr_status_changed_time) values ('%s', '%s', %d, %d, %d, '%s', %d, '%s', '%s', '%s', %d, %d, '%s', %d);`

	checkPitrFormat = `select pitr_id from mo_catalog.mo_pitr where pitr_name = "%s" and create_account = %d order by pitr_id;`

	dropPitrFormat = `delete from mo_catalog.mo_pitr where pitr_name = '%s' and create_account = %d order by pitr_id;`

	alterPitrFormat = `update mo_catalog.mo_pitr set modified_time = %d, pitr_length = %d, pitr_unit = '%s' where pitr_name = '%s' and create_account = %d;`

	getPitrFormat = `select * from mo_catalog.mo_pitr`

	checkDupPitrFormat = `select pitr_id from mo_catalog.mo_pitr where create_account = %d and obj_id = %d;`

	getSqlForCheckDatabaseFmt = `select dat_id from mo_catalog.mo_database {MO_TS = %d} where datname = '%s';`

	getSqlForCheckTableFmt = `select rel_id from mo_catalog.mo_tables {MO_TS = %d} where reldatabase = '%s' and relname = '%s';`

	getSqlForCheckAccountFmt = `select account_id from mo_catalog.mo_account {MO_TS = %d} where account_name = '%s';`

	getPubInfoWithPitrFormat = `select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs {MO_TS = %d} where account_id = %d and database_name = '%s';`

	// update mo_pitr object id
	updateMoPitrAccountObjectIdFmt = `update mo_catalog.mo_pitr set pitr_status = 0, pitr_status_changed_time = %d where account_name = '%s' and pitr_status = 1 and obj_id = %d;`

	getLengthAndUnitFmt = `select pitr_length, pitr_unit from mo_catalog.mo_pitr where account_id = %d and level = '%s'`
)

type pitrRecord struct {
	pitrId        string
	pitrName      string
	createAccount uint64
	createTime    int64
	modifiedTime  int64
	level         string
	accountId     uint64
	accountName   string
	databaseName  string
	tableName     string
	objId         uint64
	pitrValue     uint64
	pitrUnit      string
}

const (
	SYSMOCATALOGPITR = "sys_mo_catalog_pitr"
)

type restorePitrAccounts struct {
	sourceName string
	targetName string
	sourceID   uint32
	targetID   uint32
}

func normalizeRestorePitrStmt(stmt *tree.RestorePitr) {
	if len(stmt.SrcAccountName) == 0 || len(stmt.ToAccountName) > 0 {
		return
	}

	if len(stmt.AccountName) > 0 {
		stmt.ToAccountName = stmt.AccountName
	}
	stmt.AccountName = stmt.SrcAccountName
	stmt.SrcAccountName = ""
}

func getSqlForCreatePitr(
	ctx context.Context,
	pitrId, pitrName string,
	createAcc uint64,
	createTime int64,
	level string,
	accountId uint64,
	accountName, databaseName, tableName string,
	objectId uint64,
	pitrLength uint8,
	pitrValue string,

) (string, error) {

	err := inputNameIsInvalid(ctx, pitrName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		insertIntoMoPitr,
		pitrId, pitrName, createAcc,
		createTime, createTime,
		level, accountId, accountName,
		databaseName, tableName, objectId,
		pitrLength, pitrValue, createTime,
	), nil
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

func getSqlForAlterPitr(modifiedTime int64, pitrLength uint8, pitrUnit string, pitrName string, accountId uint64) string {
	return fmt.Sprintf(alterPitrFormat, modifiedTime, pitrLength, pitrUnit, pitrName, accountId)
}

func getSqlForCheckDatabaseWithPitr(ctx context.Context, ts int64, dbName string) (string, error) {
	err := inputNameIsInvalid(ctx, dbName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getSqlForCheckDatabaseFmt, ts, dbName), nil
}

func getSqlForCheckTableWithPitr(ctx context.Context, ts int64, dbName, tblName string) (string, error) {
	err := inputNameIsInvalid(ctx, dbName)
	if err != nil {
		return "", err
	}
	err = inputNameIsInvalid(ctx, tblName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getSqlForCheckTableFmt, ts, dbName, tblName), nil
}

func getSqlForCheckAccountWithPitr(ctx context.Context, ts int64, accountName string) (string, error) {
	err := inputNameIsInvalid(ctx, accountName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getSqlForCheckAccountFmt, ts, accountName), nil
}

func getSqlForCheckDupPitrFormat(accountId, objId uint64) string {
	return fmt.Sprintf(checkDupPitrFormat, accountId, objId)
}

func getPubInfoWithPitr(ts int64, accountId uint32, dbName string) string {
	return fmt.Sprintf(getPubInfoWithPitrFormat, ts, accountId, dbName)
}

func getSqlForUpdateMoPitrAccountObjectId(accountName string, objId uint64, ts int64) string {
	return fmt.Sprintf(updateMoPitrAccountObjectIdFmt, ts, accountName, objId)
}

func getSqlForGetLengthAndUnitFmt(accountId uint32, level, accName, dbName, tblName string) string {
	sql := fmt.Sprintf(getLengthAndUnitFmt, accountId, level)
	if level == "account" {
		sql += fmt.Sprintf(" and account_name = '%s'", accName)
	} else if level == "database" {
		sql += fmt.Sprintf(" and database_name = '%s'", dbName)
	} else if level == "table" {
		sql += fmt.Sprintf(" and table_name = '%s'", tblName)
	}
	return sql
}

func checkPitrDup(ctx context.Context, bh BackgroundExec, createAccount string, createAccountId uint64, stmt *tree.CreatePitr) (bool, error) {
	sql := getSqlForCheckPitrDup(createAccount, createAccountId, stmt)

	var newCtx = ctx
	if createAccountId != sysAccountID {
		newCtx = defines.AttachAccountId(newCtx, sysAccountID)
	}

	bh.ClearExecResultSet()
	err := bh.Exec(newCtx, sql)
	if err != nil {
		return false, err
	}

	erArray, err := getResultSet(newCtx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

// @param pitrLevel
// if level is cluster, check whether has a pitr which level is cluster
// if level is account, check whether has a pitr which account_name is the same
// if level is database, check whether has a pitr which account_name and db_name is the same
// if level is table, check whether has a pitr which account_name db_name and tbl_name is the same
// @return sql
func getSqlForCheckPitrDup(createAccount string, createAccountId uint64, stmt *tree.CreatePitr) string {
	sql := "select pitr_id from mo_catalog.mo_pitr where create_account = %d"
	switch stmt.Level {
	case tree.PITRLEVELCLUSTER:
		return getSqlForCheckDupPitrFormat(createAccountId, math.MaxUint64)
	case tree.PITRLEVELACCOUNT:
		if len(stmt.AccountName) > 0 {
			return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and account_name = '%s' and level = 'account' and pitr_status = 1;", stmt.AccountName)
		} else {
			return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and account_name = '%s' and level = 'account' and pitr_status = 1;", createAccount)
		}
	case tree.PITRLEVELDATABASE:
		return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and database_name = '%s' and level = 'database' and pitr_status = 1;", stmt.DatabaseName)
	case tree.PITRLEVELTABLE:
		return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and database_name = '%s' and table_name = '%s' and level = 'table' and pitr_status = 1;", stmt.DatabaseName, stmt.TableName)
	}
	return sql
}

func checkPitrExistOrNot(ctx context.Context, bh BackgroundExec, pitrName string, accountId uint64) (bool, error) {
	var (
		sql     string
		erArray []ExecResult
		err     error
	)
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
	var (
		err            error
		pitrLevel      tree.PitrLevel
		pitrForAccount string
		pitrName       string
		pitrExist      bool
		accountName    string
		databaseName   string
		tableName      string
		sql            string
		isDup          bool
		erArray        []ExecResult
		newUUid        uuid.UUID
	)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create pitr priv
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
		return moerr.NewInternalErrorf(ctx, "invalid pitr value %d", pitrVal)
	}

	// 5.check pitr unit
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return moerr.NewInternalErrorf(ctx, "invalid pitr unit %s", pitrUnit)
	}

	// 6.check pitr exists or not
	pitrName = string(stmt.Name)
	if pitrName == SYSMOCATALOGPITR {
		return moerr.NewInternalError(ctx, "pitr name is reserved")
	}

	pitrExist, err = checkPitrExistOrNot(ctx, bh, pitrName, uint64(tenantInfo.GetTenantID()))
	if err != nil {
		return err
	}
	if pitrExist {
		if !stmt.IfNotExists {
			return moerr.NewInternalErrorf(ctx, "pitr %s already exists", pitrName)
		} else {
			return nil
		}
	}

	// insert record to the system table
	// 1. get pitr id
	newUUid, err = uuid.NewV7()
	if err != nil {
		return err
	}

	// 2. get create account
	createAcc := tenantInfo.GetTenantID()

	switch pitrLevel {
	case tree.PITRLEVELCLUSTER:
		isDup, err = checkPitrDup(ctx, bh, currentAccount, uint64(createAcc), stmt)
		if err != nil {
			return err
		}
		if isDup {
			return moerr.NewInternalError(ctx, "cluster level pitr already exists")
		}
		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			time.Now().UTC().UnixNano(),
			pitrLevel.String(),
			0,
			accountName,
			databaseName,
			tableName,
			math.MaxUint64,
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
					return 0, moerr.NewInternalErrorf(ctx, "account %s does not exist", accountName)
				}
				return accountId, rtnErr
			}

			accountId, rntErr := getAccountIdFunc(pitrForAccount)
			if rntErr != nil {
				return rntErr
			}

			isDup, err = checkPitrDup(ctx, bh, currentAccount, uint64(createAcc), stmt)
			if err != nil {
				return err
			}
			if isDup {
				return moerr.NewInternalError(ctx, fmt.Sprintf("account `%s` already has a pitr", pitrForAccount))
			}

			sql, err = getSqlForCreatePitr(
				ctx,
				newUUid.String(),
				pitrName,
				uint64(createAcc),
				time.Now().UTC().UnixNano(),
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
			isDup, err = checkPitrDup(ctx, bh, currentAccount, uint64(createAcc), stmt)
			if err != nil {
				return err
			}
			if isDup {
				return moerr.NewInternalError(ctx, fmt.Sprintf("account `%s` already has a pitr", currentAccount))
			}
			sql, err = getSqlForCreatePitr(
				ctx,
				newUUid.String(),
				pitrName,
				uint64(createAcc),
				time.Now().UTC().UnixNano(),
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
		var dbId uint64
		if len(databaseName) > 0 && needSkipDb(databaseName) {
			return moerr.NewInternalError(ctx, fmt.Sprintf("can not create pitr for current database %s", databaseName))
		}
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
				return 0, moerr.NewInternalErrorf(ctx, "database %s does not exist", dbName)
			}
			return dbId, rtnErr
		}
		dbId, err = getDatabaseIdFunc(databaseName)
		if err != nil {
			return err
		}

		isDup, err = checkPitrDup(ctx, bh, currentAccount, uint64(createAcc), stmt)
		if err != nil {
			return err
		}
		if isDup {
			return moerr.NewInternalError(ctx, fmt.Sprintf("database `%s` already has a pitr", databaseName))
		}

		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			time.Now().UTC().UnixNano(),
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
		var tblId uint64
		databaseName = string(stmt.DatabaseName)
		tableName = string(stmt.TableName)
		if len(databaseName) > 0 && needSkipDb(databaseName) {
			return moerr.NewInternalError(ctx, fmt.Sprintf("can not create pitr for current table %s.%s", databaseName, tableName))
		}
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
				return 0, moerr.NewInternalErrorf(ctx, "table %s.%s does not exist", dbName, tblName)
			}
			return tblId, rtnErr
		}
		tblId, err = getTableIdFunc(databaseName, tableName)
		if err != nil {
			return err
		}

		isDup, err = checkPitrDup(ctx, bh, currentAccount, uint64(createAcc), stmt)
		if err != nil {
			return err
		}
		if isDup {
			return moerr.NewInternalError(ctx, fmt.Sprintf("database `%s` table `%s` already has a pitr", databaseName, tableName))
		}

		sql, err = getSqlForCreatePitr(
			ctx,
			newUUid.String(),
			pitrName,
			uint64(createAcc),
			time.Now().UTC().UnixNano(),
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
	getLogger(ses.GetService()).Info("create pitr", zap.String("sql", sql))
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	// handle sys_mo_catalog_pitr
	sql = fmt.Sprintf(getPitrFormat+" where pitr_name = '%s';", SYSMOCATALOGPITR)
	getLogger(ses.GetService()).Info("get system account mo_catalog pitr", zap.String("sql", sql))
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		var (
			sysPitrValue       uint64
			sysPitrUnit        string
			oldMinTs, newMinTs time.Time
		)
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			sysPitrValue, err = erArray[0].GetUint64(ctx, i, 11)
			if err != nil {
				return err
			}
			sysPitrUnit, err = erArray[0].GetString(ctx, i, 12)
			if err != nil {
				return err
			}
		}

		oldMinTs, err = addTimeSpan(time.Time{}, int(sysPitrValue), sysPitrUnit)
		if err != nil {
			return err
		}

		newMinTs, err = addTimeSpan(time.Time{}, int(pitrVal), pitrUnit)
		if err != nil {
			return err
		}

		if newMinTs.UnixNano() < oldMinTs.UnixNano() {
			// update sysMoCatalogPitr
			sql = fmt.Sprintf("update mo_catalog.mo_pitr set pitr_length = %d, pitr_unit = '%s' where pitr_name = '%s';", pitrVal, pitrUnit, SYSMOCATALOGPITR)
			getLogger(ses.GetService()).Info("update sys mo_catalog pitr", zap.String("sql", sql))
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}
	} else {
		// insert sysMoCatalogPitr
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
				return 0, moerr.NewInternalErrorf(ctx, "database %s does not exist", dbName)
			}
			return dbId, rtnErr
		}
		var (
			dbId        uint64
			mocatalogId uuid.UUID
		)
		dbId, err = getDatabaseIdFunc(catalog.MO_CATALOG)
		if err != nil {
			return err
		}

		mocatalogId, err = uuid.NewV7()
		if err != nil {
			return err
		}
		sql, err = getSqlForCreatePitr(
			ctx,
			mocatalogId.String(),
			SYSMOCATALOGPITR,
			sysAccountID,
			time.Now().UTC().UnixNano(),
			tree.PITRLEVELDATABASE.String(),
			sysAccountID,
			sysAccountName,
			catalog.MO_CATALOG,
			"",
			dbId,
			uint8(pitrVal),
			pitrUnit)

		if err != nil {
			return err
		}

		getLogger(ses.GetService()).Info("create sys mo_catalog pitr", zap.String("sql", sql))
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}

	return err
}

func doDropPitr(ctx context.Context, ses *Session, stmt *tree.DropPitr) (err error) {
	var (
		sql       string
		pitrExist bool
		erArray   []ExecResult
	)

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
			return moerr.NewInternalErrorf(ctx, "pitr %s does not exist", string(stmt.Name))
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

		// handle sys_mo_catalog_pitr
		sql = fmt.Sprintf(getPitrFormat+" where pitr_name != '%s';", SYSMOCATALOGPITR)
		getLogger(ses.GetService()).Info("get system account mo_catalog pitr", zap.String("sql", sql))
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			return err
		}
		if !execResultArrayHasData(erArray) {
			// drop sysMoCatalogPitr
			sql = getSqlForDropPitr(SYSMOCATALOGPITR, sysAccountID)
			getLogger(ses.GetService()).Info("drop sys mo_catalog pitr", zap.String("sql", sql))
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func doAlterPitr(ctx context.Context, ses *Session, stmt *tree.AlterPitr) (err error) {
	var (
		sql       string
		pitrExist bool
	)
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
		return moerr.NewInternalErrorf(ctx, "invalid pitr value %d", stmt.PitrValue)
	}

	// check pitr unit
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return moerr.NewInternalErrorf(ctx, "invalid pitr unit %s", pitrUnit)
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
			return moerr.NewInternalErrorf(ctx, "pitr %s does not exist", string(stmt.Name))
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForAlterPitr(
			time.Now().UTC().UnixNano(),
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

func doRestorePitr(ctx context.Context, ses *Session, stmt *tree.RestorePitr) (stats statistic.StatsArray, err error) {
	normalizeRestorePitrStmt(stmt)

	bh := ses.GetBackgroundExec(ctx)
	bh.SetRestore(true)
	defer func() {
		stats = bh.GetExecStatsArray()
		bh.SetRestore(false)
		bh.Close()
	}()

	var (
		restoreLevel  tree.RestoreLevel
		ts            int64
		pitrExist     bool
		sortedFkTbls  []string
		fkTableMap    map[string]*tableInfo
		accountRecord *accountRecord
	)
	// resolve timestamp
	ts, err = doResolveTimeStamp(stmt.TimeStamp)
	if err != nil {
		return stats, err
	}

	// get pitr name
	pitrName := string(stmt.Name)
	dbName := string(stmt.DatabaseName)
	tblName := string(stmt.TableName)

	isClusterRestore := false
	isNeedToCleanToDatabase := true

	// restore as a txn
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return stats, err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// check if the pitr exists
	tenantInfo := ses.GetTenantInfo()
	pitrExist, err = checkPitrExistOrNot(ctx, bh, pitrName, uint64(tenantInfo.GetTenantID()))
	if err != nil {
		return stats, err
	}

	if !pitrExist {
		return stats, moerr.NewInternalErrorf(ctx, "pitr %s does not exist", pitrName)
	}

	// check if the database can be restore
	if len(dbName) != 0 && needSkipDb(dbName) {
		return stats, moerr.NewInternalErrorf(ctx, "database %s can not be restore", dbName)
	}

	// get pitr Record
	var pitr *pitrRecord
	if pitr, err = getPitrByName(ctx, bh, pitrName, uint64(tenantInfo.GetTenantID())); err != nil {
		return stats, err
	}

	// check the restore level and the pitr level
	if err = checkPitrValidOrNot(pitr, stmt, tenantInfo); err != nil {
		return stats, err
	}

	// check the ts is valid or not
	if err = checkPitrInValidDurtion(ts, pitr); err != nil {
		return stats, err
	}

	if stmt.Level == tree.RESTORELEVELACCOUNT {
		var accounts restorePitrAccounts
		accounts, err = resolveRestorePitrAccounts(ctx, ses, bh, pitr, stmt, ts)
		if err != nil {
			return stats, err
		}

		accountRecord, err = getAccountRecordByTs(ctx, ses, bh, pitrName, ts, accounts.sourceName)
		if err != nil {
			return stats, err
		}

		getLogger(ses.GetService()).Info(
			"restore account with pitr",
			zap.String("fromAccount", accounts.sourceName),
			zap.String("toAccount", accounts.targetName),
		)

		if accounts.sourceName == accounts.targetName {
			if _, err = getAccountId(ctx, bh, accounts.targetName); err != nil {
				if err = createDroppedAccount(ctx, ses, bh, pitrName, *accountRecord); err != nil {
					return stats, err
				}
				accounts.targetID, err = getAccountId(ctx, bh, accountRecord.accountName)
				if err != nil {
					return stats, err
				}
				isNeedToCleanToDatabase = false
			}
		}

		ctx = context.WithValue(ctx, tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelAccount)
		err = restoreAccountUsingClusterSnapshotToNew(
			ctx,
			ses,
			bh,
			pitrName,
			ts,
			*accountRecord,
			uint64(accounts.targetID),
			nil,
			isClusterRestore,
			isNeedToCleanToDatabase,
		)
		if err != nil {
			return stats, err
		}
		if err = CancelCheck(ctx); err != nil {
			return stats, err
		}
		return stats, nil
	}

	restoreLevel = stmt.Level

	accounts, err := resolveRestorePitrAccounts(ctx, ses, bh, pitr, stmt, ts)
	if err != nil {
		return stats, err
	}

	// restore self account
	// check account exists or not
	var accountExist bool
	if accountExist, err = doCheckAccountExistsInPitrRestore(ctx, ses.GetService(), bh, pitrName, ts, accounts.sourceName, accounts.sourceID); err != nil {
		return stats, err
	}
	if !accountExist {
		return stats, moerr.NewInternalErrorf(ctx, "account `%s` does not exists at timestamp: %v", accounts.sourceName, nanoTimeFormat(ts))
	}

	//drop foreign key related tables first
	if err = deleteCurFkTables(ctx, ses.GetService(), bh, dbName, tblName, accounts.targetID); err != nil {
		return
	}

	// get topo sorted tables with foreign key
	sortedFkTbls, err = fkTablesTopoSortWithTS(ctx, bh, dbName, tblName, ts, accounts.sourceID, accounts.targetID)
	if err != nil {
		return
	}

	// get foreign key table infos
	fkTableMap, err = getTableInfoMapFromTS(ctx, ses.GetService(), bh, dbName, tblName, sortedFkTbls, ts, accounts.sourceID, accounts.targetID)
	if err != nil {
		return
	}

	// collect views and tables during table restoration
	viewMap := make(map[string]*tableInfo)

	// restore according the restore level
	switch restoreLevel {
	case tree.RESTORELEVELCLUSTER:
		ctx = context.WithValue(ctx, tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelCluster)
		subDbToRestore := make(map[string]*subDbRestoreRecord)
		if err = restoreToCluster(ctx, ses, bh, pitrName, ts, subDbToRestore); err != nil {
			return
		}
		if err = restorePubsWithSnapshotName(ctx, ses.GetService(), bh, pitrName, ts); err != nil {
			return
		}

		for _, subDb := range subDbToRestore {
			if err = restoreToSubDb(ctx, ses.GetService(), bh, pitrName, subDb); err != nil {
				return
			}
		}
		return
	case tree.RESTORELEVELACCOUNT:
	case tree.RESTORELEVELDATABASE:
		ctx = context.WithValue(ctx, tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelDatabase)
		if err = restoreDatabaseFromTS(ctx, ses.GetService(), bh, dbName, ts, accounts.sourceID, accounts.targetID, fkTableMap, viewMap, false, nil); err != nil {
			return
		}
	case tree.RESTORELEVELTABLE:
		ctx = context.WithValue(ctx, tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelTable)
		if err = restoreTableFromTS(ctx, ses.GetService(), bh, dbName, tblName, ts, accounts.sourceID, accounts.targetID, fkTableMap, viewMap); err != nil {
			return
		}

	default:
		return stats, moerr.NewInternalErrorf(ctx, "unknown restore level %v", restoreLevel)
	}

	if len(fkTableMap) > 0 {
		if err = restoreTablesWithFkFromTS(ctx, ses.GetService(), bh, ts, accounts.sourceID, accounts.targetID, sortedFkTbls, fkTableMap); err != nil {
			return
		}
	}

	if len(viewMap) > 0 {
		if err = restoreViewsFromTS(ctx, ses, bh, ts, accounts.sourceID, accounts.targetID, viewMap, accounts.sourceName); err != nil {
			return
		}
	}

	if err != nil {
		return
	}
	return

}

func reCreateTableWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	tblInfo *tableInfo) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore table: '%v' at timestamp %d", pitrName, tblInfo.tblName, ts))

	var isMasterTable bool
	isMasterTable, err = checkTableIsMaster(ctx, sid, bh, pitrName, tblInfo.dbName, tblInfo.tblName)
	if isMasterTable {
		// skip restore the table which is master table
		getLogger(sid).Info(fmt.Sprintf("[%s] skip restore master table: %v.%v", pitrName, tblInfo.dbName, tblInfo.tblName))
		return
	}

	if err = bh.Exec(ctx, fmt.Sprintf("use `%s`", tblInfo.dbName)); err != nil {
		return
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] start to drop table: '%v',", pitrName, tblInfo.tblName))
	if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists `%s`", tblInfo.tblName)); err != nil {
		return
	}

	if !isRestoreByCloneSql.MatchString(restoreTableDataByTsFmt) {
		// create table
		getLogger(sid).Info(fmt.Sprintf("[%s]  start to create table: '%v', create table sql: %s", pitrName, tblInfo.tblName, tblInfo.createSql))
		if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
			if strings.Contains(err.Error(), "no such table") {
				getLogger(sid).Info(fmt.Sprintf("[%s] foreign key table %v referenced table not exists, skip restore", pitrName, tblInfo.tblName))
				err = nil
			}
			return
		}
	}

	// insert data
	insertIntoSql := fmt.Sprintf(restoreTableDataByTsFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, ts)
	beginTime := time.Now()
	getLogger(sid).Info(fmt.Sprintf("[%s] start to insert select table: '%v', insert sql: %s", pitrName, tblInfo.tblName, insertIntoSql))
	if err = bh.Exec(ctx, insertIntoSql); err != nil {
		return
	}
	getLogger(sid).Info(fmt.Sprintf("[%s] insert select table: %v, cost: %v", pitrName, tblInfo.tblName, time.Since(beginTime)))

	return
}

func showFullTablesWitsTs(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string) ([]*tableInfo, error) {
	sql := fmt.Sprintf("show full tables from `%s`", dbName)
	if len(tblName) > 0 {
		sql += fmt.Sprintf(" like '%s'", tblName)
	}
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}
	getLogger(sid).Info(fmt.Sprintf("[%s] show full table `%s.%s` sql: %s ", pitrName, dbName, tblName, sql))
	// cols: table name, table type
	colsList, err := getStringColsList(ctx, bh, sql, 0, 1)
	if err != nil {
		return nil, err
	}

	ans := make([]*tableInfo, len(colsList))
	for i, cols := range colsList {
		ans[i] = &tableInfo{
			dbName:  dbName,
			tblName: cols[0],
			typ:     tableType(cols[1]),
		}
	}
	getLogger(sid).Info(fmt.Sprintf("[%s] show full table `%s.%s`, get table number `%d`", pitrName, dbName, tblName, len(ans)))
	return ans, nil
}

func getCreateTableSqlWithTs(ctx context.Context, bh BackgroundExec, ts int64, dbName string, tblName string) (string, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s`", dbName, tblName)
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	// cols: table_name, create_sql
	colsList, err := getStringColsList(ctx, bh, sql, 1)
	if err != nil {
		return "", err
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewNoSuchTable(ctx, dbName, tblName)
	}
	return colsList[0][0], nil
}

func restoreViewsWithPitr(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	viewMap map[string]*tableInfo,
	accountName string,
	curAccount uint32) error {
	getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to restore views", pitrName))
	var (
		err         error
		stmts       []tree.Statement
		sortedViews []string
		snapshot    *pbplan.Snapshot
		oldSnapshot *pbplan.Snapshot
	)
	snapshot = &pbplan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: ts},
		Tenant: &pbplan.SnapshotTenant{
			TenantName: accountName,
			TenantID:   curAccount,
		},
	}

	compCtx := ses.GetTxnCompileCtx()
	oldSnapshot = compCtx.GetSnapshot()
	compCtx.SetSnapshot(snapshot)
	defer func() {
		compCtx.SetSnapshot(oldSnapshot)
	}()

	g := toposort{next: make(map[string][]string)}
	for key, viewEntry := range viewMap {
		getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to restore view: %v", pitrName, viewEntry.tblName))
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, viewEntry.createSql, 1)
		if err != nil {
			return err
		}

		compCtx.SetDatabase(viewEntry.dbName)
		// build create sql to find dependent views
		_, err = plan.BuildPlan(compCtx, stmts[0], false)
		if err != nil {
			stmts, _ = parsers.Parse(ctx, dialect.MYSQL, viewEntry.createSql, 0)
			_, err = plan.BuildPlan(compCtx, stmts[0], false)
			if err != nil {
				return err
			}
		}

		g.addVertex(key)
		for _, depView := range compCtx.GetViews() {
			g.addEdge(depView, key)
		}
	}

	// topsort
	sortedViews, err = g.sort()
	if err != nil {
		return err
	}

	// create views

	for _, key := range sortedViews {
		// if not ok, means that view is not in this restoration task, ignore
		if tblInfo, ok := viewMap[key]; ok {
			getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to restore view: %v, restore timestamp: %d", pitrName, tblInfo.tblName, ts))

			if err = bh.Exec(ctx, "use `"+tblInfo.dbName+"`"); err != nil {
				return err
			}

			if err = bh.Exec(ctx, "drop view if exists "+tblInfo.tblName); err != nil {
				return err
			}

			if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreSystemDatabaseWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	accountId uint32,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore system database: %s", pitrName, moCatalog))
	var (
		dbName     = moCatalog
		tableInfos []*tableInfo
	)

	tableInfos, err = showFullTablesWitsTs(ctx, sid, bh, pitrName, ts, dbName, "")
	if err != nil {
		return err
	}

	for _, tblInfo := range tableInfos {
		if needSkipSystemTable(accountId, tblInfo) {
			// TODO skip tables which should not to be restored
			getLogger(sid).Info(fmt.Sprintf("[%s] skip restore system table: %v.%v, table type: %v", pitrName, moCatalog, tblInfo.tblName, tblInfo.typ))
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%s] start to restore system table: %v.%v", pitrName, moCatalog, tblInfo.tblName))
		tblInfo.createSql, err = getCreateTableSqlWithTs(ctx, bh, ts, dbName, tblInfo.tblName)
		if err != nil {
			return err
		}

		// checks if the given context has been canceled.
		if err = CancelCheck(ctx); err != nil {
			return
		}

		if err = reCreateTableWithPitr(ctx, sid, bh, pitrName, ts, tblInfo); err != nil {
			return
		}
	}
	return
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
				if record.createTime, err = er.GetInt64(ctx, row, 3); err != nil {
					return nil, err
				}
				if record.modifiedTime, err = er.GetInt64(ctx, row, 4); err != nil {
					return nil, err
				}
				var level string
				if level, err = er.GetString(ctx, row, 5); err != nil {
					return nil, err
				}
				record.level = strings.ToLower(level)
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
	newCtx := ctx
	if accountId != sysAccountID {
		newCtx = defines.AttachAccountId(ctx, sysAccountID)
	}
	if err := inputNameIsInvalid(newCtx, pitrName); err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("%s where pitr_name = '%s' and create_account = %d", getPitrFormat, pitrName, accountId)
	if records, err := getPitrRecords(newCtx, bh, sql); err != nil {
		return nil, err
	} else if len(records) != 1 {
		return nil, moerr.NewInternalErrorf(ctx, "find %v pitr records by name(%v), expect only 1", len(records), pitrName)
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

// convert utc nano time to Local format string
// @param ts: the utc nano time
// @return the local time string
func nanoTimeFormat(ts int64) string {
	t := time.Unix(0, ts)
	return t.Format("2006-01-02 15:04:05")
}

// check the ts is valid or not
// @param ts: the timestamp
// @param pitrRecord: the pitr record
// if the ts less than the duration of the pitr
// then return an error
// if the ts bigger than now(), then return an error
func checkPitrInValidDurtion(ts int64, pitrRecord *pitrRecord) (err error) {
	// if the timestamp time less than the pitrRecord create time, then return error
	// create time is utc time string, ts is coverted to utc too
	if ts <= pitrRecord.createTime {
		return moerr.NewInternalErrorNoCtxf(
			"input timestamp %v is less than the pitr valid time %v",
			nanoTimeFormat(ts), nanoTimeFormat(pitrRecord.createTime),
		)
	}

	// use utc time now sub pitr during time get the minest time
	// is ts time less than the minest time, then return error
	pitrValue := pitrRecord.pitrValue
	pitrUnit := pitrRecord.pitrUnit

	minTs, err := addTimeSpan(time.Time{}, int(pitrValue), pitrUnit)
	if err != nil {
		return err
	}

	if ts < minTs.UnixNano() {
		return moerr.NewInternalErrorNoCtxf("input timestamp %v is less than the pitr range minest timestamp %v", nanoTimeFormat(ts), nanoTimeFormat(minTs.UnixNano()))
	}

	// if the ts bigger than now(), then return an error
	if ts > time.Now().UTC().UnixNano() {
		return moerr.NewInternalErrorNoCtxf("input timestamp %v is bigger than now timestamp %v", nanoTimeFormat(ts), nanoTimeFormat(time.Now().UTC().UnixNano()))
	}

	return
}

// get pitr time span
func addTimeSpan(pivot time.Time, length int, unit string) (time.Time, error) {
	var (
		now time.Time
	)

	if !pivot.IsZero() {
		now = pivot.UTC()
	} else {
		now = time.Now().UTC()
	}

	switch unit {
	case "h":
		return now.Add(time.Duration(-length) * time.Hour), nil
	case "d":
		return now.AddDate(0, 0, -length), nil
	case "mo":
		return now.AddDate(0, -length, 0), nil
	case "y":
		return now.AddDate(-length, 0, 0), nil
	default:
		return time.Time{}, moerr.NewInternalErrorNoCtxf("unknown unit '%s'", unit)
	}
}

func checkPitrValidOrNot(pitrRecord *pitrRecord, stmt *tree.RestorePitr, tenantInfo *TenantInfo) (err error) {
	normalizeRestorePitrStmt(stmt)

	sourceAccount := string(stmt.AccountName)
	if len(sourceAccount) == 0 && stmt.Level != tree.RESTORELEVELCLUSTER {
		sourceAccount = tenantInfo.GetTenant()
	}

	targetAccount := string(stmt.ToAccountName)
	if len(targetAccount) == 0 {
		targetAccount = sourceAccount
	}

	restoreLevel := stmt.Level
	switch restoreLevel {
	case tree.RESTORELEVELCLUSTER:
		// check the level
		// if the pitr level is account/ database/table, return err
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() || pitrRecord.level == tree.PITRLEVELDATABASE.String() || pitrRecord.level == tree.PITRLEVELTABLE.String() {
			return moerr.NewInternalErrorNoCtxf("restore level %v is not allowed for cluster restore", pitrRecord.level)
		}
		// if the accout not sys account, return err
		if tenantInfo.GetTenantID() != sysAccountID {
			return moerr.NewInternalErrorNoCtxf("account %s is not allowed to restore cluster level pitr %s", tenantInfo.GetTenant(), pitrRecord.pitrName)
		}
	case tree.RESTORELEVELACCOUNT:
		if pitrRecord.level == tree.PITRLEVELDATABASE.String() || pitrRecord.level == tree.PITRLEVELTABLE.String() {
			return moerr.NewInternalErrorNoCtxf("can not restore account %s by pitr %s", sourceAccount, pitrRecord.pitrName)
		}
		if tenantInfo.GetTenantID() != sysAccountID && (sourceAccount != tenantInfo.GetTenant() || targetAccount != tenantInfo.GetTenant()) {
			return moerr.NewInternalErrorNoCtxf("account %s is not allowed to restore account %s", tenantInfo.GetTenant(), sourceAccount)
		}
		if sourceAccount == sysAccountName && targetAccount != sysAccountName {
			return moerr.NewInternalErrorNoCtxf("can not restore sys account to new account %s by pitr %s", targetAccount, pitrRecord.pitrName)
		}
		if sourceAccount != sysAccountName && targetAccount == sysAccountName {
			return moerr.NewInternalErrorNoCtxf("can not restore account %s to sys account by pitr %s", sourceAccount, pitrRecord.pitrName)
		}
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountName != sourceAccount {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %s", pitrRecord.pitrName, sourceAccount)
		}
	case tree.RESTORELEVELDATABASE:
		// check the level
		if pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
			return moerr.NewInternalErrorNoCtxf("cluster level pitr `%v` is not allowed for database restore", pitrRecord.level)
		}
		if pitrRecord.level == tree.PITRLEVELTABLE.String() {
			return moerr.NewInternalErrorNoCtxf("restore level %v is not allowed for database restore", pitrRecord.level)
		}
		if tenantInfo.GetTenantID() != sysAccountID && (sourceAccount != tenantInfo.GetTenant() || targetAccount != tenantInfo.GetTenant()) {
			return moerr.NewInternalErrorNoCtxf("account %s is not allowed to restore database %s", tenantInfo.GetTenant(), string(stmt.DatabaseName))
		}
		if sourceAccount == sysAccountName && targetAccount != sysAccountName {
			return moerr.NewInternalErrorNoCtxf("can not restore sys database %s to account %s by pitr %s", string(stmt.DatabaseName), targetAccount, pitrRecord.pitrName)
		}
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountName != sourceAccount {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %s database %v", pitrRecord.pitrName, sourceAccount, string(stmt.DatabaseName))
		}
		if pitrRecord.level == tree.PITRLEVELDATABASE.String() && (pitrRecord.accountName != sourceAccount || pitrRecord.databaseName != string(stmt.DatabaseName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore database %v", pitrRecord.pitrName, string(stmt.DatabaseName))
		}
	case tree.RESTORELEVELTABLE:
		// check the level
		if pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
			return moerr.NewInternalErrorNoCtxf("cluster level pitr `%v` is not allowed for table restore", pitrRecord.level)
		}
		if tenantInfo.GetTenantID() != sysAccountID && (sourceAccount != tenantInfo.GetTenant() || targetAccount != tenantInfo.GetTenant()) {
			return moerr.NewInternalErrorNoCtxf("account %s is not allowed to restore table %s.%s", tenantInfo.GetTenant(), string(stmt.DatabaseName), string(stmt.TableName))
		}
		if sourceAccount == sysAccountName && targetAccount != sysAccountName {
			return moerr.NewInternalErrorNoCtxf("can not restore sys table %s.%s to account %s by pitr %s", string(stmt.DatabaseName), string(stmt.TableName), targetAccount, pitrRecord.pitrName)
		}
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountName != sourceAccount {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %s database %v table %v", pitrRecord.pitrName, sourceAccount, string(stmt.DatabaseName), string(stmt.TableName))
		}
		if pitrRecord.level == tree.PITRLEVELDATABASE.String() && (pitrRecord.accountName != sourceAccount || pitrRecord.databaseName != string(stmt.DatabaseName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore database %v table %v", pitrRecord.pitrName, string(stmt.DatabaseName), string(stmt.TableName))
		}
		if pitrRecord.level == tree.PITRLEVELTABLE.String() && (pitrRecord.accountName != sourceAccount || pitrRecord.databaseName != string(stmt.DatabaseName) || pitrRecord.tableName != string(stmt.TableName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore table %v.%v", pitrRecord.pitrName, string(stmt.DatabaseName), string(stmt.TableName))
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unknown restore level %v", restoreLevel.String())
	}
	return nil

}

func resolveRestorePitrAccounts(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	pitr *pitrRecord,
	stmt *tree.RestorePitr,
	ts int64,
) (accounts restorePitrAccounts, err error) {
	tenantInfo := ses.GetTenantInfo()

	if stmt.Level == tree.RESTORELEVELCLUSTER {
		return accounts, nil
	}

	accounts.sourceName = string(stmt.AccountName)
	if len(accounts.sourceName) == 0 {
		accounts.sourceName = tenantInfo.GetTenant()
	}

	accounts.targetName = string(stmt.ToAccountName)
	if len(accounts.targetName) == 0 {
		accounts.targetName = accounts.sourceName
	}

	resolveAccountID := func(name string) (uint32, error) {
		if name == sysAccountName {
			return sysAccountID, nil
		}
		if name == tenantInfo.GetTenant() {
			return tenantInfo.GetTenantID(), nil
		}
		if pitr != nil && pitr.accountName == name && pitr.accountId <= math.MaxUint32 {
			return uint32(pitr.accountId), nil
		}

		accountID, accountErr := getAccountId(ctx, bh, name)
		if accountErr == nil {
			return accountID, nil
		}

		accountRecord, recordErr := getAccountRecordByTs(ctx, ses, bh, string(stmt.Name), ts, name)
		if recordErr != nil {
			return 0, accountErr
		}
		return uint32(accountRecord.accountId), nil
	}

	if accounts.sourceID, err = resolveAccountID(accounts.sourceName); err != nil {
		return accounts, err
	}

	if accounts.targetID, err = resolveAccountID(accounts.targetName); err != nil {
		return accounts, err
	}

	return accounts, nil
}

func restoreTableFromTS(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	tblName string,
	ts int64,
	from uint32,
	to uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
) (err error) {
	var tableInfos []*tableInfo

	tableInfos, err = getTableInfosFromTS(ctx, sid, bh, dbName, tblName, ts, from, to)
	if err != nil {
		return err
	}
	if len(tableInfos) == 0 {
		return moerr.NewInternalErrorNoCtxf("table '%s' not exists at pitr timestamp '%s'", tblName, nanoTimeFormat(ts))
	}
	if len(tableInfos) != 1 {
		return moerr.NewInternalErrorNoCtxf("find %v tableInfos by name '%s' at timestamp '%s', expect only 1", len(tableInfos), tblName, nanoTimeFormat(ts))
	}

	if _, err = getCreateDatabaseSqlFromTS(ctx, sid, bh, dbName, ts, from, to); err != nil {
		return err
	}

	toCtx := defines.AttachAccountId(ctx, to)
	if err = bh.Exec(toCtx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
		return err
	}

	tblInfo := tableInfos[0]
	key := genKey(dbName, tblInfo.tblName)
	if _, ok := fkTableMap[key]; ok {
		return nil
	}
	if tblInfo.typ == view {
		viewMap[key] = tblInfo
		return nil
	}

	return recreateTableFromTS(ctx, sid, bh, tblInfo, ts, from, to)
}

func doCheckAccountExistsInPitrRestore(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	accountName string,
	accountId uint32) (bool, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to check if account '%v' exists at timestamp %d", pitrName, accountName, ts))

	newCtx := ctx
	if accountId != sysAccountID {
		newCtx = defines.AttachAccountId(ctx, sysAccountID)
	}
	sql, err := getSqlForCheckAccountWithPitr(newCtx, ts, accountName)
	if err != nil {
		return false, err
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(newCtx, sql); err != nil {
		return false, err
	}

	resultSet, err := getResultSet(newCtx, bh)
	if err != nil {
		return false, err
	}

	if !execResultArrayHasData(resultSet) {
		return false, nil
	}
	return true, nil
}

func getCreateDatabaseSqlInPitr(ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	dbName string,
	accountId uint32,
	ts int64,
) (string, error) {

	sql := "select datname, dat_createsql from mo_catalog.mo_database"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}
	sql += fmt.Sprintf(" where datname = '%s' and account_id = %d", dbName, accountId)
	getLogger(sid).Info(fmt.Sprintf("[%s] get create database `%s` sql: %s", pitrName, dbName, sql))

	// cols: database_name, create_sql
	colsList, err := getStringColsList(ctx, bh, sql, 0, 1)
	if err != nil {
		return "", err
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewBadDB(ctx, dbName)
	}
	return colsList[0][1], nil
}

// createPubByPitr create pub after the database is created by pitr
func createPubByPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName,
	dbName string,
	toAccountId uint32,
	ts int64) (err error) {
	// read pub info from mo_pubs
	sql := getPubInfoWithPitr(ts, toAccountId, dbName)
	getLogger(sid).Info(fmt.Sprintf("[%s] create pub: get pub info sql: %s", pitrName, sql))

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}
	pubInfos, err := extractPubInfosFromExecResult(ctx, erArray)
	if err != nil {
		return err
	}

	// restore pub to toAccount
	var ast []tree.Statement
	defer func() {
		for _, s := range ast {
			s.Free()
		}
	}()

	for _, pubInfo := range pubInfos {
		toCtx := defines.AttachAccount(ctx, toAccountId, pubInfo.Owner, pubInfo.Creator)
		if ast, err = mysql.Parse(toCtx, pubInfo.GetCreateSql(), 1); err != nil {
			return
		}

		getLogger(sid).Info(fmt.Sprintf("[%s] create pub: create pub sql: %s", pitrName, pubInfo.GetCreateSql()))
		if err = createPublication(toCtx, bh, ast[0].(*tree.CreatePublication)); err != nil {
			return
		}
	}
	return
}

// updatePitrObjectId
// if mo_pitr table contains the same account name create account, then update the obj_id of mo_pitr
// otherwise, skip it
func updatePitrObjectId(ctx context.Context,
	bh BackgroundExec,
	accountName string,
	objId uint64,
	ts int64,
) (err error) {
	sql := getSqlForUpdateMoPitrAccountObjectId(
		accountName,
		objId,
		ts,
	)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return
	}
	return
}

var getPitrLengthAndUnit = func(
	ctx context.Context,
	bh BackgroundExec,
	level string,
	accName, dbName, tblName string,
) (length int64, unit string, ok bool, err error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	sql := getSqlForGetLengthAndUnitFmt(accountId, level, accName, dbName, tblName)
	ctx = defines.AttachAccountId(ctx, sysAccountID)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if !execResultArrayHasData(erArray) {
		return
	}

	if length, err = erArray[0].GetInt64(ctx, 0, 0); err != nil {
		return
	}

	if unit, err = erArray[0].GetString(ctx, 0, 1); err != nil {
		return
	}

	ok = true
	return
}
