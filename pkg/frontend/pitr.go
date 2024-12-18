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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		pitr_unit ) values ('%s', '%s', %d, '%s', '%s', '%s', %d, '%s', '%s', '%s', %d, %d, '%s');`

	checkPitrFormat = `select pitr_id from mo_catalog.mo_pitr where pitr_name = "%s" and create_account = %d order by pitr_id;`

	dropPitrFormat = `delete from mo_catalog.mo_pitr where pitr_name = '%s' and create_account = %d order by pitr_id;`

	alterPitrFormat = `update mo_catalog.mo_pitr set modified_time = '%s', pitr_length = %d, pitr_unit = '%s' where pitr_name = '%s' and create_account = %d;`

	getPitrFormat = `select * from mo_catalog.mo_pitr`

	checkDupPitrFormat = `select pitr_id from mo_catalog.mo_pitr where create_account = %d and obj_id = %d;`

	getSqlForCheckDatabaseFmt = `select dat_id from mo_catalog.mo_database {MO_TS = %d} where datname = '%s';`

	getSqlForCheckTableFmt = `select rel_id from mo_catalog.mo_tables {MO_TS = %d} where reldatabase = '%s' and relname = '%s';`

	getSqlForCheckAccountFmt = `select account_id from mo_catalog.mo_account {MO_TS = %d} where account_name = '%s';`

	getPubInfoWithPitrFormat = `select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs {MO_TS = %d} where account_id = %d and database_name = '%s';`

	// update mo_pitr object id
	updateMoPitrAccountObjectIdFmt = `update mo_catalog.mo_pitr set obj_id = %d, modified_time = '%s' where account_name = '%s';`
)

type pitrRecord struct {
	pitrId        string
	pitrName      string
	createAccount uint64
	createTime    string
	modifiedTime  string
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

func getSqlForUpdateMoPitrAccountObjectId(accountName string, objId uint64, modifiedTime string) string {
	return fmt.Sprintf(updateMoPitrAccountObjectIdFmt, objId, modifiedTime, accountName)
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
			return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and account_name = '%s' and level = 'account';", stmt.AccountName)
		} else {
			return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and account_name = '%s' and level = 'account';", createAccount)
		}
	case tree.PITRLEVELDATABASE:
		return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and database_name = '%s' and level = 'database';", stmt.DatabaseName)
	case tree.PITRLEVELTABLE:
		return fmt.Sprintf(sql, createAccountId) + fmt.Sprintf(" and database_name = '%s' and table_name = '%s' and level = 'table';", stmt.DatabaseName, stmt.TableName)
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
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
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

		oldMinTs, err = addTimeSpan(int(sysPitrValue), sysPitrUnit)
		if err != nil {
			return err
		}

		newMinTs, err = addTimeSpan(int(pitrVal), pitrUnit)
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
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
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

func doRestorePitr(ctx context.Context, ses *Session, stmt *tree.RestorePitr) (stats statistic.StatsArray, err error) {
	bh := ses.GetBackgroundExec(ctx)
	bh.SetRestore(true)
	defer func() {
		stats = bh.GetExecStatsArray()
		bh.SetRestore(false)
		bh.Close()
	}()

	var (
		restoreLevel tree.RestoreLevel
		ts           int64
		pitrExist    bool
		sortedFkTbls []string
		fkTableMap   map[string]*tableInfo
	)
	// resolve timestamp
	ts, err = doResolveTimeStamp(stmt.TimeStamp)
	if err != nil {
		return stats, err
	}

	// get pitr name
	pitrName := string(stmt.Name)
	accountName := string(stmt.AccountName)
	dbName := string(stmt.DatabaseName)
	tblName := string(stmt.TableName)

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

	if len(accountName) > 0 {
		restoreOtherAccount := func() (rtnErr error) {
			fromAccount := string(stmt.SrcAccountName)
			var (
				fromAccountId uint32
				toAccountId   uint32
			)

			if len(fromAccount) == 0 {
				fromAccount = pitr.accountName
				fromAccountId = uint32(pitr.accountId)
				if fromAccount == accountName {
					// restore to the same account
					toAccountId = fromAccountId
				} else {
					// restore to new account
					toAccountId, rtnErr = getAccountId(ctx, bh, accountName)
					if rtnErr != nil {
						return rtnErr
					}
				}
			} else {
				if fromAccount == accountName {
					// restore to the same account
					fromAccountId, rtnErr = getAccountId(ctx, bh, fromAccount)
					if rtnErr != nil {
						return rtnErr
					}
					toAccountId = fromAccountId
				} else {
					// restore to new account
					fromAccountId, rtnErr = getAccountId(ctx, bh, fromAccount)
					if rtnErr != nil {
						return rtnErr
					}
					toAccountId, rtnErr = getAccountId(ctx, bh, accountName)
					if rtnErr != nil {
						return rtnErr
					}
				}
			}

			// check account exists or not
			var accountExist bool
			if accountExist, rtnErr = doCheckAccountExistsInPitrRestore(ctx, ses.GetService(), bh, pitrName, ts, fromAccount, ses.GetAccountId()); rtnErr != nil {
				return rtnErr
			}
			if !accountExist {
				return moerr.NewInternalErrorf(ctx, "account `%s` does not exists at timestamp: %v", fromAccount, nanoTimeFormat(ts))
			}
			// mock snapshot
			var snapshotName string
			snapshotName, rtnErr = insertSnapshotRecord(ctx, ses.GetService(), bh, pitrName, ts, uint64(fromAccountId), fromAccount)
			defer func() {
				deleteSnapshotRecord(ctx, ses.GetService(), bh, pitrName, snapshotName)
			}()
			if rtnErr != nil {
				return rtnErr
			}

			restoreAccount := fromAccountId
			// drop foreign key related tables first
			rtnErr = deleteCurFkTables(ctx, ses.GetService(), bh, dbName, tblName, toAccountId)
			if err != nil {
				return
			}

			// get topo sorted tables with foreign key
			sortedFkTbls, rtnErr = fkTablesTopoSort(ctx, bh, snapshotName, dbName, tblName)
			if rtnErr != nil {
				return
			}

			// get foreign key table infos
			fkTableMap, rtnErr = getTableInfoMap(ctx, ses.GetService(), bh, snapshotName, dbName, tblName, sortedFkTbls)
			if rtnErr != nil {
				return
			}

			// collect views and tables during table restoration
			viewMap := make(map[string]*tableInfo)

			rtnErr = restoreToAccount(ctx, ses.GetService(), bh, snapshotName, toAccountId, fkTableMap, viewMap, ts, restoreAccount, false, nil)
			if rtnErr != nil {
				return rtnErr
			}

			if len(fkTableMap) > 0 {
				if rtnErr = restoreTablesWithFk(ctx, ses.GetService(), bh, snapshotName, sortedFkTbls, fkTableMap, toAccountId, ts); rtnErr != nil {
					return
				}
			}

			if len(viewMap) > 0 {
				if rtnErr = restoreViews(ctx, ses, bh, snapshotName, viewMap, toAccountId); rtnErr != nil {
					return
				}
			}
			// checks if the given context has been canceled.
			if rtnErr = CancelCheck(ctx); rtnErr != nil {
				return
			}
			return rtnErr
		}

		err = restoreOtherAccount()
		if err != nil {
			return stats, err
		}
		return stats, err
	}

	restoreLevel = stmt.Level

	// restore self account
	// check account exists or not
	var accountExist bool
	if accountExist, err = doCheckAccountExistsInPitrRestore(ctx, ses.GetService(), bh, pitrName, ts, tenantInfo.GetTenant(), tenantInfo.GetTenantID()); err != nil {
		return stats, err
	}
	if !accountExist {
		return stats, moerr.NewInternalErrorf(ctx, "account `%s` does not exists at timestamp: %v", tenantInfo.GetTenant(), nanoTimeFormat(ts))
	}

	//drop foreign key related tables first
	if err = deleteCurFkTableInPitrRestore(ctx, ses.GetService(), bh, pitrName, dbName, tblName); err != nil {
		return
	}

	// get topo sorted tables with foreign key
	sortedFkTbls, err = fkTablesTopoSortInPitrRestore(ctx, bh, ts, dbName, tblName)
	if err != nil {
		return
	}

	// get foreign key table infos
	fkTableMap, err = getTableInfoMapInPitrRestore(ctx, ses.GetService(), bh, pitrName, ts, dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// collect views and tables during table restoration
	viewMap := make(map[string]*tableInfo)

	// restore according the restore level
	switch restoreLevel {
	case tree.RESTORELEVELCLUSTER:
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
		if err = restoreToAccountWithPitr(ctx, ses.GetService(), bh, pitrName, ts, fkTableMap, viewMap, tenantInfo.TenantID); err != nil {
			return
		}
	case tree.RESTORELEVELDATABASE:
		if err = restoreToDatabaseWithPitr(ctx, ses.GetService(), bh, pitrName, ts, dbName, fkTableMap, viewMap, tenantInfo.TenantID); err != nil {
			return
		}
	case tree.RESTORELEVELTABLE:
		if err = restoreToTableWithPitr(ctx, ses.service, bh, pitrName, ts, dbName, tblName, fkTableMap, viewMap, tenantInfo.TenantID); err != nil {
			return
		}

	default:
		return stats, moerr.NewInternalErrorf(ctx, "unknown restore level %v", restoreLevel)
	}

	if len(fkTableMap) > 0 {
		if err = restoreTablesWithFkByPitr(ctx, ses.GetService(), bh, pitrName, ts, sortedFkTbls, fkTableMap); err != nil {
			return
		}
	}

	if len(viewMap) > 0 {
		if err = restoreViewsWithPitr(ctx, ses, bh, pitrName, ts, viewMap, tenantInfo.GetTenant(), tenantInfo.GetTenantID()); err != nil {
			return
		}
	}

	if err != nil {
		return
	}
	return

}

func restoreToAccountWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	curAccount uint32,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore account '%d', restore timestamp : %d", pitrName, curAccount, ts))

	var dbNames []string
	// delete current dbs
	if dbNames, err = showDatabases(ctx, sid, bh, ""); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			// drop existing cluster table
			if curAccount == 0 && dbName == moCatalog {
				if err = dropClusterTable(ctx, sid, bh, "", curAccount); err != nil {
					return
				}
			}
			getLogger(sid).Info(fmt.Sprintf("[%s]skip drop db: %v", pitrName, dbName))
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%s]drop current exists db: %v", pitrName, dbName))
		if err = dropDb(ctx, bh, dbName); err != nil {
			return
		}
	}

	// restore dbs
	if dbNames, err = showDatabasesWithPitr(
		ctx,
		sid,
		bh,
		pitrName,
		ts); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabaseWithPitr(
			ctx,
			sid,
			bh,
			pitrName,
			ts,
			dbName,
			fkTableMap,
			viewMap,
			curAccount); err != nil {
			return
		}
	}

	//restore system db
	if err = restoreSystemDatabaseWithPitr(
		ctx,
		sid,
		bh,
		pitrName,
		ts,
		curAccount); err != nil {
		return
	}
	return
}

func restoreToDatabaseWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	curAccount uint32,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore db: '%s', restore timestamp: %d", pitrName, dbName, ts))

	var databaseExist bool
	if databaseExist, err = doCheckDatabaseExistsInPitrRestore(ctx, sid, bh, pitrName, ts, dbName); err != nil {
		return err
	}
	if !databaseExist {
		return moerr.NewInternalErrorf(ctx, "database '%s' does not exists at timestamp: %v", dbName, nanoTimeFormat(ts))
	}

	return restoreToDatabaseOrTableWithPitr(
		ctx,
		sid,
		bh,
		pitrName,
		ts,
		dbName,
		"",
		fkTableMap,
		viewMap,
		curAccount)
}

func restoreToTableWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	curAccount uint32,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s]  start to restore table: '%v' at timestamp %d", pitrName, tblName, ts))

	var TableExist bool
	if TableExist, err = doCheckTableExistsInPitrRestore(ctx, sid, bh, pitrName, ts, dbName, tblName); err != nil {
		return err
	}
	if !TableExist {
		return moerr.NewInternalErrorf(ctx, "database '%s' table '%s' does not exists at timestamp: %v", dbName, tblName, nanoTimeFormat(ts))
	}
	return restoreToDatabaseOrTableWithPitr(
		ctx,
		sid,
		bh,
		pitrName,
		ts,
		dbName,
		tblName,
		fkTableMap,
		viewMap,
		curAccount)
}

func restoreToDatabaseOrTableWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	curAccount uint32,
) (err error) {
	if needSkipDb(dbName) {
		getLogger(sid).Info(fmt.Sprintf("[%s] skip restore db: '%v'", pitrName, dbName))
		return
	}

	var (
		createDbSql string
		tableInfos  []*tableInfo
		isSubDb     bool
	)
	createDbSql, err = getCreateDatabaseSqlInPitr(ctx, sid, bh, pitrName, dbName, curAccount, ts)
	if err != nil {
		return
	}

	restoreToTbl := tblName != ""

	// if restore to table, check if the db is sub db
	isSubDb, err = checkDbWhetherSub(ctx, createDbSql)
	if err != nil {
		return
	}
	if isSubDb && restoreToTbl {
		return moerr.NewInternalError(ctx, "can't restore to table for sub db")
	}

	// if restore to db, delete the same name db first
	if !restoreToTbl {
		// check whether the db is master db
		var isMasterDb bool
		isMasterDb, err = checkDatabaseIsMaster(ctx, sid, bh, pitrName, dbName)
		if err != nil {
			return err
		}
		if isMasterDb {
			getLogger(sid).Info(fmt.Sprintf("[%s] skip restore master db: %v, which has been referenced by foreign keys", pitrName, dbName))
			return
		}
		// drop db
		getLogger(sid).Info(fmt.Sprintf("[%s] start to drop database: '%v'", pitrName, dbName))
		if err = dropDb(ctx, bh, dbName); err != nil {
			return
		}
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] start to create database: '%v'", pitrName, dbName))
	if isSubDb {

		// check if the publication exists
		// if the publication exists, create the db with the publication
		// else skip restore the db

		var isPubExist bool
		isPubExist, _ = checkPubExistOrNot(ctx, sid, bh, pitrName, dbName, ts)
		if !isPubExist {
			getLogger(sid).Info(fmt.Sprintf("[%s] skip restore db: %v, no publication", pitrName, dbName))
			return
		}

		// create db with publication
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create db with pub: %v, create db sql: %s", pitrName, dbName, createDbSql))
		if err = bh.Exec(ctx, createDbSql); err != nil {
			return
		}

		return
	} else {
		createDbSql = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
		// create db
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create db: %v, create db sql: %s", pitrName, dbName, createDbSql))
		if err = bh.Exec(ctx, createDbSql); err != nil {
			return
		}
	}

	tableInfos, err = getTableInfoWithPitr(ctx, sid, bh, pitrName, ts, dbName, tblName)
	if err != nil {
		return
	}

	// if restore to table, expect only one table here
	if restoreToTbl {
		if len(tableInfos) == 0 {
			return moerr.NewInternalErrorf(ctx, "table '%s' not exists at pitr '%s'", tblName, pitrName)
		} else if len(tableInfos) != 1 {
			return moerr.NewInternalErrorf(ctx, "find %v tableInfos by name '%s' at pitr '%s', expect only 1", len(tableInfos), tblName, pitrName)
		}
	}

	for _, tblInfo := range tableInfos {
		key := genKey(dbName, tblInfo.tblName)

		// skip table which is foreign key related
		if _, ok := fkTableMap[key]; ok {
			continue
		}

		// skip view
		if tblInfo.typ == view {
			viewMap[key] = tblInfo
			continue
		}

		// checks if the given context has been canceled.
		if err = CancelCheck(ctx); err != nil {
			return
		}

		if err = reCreateTableWithPitr(ctx,
			sid,
			bh,
			pitrName,
			ts,
			tblInfo); err != nil {
			return
		}
	}

	// restore publication record
	if !restoreToTbl {
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create pub: %v", pitrName, dbName))
		if err = createPubByPitr(ctx, sid, bh, pitrName, dbName, curAccount, ts); err != nil {
			return
		}
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

	// create table
	getLogger(sid).Info(fmt.Sprintf("[%s]  start to create table: '%v', create table sql: %s", pitrName, tblInfo.tblName, tblInfo.createSql))
	if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
		if strings.Contains(err.Error(), "no such table") {
			getLogger(sid).Info(fmt.Sprintf("[%s] foreign key table %v referenced table not exists, skip restore", pitrName, tblInfo.tblName))
			err = nil
		}
		return
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

func getTableInfoWithPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string) ([]*tableInfo, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get table info: datatabse `%s`, table `%s`, ts %d", pitrName, dbName, tblName, ts))
	var (
		tableInfos []*tableInfo
		err        error
	)
	tableInfos, err = showFullTablesWitsTs(ctx,
		sid,
		bh,
		pitrName,
		ts,
		dbName,
		tblName)
	if err != nil {
		return nil, err
	}

	for _, tblInfo := range tableInfos {
		if tblInfo.createSql, err = getCreateTableSqlWithTs(ctx, bh, ts, dbName, tblInfo.tblName); err != nil {
			return nil, err
		}
	}

	return tableInfos, nil
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

func showDatabasesWithPitr(ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64) ([]string, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get all database ", pitrName))
	sql := "show databases"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	// cols: dbname
	colsList, err := getStringColsList(ctx, bh, sql, 0)
	if err != nil {
		return nil, err
	}

	dbNames := make([]string, len(colsList))
	for i, cols := range colsList {
		dbNames[i] = cols[0]
	}
	return dbNames, nil
}

// func for fk table
func deleteCurFkTableInPitrRestore(ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	dbName string,
	tblName string) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to drop cur fk tables", pitrName))
	var (
		sortedFkTbls  []string
		curFkTableMap map[string]*tableInfo
		isMasterTable bool
	)

	// get topo sorted tables with foreign key
	sortedFkTbls, err = fkTablesTopoSortInPitrRestore(ctx, bh, 0, dbName, tblName)
	if err != nil {
		return
	}
	// collect table infos which need to be dropped in current state; snapshotName must set to empty
	curFkTableMap, err = getTableInfoMap(ctx, sid, bh, "", dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// drop tables as anti-topo order
	for i := len(sortedFkTbls) - 1; i >= 0; i-- {
		key := sortedFkTbls[i]
		if tblInfo := curFkTableMap[key]; tblInfo != nil {
			isMasterTable, err = checkTableIsMaster(ctx, sid, bh, "", tblInfo.dbName, tblInfo.tblName)
			if err != nil {
				return err
			}
			if isMasterTable {
				continue
			}

			getLogger(sid).Info(fmt.Sprintf("start to drop table: %v", tblInfo.tblName))
			if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists `%s`.`%s`", tblInfo.dbName, tblInfo.tblName)); err != nil {
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

	g := toposort{next: make(map[string][]string)}
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

func restoreTablesWithFkByPitr(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	sortedFkTbls []string,
	fkTableMap map[string]*tableInfo) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to drop fk related tables", pitrName))

	// recreate tables as topo order
	for _, key := range sortedFkTbls {
		// if tblInfo is nil, means that table is not in this restoration task, ignore
		// e.g. t1.pk <- t2.fk, we only want to restore t2, fkTableMap[t1.key] is nil, ignore t1
		if tblInfo := fkTableMap[key]; tblInfo != nil {
			getLogger(sid).Info(fmt.Sprintf("[%s] start to restore table with fk: %v, restore timestamp: %d", pitrName, tblInfo.tblName, ts))
			if err = reCreateTableWithPitr(ctx, sid, bh, pitrName, ts, tblInfo); err != nil {
				return
			}
		}
	}
	return
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
				if record.createTime, err = er.GetString(ctx, row, 3); err != nil {
					return nil, err
				}
				if record.modifiedTime, err = er.GetString(ctx, row, 4); err != nil {
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
	createTimeStr := pitrRecord.createTime
	// parse createTimeStr to utc time
	t, err := time.ParseInLocation("2006-01-02 15:04:05", createTimeStr, time.UTC)
	if err != nil {
		return
	}
	utcNano := t.UTC().UnixNano()
	if ts <= utcNano {
		return moerr.NewInternalErrorNoCtxf("input timestamp %v is less than the pitr valid time %v", nanoTimeFormat(ts), nanoTimeFormat(utcNano))
	}

	// use utc time now sub pitr during time get the minest time
	// is ts time less than the minest time, then return error
	pitrValue := pitrRecord.pitrValue
	pitrUnit := pitrRecord.pitrUnit

	minTs, err := addTimeSpan(int(pitrValue), pitrUnit)
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
func addTimeSpan(length int, unit string) (time.Time, error) {
	now := time.Now().UTC()

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
		// restore self account
		if len(stmt.AccountName) == 0 {
			// check the level
			if pitrRecord.level == tree.PITRLEVELDATABASE.String() || pitrRecord.level == tree.PITRLEVELTABLE.String() || pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
				return moerr.NewInternalErrorNoCtxf("restore level %v is not allowed for account restore", pitrRecord.level)
			}
			if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountId != uint64(tenantInfo.TenantID) {
				return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %v", pitrRecord.pitrName, tenantInfo.GetTenant())
			}
		} else {
			// sys restore other account's pitr
			// if the accout not sys account, return err
			if tenantInfo.GetTenantID() != sysAccountID {
				return moerr.NewInternalErrorNoCtxf("account %s is not allowed to restore other account %s", tenantInfo.GetTenant(), string(stmt.AccountName))
			}

			// if the pitr level is database or table, return err
			if pitrRecord.level == tree.PITRLEVELDATABASE.String() || pitrRecord.level == tree.PITRLEVELTABLE.String() {
				return moerr.NewInternalErrorNoCtxf("can not restore account %s by pitr %s", string(stmt.AccountName), pitrRecord.pitrName)
			}

			if len(stmt.SrcAccountName) == 0 {
				if stmt.AccountName == sysAccountName {
					return moerr.NewInternalErrorNoCtxf("can not restore sys account to new account %s by pitr %s", string(stmt.AccountName), pitrRecord.pitrName)
				}
				// if the pitr level is cluster and src account is empty, return err
				if pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
					return moerr.NewInternalErrorNoCtxf("can not restore account %s by pitr %s, source account is empty", string(stmt.AccountName), pitrRecord.pitrName)
				}
				// if the pitr level is account, can not restore sys account
				if pitrRecord.level == tree.PITRLEVELACCOUNT.String() {
					if pitrRecord.accountName == sysAccountName {
						return moerr.NewInternalErrorNoCtxf("can not restore sys account to new account %s by pitr %s", string(stmt.AccountName), pitrRecord.pitrName)
					}
				}
			} else {
				// if src account is not empty, and the pitr level is not cluster, return err
				if pitrRecord.level != tree.PITRLEVELCLUSTER.String() {
					return moerr.NewInternalErrorNoCtxf("can not restore account %s by pitr %s, the pitr level is not cluster", string(stmt.SrcAccountName), pitrRecord.pitrName)
				}
				if stmt.SrcAccountName == sysAccountName && stmt.AccountName != sysAccountName {
					return moerr.NewInternalErrorNoCtxf("can not restore sys account to new account %s by pitr %s", string(stmt.AccountName), pitrRecord.pitrName)
				}
				if stmt.SrcAccountName != sysAccountName && stmt.AccountName == sysAccountName {
					return moerr.NewInternalErrorNoCtxf("can not restore account %s to sys account by pitr %s", string(stmt.SrcAccountName), pitrRecord.pitrName)
				}
			}
		}
	case tree.RESTORELEVELDATABASE:
		// check the level
		if pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
			return moerr.NewInternalErrorNoCtxf("cluster level pitr `%v` is not allowed for database restore", pitrRecord.level)
		}
		if pitrRecord.level == tree.PITRLEVELTABLE.String() {
			return moerr.NewInternalErrorNoCtxf("restore level %v is not allowed for database restore", pitrRecord.level)
		}
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountId != uint64(tenantInfo.TenantID) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %v database %v", pitrRecord.pitrName, tenantInfo.GetTenant(), string(stmt.DatabaseName))
		}
		if pitrRecord.level == tree.PITRLEVELDATABASE.String() && (pitrRecord.accountId != uint64(tenantInfo.TenantID) || pitrRecord.databaseName != string(stmt.DatabaseName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore database %v", pitrRecord.pitrName, string(stmt.DatabaseName))
		}
	case tree.RESTORELEVELTABLE:
		// check the level
		if pitrRecord.level == tree.PITRLEVELCLUSTER.String() {
			return moerr.NewInternalErrorNoCtxf("cluster level pitr `%v` is not allowed for table restore", pitrRecord.level)
		}
		if pitrRecord.level == tree.PITRLEVELACCOUNT.String() && pitrRecord.accountId != uint64(tenantInfo.TenantID) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore account %v database %v table %v", pitrRecord.pitrName, tenantInfo.GetTenant(), string(stmt.DatabaseName), string(stmt.TableName))
		}
		if pitrRecord.level == tree.PITRLEVELDATABASE.String() && (pitrRecord.accountId != uint64(tenantInfo.TenantID) || pitrRecord.databaseName != string(stmt.DatabaseName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore database %v table %v", pitrRecord.pitrName, string(stmt.DatabaseName), string(stmt.TableName))
		}
		if pitrRecord.level == tree.PITRLEVELTABLE.String() && (pitrRecord.accountId != uint64(tenantInfo.TenantID) || pitrRecord.databaseName != string(stmt.DatabaseName) || pitrRecord.tableName != string(stmt.TableName)) {
			return moerr.NewInternalErrorNoCtxf("pitr %s is not allowed to restore table %v.%v", pitrRecord.pitrName, string(stmt.DatabaseName), string(stmt.TableName))
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unknown restore level %v", restoreLevel.String())
	}
	return nil

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

func doCheckDatabaseExistsInPitrRestore(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string) (bool, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to check if db '%v' exists at timestamp %d", pitrName, dbName, ts))

	sql, err := getSqlForCheckDatabaseWithPitr(ctx, ts, dbName)
	if err != nil {
		return false, err
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return false, err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if !execResultArrayHasData(resultSet) {
		return false, nil
	}
	return true, nil
}

func doCheckTableExistsInPitrRestore(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pitrName string,
	ts int64,
	dbName string,
	tblName string) (bool, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to check if table '%v.%v' exists at timestamp %d", pitrName, dbName, tblName, ts))

	sql, err := getSqlForCheckTableWithPitr(ctx, ts, dbName, tblName)
	if err != nil {
		return false, err
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return false, err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if !execResultArrayHasData(resultSet) {
		return false, nil
	}
	return true, nil
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
	objId uint64) (err error) {
	sql := getSqlForUpdateMoPitrAccountObjectId(
		accountName,
		objId,
		types.CurrentTimestamp().String2(time.UTC, 0))
	err = bh.Exec(ctx, sql)
	if err != nil {
		return
	}
	return
}
