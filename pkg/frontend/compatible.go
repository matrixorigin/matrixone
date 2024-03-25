// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ctx context.Context, ses FeSession, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ctx context.Context, ses FeSession, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, ses.(*Session), st)
}

func doAlterDatabaseConfig(ctx context.Context, ses *Session, ad *tree.AlterDataBaseConfig) error {
	var sql string
	var erArray []ExecResult
	var accountName string
	var databaseOwner int64
	var currentRole uint32
	var err error

	dbName := ad.DbName
	updateConfig := ad.UpdateConfig
	tenantInfo := ses.GetTenantInfo()
	accountName = tenantInfo.GetTenant()
	currentRole = tenantInfo.GetDefaultRoleID()

	updateConfigForDatabase := func() (rtnErr error) {
		bh := ses.GetBackgroundExec(ctx)
		defer bh.Close()

		rtnErr = bh.Exec(ctx, "begin")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		// step1:check database exists or not and get database owner
		sql, rtnErr = getSqlForCheckDatabaseWithOwner(ctx, dbName, int64(ses.GetTenantInfo().GetTenantID()))
		if rtnErr != nil {
			return rtnErr
		}

		bh.ClearExecResultSet()
		rtnErr = bh.Exec(ctx, sql)
		if rtnErr != nil {
			return rtnErr
		}

		erArray, rtnErr = getResultSet(ctx, bh)
		if rtnErr != nil {
			return rtnErr
		}

		if !execResultArrayHasData(erArray) {
			rtnErr = moerr.NewInternalError(ctx, "there is no database %s to change config", dbName)
			return
		} else {
			databaseOwner, rtnErr = erArray[0].GetInt64(ctx, 0, 1)
			if rtnErr != nil {
				return rtnErr
			}

			// alter database config privileges check
			if databaseOwner != int64(currentRole) {
				rtnErr = moerr.NewInternalError(ctx, "do not have privileges to alter database config")
				return
			}
		}

		// step2: update the mo_mysql_compatibility_mode of that database
		sql, rtnErr = getSqlForupdateConfigurationByDbNameAndAccountName(ctx, updateConfig, accountName, dbName, "version_compatibility")
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(ctx, sql)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	err = updateConfigForDatabase()
	if err != nil {
		return err
	}

	// step3: update the session verison
	if len(ses.GetDatabaseName()) != 0 && ses.GetDatabaseName() == dbName {
		err = changeVersion(ctx, ses, ses.GetDatabaseName())
		if err != nil {
			return err
		}
	}

	return err
}

func doAlterAccountConfig(ctx context.Context, ses *Session, stmt *tree.AlterDataBaseConfig) error {
	var sql string
	var newCtx context.Context
	var isExist bool
	var err error

	// alter account config privileges check
	if !ses.GetTenantInfo().IsMoAdminRole() {
		return moerr.NewInternalError(ctx, "do not have privileges to alter account config")
	}

	accountName := stmt.AccountName
	update_config := stmt.UpdateConfig

	updateConfigForAccount := func() (rtnErr error) {
		bh := ses.GetBackgroundExec(ctx)
		defer bh.Close()

		rtnErr = bh.Exec(ctx, "begin")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		// step 1: check account exists or not
		newCtx = defines.AttachAccountId(ctx, catalog.System_Account)
		isExist, rtnErr = checkTenantExistsOrNot(newCtx, bh, accountName)
		if rtnErr != nil {
			return rtnErr
		}

		if !isExist {
			return moerr.NewInternalError(ctx, "there is no account %s to change config", accountName)
		}

		// step2: update the config
		sql, rtnErr = getSqlForupdateConfigurationByAccount(ctx, update_config, accountName, "version_compatibility")
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = bh.Exec(ctx, sql)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	err = updateConfigForAccount()
	if err != nil {
		return err
	}

	// step3: update the session verison
	if len(ses.GetDatabaseName()) != 0 {
		err = changeVersion(ctx, ses, ses.GetDatabaseName())
		if err != nil {
			return err
		}
	}

	return err
}

func insertRecordToMoMysqlCompatibilityMode(ctx context.Context, ses *Session, stmt tree.Statement) error {
	var sql string
	var accountId uint32
	var accountName string
	var dbName string
	var err error
	variableName := "version_compatibility"
	variableValue := getVariableValue(ses.GetSysVar("version"))

	if createDatabaseStmt, ok := stmt.(*tree.CreateDatabase); ok {
		dbName = string(createDatabaseStmt.Name)
		//if create sys database, do nothing
		if _, ok = sysDatabases[dbName]; ok {
			return nil
		}

		insertRecordFunc := func() (rtnErr error) {
			bh := ses.GetBackgroundExec(ctx)
			defer bh.Close()

			rtnErr = bh.Exec(ctx, "begin")
			defer func() {
				rtnErr = finishTxn(ctx, bh, rtnErr)
			}()
			if rtnErr != nil {
				return rtnErr
			}

			//step 1: get account_name and database_name
			if ses.GetTenantInfo() != nil {
				accountName = ses.GetTenantInfo().GetTenant()
				accountId = ses.GetTenantInfo().GetTenantID()
			} else {
				return rtnErr
			}

			//step 2: check database name
			if _, ok = bannedCatalogDatabases[dbName]; ok {
				return nil
			}

			//step 3: insert the record
			sql = fmt.Sprintf(initMoMysqlCompatbilityModeFormat, accountId, accountName, dbName, variableName, variableValue, false)

			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return rtnErr
			}
			return rtnErr
		}
		err = insertRecordFunc()
		if err != nil {
			return err
		}
	}
	return nil

}

func deleteRecordToMoMysqlCompatbilityMode(ctx context.Context, ses *Session, stmt tree.Statement) error {
	var datname string
	var sql string
	var err error

	if deleteDatabaseStmt, ok := stmt.(*tree.DropDatabase); ok {
		datname = string(deleteDatabaseStmt.Name)
		//if delete sys database, do nothing
		if _, ok = sysDatabases[datname]; ok {
			return nil
		}

		deleteRecordFunc := func() (rtnErr error) {
			bh := ses.GetBackgroundExec(ctx)
			defer bh.Close()

			rtnErr = bh.Exec(ctx, "begin")
			defer func() {
				rtnErr = finishTxn(ctx, bh, rtnErr)
			}()
			if rtnErr != nil {
				return rtnErr
			}
			sql = getSqlForDeleteMysqlCompatbilityMode(datname)

			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return rtnErr
			}
			return rtnErr
		}
		err = deleteRecordFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

func GetVersionCompatibility(ctx context.Context, ses *Session, dbName string) (ret string, err error) {
	var erArray []ExecResult
	var sql string
	var resultConfig string
	defaultConfig := "0.7"
	variableName := "version_compatibility"
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return defaultConfig, err
	}

	sql = getSqlForGetSystemVariableValueWithDatabase(dbName, variableName)

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return defaultConfig, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return defaultConfig, err
	}

	if execResultArrayHasData(erArray) {
		resultConfig, err = erArray[0].GetString(ctx, 0, 0)
		if err != nil {
			return defaultConfig, err
		}
	}

	return resultConfig, err
}
