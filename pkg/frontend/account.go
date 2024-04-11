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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"math/bits"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
)

// checkSysExistsOrNot checks the SYS tenant exists or not.
func checkSysExistsOrNot(ctx context.Context, bh BackgroundExec) (bool, error) {
	var erArray []ExecResult
	var err error
	var tableNames []string
	var tableName string

	dbSql := "show databases;"
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, dbSql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	if len(erArray) != 1 {
		return false, moerr.NewInternalError(ctx, "it must have result set")
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		_, err = erArray[0].GetString(ctx, i, 0)
		if err != nil {
			return false, err
		}
	}

	sql := "show tables from mo_catalog;"
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	if len(erArray) != 1 {
		return false, moerr.NewInternalError(ctx, "it must have result set")
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		tableName, err = erArray[0].GetString(ctx, i, 0)
		if err != nil {
			return false, err
		}
		tableNames = append(tableNames, tableName)
	}

	//if there is at least one catalog table, it denotes the sys tenant exists.
	for _, name := range tableNames {
		if _, ok := sysWantedTables[name]; ok {
			return true, nil
		}
	}

	return false, nil
}

func checkTenantExistsOrNot(ctx context.Context, bh BackgroundExec, userName string) (bool, error) {
	var sqlForCheckTenant string
	var erArray []ExecResult
	var err error
	ctx, span := trace.Debug(ctx, "checkTenantExistsOrNot")
	defer span.End()
	sqlForCheckTenant, err = getSqlForCheckTenant(ctx, userName)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sqlForCheckTenant)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

func checkDatabaseExistsOrNot(ctx context.Context, bh BackgroundExec, dbName string) (bool, error) {
	var sqlForCheckDatabase string
	var erArray []ExecResult
	var err error
	ctx, span := trace.Debug(ctx, "checkTenantExistsOrNot")
	defer span.End()
	sqlForCheckDatabase, err = getSqlForCheckDatabase(ctx, dbName)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sqlForCheckDatabase)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ctx context.Context, ses FeSession, ca *tree.CreateAccount, proc *process.Process) error {
	//step1 : create new account.
	create := &CreateAccount{
		IfNotExists:  ca.IfNotExists,
		AuthOption:   ca.AuthOption,
		StatusOption: ca.StatusOption,
		Comment:      ca.Comment,
	}

	params := proc.GetPrepareParams()
	switch val := ca.Name.(type) {
	case *tree.NumVal:
		create.Name = val.OrigString()
	case *tree.ParamExpr:
		create.Name = params.GetStringAt(val.Offset - 1)
	default:
		return moerr.NewInternalError(ctx, "invalid params type")
	}

	return InitGeneralTenant(ctx, ses.(*Session), create)
}

// InitGeneralTenant initializes the application level tenant
func InitGeneralTenant(ctx context.Context, ses *Session, ca *CreateAccount) (err error) {
	var exists bool
	var newTenant *TenantInfo
	var newTenantCtx context.Context
	var mp *mpool.MPool
	ctx, span := trace.Debug(ctx, "InitGeneralTenant")
	defer span.End()
	tenant := ses.GetTenantInfo()
	finalVersion := ses.rm.baseService.GetFinalVersion()

	if !(tenant.IsSysTenant() && tenant.IsMoAdminRole()) {
		return moerr.NewInternalError(ctx, "tenant %s user %s role %s do not have the privilege to create the new account", tenant.GetTenant(), tenant.GetUser(), tenant.GetDefaultRole())
	}

	//normalize the name
	err = normalizeNameOfAccount(ctx, ca)
	if err != nil {
		return err
	}

	ca.AuthOption.AdminName, err = normalizeName(ctx, ca.AuthOption.AdminName)
	if err != nil {
		return err
	}

	if ca.AuthOption.IdentifiedType.Typ == tree.AccountIdentifiedByPassword {
		if len(ca.AuthOption.IdentifiedType.Str) == 0 {
			return moerr.NewInternalError(ctx, "password is empty string")
		}
	}

	ctx = defines.AttachAccount(ctx, uint32(tenant.GetTenantID()), uint32(tenant.GetUserID()), uint32(tenant.GetDefaultRoleID()))

	_, st := trace.Debug(ctx, "InitGeneralTenant.init_general_tenant")
	mp, err = mpool.NewMPool("init_general_tenant", 0, mpool.NoFixed)
	if err != nil {
		st.End()
		return err
	}
	st.End()
	defer mpool.DeleteMPool(mp)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	createNewAccount := func() (rtnErr error) {
		rtnErr = bh.Exec(ctx, "begin;")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		//USE the mo_catalog
		// MOVE into txn, make sure only create ONE txn.
		rtnErr = bh.Exec(ctx, "use mo_catalog;")
		if rtnErr != nil {
			return rtnErr
		}

		// check account exists or not
		exists, rtnErr = checkTenantExistsOrNot(ctx, bh, ca.Name)
		if rtnErr != nil {
			return rtnErr
		}

		if exists {
			if !ca.IfNotExists { //do nothing
				return moerr.NewInternalError(ctx, "the tenant %s exists", ca.Name)
			}
			return rtnErr
		} else {
			newTenant, newTenantCtx, rtnErr = createTablesInMoCatalogOfGeneralTenant(ctx, bh, finalVersion, ca)
			if rtnErr != nil {
				return rtnErr
			}
		}

		// create some tables and databases for new account
		rtnErr = bh.Exec(newTenantCtx, createMoIndexesSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createMoTablePartitionsSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createAutoTableSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createMoForeignKeysSql)
		if rtnErr != nil {
			return rtnErr
		}

		//create createDbSqls
		createDbSqls := []string{
			"create database " + motrace.SystemDBConst + ";",
			"create database " + mometric.MetricDBConst + ";",
			createDbInformationSchemaSql,
			"create database mysql;",
		}
		for _, db := range createDbSqls {
			rtnErr = bh.Exec(newTenantCtx, db)
			if rtnErr != nil {
				return rtnErr
			}
		}

		// create tables for new account
		rtnErr = createTablesInMoCatalogOfGeneralTenant2(bh, ca, newTenantCtx, newTenant, gPu)
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = createTablesInSystemOfGeneralTenant(newTenantCtx, bh, newTenant)
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = createTablesInInformationSchemaOfGeneralTenant(newTenantCtx, bh)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	err = createNewAccount()
	if err != nil {
		return err
	}

	if !exists {
		//just skip nonexistent pubs
		_ = createSubscriptionDatabase(ctx, bh, newTenant, ses)
	}

	return err
}

// createTablesInMoCatalogOfGeneralTenant creates catalog tables in the database mo_catalog.
func createTablesInMoCatalogOfGeneralTenant(ctx context.Context, bh BackgroundExec, finalVersion string, ca *CreateAccount) (*TenantInfo, context.Context, error) {
	var err error
	var initMoAccount string
	var erArray []ExecResult
	var newTenantID int64
	var newUserId int64
	var comment = ""
	var newTenant *TenantInfo
	var newTenantCtx context.Context
	var sql string
	//var configuration string
	//var sql string
	ctx, span := trace.Debug(ctx, "createTablesInMoCatalogOfGeneralTenant")
	defer span.End()

	if nameIsInvalid(ca.Name) {
		return nil, nil, moerr.NewInternalError(ctx, "the account name is invalid")
	}

	if nameIsInvalid(ca.AuthOption.AdminName) {
		return nil, nil, moerr.NewInternalError(ctx, "the admin name is invalid")
	}

	//!!!NOTE : Insert into mo_account with original context.
	// Other operations with a new context with new tenant info
	//step 1: add new tenant entry to the mo_account
	if ca.Comment.Exist {
		comment = ca.Comment.Comment
	}

	//determine the status of the account
	status := sysAccountStatus
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = tree.AccountStatusSuspend.String()
		}
	}

	initMoAccount = fmt.Sprintf(initMoAccountWithoutIDFormat, ca.Name, status, types.CurrentTimestamp().String2(time.UTC, 0), comment, finalVersion)
	//execute the insert
	err = bh.Exec(ctx, initMoAccount)
	if err != nil {
		return nil, nil, err
	}

	//query the tenant id
	bh.ClearExecResultSet()
	sql, err = getSqlForCheckTenant(ctx, ca.Name)
	if err != nil {
		return nil, nil, err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, nil, err
	}

	if execResultArrayHasData(erArray) {
		newTenantID, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return nil, nil, err
		}
	} else {
		return nil, nil, moerr.NewInternalError(ctx, "get the id of tenant %s failed", ca.Name)
	}

	newUserId = int64(GetAdminUserId())

	newTenant = &TenantInfo{
		Tenant:        ca.Name,
		User:          ca.AuthOption.AdminName,
		DefaultRole:   accountAdminRoleName,
		TenantID:      uint32(newTenantID),
		UserID:        uint32(newUserId),
		DefaultRoleID: accountAdminRoleID,
	}
	//with new tenant
	newTenantCtx = defines.AttachAccount(ctx, uint32(newTenantID), uint32(newUserId), uint32(accountAdminRoleID))
	return newTenant, newTenantCtx, err
}

func createTablesInMoCatalogOfGeneralTenant2(bh BackgroundExec, ca *CreateAccount, newTenantCtx context.Context, newTenant *TenantInfo, pu *config.ParameterUnit) error {
	var err error
	var initDataSqls []string
	newTenantCtx, span := trace.Debug(newTenantCtx, "createTablesInMoCatalogOfGeneralTenant2")
	defer span.End()
	//create tables for the tenant
	for _, sql := range createSqls {
		//only the SYS tenant has the table mo_account
		if strings.HasPrefix(sql, "create table mo_account") {
			continue
		}
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			return err
		}
	}

	//initialize the default data of tables for the tenant
	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}
	//step 2:add new role entries to the mo_role
	initMoRole1 := fmt.Sprintf(initMoRoleFormat, accountAdminRoleID, accountAdminRoleName, newTenant.GetUserID(), newTenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, newTenant.GetUserID(), newTenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user
	if ca.AuthOption.IdentifiedType.Typ != tree.AccountIdentifiedByPassword {
		err = moerr.NewInternalError(newTenantCtx, "only support password verification now")
		return err
	}
	name := ca.AuthOption.AdminName
	password := ca.AuthOption.IdentifiedType.Str
	if len(password) == 0 {
		err = moerr.NewInternalError(newTenantCtx, "password is empty string")
		return err
	}
	//encryption the password
	encryption := HashPassWord(password)
	status := rootStatus
	//TODO: fix the status of user or account
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = tree.AccountStatusSuspend.String()
		}
	}
	//the first user id in the general tenant
	initMoUser1 := fmt.Sprintf(initMoUserFormat, newTenant.GetUserID(), rootHost, name, encryption, status,
		types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
		newTenant.GetUserID(), newTenant.GetDefaultRoleID(), accountAdminRoleID)
	addSqlIntoSet(initMoUser1)

	//step4: add new entries to the mo_role_privs
	//accountadmin role
	for _, t := range entriesOfAccountAdminForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			accountAdminRoleID, accountAdminRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//public role
	for _, t := range entriesOfPublicForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			publicRoleID, publicRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//step5: add new entries to the mo_user_grant
	initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, accountAdminRoleID, newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant1)
	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant2)

	//setp6: add new entries to the mo_mysql_compatibility_mode
	for _, variable := range gSysVarsDefs {
		if _, ok := configInitVariables[variable.Name]; ok {
			addsql := addInitSystemVariablesSql(int(newTenant.GetTenantID()), newTenant.GetTenant(), variable.Name, pu)
			if len(addsql) != 0 {
				addSqlIntoSet(addsql)
			}
		} else {
			initMoMysqlCompatibilityMode := fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, newTenant.GetTenantID(), newTenant.GetTenant(), variable.Name, getVariableValue(variable.Default), true)
			addSqlIntoSet(initMoMysqlCompatibilityMode)
		}
	}

	//fill the mo_role, mo_user, mo_role_privs, mo_user_grant, mo_role_grant
	for _, sql := range initDataSqls {
		bh.ClearExecResultSet()
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// createTablesInSystemOfGeneralTenant creates the database system and system_metrics as the external tables.
func createTablesInSystemOfGeneralTenant(ctx context.Context, bh BackgroundExec, newTenant *TenantInfo) error {
	ctx, span := trace.Debug(ctx, "createTablesInSystemOfGeneralTenant")
	defer span.End()

	var err error
	sqls := make([]string, 0)
	sqls = append(sqls, "use "+motrace.SystemDBConst+";")
	traceTables := motrace.GetSchemaForAccount(ctx, newTenant.GetTenant())
	sqls = append(sqls, traceTables...)
	sqls = append(sqls, "use "+mometric.MetricDBConst+";")
	metricTables := mometric.GetSchemaForAccount(ctx, newTenant.GetTenant())
	sqls = append(sqls, metricTables...)

	for _, sql := range sqls {
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

// createTablesInInformationSchemaOfGeneralTenant creates the database information_schema and the views or tables.
func createTablesInInformationSchemaOfGeneralTenant(ctx context.Context, bh BackgroundExec) error {
	ctx, span := trace.Debug(ctx, "createTablesInInformationSchemaOfGeneralTenant")
	defer span.End()
	//with new tenant
	//TODO: when we have the auto_increment column, we need new strategy.

	var err error
	sqls := make([]string, 0, len(sysview.InitInformationSchemaSysTables)+len(sysview.InitMysqlSysTables)+4)

	sqls = append(sqls, "use information_schema;")
	sqls = append(sqls, sysview.InitInformationSchemaSysTables...)
	sqls = append(sqls, "use mysql;")
	sqls = append(sqls, sysview.InitMysqlSysTables...)

	for _, sql := range sqls {
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

// handleDropAccount drops a new user-level tenant
func handleDropAccount(ctx context.Context, ses FeSession, da *tree.DropAccount) error {
	return doDropAccount(ctx, ses.(*Session), da)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ctx context.Context, ses FeSession, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, ses.(*Session), aa)
}

func doAlterAccount(ctx context.Context, ses *Session, aa *tree.AlterAccount) (err error) {
	var sql string
	var erArray []ExecResult
	var targetAccountId uint64
	var version uint64
	var accountExist bool
	var accountStatus string
	account := ses.GetTenantInfo()
	if !(account.IsSysTenant() && account.IsMoAdminRole()) {
		return moerr.NewInternalError(ctx, "tenant %s user %s role %s do not have the privilege to alter the account",
			account.GetTenant(), account.GetUser(), account.GetDefaultRole())
	}

	optionBits := uint8(0)
	if aa.AuthOption.Exist {
		optionBits |= 1
	}
	if aa.StatusOption.Exist {
		optionBits |= 1 << 1
	}
	if aa.Comment.Exist {
		optionBits |= 1 << 2
	}
	optionCount := bits.OnesCount8(optionBits)
	if optionCount == 0 {
		return moerr.NewInternalError(ctx, "at least one option at a time")
	}
	if optionCount > 1 {
		return moerr.NewInternalError(ctx, "at most one option at a time")
	}

	//normalize the name
	aa.Name, err = normalizeName(ctx, aa.Name)
	if err != nil {
		return err
	}

	if aa.AuthOption.Exist {
		aa.AuthOption.AdminName, err = normalizeName(ctx, aa.AuthOption.AdminName)
		if err != nil {
			return err
		}
		if aa.AuthOption.IdentifiedType.Typ != tree.AccountIdentifiedByPassword {
			return moerr.NewInternalError(ctx, "only support identified by password")
		}

		if len(aa.AuthOption.IdentifiedType.Str) == 0 {
			err = moerr.NewInternalError(ctx, "password is empty string")
			return err
		}
	}

	if aa.StatusOption.Exist {
		//SYS account can not be suspended
		if isSysTenant(aa.Name) {
			return moerr.NewInternalError(ctx, "account sys can not be suspended")
		}
	}

	alterAccountFunc := func() (rtnErr error) {
		bh := ses.GetBackgroundExec(ctx)
		defer bh.Close()

		rtnErr = bh.Exec(ctx, "begin")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		//step 1: check account exists or not
		//get accountID
		sql, rtnErr = getSqlForCheckTenant(ctx, aa.Name)
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

		if execResultArrayHasData(erArray) {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				targetAccountId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
				if rtnErr != nil {
					return rtnErr
				}

				accountStatus, rtnErr = erArray[0].GetString(ctx, 0, 2)
				if rtnErr != nil {
					return rtnErr
				}

				version, rtnErr = erArray[0].GetUint64(ctx, i, 3)
				if rtnErr != nil {
					return rtnErr
				}
			}
			accountExist = true
		} else {
			//IfExists :
			// false : return an error
			// true : skip and do nothing
			if !aa.IfExists {
				return moerr.NewInternalError(ctx, "there is no account %s", aa.Name)
			}
		}

		if accountExist {
			//Option 1: alter the password of admin for the account
			if aa.AuthOption.Exist {
				//!!!NOTE!!!:switch into the target account's context, then update the table mo_user.
				accountCtx := defines.AttachAccountId(ctx, uint32(targetAccountId))

				//1, check the admin exists or not
				sql, rtnErr = getSqlForPasswordOfUser(ctx, aa.AuthOption.AdminName)
				if rtnErr != nil {
					return rtnErr
				}
				bh.ClearExecResultSet()
				rtnErr = bh.Exec(accountCtx, sql)
				if rtnErr != nil {
					return rtnErr
				}

				erArray, rtnErr = getResultSet(accountCtx, bh)
				if rtnErr != nil {
					return rtnErr
				}

				if !execResultArrayHasData(erArray) {
					rtnErr = moerr.NewInternalError(accountCtx, "there is no user %s", aa.AuthOption.AdminName)
					return
				}

				//2, update the password
				//encryption the password
				encryption := HashPassWord(aa.AuthOption.IdentifiedType.Str)
				sql, rtnErr = getSqlForUpdatePasswordOfUser(ctx, encryption, aa.AuthOption.AdminName)
				if rtnErr != nil {
					return rtnErr
				}
				bh.ClearExecResultSet()
				rtnErr = bh.Exec(accountCtx, sql)
				if rtnErr != nil {
					return rtnErr
				}
			}

			//Option 2: alter the comment of the account
			if aa.Comment.Exist {
				sql, rtnErr = getSqlForUpdateCommentsOfAccount(ctx, aa.Comment.Comment, aa.Name)
				if rtnErr != nil {
					return rtnErr
				}
				bh.ClearExecResultSet()
				rtnErr = bh.Exec(ctx, sql)
				if rtnErr != nil {
					return rtnErr
				}
			}

			//Option 3: suspend or resume the account
			if aa.StatusOption.Exist {
				if aa.StatusOption.Option == tree.AccountStatusSuspend {
					sql, rtnErr = getSqlForUpdateStatusOfAccount(ctx, aa.StatusOption.Option.String(), types.CurrentTimestamp().String2(time.UTC, 0), aa.Name)
					if rtnErr != nil {
						return rtnErr
					}
					bh.ClearExecResultSet()
					rtnErr = bh.Exec(ctx, sql)
					if rtnErr != nil {
						return rtnErr
					}
				} else if aa.StatusOption.Option == tree.AccountStatusOpen {
					sql, rtnErr = getSqlForUpdateStatusAndVersionOfAccount(ctx, aa.StatusOption.Option.String(), aa.Name, (version+1)%math.MaxUint64)
					if rtnErr != nil {
						return rtnErr
					}
					bh.ClearExecResultSet()
					rtnErr = bh.Exec(ctx, sql)
					if rtnErr != nil {
						return rtnErr
					}
				} else if aa.StatusOption.Option == tree.AccountStatusRestricted {
					sql, rtnErr = getSqlForUpdateStatusOfAccount(ctx, aa.StatusOption.Option.String(), types.CurrentTimestamp().String2(time.UTC, 0), aa.Name)
					if rtnErr != nil {
						return rtnErr
					}
					bh.ClearExecResultSet()
					rtnErr = bh.Exec(ctx, sql)
					if rtnErr != nil {
						return rtnErr
					}
				}
			}
		}
		return rtnErr
	}

	err = alterAccountFunc()
	if err != nil {
		return err
	}

	//if alter account suspend, add the account to kill queue
	if accountExist {
		if aa.StatusOption.Exist && aa.StatusOption.Option == tree.AccountStatusSuspend {
			ses.getRoutineManager().accountRoutine.EnKillQueue(int64(targetAccountId), version)

			if err := postDropSuspendAccount(ctx, ses, aa.Name, int64(targetAccountId), version); err != nil {
				logutil.Errorf("post alter account suspend error: %s", err.Error())
			}
		}

		if aa.StatusOption.Exist && aa.StatusOption.Option == tree.AccountStatusRestricted {
			accountId2RoutineMap := ses.getRoutineManager().accountRoutine.deepCopyRoutineMap()
			if rtMap, ok := accountId2RoutineMap[int64(targetAccountId)]; ok {
				for rt := range rtMap {
					rt.setResricted(true)
				}
			}
			err = postAlterSessionStatus(ctx, ses, aa.Name, int64(targetAccountId), tree.AccountStatusRestricted.String())
			if err != nil {
				logutil.Errorf("post alter account restricted error: %s", err.Error())
			}
		}

		if aa.StatusOption.Exist && aa.StatusOption.Option == tree.AccountStatusOpen && accountStatus == tree.AccountStatusRestricted.String() {
			accountId2RoutineMap := ses.getRoutineManager().accountRoutine.deepCopyRoutineMap()
			if rtMap, ok := accountId2RoutineMap[int64(targetAccountId)]; ok {
				for rt := range rtMap {
					rt.setResricted(false)
				}
			}
			err = postAlterSessionStatus(ctx, ses, aa.Name, int64(targetAccountId), tree.AccountStatusOpen.String())
			if err != nil {
				logutil.Errorf("post alter account not restricted error: %s", err.Error())
			}
		}
	}

	return err
}

// doDropAccount accomplishes the DropAccount statement
func doDropAccount(ctx context.Context, ses *Session, da *tree.DropAccount) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//set backgroundHandler's default schema
	if handler, ok := bh.(*backExec); ok {
		handler.backSes.
			txnCompileCtx.dbName = catalog.MO_CATALOG
	}

	var sql, db, table string
	var erArray []ExecResult
	var databases map[string]int8
	var dbSql, prefix string
	var sqlsForDropDatabases = make([]string, 0, 5)

	var deleteCtx context.Context
	var accountId int64
	var version uint64
	var hasAccount = true
	clusterTables := make(map[string]int)

	da.Name, err = normalizeName(ctx, da.Name)
	if err != nil {
		return err
	}

	if isSysTenant(da.Name) {
		return moerr.NewInternalError(ctx, "can not delete the account %s", da.Name)
	}

	dropAccountFunc := func() (rtnErr error) {
		rtnErr = bh.Exec(ctx, "begin;")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		//check the account exists or not
		sql, rtnErr = getSqlForCheckTenant(ctx, da.Name)
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

		if execResultArrayHasData(erArray) {
			accountId, rtnErr = erArray[0].GetInt64(ctx, 0, 0)
			if rtnErr != nil {
				return rtnErr
			}
			version, rtnErr = erArray[0].GetUint64(ctx, 0, 3)
			if rtnErr != nil {
				return rtnErr
			}
		} else {
			//no such account
			if !da.IfExists { //when the "IF EXISTS" is set, just skip it.
				rtnErr = moerr.NewInternalError(ctx, "there is no account %s", da.Name)
				return
			}
			hasAccount = false
		}

		if !hasAccount {
			return rtnErr
		}

		//drop tables of the tenant
		//NOTE!!!: single DDL drop statement per single transaction
		//SWITCH TO THE CONTEXT of the deleted context
		deleteCtx = defines.AttachAccountId(ctx, uint32(accountId))

		//step 2 : drop table mo_user
		//step 3 : drop table mo_role
		//step 4 : drop table mo_user_grant
		//step 5 : drop table mo_role_grant
		//step 6 : drop table mo_role_privs
		//step 7 : drop table mo_user_defined_function
		//step 8 : drop table mo_mysql_compatibility_mode
		//step 9 : drop table %!%mo_increment_columns
		for _, sql = range getSqlForDropAccount() {
			rtnErr = bh.Exec(deleteCtx, sql)
			if rtnErr != nil {
				return rtnErr
			}
		}

		//drop databases created by user
		databases = make(map[string]int8)
		dbSql = "show databases;"
		bh.ClearExecResultSet()
		rtnErr = bh.Exec(deleteCtx, dbSql)
		if rtnErr != nil {
			return rtnErr
		}

		erArray, rtnErr = getResultSet(ctx, bh)
		if rtnErr != nil {
			return rtnErr
		}

		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			db, rtnErr = erArray[0].GetString(ctx, i, 0)
			if rtnErr != nil {
				return rtnErr
			}
			databases[db] = 0
		}

		prefix = "drop database if exists "

		for db = range databases {
			if db == "mo_catalog" {
				continue
			}
			bb := &bytes.Buffer{}
			bb.WriteString(prefix)
			//handle the database annotated by '`'
			if db != strings.ToLower(db) {
				bb.WriteString("`")
				bb.WriteString(db)
				bb.WriteString("`")
			} else {
				bb.WriteString(db)
			}
			bb.WriteString(";")
			sqlsForDropDatabases = append(sqlsForDropDatabases, bb.String())
		}

		for _, sql = range sqlsForDropDatabases {
			rtnErr = bh.Exec(deleteCtx, sql)
			if rtnErr != nil {
				return rtnErr
			}
		}

		// drop table mo_mysql_compatibility_mode
		rtnErr = bh.Exec(deleteCtx, dropMoMysqlCompatibilityModeSql)
		if rtnErr != nil {
			return rtnErr
		}

		// drop table mo_pubs
		rtnErr = bh.Exec(deleteCtx, dropMoPubsSql)
		if rtnErr != nil {
			return rtnErr
		}

		// drop autoIcr table
		rtnErr = bh.Exec(deleteCtx, dropAutoIcrColSql)
		if rtnErr != nil {
			return rtnErr
		}

		// drop mo_catalog.mo_indexes under general tenant
		rtnErr = bh.Exec(deleteCtx, dropMoIndexes)
		if rtnErr != nil {
			return rtnErr
		}

		// drop mo_catalog.mo_table_partitions under general tenant
		rtnErr = bh.Exec(deleteCtx, dropMoTablePartitions)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(deleteCtx, dropMoForeignKeys)
		if rtnErr != nil {
			return rtnErr
		}

		// delete the account in the mo_account of the sys account
		sql, rtnErr = getSqlForDeleteAccountFromMoAccount(ctx, da.Name)
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = bh.Exec(ctx, sql)
		if rtnErr != nil {
			return rtnErr
		}

		// get all cluster table in the mo_catalog
		sql = "show tables from mo_catalog;"
		bh.ClearExecResultSet()
		rtnErr = bh.Exec(ctx, sql)
		if rtnErr != nil {
			return rtnErr
		}

		erArray, rtnErr = getResultSet(ctx, bh)
		if rtnErr != nil {
			return rtnErr
		}

		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			table, rtnErr = erArray[0].GetString(ctx, i, 0)
			if rtnErr != nil {
				return rtnErr
			}
			if isClusterTable("mo_catalog", table) {
				clusterTables[table] = 0
			}
		}

		// delete all data of the account in the cluster table
		for clusterTable := range clusterTables {
			sql = fmt.Sprintf("delete from mo_catalog.`%s` where account_id = %d;", clusterTable, accountId)
			bh.ClearExecResultSet()
			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return rtnErr
			}
		}
		return rtnErr
	}

	err = dropAccountFunc()
	if err != nil {
		return err
	}

	// if drop the account, add the account to kill queue
	ses.getRoutineManager().accountRoutine.EnKillQueue(accountId, version)

	if err := postDropSuspendAccount(ctx, ses, da.Name, accountId, version); err != nil {
		logutil.Errorf("post drop account error: %s", err.Error())
	}

	return err
}

func postDropSuspendAccount(
	ctx context.Context, ses *Session, accountName string, accountID int64, version uint64,
) (err error) {
	qc := gPu.QueryClient
	if qc == nil {
		return moerr.NewInternalError(ctx, "query client is not initialized")
	}
	var nodes []string
	currTenant := ses.GetTenantInfo().Tenant
	currUser := ses.GetTenantInfo().User
	labels := clusterservice.NewSelector().SelectByLabel(
		map[string]string{"account": accountName}, clusterservice.Contain)
	sysTenant := isSysTenant(currTenant)
	if sysTenant {
		route.RouteForSuperTenant(clusterservice.NewSelector(), currUser, nil,
			func(s *metadata.CNService) {
				nodes = append(nodes, s.QueryAddress)
			})
	} else {
		route.RouteForCommonTenant(labels, nil, func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})
	}

	var retErr error
	genRequest := func() *query.Request {
		req := qc.NewRequest(query.CmdMethod_KillConn)
		req.KillConnRequest = &query.KillConnRequest{
			AccountID: accountID,
			Version:   version,
		}
		return req
	}

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.KillConnResponse != nil && !rsp.KillConnResponse.Success {
			retErr = moerr.NewInternalError(ctx,
				fmt.Sprintf("kill connection for account %s failed on node %s", accountName, nodeAddr))
		}
	}

	handleInvalidResponse := func(nodeAddr string) {
		retErr = moerr.NewInternalError(ctx,
			fmt.Sprintf("kill connection for account %s failed on node %s", accountName, nodeAddr))
	}

	err = queryservice.RequestMultipleCn(ctx, nodes, qc, genRequest, handleValidResponse, handleInvalidResponse)
	return errors.Join(err, retErr)
}

// postAlterSessionStatus post alter all nodes session status which the tenant has been alter restricted or open.
func postAlterSessionStatus(
	ctx context.Context,
	ses *Session,
	accountName string,
	tenantId int64,
	status string) error {
	qc := gPu.QueryClient
	if qc == nil {
		return moerr.NewInternalError(ctx, "query client is not initialized")
	}
	currTenant := ses.GetTenantInfo().Tenant
	currUser := ses.GetTenantInfo().User
	var nodes []string
	labels := clusterservice.NewSelector().SelectByLabel(
		map[string]string{"account": accountName}, clusterservice.Contain)
	sysTenant := isSysTenant(currTenant)
	if sysTenant {
		route.RouteForSuperTenant(clusterservice.NewSelector(), currUser, nil,
			func(s *metadata.CNService) {
				nodes = append(nodes, s.QueryAddress)
			})
	} else {
		route.RouteForCommonTenant(labels, nil, func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})
	}

	var retErr, err error

	genRequest := func() *query.Request {
		req := qc.NewRequest(query.CmdMethod_AlterAccount)
		req.AlterAccountRequest = &query.AlterAccountRequest{
			TenantId: tenantId,
			Status:   status,
		}
		return req
	}

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.AlterAccountResponse != nil && !rsp.AlterAccountResponse.AlterSuccess {
			retErr = moerr.NewInternalError(ctx,
				fmt.Sprintf("alter account status for account %s failed on node %s", accountName, nodeAddr))
		}
	}

	handleInvalidResponse := func(nodeAddr string) {
		retErr = moerr.NewInternalError(ctx,
			fmt.Sprintf("alter account status for account %s failed on node %s", accountName, nodeAddr))
	}

	err = queryservice.RequestMultipleCn(ctx, nodes, qc, genRequest, handleValidResponse, handleInvalidResponse)
	return errors.Join(err, retErr)
}

func addInitSystemVariablesSql(accountId int, accountName, variable_name string, pu *config.ParameterUnit) string {
	var initMoMysqlCompatibilityMode string

	switch variable_name {
	case SaveQueryResult:
		if strings.ToLower(pu.SV.SaveQueryResult) == "on" {
			initMoMysqlCompatibilityMode = fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, accountId, accountName, "save_query_result", getVariableValue(pu.SV.SaveQueryResult), true)

		} else {
			initMoMysqlCompatibilityMode = fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, accountId, accountName, "save_query_result", getVariableValue("off"), true)
		}

	case QueryResultMaxsize:
		initMoMysqlCompatibilityMode = fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, accountId, accountName, "query_result_maxsize", getVariableValue(pu.SV.QueryResultMaxsize), true)

	case QueryResultTimeout:
		initMoMysqlCompatibilityMode = fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, accountId, accountName, "query_result_timeout", getVariableValue(pu.SV.QueryResultTimeout), true)

	case LowerCaseTableNames:
		initMoMysqlCompatibilityMode = fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, accountId, accountName, "lower_case_table_names", getVariableValue(pu.SV.LowerCaseTableNames), true)
	}

	return initMoMysqlCompatibilityMode
}
