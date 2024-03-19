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
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// InitSysTenant initializes the tenant SYS before any tenants and accepting any requests
// during the system is booting.
func InitSysTenant2(ctx context.Context, txn executor.TxnExecutor, finalVersion string) (err error) {
	txn.Use(catalog.MO_CATALOG)
	res, err := txn.Exec(createDbInformationSchemaSql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()

	exists, err := checkSysExistsOrNot2(ctx, txn)
	if err != nil {
		return err
	}

	if !exists {
		if err = createTablesInMoCatalog2(ctx, txn, finalVersion); err != nil {
			return err
		}
	}
	return err
}

// createTablesInMoCatalog2 creates catalog tables in the database mo_catalog.
func createTablesInMoCatalog2(ctx context.Context, txn executor.TxnExecutor, finalVersion string) error {
	var initMoAccount string
	var initDataSqls []string

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//create tables for the tenant
	for _, sql := range createSqls {
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant
	//step 1: add new tenant entry to the mo_account
	initMoAccount = fmt.Sprintf(initMoAccountFormat, sysAccountID, sysAccountName, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), sysAccountComments, finalVersion)
	addSqlIntoSet(initMoAccount)

	//step 2:add new role entries to the mo_role

	initMoRole1 := fmt.Sprintf(initMoRoleFormat, moAdminRoleID, moAdminRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user
	defaultPassword := rootPassword
	if d := os.Getenv(defaultPasswordEnv); d != "" {
		defaultPassword = d
	}

	//encryption the password
	encryption := HashPassWord(defaultPassword)

	initMoUser1 := fmt.Sprintf(initMoUserFormat, rootID, rootHost, rootName, encryption, rootStatus, types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType, rootCreatorID, rootOwnerRoleID, rootDefaultRoleID)
	initMoUser2 := fmt.Sprintf(initMoUserFormat, dumpID, dumpHost, dumpName, encryption, dumpStatus, types.CurrentTimestamp().String2(time.UTC, 0), dumpExpiredTime, dumpLoginType, dumpCreatorID, dumpOwnerRoleID, dumpDefaultRoleID)
	addSqlIntoSet(initMoUser1)
	addSqlIntoSet(initMoUser2)

	//step4: add new entries to the mo_role_privs
	//moadmin role
	for _, t := range entriesOfMoAdminForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			moAdminRoleID, moAdminRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			rootID, types.CurrentTimestamp().String2(time.UTC, 0),
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
			rootID, types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//step5: add new entries to the mo_user_grant

	initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, moAdminRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	addSqlIntoSet(initMoUserGrant1)
	addSqlIntoSet(initMoUserGrant2)
	initMoUserGrant4 := fmt.Sprintf(initMoUserGrantFormat, moAdminRoleID, dumpID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	initMoUserGrant5 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, dumpID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	addSqlIntoSet(initMoUserGrant4)
	addSqlIntoSet(initMoUserGrant5)

	//setp6: add new entries to the mo_mysql_compatibility_mode
	pu := config.GetParameterUnit(ctx)
	for _, variable := range gSysVarsDefs {
		if _, ok := configInitVariables[variable.Name]; ok {
			addsql := addInitSystemVariablesSql(sysAccountID, sysAccountName, variable.Name, pu)
			if len(addsql) != 0 {
				addSqlIntoSet(addsql)
			}
		} else {
			initMoMysqlCompatibilityMode := fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, sysAccountID, sysAccountName, variable.Name, getVariableValue(variable.Default), true)
			addSqlIntoSet(initMoMysqlCompatibilityMode)
		}
	}

	//fill the mo_account, mo_role, mo_user, mo_role_privs, mo_user_grant, mo_mysql_compatibility_mode
	for _, sql := range initDataSqls {
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

// checkSysExistsOrNot checks the SYS tenant exists or not.
func checkSysExistsOrNot2(ctx context.Context, txn executor.TxnExecutor) (bool, error) {
	if res, err := txn.Exec("show databases;", executor.StatementOption{}); err != nil {
		return false, err
	} else {
		count := 0
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			count += rows
			return true
		})
		res.Close()

		if count == 0 {
			return false, moerr.NewInternalError(ctx, "it must have result set")
		}
	}

	if res, err := txn.Exec("show tables from mo_catalog;", executor.StatementOption{}); err != nil {
		return false, err
	} else {
		count := 0
		sysTenantExists := false
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				count += i
				tableName := cols[0].GetStringAt(i)
				// if there is at least one catalog table, it denotes the sys tenant exists.
				if _, ok := sysWantedTables[tableName]; ok {
					sysTenantExists = true
				}
			}
			return true
		})
		res.Close()

		if count == 0 {
			return false, moerr.NewInternalError(ctx, "it must have result set")
		}

		if sysTenantExists {
			return true, nil
		}
	}
	return false, nil
}

// GenSQLForInsertUpgradeAccountPrivilege generates SQL statements for inserting upgrade account permissions
func GenSQLForInsertUpgradeAccountPrivilege() string {
	entry := privilegeEntry{
		privilegeId:       PrivilegeTypeUpgradeAccount,
		privilegeLevel:    privilegeLevelStar,
		objType:           objectTypeAccount,
		objId:             objectIDAll,
		withGrantOption:   false,
		databaseName:      "",
		tableName:         "",
		privilegeEntryTyp: privilegeEntryTypeGeneral,
		compound:          nil,
	}
	return fmt.Sprintf(initMoRolePrivFormat,
		moAdminRoleID, moAdminRoleName,
		entry.objType, entry.objId,
		entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
		rootID, types.CurrentTimestamp().String2(time.UTC, 0),
		entry.withGrantOption)
}

// GenSQLForCheckUpgradeAccountPrivilegeExist generates an SQL statement to check for the existence of upgrade account permissions.
func GenSQLForCheckUpgradeAccountPrivilegeExist() string {
	entry := privilegeEntry{
		privilegeId:       PrivilegeTypeUpgradeAccount,
		privilegeLevel:    privilegeLevelStar,
		objType:           objectTypeAccount,
		objId:             objectIDAll,
		withGrantOption:   false,
		databaseName:      "",
		tableName:         "",
		privilegeEntryTyp: privilegeEntryTypeGeneral,
		compound:          nil,
	}

	sql := fmt.Sprintf("select * from mo_catalog.mo_role_privs where role_id = %d and obj_type = '%s' and obj_id = %d and privilege_id = %d and privilege_level = '%s'",
		moAdminRoleID, entry.objType, entry.objId, entry.privilegeId, entry.privilegeLevel)
	return sql
}
