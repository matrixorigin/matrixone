// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"math/rand"
	"strings"
	"time"
)

type TenantInfo struct {
	Tenant      string
	User        string
	DefaultRole string

	TenantID      int
	UserID        int
	DefaultRoleID int
}

func (ti *TenantInfo) String() string {
	return fmt.Sprintf("{tenantInfo %s:%s:%s -- %d:%d:%d}", ti.Tenant, ti.User, ti.DefaultRole, ti.TenantID, ti.UserID, ti.DefaultRoleID)
}

func (ti *TenantInfo) GetTenant() string {
	return ti.Tenant
}

func (ti *TenantInfo) GetTenantID() int {
	return ti.TenantID
}

func (ti *TenantInfo) SetTenantID(id int) {
	ti.TenantID = id
}

func (ti *TenantInfo) GetUser() string {
	return ti.User
}

func (ti *TenantInfo) GetUserID() int {
	return ti.UserID
}

func (ti *TenantInfo) SetUserID(id int) {
	ti.UserID = id
}

func (ti *TenantInfo) GetDefaultRole() string {
	return ti.DefaultRole
}

func (ti *TenantInfo) GetDefaultRoleID() int {
	return ti.DefaultRoleID
}

func (ti *TenantInfo) SetDefaultRoleID(id int) {
	ti.DefaultRoleID = id
}

func (ti *TenantInfo) IsSysTenant() bool {
	return ti.GetTenant() == GetDefaultTenant()
}

func (ti *TenantInfo) IsDefaultRole() bool {
	return ti.GetDefaultRole() == GetDefaultRole()
}

func GetDefaultTenant() string {
	return "sys"
}

func GetDefaultRole() string {
	return "public"
}

//GetTenantInfo extract tenant info from the input of the user.
/**
The format of the user
1. tenant:user:role
2. tenant:user
3. user
*/
func GetTenantInfo(userInput string) (*TenantInfo, error) {
	p := strings.IndexByte(userInput, ':')
	if p == -1 {
		return &TenantInfo{
			Tenant:      GetDefaultTenant(),
			User:        userInput,
			DefaultRole: GetDefaultRole(),
		}, nil
	} else {
		tenant := userInput[:p]
		tenant = strings.TrimSpace(tenant)
		if len(tenant) == 0 {
			return &TenantInfo{}, fmt.Errorf("invalid tenant name '%s'", tenant)
		}
		userRole := userInput[p+1:]
		p2 := strings.IndexByte(userRole, ':')
		if p2 == -1 {
			//tenant:user
			user := userRole
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid user name '%s'", user)
			}
			return &TenantInfo{
				Tenant:      strings.ToLower(tenant),
				User:        strings.ToLower(user),
				DefaultRole: GetDefaultRole(),
			}, nil
		} else {
			user := userRole[:p2]
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid user name '%s'", user)
			}
			role := userRole[p2+1:]
			role = strings.TrimSpace(role)
			if len(role) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid role name '%s'", role)
			}
			return &TenantInfo{
				Tenant:      strings.ToLower(tenant),
				User:        strings.ToLower(user),
				DefaultRole: strings.ToLower(role),
			}, nil
		}
	}
}

const (
	createMoUserIndex      = 0
	createMoAccountIndex   = 1
	createMoRoleIndex      = 2
	createMoUserGrantIndex = 3
	createMoRoleGrantIndex = 4
	createMoRolePrivIndex  = 5
)

const (
	//tenant
	sysAccountID       = 0
	sysAccountName     = "sys"
	sysAccountStatus   = "open"
	sysAccountComments = "system account"

	//role
	moAdminRoleID           = 0
	moAdminRoleName         = "moadmin"
	moAdminRoleComment      = "super admin role"
	publicRoleID            = 1
	publicRoleName          = "public"
	publicRoleComment       = "public role"
	accountAdminRoleID      = 2
	accountAdminRoleName    = "accountadmin"
	accountAdminRoleComment = "account admin role"

	//user
	rootID            = 0
	rootHost          = "NULL"
	rootName          = "root"
	rootPassword      = "111"
	rootStatus        = "open"
	rootExpiredTime   = "NULL"
	rootLoginType     = "PASSWORD"
	rootCreatorID     = rootID
	rootOwnerRoleID   = moAdminRoleID
	rootDefaultRoleID = moAdminRoleID

	dumpID            = 1
	dumpHost          = "NULL"
	dumpName          = "dump"
	dumpPassword      = "111"
	dumpStatus        = "open"
	dumpExpiredTime   = "NULL"
	dumpLoginType     = "PASSWORD"
	dumpCreatorID     = rootID
	dumpOwnerRoleID   = moAdminRoleID
	dumpDefaultRoleID = moAdminRoleID
)

const (
	objectTypeDatabase = "database"
	objectTypeTable    = "table"
	objectTypeFunction = "function"
	objectTypeView     = "view"
	objectTypeIndex    = "index"

	objectIDAll = 0 //denotes all objects in the object type
)

const (
	//*
	privilegeLevelStar = "*"
	//*.*
	privilegeLevelStarStar = "**"
	//db_name.*
	privilegeLevelDatabaseStar = "_*"
	//db_name.tbl_name
	privilegeLevelDatabaseTable = "d_t"
	//tbl_name
	privilegeLevelTable = "t"
	//db_name.routine_name
	privilegeLevelRoutine = "r"
)

type PrivilegeType int

const (
	PrivilegeTypeCreateAccount PrivilegeType = iota
	PrivilegeTypeDropAccount
	PrivilegeTypeAlterAccount
	PrivilegeTypeCreateUser
	PrivilegeTypeDropUser
	PrivilegeTypeAlterUser
	PrivilegeTypeCreateRole
	PrivilegeTypeDropRole
	PrivilegeTypeAlterRole
	PrivilegeTypeCreateDatabase
	PrivilegeTypeDropDatabase
	PrivilegeTypeShowDatabases
	PrivilegeTypeConnect
	PrivilegeTypeManageGrants
	PrivilegeTypeAll
	PrivilegeTypeOwnership
	PrivilegeTypeShowTables
	PrivilegeTypeCreateTable
	PrivilegeTypeCreateView
	PrivilegeTypeDropTable
	PrivilegeTypeDropView
	PrivilegeTypeAlterTable
	PrivilegeTypeAlterView
	PrivilegeTypeSelect
	PrivilegeTypeInsert
	PrivilegeTypeUpdate
	PrivilegeTypeTruncate
	PrivilegeTypeDelete
	PrivilegeTypeReference
	PrivilegeTypeIndex //include create/alter/drop index
	PrivilegeTypeExecute
)

type PrivilegeScope uint8

const (
	PrivilegeScopeSys      PrivilegeScope = 1
	PrivilegeScopeAccount                 = 2
	PrivilegeScopeUser                    = 4
	PrivilegeScopeRole                    = 8
	PrivilegeScopeDatabase                = 16
	PrivilegeScopeTable                   = 32
	PrivilegeScopeRoutine                 = 64
)

func (ps PrivilegeScope) String() string {
	sb := strings.Builder{}
	for i := 0; i < 8; i++ {
		if i != 0 {
			sb.WriteString(",")
		}
		switch ps & (1 << i) {
		case PrivilegeScopeSys:
			sb.WriteString("sys")
		case PrivilegeScopeAccount:
			sb.WriteString("account")
		case PrivilegeScopeUser:
			sb.WriteString("user")
		case PrivilegeScopeRole:
			sb.WriteString("role")
		case PrivilegeScopeDatabase:
			sb.WriteString("database")
		case PrivilegeScopeTable:
			sb.WriteString("table")
		case PrivilegeScopeRoutine:
			sb.WriteString("routine")
		}
	}
	return sb.String()
}

func (pt PrivilegeType) String() string {
	switch pt {
	case PrivilegeTypeCreateAccount:
		return "create account"
	case PrivilegeTypeDropAccount:
		return "drop account"
	case PrivilegeTypeAlterAccount:
		return "alter account"
	case PrivilegeTypeCreateUser:
		return "create user"
	case PrivilegeTypeDropUser:
		return "drop user"
	case PrivilegeTypeAlterUser:
		return "alter user"
	case PrivilegeTypeCreateRole:
		return "create role"
	case PrivilegeTypeDropRole:
		return "drop role"
	case PrivilegeTypeAlterRole:
		return "alter role"
	case PrivilegeTypeCreateDatabase:
		return "create database"
	case PrivilegeTypeDropDatabase:
		return "drop database"
	case PrivilegeTypeShowDatabases:
		return "show databases"
	case PrivilegeTypeConnect:
		return "connect"
	case PrivilegeTypeManageGrants:
		return "manage grants"
	case PrivilegeTypeAll:
		return "all"
	case PrivilegeTypeOwnership:
		return "ownership"
	case PrivilegeTypeShowTables:
		return "show tables"
	case PrivilegeTypeCreateTable:
		return "create table"
	case PrivilegeTypeCreateView:
		return "create view"
	case PrivilegeTypeDropTable:
		return "drop table"
	case PrivilegeTypeDropView:
		return "drop view"
	case PrivilegeTypeAlterTable:
		return "alter table"
	case PrivilegeTypeAlterView:
		return "alter view"
	case PrivilegeTypeSelect:
		return "select"
	case PrivilegeTypeInsert:
		return "insert"
	case PrivilegeTypeUpdate:
		return "update"
	case PrivilegeTypeTruncate:
		return "truncate"
	case PrivilegeTypeDelete:
		return "delete"
	case PrivilegeTypeReference:
		return "reference"
	case PrivilegeTypeIndex:
		return "create/alter/drop index"
	case PrivilegeTypeExecute:
		return "execute"
	}
	panic(fmt.Sprintf("no such privilege type %d", pt))
}

func (pt PrivilegeType) Scope() PrivilegeScope {
	switch pt {
	case PrivilegeTypeCreateAccount:
		return PrivilegeScopeSys
	case PrivilegeTypeDropAccount:
		return PrivilegeScopeSys
	case PrivilegeTypeAlterAccount:
		return PrivilegeScopeSys
	case PrivilegeTypeCreateUser:
		return PrivilegeScopeAccount
	case PrivilegeTypeDropUser:
		return PrivilegeScopeAccount
	case PrivilegeTypeAlterUser:
		return PrivilegeScopeAccount
	case PrivilegeTypeCreateRole:
		return PrivilegeScopeAccount
	case PrivilegeTypeDropRole:
		return PrivilegeScopeAccount
	case PrivilegeTypeAlterRole:
		return PrivilegeScopeAccount
	case PrivilegeTypeCreateDatabase:
		return PrivilegeScopeAccount
	case PrivilegeTypeDropDatabase:
		return PrivilegeScopeAccount
	case PrivilegeTypeShowDatabases:
		return PrivilegeScopeAccount
	case PrivilegeTypeConnect:
		return PrivilegeScopeAccount
	case PrivilegeTypeManageGrants:
		return PrivilegeScopeAccount
	case PrivilegeTypeAll:
		return PrivilegeScopeAccount | PrivilegeScopeDatabase | PrivilegeScopeTable
	case PrivilegeTypeOwnership:
		return PrivilegeScopeAccount | PrivilegeScopeUser | PrivilegeScopeRole | PrivilegeScopeDatabase | PrivilegeScopeTable
	case PrivilegeTypeShowTables:
		return PrivilegeScopeDatabase
	case PrivilegeTypeCreateTable:
		return PrivilegeScopeDatabase
	case PrivilegeTypeCreateView:
		return PrivilegeScopeTable
	case PrivilegeTypeDropTable:
		return PrivilegeScopeDatabase
	case PrivilegeTypeDropView:
		return PrivilegeScopeTable
	case PrivilegeTypeAlterTable:
		return PrivilegeScopeDatabase
	case PrivilegeTypeAlterView:
		return PrivilegeScopeTable
	case PrivilegeTypeSelect:
		return PrivilegeScopeTable
	case PrivilegeTypeInsert:
		return PrivilegeScopeTable
	case PrivilegeTypeUpdate:
		return PrivilegeScopeTable
	case PrivilegeTypeTruncate:
		return PrivilegeScopeTable
	case PrivilegeTypeDelete:
		return PrivilegeScopeTable
	case PrivilegeTypeReference:
		return PrivilegeScopeTable
	case PrivilegeTypeIndex:
		return PrivilegeScopeTable
	case PrivilegeTypeExecute:
		return PrivilegeScopeTable
	}
	panic(fmt.Sprintf("no such privilege type %d", pt))
}

var (
	sysWantedDatabases = map[string]int8{
		"mo_catalog":         0,
		"information_schema": 0,
		"system":             0,
		"system_metrics":     0,
	}
	sysWantedTables = map[string]int8{
		"mo_database":         0,
		"mo_tables":           0,
		"mo_columns":          0,
		"mo_user":             0,
		"mo_account":          0,
		"mo_role":             0,
		"mo_user_grant":       0,
		"mo_role_grant":       0,
		"mo_role_priv":        0,
		"mo_global_variables": 0,
	}
	//the sqls creating many tables for the tenant.
	//Wrap them in a transaction
	createSqls = []string{
		`create table mo_user(
				user_id int,
				user_host varchar(100),
				user_name varchar(100),
				authentication_string varchar(100),
				status   varchar(8),
				created_time  timestamp,
				expired_time timestamp,
				login_type  varchar(16),
				creator int,
				owner int,
				default_role int
    		);`,
		`create table mo_account(
				account_id int,
				account_name varchar(100),
				status varchar(100),
				created_time timestamp,
				comments varchar(256)
			);`,
		`create table mo_role(
				role_id int,
				role_name varchar(100),
				creator int,
				owner int,
				created_time timestamp,
				comments text
			);`,
		`create table mo_user_grant(
				role_id int,
				user_id int,
				granted_time timestamp,
				with_grant_option bool
			);`,
		`create table mo_role_grant(
				granted_id int,
				grantee_id int,
				operation_role_id int,
				operation_user_id int,
				granted_time timestamp,
				with_grant_option bool
			);`,
		`create table mo_role_priv(
				role_id int,
				role_name  varchar(100),
				obj_type  varchar(16),
				obj_id int,
				privilege_id int,
				privilege_name varchar(100),
				privilege_level varchar(100),
				operation_user_id int,
				granted_time timestamp,
				with_grant_option bool
			);`,
		`create table mo_global_variables(
    			gv_variable_name varchar(256),
    			gv_variable_value varchar(1024)
    		);`,
	}

	initMoAccountFormat = `insert into mo_account(
				account_id,
				account_name,
				status,
				created_time,
				comments) values (%d,"%s","%s","%s","%s");`
	initMoRoleFormat = `insert into mo_role(
				role_id,
				role_name,
				creator,
				owner,
				created_time,
				comments
			) values (%d,"%s",%d,%d,"%s","%s");`
	initMoUserFormat = `insert into mo_user(
				user_id,
				user_host,
				user_name,
				authentication_string,
				status,
				created_time,
				expired_time,
				login_type,
				creator,
				owner,
				default_role
    		) values(%d,%s,"%s","%s","%s","%s",%s,"%s",%d,%d,%d);`
	initMoRolePrivFormat = `insert into mo_role_priv(
				role_id,
				role_name,
				obj_type,
				obj_id,
				privilege_id,
				privilege_name,
				privilege_level,
				operation_user_id,
				granted_time,
				with_grant_option
			) values(%d,"%s","%s",%d,%d,"%s","%s",%d,"%s",%v);`
	initMoUserGrantFormat = `insert into mo_user_grant(
            	role_id,
				user_id,
				granted_time,
				with_grant_option
			) values(%d,%d,"%s",%v);`
)

var (
	//privilege verification
	checkTenantFormat = `select account_id,account_name from mo_catalog.mo_account where account_name = "%s";`

	getPasswordOfUserFormat = `select user_id,authentication_string,default_role from mo_catalog.mo_user where user_name = "%s";`

	checkRoleExistsFormat = `select role_id from mo_catalog.mo_role where role_id = %d and role_name = "%s";`

	getRoleOfUserFormat = `select r.role_id from  mo_catalog.mo_role r, mo_catalog.mo_user_grant ug where ug.role_id = r.role_id and ug.user_id = %d and r.role_name = "%s";`
)

func getSqlForCheckTenant(tenant string) string {
	return fmt.Sprintf(checkTenantFormat, tenant)
}

func getSqlForPasswordOfUser(user string) string {
	return fmt.Sprintf(getPasswordOfUserFormat, user)
}

func getSqlForCheckRoleExists(roleID int, roleName string) string {
	return fmt.Sprintf(checkRoleExistsFormat, roleID, roleName)
}

func getSqlForRoleOfUser(userID int, roleName string) string {
	return fmt.Sprintf(getRoleOfUserFormat, userID, roleName)
}

type role struct {
	id   int
	name string
}

type object struct {
	typ string
	id  int
}

type privilege struct {
	id              PrivilegeType
	level           string
	withGrantOption bool
}

var (
	//the content to fill the table mo_role_priv
	sysRoles = []role{
		{moAdminRoleID, moAdminRoleName},
		{publicRoleID, publicRoleName},
	}

	sysObjects = []object{
		{objectTypeDatabase, objectIDAll},
		{objectTypeTable, objectIDAll},
		{objectTypeView, objectIDAll},
		{objectTypeIndex, objectIDAll},
		{objectTypeFunction, objectIDAll},
	}

	sysPrivileges = []privilege{
		{PrivilegeTypeCreateAccount, privilegeLevelStar, false},
		{PrivilegeTypeDropAccount, privilegeLevelStar, false},
		{PrivilegeTypeAlterAccount, privilegeLevelStar, false},
		{PrivilegeTypeCreateUser, privilegeLevelStar, true},
		{PrivilegeTypeDropUser, privilegeLevelStar, true},
		{PrivilegeTypeAlterUser, privilegeLevelStar, true},
		{PrivilegeTypeCreateRole, privilegeLevelStar, true},
		{PrivilegeTypeDropRole, privilegeLevelStar, true},
		{PrivilegeTypeAlterRole, privilegeLevelStar, true},
		{PrivilegeTypeCreateDatabase, privilegeLevelStar, true},
		{PrivilegeTypeDropDatabase, privilegeLevelStar, true},
		{PrivilegeTypeShowDatabases, privilegeLevelStar, true},
		{PrivilegeTypeConnect, privilegeLevelStar, true},
		{PrivilegeTypeManageGrants, privilegeLevelStar, true},
		{PrivilegeTypeAll, privilegeLevelStar, true},
		{PrivilegeTypeAll, privilegeLevelDatabaseStar, true},
		{PrivilegeTypeAll, privilegeLevelTable, true},
		{PrivilegeTypeOwnership, privilegeLevelStar, true},         //multiple
		{PrivilegeTypeOwnership, privilegeLevelStar, true},         //multiple
		{PrivilegeTypeOwnership, privilegeLevelStar, true},         //multiple
		{PrivilegeTypeOwnership, privilegeLevelDatabaseStar, true}, //multiple
		{PrivilegeTypeOwnership, privilegeLevelTable, true},        //multiple
		{PrivilegeTypeShowTables, privilegeLevelDatabaseStar, true},
		{PrivilegeTypeCreateTable, privilegeLevelDatabaseStar, true},
		{PrivilegeTypeDropTable, privilegeLevelDatabaseStar, true},
		{PrivilegeTypeAlterTable, privilegeLevelDatabaseStar, true},
		{PrivilegeTypeSelect, privilegeLevelTable, true},
		{PrivilegeTypeInsert, privilegeLevelTable, true},
		{PrivilegeTypeUpdate, privilegeLevelTable, true},
		{PrivilegeTypeTruncate, privilegeLevelTable, true},
		{PrivilegeTypeDelete, privilegeLevelTable, true},
		{PrivilegeTypeReference, privilegeLevelTable, true},
		{PrivilegeTypeCreateView, privilegeLevelTable, true},
		{PrivilegeTypeDropView, privilegeLevelTable, true},
		{PrivilegeTypeAlterView, privilegeLevelTable, true},
		{PrivilegeTypeIndex, privilegeLevelTable, true},
		{PrivilegeTypeExecute, privilegeLevelTable, true},
	}
)

// checkSysExistsOrNot checks the SYS tenant exists or not.
func checkSysExistsOrNot(ctx context.Context, pu *config.ParameterUnit) (bool, error) {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()

	dbSql := "show databases;"
	err := bh.Exec(dbSql)
	if err != nil {
		return false, err
	}

	rsset := bh.ses.GetAllMysqlResultSet()
	if len(rsset) != 1 {
		panic("it must have result set")
	}

	dbNames := []string{}
	for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
		dbName, err := rsset[0].GetString(i, 0)
		if err != nil {
			return false, err
		}
		dbNames = append(dbNames, dbName)
	}

	for _, name := range dbNames {
		if _, ok := sysWantedDatabases[name]; !ok {
			return false, moerr.NewInternalError(fmt.Sprintf("sys tenant does not have the database %s", name))
		}
	}

	if len(dbNames) != len(sysWantedDatabases) {
		return false, nil
	}

	bh.ses.ClearAllMysqlResultSet()

	sql := "show tables from mo_catalog;"
	err = bh.Exec(sql)
	if err != nil {
		return false, err
	}

	rsset = bh.ses.GetAllMysqlResultSet()
	if len(rsset) != 1 {
		panic("it must have result set")
	}

	tableNames := []string{}
	for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
		tableName, err := rsset[0].GetString(i, 0)
		if err != nil {
			return false, err
		}
		tableNames = append(tableNames, tableName)
	}

	for _, name := range tableNames {
		if _, ok := sysWantedTables[name]; !ok {
			return false, moerr.NewInternalError(fmt.Sprintf("sys tenant does not have the table %s", name))
		}
	}

	if len(tableNames) != len(sysWantedTables) {
		return false, nil
	}

	return true, nil
}

// InitSysTenant initializes the tenant SYS before any tenants and accepting any requests
// during the system is booting.
func InitSysTenant(ctx context.Context, tenant *TenantInfo) error {
	var err error
	var exists bool
	pu := config.GetParameterUnit(ctx)

	ctx = context.WithValue(ctx, moengine.TenantIDKey{}, uint32(sysAccountID))
	ctx = context.WithValue(ctx, moengine.UserIDKey{}, uint32(rootID))
	ctx = context.WithValue(ctx, moengine.RoleIDKey{}, uint32(moAdminRoleID))

	exists, err = checkSysExistsOrNot(ctx, pu)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	err = createTablesInMoCatalog(ctx, tenant, pu)
	if err != nil {
		return err
	}

	err = createTablesInInformationSchema(ctx, tenant, pu)
	if err != nil {
		return err
	}

	return nil
}

// createTablesInMoCatalog creates catalog tables in the database mo_catalog.
func createTablesInMoCatalog(ctx context.Context, tenant *TenantInfo, pu *config.ParameterUnit) error {
	var err error
	var initMoAccount string
	var initDataSqls []string
	isSys := tenant.IsSysTenant()

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//USE the mo_catalog
	addSqlIntoSet("use mo_catalog;")

	//BEGIN the transaction
	addSqlIntoSet("begin;")

	//create tables for the tenant
	for i, sql := range createSqls {
		if !isSys && i == createMoAccountIndex {
			continue
		}
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant
	//step 1: add new tenant entry to the mo_account
	if isSys {
		initMoAccount = fmt.Sprintf(initMoAccountFormat, sysAccountID, sysAccountName, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), sysAccountComments)
	} else {
		//TODO: use auto increment
		initMoAccount = fmt.Sprintf(initMoAccountFormat, rand.Int(), tenant.GetTenant(), sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), "")
	}
	addSqlIntoSet(initMoAccount)

	//step 2:add new role entries to the mo_role
	if isSys {
		initMoRole1 := fmt.Sprintf(initMoRoleFormat, moAdminRoleID, moAdminRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), moAdminRoleComment)
		initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), publicRoleComment)
		addSqlIntoSet(initMoRole1)
		addSqlIntoSet(initMoRole2)
	}

	//step 3:add new user entry to the mo_user
	if isSys {
		initMoUser1 := fmt.Sprintf(initMoUserFormat, rootID, rootHost, rootName, rootPassword, rootStatus, types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType, rootCreatorID, rootOwnerRoleID, rootDefaultRoleID)
		initMoUser2 := fmt.Sprintf(initMoUserFormat, dumpID, dumpHost, dumpName, dumpPassword, dumpStatus, types.CurrentTimestamp().String2(time.UTC, 0), dumpExpiredTime, dumpLoginType, dumpCreatorID, dumpOwnerRoleID, dumpDefaultRoleID)
		addSqlIntoSet(initMoUser1)
		addSqlIntoSet(initMoUser2)
	}

	//step4: add new entries to the mo_role_priv
	if isSys {
		for i := 0; i < len(sysRoles); i++ {
			for j := 0; j < len(sysObjects); j++ {
				for k := 0; k < len(sysPrivileges); k++ {
					r := sysRoles[i]
					o := sysObjects[j]
					p := sysPrivileges[k]
					if r.id == publicRoleID && p.id != PrivilegeTypeConnect {
						continue
					}
					initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
						r.id, r.name,
						o.typ, o.id,
						p.id, p.id.String(), p.level,
						rootID, types.CurrentTimestamp().String2(time.UTC, 0),
						p.withGrantOption)
					addSqlIntoSet(initMoRolePriv)
				}
			}
		}
	}

	//step5: add new entries to the mo_user_grant
	if isSys {
		initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, moAdminRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), false)
		initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), true)
		addSqlIntoSet(initMoUserGrant1)
		addSqlIntoSet(initMoUserGrant2)
	}

	addSqlIntoSet("commit;")

	//fill the mo_account, mo_role, mo_user, mo_role_priv, mo_user_grant
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	for _, sql := range initDataSqls {
		err = bh.Exec(sql)
		if err != nil {
			goto handleFailed
		}
	}

	return nil

handleFailed:
	//ROLLBACK the transaction
	err = bh.Exec("rollback;")
	if err != nil {
		return err
	}
	return nil
}

// createTablesInInformationSchema creates the database information_schema and the views or tables.
func createTablesInInformationSchema(ctx context.Context, tenant *TenantInfo, pu *config.ParameterUnit) error {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	err := bh.Exec("create database information_schema;")
	if err != nil {
		return err
	}
	return nil
}
