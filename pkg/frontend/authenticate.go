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
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/sysview"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/defines"
)

type TenantInfo struct {
	Tenant      string
	User        string
	DefaultRole string

	TenantID      uint32
	UserID        uint32
	DefaultRoleID uint32

	// true: use secondary role all
	// false: use secondary role none
	useAllSecondaryRole bool
}

func (ti *TenantInfo) String() string {
	return fmt.Sprintf("{tenantInfo %s:%s:%s -- %d:%d:%d}", ti.Tenant, ti.User, ti.DefaultRole, ti.TenantID, ti.UserID, ti.DefaultRoleID)
}

func (ti *TenantInfo) GetTenant() string {
	return ti.Tenant
}

func (ti *TenantInfo) GetTenantID() uint32 {
	return ti.TenantID
}

func (ti *TenantInfo) SetTenantID(id uint32) {
	ti.TenantID = id
}

func (ti *TenantInfo) GetUser() string {
	return ti.User
}

func (ti *TenantInfo) GetUserID() uint32 {
	return ti.UserID
}

func (ti *TenantInfo) SetUserID(id uint32) {
	ti.UserID = id
}

func (ti *TenantInfo) GetDefaultRole() string {
	return ti.DefaultRole
}

func (ti *TenantInfo) SetDefaultRole(r string) {
	ti.DefaultRole = r
}

func (ti *TenantInfo) HasDefaultRole() bool {
	return len(ti.GetDefaultRole()) != 0
}

func (ti *TenantInfo) GetDefaultRoleID() uint32 {
	return ti.DefaultRoleID
}

func (ti *TenantInfo) SetDefaultRoleID(id uint32) {
	ti.DefaultRoleID = id
}

func (ti *TenantInfo) IsSysTenant() bool {
	return strings.ToLower(ti.GetTenant()) == GetDefaultTenant()
}

func (ti *TenantInfo) IsDefaultRole() bool {
	return ti.GetDefaultRole() == GetDefaultRole()
}

func (ti *TenantInfo) IsMoAdminRole() bool {
	return ti.IsSysTenant() && strings.ToLower(ti.GetDefaultRole()) == moAdminRoleName
}

func (ti *TenantInfo) IsAccountAdminRole() bool {
	return !ti.IsSysTenant() && strings.ToLower(ti.GetDefaultRole()) == accountAdminRoleName
}

func (ti *TenantInfo) IsAdminRole() bool {
	return ti.IsMoAdminRole() || ti.IsAccountAdminRole()
}

func (ti *TenantInfo) IsNameOfAdminRoles(name string) bool {
	n := strings.ToLower(name)
	if ti.IsSysTenant() {
		return n == moAdminRoleName
	} else {
		return n == accountAdminRoleName
	}
}

func (ti *TenantInfo) SetUseSecondaryRole(v bool) {
	ti.useAllSecondaryRole = v
}

func (ti *TenantInfo) GetUseSecondaryRole() bool {
	return ti.useAllSecondaryRole
}

func GetDefaultTenant() string {
	return sysAccountName
}

func GetDefaultRole() string {
	return moAdminRoleName
}

func isCaseInsensitiveEqual(n string, role string) bool {
	return strings.ToLower(n) == role
}

func isPublicRole(n string) bool {
	return isCaseInsensitiveEqual(n, publicRoleName)
}

func isPredefinedRole(r string) bool {
	n := strings.ToLower(r)
	return n == publicRoleName || n == accountAdminRoleName || n == moAdminRoleName
}

func isSysTenant(n string) bool {
	return isCaseInsensitiveEqual(n, sysAccountName)
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
			Tenant: GetDefaultTenant(),
			User:   userInput,
		}, nil
	} else {
		tenant := userInput[:p]
		tenant = strings.TrimSpace(tenant)
		if len(tenant) == 0 {
			return &TenantInfo{}, moerr.NewInternalError("invalid tenant name '%s'", tenant)
		}
		userRole := userInput[p+1:]
		p2 := strings.IndexByte(userRole, ':')
		if p2 == -1 {
			//tenant:user
			user := userRole
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, moerr.NewInternalError("invalid user name '%s'", user)
			}
			return &TenantInfo{
				Tenant: tenant,
				User:   user,
			}, nil
		} else {
			user := userRole[:p2]
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, moerr.NewInternalError("invalid user name '%s'", user)
			}
			role := userRole[p2+1:]
			role = strings.TrimSpace(role)
			if len(role) == 0 {
				return &TenantInfo{}, moerr.NewInternalError("invalid role name '%s'", role)
			}
			return &TenantInfo{
				Tenant:      tenant,
				User:        user,
				DefaultRole: role,
			}, nil
		}
	}
}

// initUser for initialization or something special
type initUser struct {
	account  *TenantInfo
	password []byte
}

var (
	specialUser atomic.Value
)

// SetSpecialUser saves the user for initialization
// !!!NOTE: userName must not contain Colon ':'
func SetSpecialUser(userName string, password []byte) {
	acc := &TenantInfo{
		Tenant:        sysAccountName,
		User:          userName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        math.MaxUint32,
		DefaultRoleID: moAdminRoleID,
	}

	user := &initUser{
		account:  acc,
		password: password,
	}

	specialUser.Store(user)
}

// isSpecialUser checks the user is the one for initialization
func isSpecialUser(userName string) (bool, []byte, *TenantInfo) {
	user := getSpecialUser()
	if user != nil && user.account.GetUser() == userName {
		return true, user.password, user.account
	}
	return false, nil, nil
}

// getSpecialUser loads the user for initialization
func getSpecialUser() *initUser {
	value := specialUser.Load()
	if value == nil {
		return nil
	}
	return value.(*initUser)
}

const (
// createMoUserIndex      = 0
// createMoAccountIndex = 1
// createMoRoleIndex      = 2
// createMoUserGrantIndex = 3
// createMoRoleGrantIndex = 4
// createMoRolePrivIndex  = 5
)

const (
	//tenant
	sysAccountID       = 0
	sysAccountName     = "sys"
	sysAccountStatus   = "open"
	sysAccountComments = "system account"

	//role
	moAdminRoleID   = 0
	moAdminRoleName = "moadmin"
	//	moAdminRoleComment      = "super admin role"
	publicRoleID   = 1
	publicRoleName = "public"
	//	publicRoleComment       = "public role"
	accountAdminRoleID   = 2
	accountAdminRoleName = "accountadmin"
	//	accountAdminRoleComment = "account admin role"

	//user
	userStatusLock   = "lock"
	userStatusUnlock = "unlock"

	rootID            = 0
	rootHost          = "localhost"
	rootName          = "root"
	rootPassword      = "111"
	rootStatus        = userStatusUnlock
	rootExpiredTime   = "NULL"
	rootLoginType     = "PASSWORD"
	rootCreatorID     = rootID
	rootOwnerRoleID   = moAdminRoleID
	rootDefaultRoleID = moAdminRoleID

	dumpID            = 1
	dumpHost          = "localhost"
	dumpName          = "dump"
	dumpPassword      = "111"
	dumpStatus        = userStatusUnlock
	dumpExpiredTime   = "NULL"
	dumpLoginType     = "PASSWORD"
	dumpCreatorID     = rootID
	dumpOwnerRoleID   = moAdminRoleID
	dumpDefaultRoleID = moAdminRoleID
)

type objectType int

const (
	objectTypeDatabase objectType = iota
	objectTypeTable
	objectTypeFunction
	objectTypeAccount
	objectTypeNone

	objectIDAll = 0 //denotes all objects in the object type
)

func (ot objectType) String() string {
	switch ot {
	case objectTypeDatabase:
		return "database"
	case objectTypeTable:
		return "table"
	case objectTypeFunction:
		return "function"
	case objectTypeAccount:
		return "account"
	case objectTypeNone:
		return "none"
	}
	panic("unsupported object type")
}

type privilegeLevelType int

const (
	//*
	privilegeLevelStar privilegeLevelType = iota
	//*.*
	privilegeLevelStarStar
	//db_name
	privilegeLevelDatabase
	//db_name.*
	privilegeLevelDatabaseStar
	//db_name.tbl_name
	privilegeLevelDatabaseTable
	//tbl_name
	privilegeLevelTable
	//db_name.routine_name
	privilegeLevelRoutine
	//
	privilegeLevelEnd
)

func (plt privilegeLevelType) String() string {
	switch plt {
	case privilegeLevelStar:
		return "*"
	case privilegeLevelStarStar:
		return "*.*"
	case privilegeLevelDatabase:
		return "d"
	case privilegeLevelDatabaseStar:
		return "d.*"
	case privilegeLevelDatabaseTable:
		return "d.t"
	case privilegeLevelTable:
		return "t"
	case privilegeLevelRoutine:
		return "r"
	}
	panic(fmt.Sprintf("no such privilege level type %d", plt))
}

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
	PrivilegeTypeAccountAll
	PrivilegeTypeAccountOwnership
	PrivilegeTypeUserOwnership
	PrivilegeTypeRoleOwnership
	PrivilegeTypeShowTables
	PrivilegeTypeCreateObject //includes: table, view, stream, sequence, function, dblink,etc
	PrivilegeTypeCreateTable
	PrivilegeTypeCreateView
	PrivilegeTypeDropObject
	PrivilegeTypeDropTable
	PrivilegeTypeDropView
	PrivilegeTypeAlterObject
	PrivilegeTypeAlterTable
	PrivilegeTypeAlterView
	PrivilegeTypeDatabaseAll
	PrivilegeTypeDatabaseOwnership
	PrivilegeTypeSelect
	PrivilegeTypeInsert
	PrivilegeTypeReplace
	PrivilegeTypeUpdate
	PrivilegeTypeTruncate
	PrivilegeTypeDelete
	PrivilegeTypeReference
	PrivilegeTypeIndex //include create/alter/drop index
	PrivilegeTypeTableAll
	PrivilegeTypeTableOwnership
	PrivilegeTypeExecute
	PrivilegeTypeCanGrantRoleToOthersInCreateUser // used in checking the privilege of CreateUser with the default role
	PrivilegeTypeValues
)

type PrivilegeScope uint8

const (
	PrivilegeScopeSys      PrivilegeScope = 1
	PrivilegeScopeAccount  PrivilegeScope = 2
	PrivilegeScopeUser     PrivilegeScope = 4
	PrivilegeScopeRole     PrivilegeScope = 8
	PrivilegeScopeDatabase PrivilegeScope = 16
	PrivilegeScopeTable    PrivilegeScope = 32
	PrivilegeScopeRoutine  PrivilegeScope = 64
)

func (ps PrivilegeScope) String() string {
	sb := strings.Builder{}
	first := true
	for i := 0; i < 8; i++ {
		var s string
		switch ps & (1 << i) {
		case PrivilegeScopeSys:
			s = "sys"
		case PrivilegeScopeAccount:
			s = "account"
		case PrivilegeScopeUser:
			s = "user"
		case PrivilegeScopeRole:
			s = "role"
		case PrivilegeScopeDatabase:
			s = "database"
		case PrivilegeScopeTable:
			s = "table"
		case PrivilegeScopeRoutine:
			s = "routine"
		default:
			s = ""
		}
		if len(s) != 0 {
			if !first {
				sb.WriteString(",")
			} else {
				first = false
			}
			sb.WriteString(s)
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
	case PrivilegeTypeAccountAll:
		return "account all"
	case PrivilegeTypeAccountOwnership:
		return "account ownership"
	case PrivilegeTypeUserOwnership:
		return "user ownership"
	case PrivilegeTypeRoleOwnership:
		return "role ownership"
	case PrivilegeTypeShowTables:
		return "show tables"
	case PrivilegeTypeCreateObject:
		return "create object"
	case PrivilegeTypeCreateTable:
		return "create table"
	case PrivilegeTypeCreateView:
		return "create view"
	case PrivilegeTypeDropObject:
		return "drop object"
	case PrivilegeTypeDropTable:
		return "drop table"
	case PrivilegeTypeDropView:
		return "drop view"
	case PrivilegeTypeAlterObject:
		return "alter object"
	case PrivilegeTypeAlterTable:
		return "alter table"
	case PrivilegeTypeAlterView:
		return "alter view"
	case PrivilegeTypeDatabaseAll:
		return "database all"
	case PrivilegeTypeDatabaseOwnership:
		return "database ownership"
	case PrivilegeTypeSelect:
		return "select"
	case PrivilegeTypeInsert:
		return "insert"
	case PrivilegeTypeReplace:
		return "replace"
	case PrivilegeTypeUpdate:
		return "update"
	case PrivilegeTypeTruncate:
		return "truncate"
	case PrivilegeTypeDelete:
		return "delete"
	case PrivilegeTypeReference:
		return "reference"
	case PrivilegeTypeIndex:
		return "index"
	case PrivilegeTypeTableAll:
		return "table all"
	case PrivilegeTypeTableOwnership:
		return "table ownership"
	case PrivilegeTypeExecute:
		return "execute"
	case PrivilegeTypeValues:
		return "values"
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
	case PrivilegeTypeAccountAll:
		return PrivilegeScopeAccount
	case PrivilegeTypeAccountOwnership:
		return PrivilegeScopeAccount
	case PrivilegeTypeUserOwnership:
		return PrivilegeScopeUser
	case PrivilegeTypeRoleOwnership:
		return PrivilegeScopeRole
	case PrivilegeTypeShowTables:
		return PrivilegeScopeDatabase
	case PrivilegeTypeCreateObject, PrivilegeTypeCreateTable, PrivilegeTypeCreateView:
		return PrivilegeScopeDatabase
	case PrivilegeTypeDropObject, PrivilegeTypeDropTable, PrivilegeTypeDropView:
		return PrivilegeScopeDatabase
	case PrivilegeTypeAlterObject, PrivilegeTypeAlterTable, PrivilegeTypeAlterView:
		return PrivilegeScopeDatabase
	case PrivilegeTypeDatabaseAll:
		return PrivilegeScopeDatabase
	case PrivilegeTypeDatabaseOwnership:
		return PrivilegeScopeDatabase
	case PrivilegeTypeSelect:
		return PrivilegeScopeTable
	case PrivilegeTypeInsert:
		return PrivilegeScopeTable
	case PrivilegeTypeReplace:
		return PrivilegeScopeRole
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
	case PrivilegeTypeTableAll:
		return PrivilegeScopeTable
	case PrivilegeTypeTableOwnership:
		return PrivilegeScopeTable
	case PrivilegeTypeExecute:
		return PrivilegeScopeTable
	case PrivilegeTypeValues:
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
		"mo_user":                 0,
		"mo_account":              0,
		"mo_role":                 0,
		"mo_user_grant":           0,
		"mo_role_grant":           0,
		"mo_role_privs":           0,
		`%!%mo_increment_columns`: 0,
	}
	//the sqls creating many tables for the tenant.
	//Wrap them in a transaction
	createSqls = []string{
		"create table `%!%mo_increment_columns`(name varchar(770) primary key, offset bigint unsigned, step bigint unsigned);",
		`create table mo_user(
				user_id int signed auto_increment,
				user_host varchar(100),
				user_name varchar(100),
				authentication_string varchar(100),
				status   varchar(8),
				created_time  timestamp,
				expired_time timestamp,
				login_type  varchar(16),
				creator int signed,
				owner int signed,
				default_role int signed
    		);`,
		`create table mo_account(
				account_id int signed auto_increment,
				account_name varchar(100),
				status varchar(100),
				created_time timestamp,
				comments varchar(256)
			);`,
		`create table mo_role(
				role_id int signed auto_increment,
				role_name varchar(100),
				creator int signed,
				owner int signed,
				created_time timestamp,
				comments text
			);`,
		`create table mo_user_grant(
				role_id int signed,
				user_id int signed,
				granted_time timestamp,
				with_grant_option bool
			);`,
		`create table mo_role_grant(
				granted_id int signed,
				grantee_id int signed,
				operation_role_id int signed,
				operation_user_id int signed,
				granted_time timestamp,
				with_grant_option bool
			);`,
		`create table mo_role_privs(
				role_id int signed,
				role_name  varchar(100),
				obj_type  varchar(16),
				obj_id bigint unsigned,
				privilege_id int,
				privilege_name varchar(100),
				privilege_level varchar(100),
				operation_user_id int unsigned,
				granted_time timestamp,
				with_grant_option bool
			);`,
	}

	//drop tables for the tenant
	dropSqls = []string{
		`drop table if exists mo_catalog.mo_user;`,
		`drop table if exists mo_catalog.mo_role;`,
		`drop table if exists mo_catalog.mo_user_grant;`,
		`drop table if exists mo_catalog.mo_role_grant;`,
		`drop table if exists mo_catalog.mo_role_privs;`,
		//"drop table if exists mo_catalog.`%!%mo_increment_columns`;",
	}

	initMoAccountFormat = `insert into mo_catalog.mo_account(
				account_id,
				account_name,
				status,
				created_time,
				comments) values (%d,"%s","%s","%s","%s");`
	initMoAccountWithoutIDFormat = `insert into mo_catalog.mo_account(
				account_name,
				status,
				created_time,
				comments) values ("%s","%s","%s","%s");`
	initMoRoleFormat = `insert into mo_catalog.mo_role(
				role_id,
				role_name,
				creator,
				owner,
				created_time,
				comments
			) values (%d,"%s",%d,%d,"%s","%s");`
	initMoRoleWithoutIDFormat = `insert into mo_catalog.mo_role(
				role_name,
				creator,
				owner,
				created_time,
				comments
			) values ("%s",%d,%d,"%s","%s");`
	initMoUserFormat = `insert into mo_catalog.mo_user(
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
    		) values(%d,"%s","%s","%s","%s","%s",%s,"%s",%d,%d,%d);`
	initMoUserWithoutIDFormat = `insert into mo_catalog.mo_user(
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
    		) values("%s","%s","%s","%s","%s",%s,"%s",%d,%d,%d);`
	initMoRolePrivFormat = `insert into mo_catalog.mo_role_privs(
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
	initMoUserGrantFormat = `insert into mo_catalog.mo_user_grant(
            	role_id,
				user_id,
				granted_time,
				with_grant_option
			) values(%d,%d,"%s",%v);`
)

const (
	//privilege verification
	checkTenantFormat = `select account_id,account_name from mo_catalog.mo_account where account_name = "%s";`

	deleteAccountFromMoAccountFormat = `delete from mo_catalog.mo_account where account_name = "%s";`

	getPasswordOfUserFormat = `select user_id,authentication_string,default_role from mo_catalog.mo_user where user_name = "%s";`

	checkRoleExistsFormat = `select role_id from mo_catalog.mo_role where role_id = %d and role_name = "%s";`

	roleNameOfRoleIdFormat = `select role_name from mo_catalog.mo_role where role_id = %d;`

	roleIdOfRoleFormat = `select role_id from mo_catalog.mo_role where role_name = "%s";`

	//operations on the mo_user_grant
	getRoleOfUserFormat = `select r.role_id from  mo_catalog.mo_role r, mo_catalog.mo_user_grant ug where ug.role_id = r.role_id and ug.user_id = %d and r.role_name = "%s";`

	getRoleIdOfUserIdFormat = `select role_id,with_grant_option from mo_catalog.mo_user_grant where user_id = %d;`

	checkUserGrantFormat = `select role_id,user_id,with_grant_option from mo_catalog.mo_user_grant where role_id = %d and user_id = %d;`

	checkUserHasRoleFormat = `select u.user_id,ug.role_id from mo_catalog.mo_user u, mo_catalog.mo_user_grant ug where u.user_id = ug.user_id and u.user_name = "%s" and ug.role_id = %d;`

	//with_grant_option = true
	checkUserGrantWGOFormat = `select role_id,user_id from mo_catalog.mo_user_grant where with_grant_option = true and role_id = %d and user_id = %d;`

	updateUserGrantFormat = `update mo_catalog.mo_user_grant set granted_time = "%s", with_grant_option = %v where role_id = %d and user_id = %d;`

	insertUserGrantFormat = `insert into mo_catalog.mo_user_grant(role_id,user_id,granted_time,with_grant_option) values (%d,%d,"%s",%v);`

	deleteUserGrantFormat = `delete from mo_catalog.mo_user_grant where role_id = %d and user_id = %d;`

	//operations on the mo_role_grant
	checkRoleGrantFormat = `select granted_id,grantee_id,with_grant_option from mo_catalog.mo_role_grant where granted_id = %d and grantee_id = %d;`

	//with_grant_option = true
	getRoleGrantWGOFormat = `select grantee_id from mo_catalog.mo_role_grant where with_grant_option = true and granted_id = %d;`

	updateRoleGrantFormat = `update mo_catalog.mo_role_grant set operation_role_id = %d, operation_user_id = %d, granted_time = "%s", with_grant_option = %v where granted_id = %d and grantee_id = %d;`

	insertRoleGrantFormat = `insert mo_catalog.mo_role_grant(granted_id,grantee_id,operation_role_id,operation_user_id,granted_time,with_grant_option) values (%d,%d,%d,%d,"%s",%v);`

	deleteRoleGrantFormat = `delete from mo_catalog.mo_role_grant where granted_id = %d and grantee_id = %d;`

	getAllStuffRoleGrantFormat = `select granted_id,grantee_id,with_grant_option from mo_catalog.mo_role_grant;`

	getInheritedRoleIdOfRoleIdFormat = `select granted_id,with_grant_option from mo_catalog.mo_role_grant where grantee_id = %d;`

	checkRoleHasPrivilegeFormat = `select role_id,with_grant_option from mo_catalog.mo_role_privs where role_id = %d and obj_type = "%s" and obj_id = %d and privilege_id = %d;`

	//with_grant_option = true
	checkRoleHasPrivilegeWGOFormat = `select role_id from mo_catalog.mo_role_privs where with_grant_option = true and privilege_id = %d;`

	updateRolePrivsFormat = `update mo_catalog.mo_role_privs set operation_user_id = %d, granted_time = "%s", with_grant_option = %v where role_id = %d and obj_type = "%s" and obj_id = %d and privilege_id = %d;`

	insertRolePrivsFormat = `insert into mo_catalog.mo_role_privs(role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,granted_time,with_grant_option) 
								values (%d,"%s","%s",%d,%d,"%s","%s",%d,"%s",%v);`

	deleteRolePrivsFormat = `delete from mo_catalog.mo_role_privs 
       									where role_id = %d 
       									    and obj_type = "%s" 
       									    and obj_id = %d 
       									    and privilege_id = %d 
       									    and privilege_level = "%s";`

	checkDatabaseFormat = `select dat_id from mo_catalog.mo_database where datname = "%s";`

	checkDatabaseTableFormat = `select t.rel_id from mo_catalog.mo_database d, mo_catalog.mo_tables t
										where d.dat_id = t.reldatabase_id
											and d.datname = "%s"
											and t.relname = "%s";`

	//TODO:fix privilege_level string and obj_type string
	//For object_type : table, privilege_level : *.*
	checkWithGrantOptionForTableStarStar = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and rp.with_grant_option = true;`

	//For object_type : table, privilege_level : db.*
	checkWithGrantOptionForTableDatabaseStar = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and d.datname = "%s"
					and rp.with_grant_option = true;`

	//For object_type : table, privilege_level : db.table
	checkWithGrantOptionForTableDatabaseTable = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = t.rel_id
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and d.datname = "%s"
					and t.relname = "%s"
					and rp.with_grant_option = true;`

	//For object_type : database, privilege_level : *
	checkWithGrantOptionForDatabaseStar = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and rp.with_grant_option = true;`

	//For object_type : database, privilege_level : *.*
	checkWithGrantOptionForDatabaseStarStar = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and rp.with_grant_option = true;`

	//For object_type : database, privilege_level : db
	checkWithGrantOptionForDatabaseDB = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = d.dat_id
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and  d.datname = "%s"
					and rp.with_grant_option = true;`

	//For object_type : account, privilege_level : *
	checkWithGrantOptionForAccountStar = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and rp.with_grant_option = true;`

	//for database.table or table
	//check the role has the table level privilege for the privilege level (d.t or t)
	checkRoleHasTableLevelPrivilegeFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_tables t, mo_catalog.mo_role_privs rp
				where d.dat_id = t.reldatabase_id
					and rp.obj_id = t.rel_id
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level in ("%s","%s")
					and d.datname = "%s"
					and t.relname = "%s";`

	//for database.* or *
	checkRoleHasTableLevelForDatabaseStarFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_role_privs rp
				where d.dat_id = rp.obj_id
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level in ("%s","%s")
					and d.datname = "%s";`

	//for *.*
	checkRoleHasTableLevelForStarStarFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_role_privs rp
				where rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s";`

	//for * or *.*
	checkRoleHasDatabaseLevelForStarStarFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_role_privs rp
				where rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s";`

	//for database
	checkRoleHasDatabaseLevelForDatabaseFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_database d, mo_catalog.mo_role_privs rp
				where d.dat_id = rp.obj_id
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s"
					and d.datname = "%s";`

	//for *
	checkRoleHasAccountLevelForStarFormat = `select rp.privilege_id,rp.with_grant_option
				from mo_catalog.mo_role_privs rp
				where rp.obj_id = 0
					and rp.obj_type = "%s"
					and rp.role_id = %d
					and rp.privilege_id = %d
					and rp.privilege_level = "%s";`

	//delete role from mo_role,mo_user_grant,mo_role_grant,mo_role_privs
	deleteRoleFromMoRoleFormat = `delete from mo_catalog.mo_role where role_id = %d;`

	deleteRoleFromMoUserGrantFormat = `delete from mo_catalog.mo_user_grant where role_id = %d;`

	deleteRoleFromMoRoleGrantFormat = `delete from mo_catalog.mo_role_grant where granted_id = %d or grantee_id = %d;`

	deleteRoleFromMoRolePrivsFormat = `delete from mo_catalog.mo_role_privs where role_id = %d;`

	//delete user from mo_user,mo_user_grant
	deleteUserFromMoUserFormat = `delete from mo_catalog.mo_user where user_id = %d;`

	deleteUserFromMoUserGrantFormat = `delete from mo_catalog.mo_user_grant where user_id = %d;`
)

var (
	objectType2privilegeLevels = map[objectType][]privilegeLevelType{
		objectTypeAccount: {privilegeLevelStar},
		objectTypeDatabase: {privilegeLevelDatabase,
			privilegeLevelStar, privilegeLevelStarStar},
		objectTypeTable: {privilegeLevelStarStar,
			privilegeLevelDatabaseStar, privilegeLevelStar,
			privilegeLevelDatabaseTable, privilegeLevelTable},
	}

	// the databases that can not operated by the real user
	bannedCatalogDatabases = map[string]int8{
		"mo_catalog":         0,
		"information_schema": 0,
		"system":             0,
		"system_metrics":     0,
		"mysql":              0,
	}

	// the privileges that can not be granted or revoked
	bannedPrivileges = map[PrivilegeType]int8{
		PrivilegeTypeCreateAccount: 0,
		PrivilegeTypeAlterAccount:  0,
		PrivilegeTypeDropAccount:   0,
	}
)

func getSqlForCheckTenant(tenant string) string {
	return fmt.Sprintf(checkTenantFormat, tenant)
}

func getSqlForDeleteAccountFromMoAccount(account string) string {
	return fmt.Sprintf(deleteAccountFromMoAccountFormat, account)
}

func getSqlForPasswordOfUser(user string) string {
	return fmt.Sprintf(getPasswordOfUserFormat, user)
}

func getSqlForCheckRoleExists(roleID int, roleName string) string {
	return fmt.Sprintf(checkRoleExistsFormat, roleID, roleName)
}

func getSqlForRoleNameOfRoleId(roleId int64) string {
	return fmt.Sprintf(roleNameOfRoleIdFormat, roleId)
}

func getSqlForRoleIdOfRole(roleName string) string {
	return fmt.Sprintf(roleIdOfRoleFormat, roleName)
}

func getSqlForRoleOfUser(userID int64, roleName string) string {
	return fmt.Sprintf(getRoleOfUserFormat, userID, roleName)
}

func getSqlForRoleIdOfUserId(userId int) string {
	return fmt.Sprintf(getRoleIdOfUserIdFormat, userId)
}

func getSqlForCheckUserGrant(roleId, userId int64) string {
	return fmt.Sprintf(checkUserGrantFormat, roleId, userId)
}

func getSqlForCheckUserHasRole(userName string, roleId int64) string {
	return fmt.Sprintf(checkUserHasRoleFormat, userName, roleId)
}

func getSqlForCheckUserGrantWGO(roleId, userId int64) string {
	return fmt.Sprintf(checkUserGrantWGOFormat, roleId, userId)
}

func getSqlForUpdateUserGrant(roleId, userId int64, timestamp string, withGrantOption bool) string {
	return fmt.Sprintf(updateUserGrantFormat, timestamp, withGrantOption, roleId, userId)
}

func getSqlForInsertUserGrant(roleId, userId int64, timestamp string, withGrantOption bool) string {
	return fmt.Sprintf(insertUserGrantFormat, roleId, userId, timestamp, withGrantOption)
}

func getSqlForDeleteUserGrant(roleId, userId int64) string {
	return fmt.Sprintf(deleteUserGrantFormat, roleId, userId)
}

func getSqlForCheckRoleGrant(grantedId, granteeId int64) string {
	return fmt.Sprintf(checkRoleGrantFormat, grantedId, granteeId)
}

func getSqlForCheckRoleGrantWGO(grantedId int64) string {
	return fmt.Sprintf(getRoleGrantWGOFormat, grantedId)
}

func getSqlForUpdateRoleGrant(grantedId, granteeId, operationRoleId, operationUserId int64, timestamp string, withGrantOption bool) string {
	return fmt.Sprintf(updateRoleGrantFormat, operationRoleId, operationUserId, timestamp, withGrantOption, grantedId, granteeId)
}

func getSqlForInsertRoleGrant(grantedId, granteeId, operationRoleId, operationUserId int64, timestamp string, withGrantOption bool) string {
	return fmt.Sprintf(insertRoleGrantFormat, grantedId, granteeId, operationRoleId, operationUserId, timestamp, withGrantOption)
}

func getSqlForDeleteRoleGrant(grantedId, granteeId int64) string {
	return fmt.Sprintf(deleteRoleGrantFormat, grantedId, granteeId)
}

func getSqlForGetAllStuffRoleGrantFormat() string {
	return getAllStuffRoleGrantFormat
}

func getSqlForInheritedRoleIdOfRoleId(roleId int64) string {
	return fmt.Sprintf(getInheritedRoleIdOfRoleIdFormat, roleId)
}

func getSqlForCheckRoleHasPrivilege(roleId int64, objType objectType, objId, privilegeId int64) string {
	return fmt.Sprintf(checkRoleHasPrivilegeFormat, roleId, objType, objId, privilegeId)
}

func getSqlForCheckRoleHasPrivilegeWGO(privilegeId int64) string {
	return fmt.Sprintf(checkRoleHasPrivilegeWGOFormat, privilegeId)
}

func getSqlForUpdateRolePrivs(userId int64, timestamp string, withGrantOption bool, roleId int64, objType objectType, objId, privilegeId int64) string {
	return fmt.Sprintf(updateRolePrivsFormat, userId, timestamp, withGrantOption, roleId, objType, objId, privilegeId)
}

func getSqlForInsertRolePrivs(roleId int64, roleName, objType string, objId, privilegeId int64, privilegeName, privilegeLevel string, operationUserId int64, grantedTime string, withGrantOption bool) string {
	return fmt.Sprintf(insertRolePrivsFormat, roleId, roleName, objType, objId, privilegeId, privilegeName, privilegeLevel, operationUserId, grantedTime, withGrantOption)
}

func getSqlForDeleteRolePrivs(roleId int64, objType string, objId, privilegeId int64, privilegeLevel string) string {
	return fmt.Sprintf(deleteRolePrivsFormat, roleId, objType, objId, privilegeId, privilegeLevel)
}

func getSqlForCheckWithGrantOptionForTableStarStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkWithGrantOptionForTableStarStar, objectTypeTable, roleId, privId, privilegeLevelStarStar)
}

func getSqlForCheckWithGrantOptionForTableDatabaseStar(roleId int64, privId PrivilegeType, dbName string) string {
	return fmt.Sprintf(checkWithGrantOptionForTableDatabaseStar, objectTypeTable, roleId, privId, privilegeLevelDatabaseStar, dbName)
}

func getSqlForCheckWithGrantOptionForTableDatabaseTable(roleId int64, privId PrivilegeType, dbName string, tableName string) string {
	return fmt.Sprintf(checkWithGrantOptionForTableDatabaseTable, objectTypeTable, roleId, privId, privilegeLevelDatabaseTable, dbName, tableName)
}

func getSqlForCheckWithGrantOptionForDatabaseStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkWithGrantOptionForDatabaseStar, objectTypeDatabase, roleId, privId, privilegeLevelStar)
}

func getSqlForCheckWithGrantOptionForDatabaseStarStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkWithGrantOptionForDatabaseStarStar, objectTypeDatabase, roleId, privId, privilegeLevelStarStar)
}

func getSqlForCheckWithGrantOptionForDatabaseDB(roleId int64, privId PrivilegeType, dbName string) string {
	return fmt.Sprintf(checkWithGrantOptionForDatabaseDB, objectTypeDatabase, roleId, privId, privilegeLevelDatabase, dbName)
}

func getSqlForCheckWithGrantOptionForAccountStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkWithGrantOptionForAccountStar, objectTypeAccount, roleId, privId, privilegeLevelStarStar)
}

func getSqlForCheckRoleHasTableLevelPrivilege(roleId int64, privId PrivilegeType, dbName string, tableName string) string {
	return fmt.Sprintf(checkRoleHasTableLevelPrivilegeFormat, objectTypeTable, roleId, privId, privilegeLevelDatabaseTable, privilegeLevelTable, dbName, tableName)
}

func getSqlForCheckRoleHasTableLevelForDatabaseStar(roleId int64, privId PrivilegeType, dbName string) string {
	return fmt.Sprintf(checkRoleHasTableLevelForDatabaseStarFormat, objectTypeTable, roleId, privId, privilegeLevelDatabaseStar, privilegeLevelStar, dbName)
}

func getSqlForCheckRoleHasTableLevelForStarStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkRoleHasTableLevelForStarStarFormat, objectTypeTable, roleId, privId, privilegeLevelStarStar)
}

func getSqlForCheckRoleHasDatabaseLevelForStarStar(roleId int64, privId PrivilegeType, level privilegeLevelType) string {
	return fmt.Sprintf(checkRoleHasDatabaseLevelForStarStarFormat, objectTypeDatabase, roleId, privId, level)
}

func getSqlForCheckRoleHasDatabaseLevelForDatabase(roleId int64, privId PrivilegeType, dbName string) string {
	return fmt.Sprintf(checkRoleHasDatabaseLevelForDatabaseFormat, objectTypeDatabase, roleId, privId, privilegeLevelDatabase, dbName)
}

func getSqlForCheckRoleHasAccountLevelForStar(roleId int64, privId PrivilegeType) string {
	return fmt.Sprintf(checkRoleHasAccountLevelForStarFormat, objectTypeAccount, roleId, privId, privilegeLevelStar)
}

func getSqlForCheckDatabase(dbName string) string {
	return fmt.Sprintf(checkDatabaseFormat, dbName)
}

func getSqlForCheckDatabaseTable(dbName, tableName string) string {
	return fmt.Sprintf(checkDatabaseTableFormat, dbName, tableName)
}

func getSqlForDeleteRole(roleId int64) []string {
	return []string{
		fmt.Sprintf(deleteRoleFromMoRoleFormat, roleId),
		fmt.Sprintf(deleteRoleFromMoUserGrantFormat, roleId),
		fmt.Sprintf(deleteRoleFromMoRoleGrantFormat, roleId, roleId),
		fmt.Sprintf(deleteRoleFromMoRolePrivsFormat, roleId),
	}
}

func getSqlForDropTablesOfAccount() []string {
	return dropSqls
}

func getSqlForDeleteUser(userId int64) []string {
	return []string{
		fmt.Sprintf(deleteUserFromMoUserFormat, userId),
		fmt.Sprintf(deleteUserFromMoUserGrantFormat, userId),
	}
}

func isBannedDatabase(dbName string) bool {
	_, ok := bannedCatalogDatabases[dbName]
	return ok
}

func isBannedPrivilege(priv PrivilegeType) bool {
	_, ok := bannedPrivileges[priv]
	return ok
}

type specialTag int

const (
	specialTagNone            specialTag = 0
	specialTagAdmin           specialTag = 1
	specialTagWithGrantOption specialTag = 2
	specialTagOwnerOfObject   specialTag = 4
)

type privilegeKind int

const (
	privilegeKindGeneral privilegeKind = iota //as same as definition in the privilegeEntriesMap
	privilegeKindInherit                      //General + with_grant_option
	privilegeKindSpecial                      //no obj_type,obj_id,privilege_level. only needs (MOADMIN / ACCOUNTADMIN, with_grant_option, owner of object)
	privilegeKindNone                         //does not need any privilege
)

type privilege struct {
	kind privilegeKind
	//account: the privilege can be defined before constructing the plan.
	//database: (do not need the database_id) the privilege can be defined before constructing the plan.
	//table: need table id. the privilege can be defined after constructing the plan.
	//function: need function id ?
	objType objectType
	entries []privilegeEntry
	special specialTag
	//the statement writes the database or table directly like drop database and table
	writeDBTableDirect bool
}

func (p *privilege) objectType() objectType {
	return p.objType
}

func (p *privilege) privilegeKind() privilegeKind {
	return p.kind
}

type privilegeEntryType int

const (
	privilegeEntryTypeGeneral  privilegeEntryType = iota
	privilegeEntryTypeCompound                    //multi privileges take effect together
)

// privilegeItem is the item for in the compound entry
type privilegeItem struct {
	privilegeTyp PrivilegeType
	role         *tree.Role
	users        []*tree.User
	dbName       string
	tableName    string
}

// compoundEntry is the entry has multi privilege items
type compoundEntry struct {
	items []privilegeItem
}

// privilegeEntry denotes the entry of the privilege that appears in the table mo_role_privs
type privilegeEntry struct {
	privilegeId PrivilegeType
	//the predefined privilege level for the privilege.
	//it is not always the same as the one in the runtime.
	privilegeLevel  privilegeLevelType
	objType         objectType
	objId           int
	withGrantOption bool
	//for object type table
	databaseName      string
	tableName         string
	privilegeEntryTyp privilegeEntryType
	compound          *compoundEntry
}

var (
	//initial privilege entries
	privilegeEntriesMap = map[PrivilegeType]privilegeEntry{
		PrivilegeTypeCreateAccount:     {PrivilegeTypeCreateAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropAccount:       {PrivilegeTypeDropAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterAccount:      {PrivilegeTypeAlterAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateUser:        {PrivilegeTypeCreateUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropUser:          {PrivilegeTypeDropUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterUser:         {PrivilegeTypeAlterUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateRole:        {PrivilegeTypeCreateRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropRole:          {PrivilegeTypeDropRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterRole:         {PrivilegeTypeAlterRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateDatabase:    {PrivilegeTypeCreateDatabase, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropDatabase:      {PrivilegeTypeDropDatabase, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeShowDatabases:     {PrivilegeTypeShowDatabases, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeConnect:           {PrivilegeTypeConnect, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeManageGrants:      {PrivilegeTypeManageGrants, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAccountAll:        {PrivilegeTypeAccountAll, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAccountOwnership:  {PrivilegeTypeAccountOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeUserOwnership:     {PrivilegeTypeUserOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeRoleOwnership:     {PrivilegeTypeRoleOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeShowTables:        {PrivilegeTypeShowTables, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateObject:      {PrivilegeTypeCreateObject, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateTable:       {PrivilegeTypeCreateTable, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeCreateView:        {PrivilegeTypeCreateView, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropObject:        {PrivilegeTypeDropObject, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropTable:         {PrivilegeTypeDropTable, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDropView:          {PrivilegeTypeDropView, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterObject:       {PrivilegeTypeAlterObject, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterTable:        {PrivilegeTypeAlterTable, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeAlterView:         {PrivilegeTypeAlterView, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDatabaseAll:       {PrivilegeTypeDatabaseAll, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDatabaseOwnership: {PrivilegeTypeDatabaseOwnership, privilegeLevelStar, objectTypeDatabase, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeSelect:            {PrivilegeTypeSelect, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeInsert:            {PrivilegeTypeInsert, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeReplace:           {PrivilegeTypeReplace, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeUpdate:            {PrivilegeTypeUpdate, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeTruncate:          {PrivilegeTypeTruncate, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeDelete:            {PrivilegeTypeDelete, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeReference:         {PrivilegeTypeReference, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeIndex:             {PrivilegeTypeIndex, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeTableAll:          {PrivilegeTypeTableAll, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeTableOwnership:    {PrivilegeTypeTableOwnership, privilegeLevelStarStar, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeExecute:           {PrivilegeTypeExecute, privilegeLevelRoutine, objectTypeFunction, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
		PrivilegeTypeValues:            {PrivilegeTypeValues, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", "", privilegeEntryTypeGeneral, nil},
	}

	//the initial entries of mo_role_privs for the role 'moadmin'
	entriesOfMoAdminForMoRolePrivsFor = []PrivilegeType{
		PrivilegeTypeCreateAccount,
		PrivilegeTypeDropAccount,
		PrivilegeTypeAlterAccount,
		PrivilegeTypeCreateUser,
		PrivilegeTypeDropUser,
		PrivilegeTypeAlterUser,
		PrivilegeTypeCreateRole,
		PrivilegeTypeDropRole,
		PrivilegeTypeCreateDatabase,
		PrivilegeTypeDropDatabase,
		PrivilegeTypeShowDatabases,
		PrivilegeTypeConnect,
		PrivilegeTypeManageGrants,
		PrivilegeTypeAccountAll,
		PrivilegeTypeShowTables,
		PrivilegeTypeCreateTable,
		PrivilegeTypeDropTable,
		PrivilegeTypeAlterTable,
		PrivilegeTypeCreateView,
		PrivilegeTypeDropView,
		PrivilegeTypeAlterView,
		PrivilegeTypeDatabaseAll,
		PrivilegeTypeDatabaseOwnership,
		PrivilegeTypeSelect,
		PrivilegeTypeInsert,
		PrivilegeTypeReplace,
		PrivilegeTypeUpdate,
		PrivilegeTypeTruncate,
		PrivilegeTypeDelete,
		PrivilegeTypeReference,
		PrivilegeTypeIndex,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
		PrivilegeTypeValues,
	}

	//the initial entries of mo_role_privs for the role 'accountadmin'
	entriesOfAccountAdminForMoRolePrivsFor = []PrivilegeType{
		PrivilegeTypeCreateUser,
		PrivilegeTypeDropUser,
		PrivilegeTypeAlterUser,
		PrivilegeTypeCreateRole,
		PrivilegeTypeDropRole,
		PrivilegeTypeCreateDatabase,
		PrivilegeTypeDropDatabase,
		PrivilegeTypeShowDatabases,
		PrivilegeTypeConnect,
		PrivilegeTypeManageGrants,
		PrivilegeTypeAccountAll,
		PrivilegeTypeShowTables,
		PrivilegeTypeCreateTable,
		PrivilegeTypeDropTable,
		PrivilegeTypeAlterTable,
		PrivilegeTypeCreateView,
		PrivilegeTypeDropView,
		PrivilegeTypeAlterView,
		PrivilegeTypeDatabaseAll,
		PrivilegeTypeDatabaseOwnership,
		PrivilegeTypeSelect,
		PrivilegeTypeInsert,
		PrivilegeTypeReplace,
		PrivilegeTypeUpdate,
		PrivilegeTypeTruncate,
		PrivilegeTypeDelete,
		PrivilegeTypeReference,
		PrivilegeTypeIndex,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
		PrivilegeTypeValues,
	}

	//the initial entries of mo_role_privs for the role 'public'
	entriesOfPublicForMoRolePrivsFor = []PrivilegeType{
		PrivilegeTypeConnect,
	}
)

type verifiedRoleType int

const (
	roleType verifiedRoleType = iota
	userType
)

// privilegeCache cache privileges on table
type privilegeCache struct {
	//For objectType table
	//For objectType table *, *.*
	storeForTable [int(privilegeLevelEnd)]btree.Set[PrivilegeType]
	//For objectType table database.*
	storeForTable2 btree.Map[string, *btree.Set[PrivilegeType]]
	//For objectType table database.table , table
	storeForTable3 btree.Map[string, *btree.Map[string, *btree.Set[PrivilegeType]]]

	//For objectType database *, *.*
	storeForDatabase [int(privilegeLevelEnd)]btree.Set[PrivilegeType]
	//For objectType database
	storeForDatabase2 btree.Map[string, *btree.Set[PrivilegeType]]
	//For objectType account *
	storeForAccount [int(privilegeLevelEnd)]btree.Set[PrivilegeType]
	total           atomic.Uint64
	hit             atomic.Uint64
}

// has checks the cache has privilege on a table
func (pc *privilegeCache) has(objTyp objectType, plt privilegeLevelType, dbName, tableName string, priv PrivilegeType) bool {
	pc.total.Add(1)
	privSet := pc.getPrivilegeSet(objTyp, plt, dbName, tableName)
	if privSet != nil && privSet.Contains(priv) {
		pc.hit.Add(1)
		return true
	}
	return false
}

func (pc *privilegeCache) getPrivilegeSet(objTyp objectType, plt privilegeLevelType, dbName, tableName string) *btree.Set[PrivilegeType] {
	switch objTyp {
	case objectTypeTable:
		switch plt {
		case privilegeLevelStarStar, privilegeLevelStar:
			return &pc.storeForTable[plt]
		case privilegeLevelDatabaseStar:
			dbStore, ok1 := pc.storeForTable2.Get(dbName)
			if !ok1 {
				dbStore = &btree.Set[PrivilegeType]{}
				pc.storeForTable2.Set(dbName, dbStore)
			}
			return dbStore
		case privilegeLevelDatabaseTable, privilegeLevelTable:
			tableStore, ok1 := pc.storeForTable3.Get(dbName)
			if !ok1 {
				tableStore = &btree.Map[string, *btree.Set[PrivilegeType]]{}
				pc.storeForTable3.Set(dbName, tableStore)
			}
			privSet, ok2 := tableStore.Get(tableName)
			if !ok2 {
				privSet = &btree.Set[PrivilegeType]{}
				tableStore.Set(tableName, privSet)
			}
			return privSet
		default:
			return nil
		}
	case objectTypeDatabase:
		switch plt {
		case privilegeLevelStar, privilegeLevelStarStar:
			return &pc.storeForDatabase[plt]
		case privilegeLevelDatabase:
			dbStore, ok1 := pc.storeForDatabase2.Get(dbName)
			if !ok1 {
				dbStore = &btree.Set[PrivilegeType]{}
				pc.storeForDatabase2.Set(dbName, dbStore)
			}
			return dbStore
		default:
			return nil
		}
	case objectTypeAccount:
		return &pc.storeForAccount[plt]
	default:
		return nil
	}

}

// set replaces the privileges by new ones
func (pc *privilegeCache) set(objTyp objectType, plt privilegeLevelType, dbName, tableName string, priv ...PrivilegeType) {
	privSet := pc.getPrivilegeSet(objTyp, plt, dbName, tableName)
	if privSet != nil {
		privSet.Clear()
		for _, p := range priv {
			privSet.Insert(p)
		}
	}
}

// add puts the privileges without replacing existed ones
func (pc *privilegeCache) add(objTyp objectType, plt privilegeLevelType, dbName, tableName string, priv ...PrivilegeType) {
	privSet := pc.getPrivilegeSet(objTyp, plt, dbName, tableName)
	if privSet != nil {
		for _, p := range priv {
			privSet.Insert(p)
		}
	}
}

// invalidate makes the cache empty
func (pc *privilegeCache) invalidate() {
	//total := pc.total.Swap(0)
	//hit := pc.hit.Swap(0)
	for i := privilegeLevelStar; i < privilegeLevelEnd; i++ {
		pc.storeForTable[i].Clear()
		pc.storeForDatabase[i].Clear()
		pc.storeForAccount[i].Clear()
	}
	pc.storeForTable2.Clear()
	pc.storeForTable3.Clear()
	pc.storeForDatabase2.Clear()
	//ratio := float64(0)
	//if total == 0 {
	//	ratio = 0
	//} else {
	//	ratio = float64(hit) / float64(total)
	//}
	//logutil.Debugf("-->hit %d total %d ratio %f", hit, total, ratio)
}

// verifiedRole holds the role info that has been checked
type verifiedRole struct {
	typ         verifiedRoleType
	name        string
	id          int64
	userIsAdmin bool
}

// verifyRoleFunc gets result set from mo_role_grant or mo_user_grant
func verifyRoleFunc(ctx context.Context, bh BackgroundExec, sql, name string, typ verifiedRoleType) (*verifiedRole, error) {
	var err error
	var erArray []ExecResult
	var roleId int64
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		roleId, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			return nil, err
		}
		return &verifiedRole{typ, name, roleId, false}, nil
	}
	return nil, nil
}

// userIsAdministrator checks the user is the administrator
func userIsAdministrator(ctx context.Context, bh BackgroundExec, userId int64, account *TenantInfo) (bool, error) {
	var err error
	var erArray []ExecResult
	var sql string
	if account.IsSysTenant() {
		sql = getSqlForRoleOfUser(userId, moAdminRoleName)
	} else {
		sql = getSqlForRoleOfUser(userId, accountAdminRoleName)
	}

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

type visitTag int

const (
	vtUnVisited visitTag = 0
	vtVisited   visitTag = 1
	vtVisiting  visitTag = -1
)

// edge <from,to> in the graph
type edge struct {
	from    int64
	to      int64
	invalid bool
}

func (e *edge) isInvalid() bool {
	return e.invalid
}

func (e *edge) setInvalid() {
	e.invalid = true
}

// graph the acyclic graph
type graph struct {
	edges    []*edge
	vertexes map[int64]int
	adjacent map[int64][]int
}

func NewGraph() *graph {
	return &graph{
		vertexes: make(map[int64]int),
		adjacent: make(map[int64][]int),
	}
}

// addEdge adds the directed edge <from,to> into the graph
func (g *graph) addEdge(from, to int64) int {
	edgeId := len(g.edges)
	g.edges = append(g.edges, &edge{from, to, false})
	g.adjacent[from] = append(g.adjacent[from], edgeId)
	g.vertexes[from] = 0
	g.vertexes[to] = 0
	return edgeId
}

// removeEdge removes the directed edge (edgeId) from the graph
func (g *graph) removeEdge(edgeId int) {
	e := g.getEdge(edgeId)
	e.setInvalid()
}

func (g *graph) getEdge(eid int) *edge {
	return g.edges[eid]
}

// dfs use the toposort to check the loop
func (g *graph) toposort(u int64, visited map[int64]visitTag) bool {
	visited[u] = vtVisiting
	//loop on adjacent vertex
	for _, eid := range g.adjacent[u] {
		e := g.getEdge(eid)
		if e.isInvalid() {
			continue
		}
		if visited[e.to] == vtVisiting { //find the loop in the vertex
			return false
		} else if visited[e.to] == vtUnVisited && !g.toposort(e.to, visited) { //find the loop in the adjacent vertexes
			return false
		}
	}
	visited[u] = vtVisited
	return true
}

// hasLoop checks the loop
func (g *graph) hasLoop(start int64) bool {
	visited := make(map[int64]visitTag)
	for v := range g.vertexes {
		visited[v] = vtUnVisited
	}

	return !g.toposort(start, visited)
}

// nameIsInvalid checks the name of account/user/role is valid or not
func nameIsInvalid(name string) bool {
	s := strings.TrimSpace(name)
	if len(s) == 0 {
		return true
	}
	return strings.Contains(s, ":")
}

// normalizeName normalizes and checks the name
func normalizeName(name string) (string, error) {
	s := strings.TrimSpace(name)
	if nameIsInvalid(s) {
		return "", moerr.NewInternalError(`the name "%s" is invalid`, name)
	}
	return s, nil
}

// normalizeNameOfRole normalizes the name
func normalizeNameOfRole(role *tree.Role) error {
	var err error
	role.UserName, err = normalizeName(role.UserName)
	return err
}

// normalizeNamesOfRoles normalizes the names and checks them
func normalizeNamesOfRoles(roles []*tree.Role) error {
	var err error
	for i := 0; i < len(roles); i++ {
		err = normalizeNameOfRole(roles[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// normalizeNameOfUser normalizes the name
func normalizeNameOfUser(user *tree.User) error {
	var err error
	user.Username, err = normalizeName(user.Username)
	return err
}

// normalizeNamesOfUsers normalizes the names and checks them
func normalizeNamesOfUsers(users []*tree.User) error {
	var err error
	for i := 0; i < len(users); i++ {
		err = normalizeNameOfUser(users[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// doSwitchRole accomplishes the Use Role and Use Secondary Role statement
func doSwitchRole(ctx context.Context, ses *Session, sr *tree.SetRole) error {
	var err error
	var sql string
	var erArray []ExecResult
	var roleId int64

	account := ses.GetTenantInfo()

	if sr.SecondaryRole {
		//use secondary role all or none
		switch sr.SecondaryRoleType {
		case tree.SecondaryRoleTypeAll:
			account.SetUseSecondaryRole(true)
		case tree.SecondaryRoleTypeNone:
			account.SetUseSecondaryRole(false)
		}
	} else if sr.Role != nil {
		err = normalizeNameOfRole(sr.Role)
		if err != nil {
			return err
		}

		//step1 : check the role exists or not;
		bh := ses.GetBackgroundExec(ctx)
		defer bh.Close()

		err = bh.Exec(ctx, "begin;")
		if err != nil {
			goto handleFailed
		}

		sql = getSqlForRoleIdOfRole(sr.Role.UserName)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}
		if execResultArrayHasData(erArray) {
			roleId, err = erArray[0].GetInt64(0, 0)
			if err != nil {
				goto handleFailed
			}
		} else {
			err = moerr.NewInternalError("there is no role %s", sr.Role.UserName)
			goto handleFailed
		}

		//step2 : check the role has been granted to the user or not
		sql = getSqlForCheckUserGrant(roleId, int64(account.GetUserID()))
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}

		if !execResultArrayHasData(erArray) {
			err = moerr.NewInternalError("the role %s has not be granted to the user %s", sr.Role.UserName, account.GetUser())
			goto handleFailed
		}

		err = bh.Exec(ctx, "commit;")
		if err != nil {
			goto handleFailed
		}

		//step3 : switch the default role and role id;
		account.SetDefaultRoleID(uint32(roleId))
		account.SetDefaultRole(sr.Role.UserName)
		//then, reset secondary role to none
		account.SetUseSecondaryRole(false)

		return err

	handleFailed:
		//ROLLBACK the transaction
		rbErr := bh.Exec(ctx, "rollback;")
		if rbErr != nil {
			return rbErr
		}
		return err
	}

	return err
}

// doDropAccount accomplishes the DropAccount statement
func doDropAccount(ctx context.Context, ses *Session, da *tree.DropAccount) error {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	var err error
	var sql string
	var erArray []ExecResult

	var deleteCtx context.Context
	var accountId int64
	var hasAccount = true

	da.Name, err = normalizeName(da.Name)
	if err != nil {
		return err
	}

	if isSysTenant(da.Name) {
		return moerr.NewInternalError("can not delete the account %s", da.Name)
	}

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//check the account exists or not
	sql = getSqlForCheckTenant(da.Name)
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		goto handleFailed
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		goto handleFailed
	}

	if execResultArrayHasData(erArray) {
		accountId, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			goto handleFailed
		}
	} else {
		//no such account
		if !da.IfExists { //when the "IF EXISTS" is set, just skip it.
			err = moerr.NewInternalError("there is no account %s", da.Name)
			goto handleFailed
		}
		hasAccount = false
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	//drop tables of the tenant
	if hasAccount {
		//NOTE!!!: single DDL drop statement per single transaction
		//SWITCH TO THE CONTEXT of the deleted context
		deleteCtx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(accountId))

		//step 2 : drop table mo_user
		//step 3 : drop table mo_role
		//step 4 : drop table mo_user_grant
		//step 5 : drop table mo_role_grant
		//step 6 : drop table mo_role_privs
		for _, sql = range getSqlForDropTablesOfAccount() {
			err = bh.Exec(deleteCtx, sql)
			if err != nil {
				return err
			}
		}
	}

	//step 1 : delete the account in the mo_account of the sys account
	sql = getSqlForDeleteAccountFromMoAccount(da.Name)
	err = bh.Exec(ctx, sql)
	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// doDropUser accomplishes the DropUser statement
func doDropUser(ctx context.Context, ses *Session, du *tree.DropUser) error {
	var err error
	var vr *verifiedRole
	var sql string
	var sqls []string
	var erArray []ExecResult
	account := ses.GetTenantInfo()
	err = normalizeNamesOfUsers(du.Users)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step1: check users exists or not.
	//handle "IF EXISTS"
	for _, user := range du.Users {
		sql = getSqlForPasswordOfUser(user.Username)
		vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, roleType)
		if err != nil {
			goto handleFailed
		}

		if vr == nil {
			if !du.IfExists { //when the "IF EXISTS" is set, just skip it.
				err = moerr.NewInternalError("there is no user %s", user.Username)
				goto handleFailed
			}
		}

		if vr == nil {
			continue
		}

		//if the user is admin user with the role moadmin or accountadmin,
		//the user can not be deleted.
		if account.IsSysTenant() {
			sql = getSqlForCheckUserHasRole(user.Username, moAdminRoleID)
		} else {
			sql = getSqlForCheckUserHasRole(user.Username, accountAdminRoleID)
		}

		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}

		if execResultArrayHasData(erArray) {
			err = moerr.NewInternalError("can not delete the user %s", user.Username)
			goto handleFailed
		}

		//step2 : delete mo_user
		//step3 : delete mo_user_grant
		sqls = getSqlForDeleteUser(vr.id)
		for _, sqlx := range sqls {
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlx)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// doDropRole accomplishes the DropRole statement
func doDropRole(ctx context.Context, ses *Session, dr *tree.DropRole) error {
	var err error
	var vr *verifiedRole
	account := ses.GetTenantInfo()
	err = normalizeNamesOfRoles(dr.Roles)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step1: check roles exists or not.
	//handle "IF EXISTS"
	for _, role := range dr.Roles {
		sql := getSqlForRoleIdOfRole(role.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			goto handleFailed
		}

		if vr == nil {
			if !dr.IfExists { //when the "IF EXISTS" is set, just skip it.
				err = moerr.NewInternalError("there is no role %s", role.UserName)
				goto handleFailed
			}
		}

		//step2 : delete mo_role
		//step3 : delete mo_user_grant
		//step4 : delete mo_role_grant
		//step5 : delete mo_role_privs
		if vr == nil {
			continue
		}

		//NOTE: if the role is the admin role (moadmin,accountadmin) or public,
		//the role can not be deleted.
		if account.IsNameOfAdminRoles(vr.name) || isPublicRole(vr.name) {
			err = moerr.NewInternalError("can not delete the role %s", vr.name)
			goto handleFailed
		}

		sqls := getSqlForDeleteRole(vr.id)
		for _, sqlx := range sqls {
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlx)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// doRevokePrivilege accomplishes the RevokePrivilege statement
func doRevokePrivilege(ctx context.Context, ses *Session, rp *tree.RevokePrivilege) error {
	var err error
	var vr *verifiedRole
	var objType objectType
	var privLevel privilegeLevelType
	var objId int64
	var privType PrivilegeType
	err = normalizeNamesOfRoles(rp.Roles)
	if err != nil {
		return err
	}

	account := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	verifiedRoles := make([]*verifiedRole, len(rp.Roles))
	checkedPrivilegeTypes := make([]PrivilegeType, len(rp.Privileges))

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//handle "IF EXISTS"
	//step 1: check roles. exists or not.
	for i, user := range rp.Roles {
		//check Revoke privilege on xxx yyy from moadmin(accountadmin)
		if account.IsNameOfAdminRoles(user.UserName) {
			err = moerr.NewInternalError("the privilege can not be revoked from the role %s", user.UserName)
			goto handleFailed
		}
		sql := getSqlForRoleIdOfRole(user.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, user.UserName, roleType)
		if err != nil {
			goto handleFailed
		}
		verifiedRoles[i] = vr
		if vr == nil {
			if !rp.IfExists { //when the "IF EXISTS" is set, just skip it.
				err = moerr.NewInternalError("there is no role %s", user.UserName)
				goto handleFailed
			}
		}
	}

	//get the object type
	objType, err = convertAstObjectTypeToObjectType(rp.ObjType)
	if err != nil {
		goto handleFailed
	}

	//check the privilege and the object type
	for i, priv := range rp.Privileges {
		privType, err = convertAstPrivilegeTypeToPrivilegeType(priv.Type, rp.ObjType)
		if err != nil {
			goto handleFailed
		}
		//check the match between the privilegeScope and the objectType
		err = matchPrivilegeTypeWithObjectType(privType, objType)
		if err != nil {
			goto handleFailed
		}
		checkedPrivilegeTypes[i] = privType
	}

	//step 2: decide the object type , the object id and the privilege_level
	privLevel, objId, err = checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, rp.ObjType, *rp.Level)
	if err != nil {
		goto handleFailed
	}

	//step 3: delete the granted privilege
	for _, privType = range checkedPrivilegeTypes {
		for _, role := range verifiedRoles {
			if role == nil {
				continue
			}
			if privType == PrivilegeTypeConnect && isPublicRole(role.name) {
				err = moerr.NewInternalError("the privilege %s can not be revoked from the role %s", privType, role.name)
				goto handleFailed
			}
			sql := getSqlForDeleteRolePrivs(role.id, objType.String(), objId, int64(privType), privLevel.String())
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// getDatabaseOrTableId gets the id of the database or the table
func getDatabaseOrTableId(ctx context.Context, bh BackgroundExec, isDb bool, dbName, tableName string) (int64, error) {
	var err error
	var sql string
	var erArray []ExecResult
	var id int64
	if isDb {
		sql = getSqlForCheckDatabase(dbName)
	} else {
		sql = getSqlForCheckDatabaseTable(dbName, tableName)
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return 0, err
	}

	if execResultArrayHasData(erArray) {
		id, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			return 0, err
		}
		return id, nil
	}
	if isDb {
		return 0, moerr.NewInternalError(`there is no database "%s"`, dbName)
	} else {
		//TODO: check the database exists or not first
		return 0, moerr.NewInternalError(`there is no table "%s" in database "%s"`, tableName, dbName)
	}
}

// convertAstObjectTypeToObjectType gets the object type from the ast
func convertAstObjectTypeToObjectType(ot tree.ObjectType) (objectType, error) {
	var objType objectType
	switch ot {
	case tree.OBJECT_TYPE_TABLE:
		objType = objectTypeTable
	case tree.OBJECT_TYPE_DATABASE:
		objType = objectTypeDatabase
	case tree.OBJECT_TYPE_ACCOUNT:
		objType = objectTypeAccount
	default:
		return 0, moerr.NewInternalError(`the object type "%s" is unsupported`, ot.String())
	}
	return objType, nil
}

// checkPrivilegeObjectTypeAndPrivilegeLevel checks the relationship among the privilege type, the object type and the privilege level.
// it returns the converted object type, the privilege level and the object id.
func checkPrivilegeObjectTypeAndPrivilegeLevel(ctx context.Context, ses *Session, bh BackgroundExec,
	ot tree.ObjectType, pl tree.PrivilegeLevel) (privilegeLevelType, int64, error) {
	var privLevel privilegeLevelType
	var objId int64
	var err error
	var dbName string

	switch ot {
	case tree.OBJECT_TYPE_TABLE:
		switch pl.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId, err = getDatabaseOrTableId(ctx, bh, true, ses.GetDatabaseName(), "")
			if err != nil {
				return 0, 0, err
			}
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			privLevel = privilegeLevelStarStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
			privLevel = privilegeLevelDatabaseStar
			objId, err = getDatabaseOrTableId(ctx, bh, true, pl.DbName, "")
			if err != nil {
				return 0, 0, err
			}
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
			privLevel = privilegeLevelDatabaseTable
			objId, err = getDatabaseOrTableId(ctx, bh, false, pl.DbName, pl.TabName)
			if err != nil {
				return 0, 0, err
			}
		case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
			privLevel = privilegeLevelTable
			objId, err = getDatabaseOrTableId(ctx, bh, false, ses.GetDatabaseName(), pl.TabName)
			if err != nil {
				return 0, 0, err
			}
		default:
			err = moerr.NewInternalError(`in the object type "%s" the privilege level "%s" is unsupported`, ot.String(), pl.String())
			return 0, 0, err
		}
	case tree.OBJECT_TYPE_DATABASE:
		switch pl.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			privLevel = privilegeLevelStarStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
			//in the syntax, we can not distinguish the table name from the database name.
			privLevel = privilegeLevelDatabase
			dbName = pl.TabName
			objId, err = getDatabaseOrTableId(ctx, bh, true, dbName, "")
			if err != nil {
				return 0, 0, err
			}
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE:
			privLevel = privilegeLevelDatabase
			dbName = pl.DbName
			objId, err = getDatabaseOrTableId(ctx, bh, true, dbName, "")
			if err != nil {
				return 0, 0, err
			}
		default:
			err = moerr.NewInternalError(`in the object type "%s" the privilege level "%s" is unsupported`, ot.String(), pl.String())
			return 0, 0, err
		}
	case tree.OBJECT_TYPE_ACCOUNT:
		switch pl.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId = objectIDAll
		default:
			err = moerr.NewInternalError(`in the object type "%s" the privilege level "%s" is unsupported`, ot.String(), pl.String())
			return 0, 0, err
		}
	default:
		err = moerr.NewInternalError(`the object type "%s" is unsupported`, ot.String())
		return 0, 0, err
	}

	return privLevel, objId, err
}

// matchPrivilegeTypeWithObjectType matches the privilege type with the object type
func matchPrivilegeTypeWithObjectType(privType PrivilegeType, objType objectType) error {
	var err error
	switch privType.Scope() {
	case PrivilegeScopeSys, PrivilegeScopeAccount, PrivilegeScopeUser, PrivilegeScopeRole:
		if objType != objectTypeAccount {
			err = moerr.NewInternalError(`the privilege "%s" can only be granted to the object type "account"`, privType)
		}
	case PrivilegeScopeDatabase:
		if objType != objectTypeDatabase {
			err = moerr.NewInternalError(`the privilege "%s" can only be granted to the object type "database"`, privType)
		}
	case PrivilegeScopeTable:
		if objType != objectTypeTable {
			err = moerr.NewInternalError(`the privilege "%s" can only be granted to the object type "table"`, privType)
		}
	case PrivilegeScopeRoutine:
		if objType != objectTypeFunction {
			err = moerr.NewInternalError(`the privilege "%s" can only be granted to the object type "function"`, privType)
		}
	}
	return err
}

// doGrantPrivilege accomplishes the GrantPrivilege statement
func doGrantPrivilege(ctx context.Context, ses *Session, gp *tree.GrantPrivilege) error {
	var err error
	var erArray []ExecResult
	var roleId int64
	var privType PrivilegeType
	var objType objectType
	var privLevel privilegeLevelType
	var objId int64

	err = normalizeNamesOfRoles(gp.Roles)
	if err != nil {
		return err
	}

	account := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//Get primary keys
	//step 1: get role_id
	verifiedRoles := make([]*verifiedRole, len(gp.Roles))
	checkedPrivilegeTypes := make([]PrivilegeType, len(gp.Privileges))

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	for i, role := range gp.Roles {
		//check Grant privilege on xxx yyy to moadmin(accountadmin)
		if account.IsNameOfAdminRoles(role.UserName) {
			err = moerr.NewInternalError("the privilege can not be granted to the role %s", role.UserName)
			goto handleFailed
		}
		sql := getSqlForRoleIdOfRole(role.UserName)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}

		if execResultArrayHasData(erArray) {
			for j := uint64(0); j < erArray[0].GetRowCount(); j++ {
				roleId, err = erArray[0].GetInt64(j, 0)
				if err != nil {
					goto handleFailed
				}
			}
		} else {
			err = moerr.NewInternalError("there is no role %s", role.UserName)
			goto handleFailed
		}
		verifiedRoles[i] = &verifiedRole{
			typ:  roleType,
			name: role.UserName,
			id:   roleId,
		}
	}

	//get the object type
	objType, err = convertAstObjectTypeToObjectType(gp.ObjType)
	if err != nil {
		goto handleFailed
	}

	//check the privilege and the object type
	for i, priv := range gp.Privileges {
		privType, err = convertAstPrivilegeTypeToPrivilegeType(priv.Type, gp.ObjType)
		if err != nil {
			goto handleFailed
		}
		if isBannedPrivilege(privType) {
			err = moerr.NewInternalError("the privilege %s can not be granted", privType)
			goto handleFailed
		}
		//check the match between the privilegeScope and the objectType
		err = matchPrivilegeTypeWithObjectType(privType, objType)
		if err != nil {
			goto handleFailed
		}
		checkedPrivilegeTypes[i] = privType
	}

	//step 2: get obj_type, privilege_level
	//step 3: get obj_id
	privLevel, objId, err = checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, gp.ObjType, *gp.Level)
	if err != nil {
		goto handleFailed
	}

	//step 4: get privilege_id
	//step 5: check exists
	//step 6: update or insert

	for _, privType = range checkedPrivilegeTypes {
		for _, role := range verifiedRoles {
			sql := getSqlForCheckRoleHasPrivilege(role.id, objType, objId, int64(privType))
			//check exists
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				goto handleFailed
			}

			//choice 1 : update the record
			//choice 2 : inset new record
			choice := 1
			if execResultArrayHasData(erArray) {
				for j := uint64(0); j < erArray[0].GetRowCount(); j++ {
					_, err = erArray[0].GetInt64(j, 0)
					if err != nil {
						goto handleFailed
					}
				}
			} else {
				choice = 2
			}

			if choice == 1 { //update the record
				sql = getSqlForUpdateRolePrivs(int64(account.GetUserID()),
					types.CurrentTimestamp().String2(time.UTC, 0),
					gp.GrantOption, role.id, objType, objId, int64(privType))
			} else if choice == 2 { //insert new record
				sql = getSqlForInsertRolePrivs(role.id, role.name, objType.String(), objId,
					int64(privType), privType.String(), privLevel.String(), int64(account.GetUserID()),
					types.CurrentTimestamp().String2(time.UTC, 0), gp.GrantOption)
			}

			//insert or update
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// doRevokeRole accomplishes the RevokeRole statement
func doRevokeRole(ctx context.Context, ses *Session, rr *tree.RevokeRole) error {
	var err error
	err = normalizeNamesOfRoles(rr.Roles)
	if err != nil {
		return err
	}
	err = normalizeNamesOfUsers(rr.Users)
	if err != nil {
		return err
	}

	account := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//step1 : check Roles exists or not
	var vr *verifiedRole

	verifiedFromRoles := make([]*verifiedRole, len(rr.Roles))
	verifiedToRoles := make([]*verifiedRole, len(rr.Users))

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//handle "IF EXISTS"
	//step1 : check Users are real Users or Roles,  exists or not
	for i, user := range rr.Users {
		sql := getSqlForRoleIdOfRole(user.Username)
		vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, roleType)
		if err != nil {
			goto handleFailed
		}
		if vr != nil {
			verifiedToRoles[i] = vr
		} else {
			//check user
			sql = getSqlForPasswordOfUser(user.Username)
			vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, userType)
			if err != nil {
				goto handleFailed
			}
			verifiedToRoles[i] = vr
			if vr == nil {
				if !rr.IfExists { //when the "IF EXISTS" is set, just skip the check
					err = moerr.NewInternalError("there is no role or user %s", user.Username)
					goto handleFailed
				}
			}
		}
	}

	//handle "IF EXISTS"
	//step2 : check roles before the FROM clause
	for i, role := range rr.Roles {
		sql := getSqlForRoleIdOfRole(role.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			goto handleFailed
		}
		if vr == nil {
			err = moerr.NewInternalError("there is no role %s", role.UserName)
			goto handleFailed
		}
		verifiedFromRoles[i] = vr
	}

	//step3 : process Revoke role from role
	//step4 : process Revoke role from user
	for _, from := range verifiedFromRoles {
		for _, to := range verifiedToRoles {
			if to == nil { //Under "IF EXISTS"
				continue
			}
			if account.IsNameOfAdminRoles(from.name) {
				//check Revoke moadmin from root,dump,userX
				//check Revoke accountadmin from root,dump,userX
				//check Revoke moadmin(accountadmin) from roleX
				err = moerr.NewInternalError("the role %s can not be revoked", from.name)
				goto handleFailed
			} else if isPublicRole(from.name) {
				//
				err = moerr.NewInternalError("the role %s can not be revoked", from.name)
				goto handleFailed
			}

			if to.typ == roleType {
				//check Revoke roleX from moadmin(accountadmin)
				if account.IsNameOfAdminRoles(to.name) {
					err = moerr.NewInternalError("the role %s can not be revoked from the role %s", from.name, to.name)
					goto handleFailed
				} else if isPublicRole(to.name) {
					//check Revoke roleX from public
					err = moerr.NewInternalError("the role %s can not be revoked from the role %s", from.name, to.name)
					goto handleFailed
				}
			}

			sql := ""
			if to.typ == roleType {
				//revoke from role
				//delete (granted_id,grantee_id) from the mo_role_grant
				sql = getSqlForDeleteRoleGrant(from.id, to.id)
			} else {
				//revoke from user
				//delete (roleId,userId) from the mo_user_grant
				sql = getSqlForDeleteUserGrant(from.id, to.id)
			}
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// verifySpecialRolesInGrant verifies the special roles in the Grant statement
func verifySpecialRolesInGrant(account *TenantInfo, from, to *verifiedRole) error {
	if account.IsNameOfAdminRoles(from.name) {
		if to.typ == userType {
			//check Grant moadmin to root,dump
			//check Grant accountadmin to admin_name
			//check Grant moadmin to userX
			//check Grant accountadmin to userX
			if !to.userIsAdmin {
				return moerr.NewInternalError("the role %s can not be granted to non administration user %s", from.name, to.name)
			}
		} else {
			//check Grant moadmin(accountadmin) to roleX
			if !account.IsNameOfAdminRoles(to.name) {
				return moerr.NewInternalError("the role %s can not be granted to the other role %s", from.name, to.name)
			}
		}
	} else if isPublicRole(from.name) && to.typ == roleType {
		return moerr.NewInternalError("the role %s can not be granted to the other role %s", from.name, to.name)
	}

	if to.typ == roleType {
		//check Grant roleX to moadmin(accountadmin)
		if account.IsNameOfAdminRoles(to.name) {
			return moerr.NewInternalError("the role %s can not be granted to the role %s", from.name, to.name)
		} else if isPublicRole(to.name) {
			//check Grant roleX to public
			return moerr.NewInternalError("the role %s can not be granted to the role %s", from.name, to.name)
		}
	}
	return nil
}

// doGrantRole accomplishes the GrantRole statement
func doGrantRole(ctx context.Context, ses *Session, gr *tree.GrantRole) error {
	var erArray []ExecResult
	var err error
	var withGrantOption int64
	err = normalizeNamesOfRoles(gr.Roles)
	if err != nil {
		return err
	}
	err = normalizeNamesOfUsers(gr.Users)
	if err != nil {
		return err
	}

	account := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//step1 : check Roles exists or not
	var vr *verifiedRole
	var needLoadMoRoleGrant bool
	var grantedId, granteeId int64
	var useIsAdmin bool

	verifiedFromRoles := make([]*verifiedRole, len(gr.Roles))
	verifiedToRoles := make([]*verifiedRole, len(gr.Users))

	//load mo_role_grant into memory for
	checkLoopGraph := NewGraph()

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	for i, role := range gr.Roles {
		sql := getSqlForRoleIdOfRole(role.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			goto handleFailed
		}
		if vr == nil {
			err = moerr.NewInternalError("there is no role %s", role.UserName)
			goto handleFailed
		}
		verifiedFromRoles[i] = vr
	}

	//step2 : check Users are real Users or Roles,  exists or not
	for i, user := range gr.Users {
		sql := getSqlForRoleIdOfRole(user.Username)
		vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, roleType)
		if err != nil {
			goto handleFailed
		}
		if vr != nil {
			verifiedToRoles[i] = vr
		} else {
			//check user exists or not
			sql = getSqlForPasswordOfUser(user.Username)
			vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, userType)
			if err != nil {
				goto handleFailed
			}
			if vr == nil {
				err = moerr.NewInternalError("there is no role or user %s", user.Username)
				goto handleFailed
			}
			verifiedToRoles[i] = vr

			//the user is the administrator or not
			useIsAdmin, err = userIsAdministrator(ctx, bh, vr.id, account)
			if err != nil {
				goto handleFailed
			}
			verifiedToRoles[i].userIsAdmin = useIsAdmin
		}
	}

	//If there is at least one role in the verifiedToRoles,
	//it is necessary to load the mo_role_grant
	for _, role := range verifiedToRoles {
		if role.typ == roleType {
			needLoadMoRoleGrant = true
			break
		}
	}

	if needLoadMoRoleGrant {
		//load mo_role_grant
		sql := getSqlForGetAllStuffRoleGrantFormat()
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}

		if execResultArrayHasData(erArray) {
			for j := uint64(0); j < erArray[0].GetRowCount(); j++ {
				//column grantedId
				grantedId, err = erArray[0].GetInt64(j, 0)
				if err != nil {
					goto handleFailed
				}

				//column granteeId
				granteeId, err = erArray[0].GetInt64(j, 1)
				if err != nil {
					goto handleFailed
				}

				checkLoopGraph.addEdge(grantedId, granteeId)
			}
		}
	}

	//step3 : process Grant role to role
	//step4 : process Grant role to user

	for _, from := range verifiedFromRoles {
		for _, to := range verifiedToRoles {
			err = verifySpecialRolesInGrant(account, from, to)
			if err != nil {
				goto handleFailed
			}

			sql := ""
			if to.typ == roleType {
				if from.id == to.id { //direct loop
					err = moerr.NewRoleGrantedToSelf(from.name, to.name)
					goto handleFailed
				} else {
					//check the indirect loop
					edgeId := checkLoopGraph.addEdge(from.id, to.id)
					has := checkLoopGraph.hasLoop(from.id)
					if has {
						err = moerr.NewRoleGrantedToSelf(from.name, to.name)
						goto handleFailed
					}
					//restore the graph
					checkLoopGraph.removeEdge(edgeId)
				}

				//grant to role
				//get (granted_id,grantee_id,with_grant_option) from the mo_role_grant
				sql = getSqlForCheckRoleGrant(from.id, to.id)
			} else {
				//grant to user
				//get (roleId,userId,with_grant_option) from the mo_user_grant
				sql = getSqlForCheckUserGrant(from.id, to.id)
			}
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				goto handleFailed
			}

			//For Grant role to role
			//choice 1: (granted_id,grantee_id) exists and with_grant_option is same.
			//	Do nothing.
			//choice 2: (granted_id,grantee_id) exists and with_grant_option is different.
			//	Update.
			//choice 3: (granted_id,grantee_id) does not exist.
			// Insert.

			//For Grant role to user
			//choice 1: (roleId,userId) exists and with_grant_option is same.
			//	Do nothing.
			//choice 2: (roleId,userId) exists and with_grant_option is different.
			//	Update.
			//choice 3: (roleId,userId) does not exist.
			// Insert.
			choice := 1
			if execResultArrayHasData(erArray) {
				for j := uint64(0); j < erArray[0].GetRowCount(); j++ {
					withGrantOption, err = erArray[0].GetInt64(j, 2)
					if err != nil {
						goto handleFailed
					}
					if (withGrantOption == 1) != gr.GrantOption {
						choice = 2
					}
				}
			} else {
				choice = 3
			}

			sql = ""
			if choice == 2 {
				//update grant time
				if to.typ == roleType {
					sql = getSqlForUpdateRoleGrant(from.id, to.id, int64(account.GetDefaultRoleID()), int64(account.GetUserID()), types.CurrentTimestamp().String2(time.UTC, 0), gr.GrantOption)
				} else {
					sql = getSqlForUpdateUserGrant(from.id, to.id, types.CurrentTimestamp().String2(time.UTC, 0), gr.GrantOption)
				}
			} else if choice == 3 {
				//insert new record
				if to.typ == roleType {
					sql = getSqlForInsertRoleGrant(from.id, to.id, int64(account.GetDefaultRoleID()), int64(account.GetUserID()), types.CurrentTimestamp().String2(time.UTC, 0), gr.GrantOption)
				} else {
					sql = getSqlForInsertUserGrant(from.id, to.id, types.CurrentTimestamp().String2(time.UTC, 0), gr.GrantOption)
				}
			}

			if choice != 1 {
				err = bh.Exec(ctx, sql)
				if err != nil {
					goto handleFailed
				}
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// determinePrivilegeSetOfStatement decides the privileges that the statement needs before running it.
// That is the Set P for the privilege Set .
func determinePrivilegeSetOfStatement(stmt tree.Statement) *privilege {
	typs := make([]PrivilegeType, 0, 5)
	kind := privilegeKindGeneral
	special := specialTagNone
	objType := objectTypeAccount
	var extraEntries []privilegeEntry
	writeDBTableDirect := false
	dbName := ""
	switch st := stmt.(type) {
	case *tree.CreateAccount:
		typs = append(typs, PrivilegeTypeCreateAccount)
	case *tree.DropAccount:
		typs = append(typs, PrivilegeTypeDropAccount)
	case *tree.AlterAccount:
		typs = append(typs, PrivilegeTypeAlterAccount)
	case *tree.CreateUser:
		if st.Role == nil {
			typs = append(typs, PrivilegeTypeCreateUser, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
		} else {
			typs = append(typs, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
			me1 := &compoundEntry{
				items: []privilegeItem{
					{PrivilegeTypeCreateUser, nil, nil, "", ""},
					{PrivilegeTypeManageGrants, nil, nil, "", ""},
				},
			}
			me2 := &compoundEntry{
				items: []privilegeItem{
					{PrivilegeTypeCreateUser, nil, nil, "", ""},
					{PrivilegeTypeCanGrantRoleToOthersInCreateUser, st.Role, st.Users, "", ""},
				},
			}

			entry1 := privilegeEntry{
				privilegeEntryTyp: privilegeEntryTypeCompound,
				compound:          me1,
			}
			entry2 := privilegeEntry{
				privilegeEntryTyp: privilegeEntryTypeCompound,
				compound:          me2,
			}
			extraEntries = append(extraEntries, entry1, entry2)
		}
	case *tree.DropUser:
		typs = append(typs, PrivilegeTypeDropUser, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeUserOwnership*/)
	case *tree.AlterUser:
		typs = append(typs, PrivilegeTypeAlterUser, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeUserOwnership*/)
	case *tree.CreateRole:
		typs = append(typs, PrivilegeTypeCreateRole, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
	case *tree.DropRole:
		typs = append(typs, PrivilegeTypeDropRole, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeRoleOwnership*/)
	case *tree.Grant:
		if st.Typ == tree.GrantTypeRole {
			kind = privilegeKindInherit
			typs = append(typs, PrivilegeTypeManageGrants, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeRoleOwnership*/)
		} else if st.Typ == tree.GrantTypePrivilege {
			objType = objectTypeNone
			kind = privilegeKindSpecial
			special = specialTagAdmin | specialTagWithGrantOption | specialTagOwnerOfObject
		}
	case *tree.GrantRole:
		kind = privilegeKindInherit
		typs = append(typs, PrivilegeTypeManageGrants, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeRoleOwnership*/)
	case *tree.GrantPrivilege:
		objType = objectTypeNone
		kind = privilegeKindSpecial
		special = specialTagAdmin | specialTagWithGrantOption | specialTagOwnerOfObject
	case *tree.Revoke:
		if st.Typ == tree.RevokeTypeRole {
			typs = append(typs, PrivilegeTypeManageGrants, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeRoleOwnership*/)
		} else if st.Typ == tree.RevokeTypePrivilege {
			objType = objectTypeNone
			kind = privilegeKindSpecial
			special = specialTagAdmin
		}
	case *tree.RevokeRole:
		typs = append(typs, PrivilegeTypeManageGrants, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership, PrivilegeTypeRoleOwnership*/)
	case *tree.RevokePrivilege:
		objType = objectTypeNone
		kind = privilegeKindSpecial
		special = specialTagAdmin
	case *tree.CreateDatabase:
		typs = append(typs, PrivilegeTypeCreateDatabase, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
	case *tree.DropDatabase:
		typs = append(typs, PrivilegeTypeDropDatabase, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
		writeDBTableDirect = true
		dbName = string(st.Name)
	case *tree.ShowDatabases:
		typs = append(typs, PrivilegeTypeShowDatabases, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
	case *tree.Use:
		typs = append(typs, PrivilegeTypeConnect, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
	case *tree.ShowTables, *tree.ShowCreateTable, *tree.ShowColumns, *tree.ShowCreateView, *tree.ShowCreateDatabase:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeShowTables, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership)
	case *tree.CreateTable:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeCreateTable, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership)
		writeDBTableDirect = true
		dbName = string(st.Table.SchemaName)
	case *tree.CreateView:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeCreateView, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership)
		writeDBTableDirect = true
		if st.Name != nil {
			dbName = string(st.Name.SchemaName)
		}
	case *tree.DropTable:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeDropTable, PrivilegeTypeDropObject, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership)
		writeDBTableDirect = true
		if len(st.Names) != 0 {
			dbName = string(st.Names[0].SchemaName)
		}
	case *tree.DropView:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeDropView, PrivilegeTypeDropObject, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership)
		writeDBTableDirect = true
		if len(st.Names) != 0 {
			dbName = string(st.Names[0].SchemaName)
		}
	case *tree.Select, *tree.Do:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeSelect, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
	case *tree.Insert:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeInsert, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
	case *tree.Replace:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeReplace, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
	case *tree.Load:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeInsert, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
		if st.Table != nil {
			dbName = string(st.Table.SchemaName)
		}
	case *tree.Import:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeInsert, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
		if st.Table != nil {
			dbName = string(st.Table.SchemaName)
		}
	case *tree.Update:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeUpdate, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
	case *tree.Delete:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeDelete, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
		writeDBTableDirect = true
	case *tree.CreateIndex, *tree.DropIndex, *tree.ShowIndex:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeIndex)
		writeDBTableDirect = true
	case *tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables,
		*tree.ShowStatus, *tree.ShowTarget, *tree.ShowTableStatus, *tree.ShowGrants, *tree.ShowCollation:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.ExplainFor, *tree.ExplainAnalyze, *tree.ExplainStmt:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction, *tree.SetVar:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.PrepareStmt, *tree.PrepareString, *tree.Deallocate:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.Execute:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.Declare:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *InternalCmdFieldList:
		objType = objectTypeNone
		kind = privilegeKindNone
	case *tree.ValuesStatement:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeValues, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
	case *tree.TruncateTable:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeTruncate, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership)
	default:
		panic(fmt.Sprintf("does not have the privilege definition of the statement %s", stmt))
	}

	entries := make([]privilegeEntry, len(typs))
	for i, typ := range typs {
		entries[i] = privilegeEntriesMap[typ]
		entries[i].databaseName = dbName
	}
	entries = append(entries, extraEntries...)
	return &privilege{kind, objType, entries, special, writeDBTableDirect}
}

// privilege will be done on the table
type privilegeTips struct {
	typ          PrivilegeType
	databaseName string
	tableName    string
}

type privilegeTipsArray []privilegeTips

func (pot privilegeTips) String() string {
	return fmt.Sprintf("%s %s %s", pot.typ, pot.databaseName, pot.tableName)
}

func (pota privilegeTipsArray) String() string {
	b := strings.Builder{}
	for _, table := range pota {
		b.WriteString(table.String())
		b.WriteString("\n")
	}
	return b.String()
}

// extractPrivilegeTipsFromPlan extracts the privilege tips from the plan
func extractPrivilegeTipsFromPlan(p *plan2.Plan) privilegeTipsArray {
	//NOTE: the pots may be nil when the plan does operate any table.
	var pots privilegeTipsArray
	appendPot := func(pot privilegeTips) {
		pots = append(pots, pot)
	}
	if p.GetQuery() != nil { //select,insert select, update, delete
		q := p.GetQuery()
		lastNode := q.Nodes[len(q.Nodes)-1]
		var t PrivilegeType

		for _, node := range q.Nodes {
			if node.NodeType == plan.Node_TABLE_SCAN {
				switch lastNode.NodeType {
				case plan.Node_UPDATE:
					t = PrivilegeTypeUpdate
				case plan.Node_DELETE:
					t = PrivilegeTypeDelete
				default:
					t = PrivilegeTypeSelect
				}
				if node.ObjRef != nil {
					appendPot(privilegeTips{
						t,
						node.ObjRef.GetSchemaName(),
						node.ObjRef.GetObjName(),
					})
				}
			} else if node.NodeType == plan.Node_INSERT { //insert select
				if node.ObjRef != nil {
					appendPot(privilegeTips{
						PrivilegeTypeInsert,
						node.ObjRef.GetSchemaName(),
						node.ObjRef.GetObjName(),
					})
				}
			} else if node.NodeType == plan.Node_DELETE {
				if node.ObjRef != nil {
					appendPot(privilegeTips{
						PrivilegeTypeDelete,
						node.ObjRef.GetSchemaName(),
						node.ObjRef.GetObjName(),
					})
				}
			}
		}
	} else if p.GetIns() != nil { //insert into values
		ins := p.GetIns()
		appendPot(privilegeTips{
			PrivilegeTypeInsert,
			ins.GetDbName(),
			ins.GetTblName()})
	}
	return pots
}

// convertPrivilegeTipsToPrivilege constructs the privilege entries from the privilege tips from the plan
func convertPrivilegeTipsToPrivilege(priv *privilege, arr privilegeTipsArray) {
	//rewirte the privilege entries based on privilege tips
	if priv.objectType() != objectTypeTable {
		return
	}

	//NOTE: when the arr is nil, it denotes that there is no operation on the table.

	type pair struct {
		databaseName string
		tableName    string
	}

	dedup := make(map[pair]int8)

	//multi privileges take effect together
	entries := make([]privilegeEntry, 0, len(arr))
	multiPrivs := make([]privilegeItem, 0, len(arr))
	for _, tips := range arr {
		multiPrivs = append(multiPrivs, privilegeItem{
			privilegeTyp: tips.typ,
			dbName:       tips.databaseName,
			tableName:    tips.tableName,
		})

		dedup[pair{tips.databaseName, tips.tableName}] = 1
	}

	me := &compoundEntry{multiPrivs}
	entries = append(entries, privilegeEntry{privilegeEntryTyp: privilegeEntryTypeCompound, compound: me})

	//optional predefined privilege : tableAll, ownership
	predefined := []PrivilegeType{PrivilegeTypeTableAll, PrivilegeTypeTableOwnership}
	for _, p := range predefined {
		for par := range dedup {
			e := privilegeEntriesMap[p]
			e.databaseName = par.databaseName
			e.tableName = par.tableName
			entries = append(entries, e)
		}
	}

	priv.entries = entries
}

// getSqlFromPrivilegeEntry generates the query sql for the privilege entry
func getSqlFromPrivilegeEntry(roleId int64, entry privilegeEntry) (string, error) {
	var sql string
	//for object type table, need concrete tableid
	//TODO: table level check should be done after getting the plan
	if entry.objType == objectTypeTable {
		switch entry.privilegeLevel {
		case privilegeLevelDatabaseTable, privilegeLevelTable:
			sql = getSqlForCheckRoleHasTableLevelPrivilege(roleId, entry.privilegeId, entry.databaseName, entry.tableName)
		case privilegeLevelDatabaseStar, privilegeLevelStar:
			sql = getSqlForCheckRoleHasTableLevelForDatabaseStar(roleId, entry.privilegeId, entry.databaseName)
		case privilegeLevelStarStar:
			sql = getSqlForCheckRoleHasTableLevelForStarStar(roleId, entry.privilegeId)
		default:
			return "", moerr.NewInternalError("unsupported privilegel level %s for the privilege %s", entry.privilegeLevel, entry.privilegeId)
		}
	} else if entry.objType == objectTypeDatabase {
		switch entry.privilegeLevel {
		case privilegeLevelStar, privilegeLevelStarStar:
			sql = getSqlForCheckRoleHasDatabaseLevelForStarStar(roleId, entry.privilegeId, entry.privilegeLevel)
		case privilegeLevelDatabase:
			sql = getSqlForCheckRoleHasDatabaseLevelForDatabase(roleId, entry.privilegeId, entry.databaseName)
		default:
			return "", moerr.NewInternalError("unsupported privilegel level %s for the privilege %s", entry.privilegeLevel, entry.privilegeId)
		}
	} else if entry.objType == objectTypeAccount {
		switch entry.privilegeLevel {
		case privilegeLevelStar:
			sql = getSqlForCheckRoleHasAccountLevelForStar(roleId, entry.privilegeId)
		default:
			return "false", moerr.NewInternalError("unsupported privilegel level %s for the privilege %s", entry.privilegeLevel, entry.privilegeId)
		}
	} else {
		sql = getSqlForCheckRoleHasPrivilege(roleId, entry.objType, int64(entry.objId), int64(entry.privilegeId))
	}
	return sql, nil
}

// getPrivilegeLevelsOfObjectType gets the privilege levels of the objectType
func getPrivilegeLevelsOfObjectType(objType objectType) ([]privilegeLevelType, error) {
	if ret, ok := objectType2privilegeLevels[objType]; ok {
		return ret, nil
	}
	return nil, moerr.NewInternalError("do not support the object type %s", objType.String())
}

// getSqlForPrivilege generates the query sql for the privilege entry
func getSqlForPrivilege(roleId int64, entry privilegeEntry, pl privilegeLevelType) (string, error) {
	var sql string
	//for object type table, need concrete tableid
	switch entry.objType {
	case objectTypeTable:
		switch pl {
		case privilegeLevelDatabaseTable, privilegeLevelTable:
			sql = getSqlForCheckRoleHasTableLevelPrivilege(roleId, entry.privilegeId, entry.databaseName, entry.tableName)
		case privilegeLevelDatabaseStar, privilegeLevelStar:
			sql = getSqlForCheckRoleHasTableLevelForDatabaseStar(roleId, entry.privilegeId, entry.databaseName)
		case privilegeLevelStarStar:
			sql = getSqlForCheckRoleHasTableLevelForStarStar(roleId, entry.privilegeId)
		default:
			return "", moerr.NewInternalError("the privilege level %s for the privilege %s is unsupported", pl, entry.privilegeId)
		}
	case objectTypeDatabase:
		switch pl {
		case privilegeLevelStar, privilegeLevelStarStar:
			sql = getSqlForCheckRoleHasDatabaseLevelForStarStar(roleId, entry.privilegeId, pl)
		case privilegeLevelDatabase:
			sql = getSqlForCheckRoleHasDatabaseLevelForDatabase(roleId, entry.privilegeId, entry.databaseName)
		default:
			return "", moerr.NewInternalError("the privilege level %s for the privilege %s is unsupported", pl, entry.privilegeId)
		}
	case objectTypeAccount:
		switch pl {
		case privilegeLevelStar:
			sql = getSqlForCheckRoleHasAccountLevelForStar(roleId, entry.privilegeId)
		default:
			return "false", moerr.NewInternalError("the privilege level %s for the privilege %s is unsupported", pl, entry.privilegeId)
		}
	default:
		sql = getSqlForCheckRoleHasPrivilege(roleId, entry.objType, int64(entry.objId), int64(entry.privilegeId))
	}

	return sql, nil
}

// getSqlForPrivilege2 complements the database name and calls getSqlForPrivilege
func getSqlForPrivilege2(ses *Session, roleId int64, entry privilegeEntry, pl privilegeLevelType) (string, error) {
	//handle the empty database
	if len(entry.databaseName) == 0 {
		entry.databaseName = ses.GetDatabaseName()
	}
	return getSqlForPrivilege(roleId, entry, pl)
}

// determineRoleSetHasPrivilegeSet decides the role set has at least one privilege of the privilege set.
// The algorithm 2.
func determineRoleSetHasPrivilegeSet(ctx context.Context, bh BackgroundExec, ses *Session, roleIds *btree.Set[int64], priv *privilege) (bool, error) {
	var erArray []ExecResult
	var sql string
	var err error
	var pls []privilegeLevelType

	var yes bool
	var operateCatalog bool
	//there is no privilege needs, just approve
	if len(priv.entries) == 0 {
		return false, nil
	}

	cache := ses.GetPrivilegeCache()

	verifyPrivilegeEntryInMultiPrivilegeLevels := func(ses *Session, roleId int64, entry privilegeEntry, pls []privilegeLevelType) (bool, error) {
		dbName := entry.databaseName
		if len(dbName) == 0 {
			dbName = ses.GetDatabaseName()
		}
		for _, pl := range pls {
			yes = cache.has(entry.objType, pl, dbName, entry.tableName, entry.privilegeId)
			if yes {
				return true, nil
			}
			sql, err = getSqlForPrivilege2(ses, roleId, entry, pl)
			if err != nil {
				return false, err
			}

			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				return false, err
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				return false, err
			}

			if execResultArrayHasData(erArray) {
				cache.add(entry.objType, pl, dbName, entry.tableName, entry.privilegeId)
				return true, nil
			}
		}
		return false, nil
	}

	verifyRealUserOperatesCatalog := func(ses *Session, dbName string, writeDBTableDirect bool) bool {
		if ses.GetFromRealUser() && writeDBTableDirect {
			if len(dbName) == 0 {
				dbName = ses.GetDatabaseName()
			}
			if ok2 := isBannedDatabase(dbName); ok2 {
				return ok2
			}
		}
		return false
	}

	for _, roleId := range roleIds.Keys() {
		for _, entry := range priv.entries {
			if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
				pls, err = getPrivilegeLevelsOfObjectType(entry.objType)
				if err != nil {
					return false, err
				}

				yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ses, roleId, entry, pls)
				if err != nil {
					return false, err
				}
				operateCatalog = verifyRealUserOperatesCatalog(ses, entry.databaseName, priv.writeDBTableDirect)
				if operateCatalog {
					yes = false
				}
				if yes {
					return true, nil
				}
			} else if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
				if entry.compound != nil {
					allTrue := true
					//multi privileges take effect together
					for _, mi := range entry.compound.items {
						if mi.privilegeTyp == PrivilegeTypeCanGrantRoleToOthersInCreateUser {
							//TODO: normalize the name
							//TODO: simplify the logic
							yes, err = determineUserCanGrantRolesToOthersInternal(ctx, bh, ses, []*tree.Role{mi.role})
							if err != nil {
								return false, err
							}
							if yes {
								from := &verifiedRole{
									typ:  roleType,
									name: mi.role.UserName,
								}
								for _, user := range mi.users {
									to := &verifiedRole{
										typ:  userType,
										name: user.Username,
									}
									err = verifySpecialRolesInGrant(ses.GetTenantInfo(), from, to)
									if err != nil {
										return false, err
									}
								}
							}
						} else {
							tempEntry := privilegeEntriesMap[mi.privilegeTyp]
							tempEntry.databaseName = mi.dbName
							tempEntry.tableName = mi.tableName
							tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
							tempEntry.compound = nil
							pls, err = getPrivilegeLevelsOfObjectType(tempEntry.objType)
							if err != nil {
								return false, err
							}

							//At least there is one success
							yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ses, roleId, tempEntry, pls)
							if err != nil {
								return false, err
							}
							operateCatalog = verifyRealUserOperatesCatalog(ses, tempEntry.databaseName, priv.writeDBTableDirect)
							if operateCatalog {
								yes = false
							}
						}
						if !yes {
							allTrue = false
							break
						}
					}

					if allTrue {
						return allTrue, nil
					}
				}
			}
		}
	}
	return false, nil
}

// determineUserHasPrivilegeSet decides the privileges of user can satisfy the requirement of the privilege set
// The algorithm 1.
func determineUserHasPrivilegeSet(ctx context.Context, ses *Session, priv *privilege, stmt tree.Statement) (bool, error) {
	var erArray []ExecResult
	var yes bool
	var err error
	var roleB int64
	var ret bool
	var ok bool
	var grantedIds *btree.Set[int64]

	tenant := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//the set of roles the (k+1) th iteration during the execution
	roleSetOfKPlusOneThIteration := &btree.Set[int64]{}
	//the set of roles the k th iteration during the execution
	roleSetOfKthIteration := &btree.Set[int64]{}
	//the set of roles visited by traversal algorithm
	roleSetOfVisited := &btree.Set[int64]{}
	//simple mo_role_grant cache
	cacheOfMoRoleGrant := &btree.Map[int64, *btree.Set[int64]]{}

	//step 1: The Set R1 {default role id}
	//The primary role (in use)
	roleSetOfKthIteration.Insert((int64)(tenant.GetDefaultRoleID()))

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step 2: The Set R2 {the roleid granted to the userid}
	//If the user uses the all secondary roles, the secondary roles needed to be loaded
	err = loadAllSecondaryRoles(ctx, bh, tenant, roleSetOfKthIteration)
	if err != nil {
		goto handleFailed
	}

	//init RVisited = Rk
	roleSetOfKthIteration.Scan(func(roleId int64) bool {
		roleSetOfVisited.Insert(roleId)
		return true
	})

	//Call the algorithm 2.
	//If the result of the algorithm 2 is true, Then return true;
	yes, err = determineRoleSetHasPrivilegeSet(ctx, bh, ses, roleSetOfKthIteration, priv)
	if err != nil {
		goto handleFailed
	}
	if yes {
		ret = true
		goto handleSuccess
	}
	/*
		step 3: !!!NOTE all roleid in Rk has been processed by the algorithm 2.
		RVisited is the set of all roleid that has been processed.
		RVisited = Rk;
		For {
			For roleA in Rk {
				Find the peer roleB in the table mo_role_grant(granted_id,grantee_id) with grantee_id = roleA;
				If roleB is not in RVisited, Then add roleB into R(k+1);
					add roleB into RVisited;
			}

			If R(k+1) is empty, Then return false;
			//Call the algorithm 2.
			If the result of the algorithm 2 is true, Then return true;
			Rk = R(k+1);
			R(k+1) = {};
		}
	*/
	for {
		quit := false
		select {
		case <-ctx.Done():
			quit = true
		default:
		}
		if quit {
			break
		}

		roleSetOfKPlusOneThIteration.Clear()

		//get roleB of roleA
		for _, roleA := range roleSetOfKthIteration.Keys() {
			if grantedIds, ok = cacheOfMoRoleGrant.Get(roleA); ok {
				for _, grantedId := range grantedIds.Keys() {
					roleSetOfKPlusOneThIteration.Insert(grantedId)
				}
				continue
			}
			grantedIds = &btree.Set[int64]{}
			cacheOfMoRoleGrant.Set(roleA, grantedIds)
			sqlForInheritedRoleIdOfRoleId := getSqlForInheritedRoleIdOfRoleId(roleA)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForInheritedRoleIdOfRoleId)
			if err != nil {
				err = moerr.NewInternalError("get inherited role id of the role id. error:%v", err)
				goto handleFailed
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				goto handleFailed
			}

			if execResultArrayHasData(erArray) {
				for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
					roleB, err = erArray[0].GetInt64(i, 0)
					if err != nil {
						goto handleFailed
					}

					if !roleSetOfVisited.Contains(roleB) {
						roleSetOfVisited.Insert(roleB)
						roleSetOfKPlusOneThIteration.Insert(roleB)
						grantedIds.Insert(roleB)
					}
				}
			}
		}

		//no more roleB, it is done
		if roleSetOfKPlusOneThIteration.Len() == 0 {
			ret = false
			goto handleSuccess
		}

		//Call the algorithm 2.
		//If the result of the algorithm 2 is true, Then return true;
		yes, err = determineRoleSetHasPrivilegeSet(ctx, bh, ses, roleSetOfKPlusOneThIteration, priv)
		if err != nil {
			goto handleFailed
		}

		if yes {
			ret = true
			goto handleSuccess
		}
		roleSetOfKthIteration, roleSetOfKPlusOneThIteration = roleSetOfKPlusOneThIteration, roleSetOfKthIteration
	}

handleSuccess:
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return ret, err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return false, rbErr
	}
	return false, err
}

const (
	goOn        int = iota
	successDone     //ri has indirect relation with the Uc
)

// loadAllSecondaryRoles loads all secondary roles
func loadAllSecondaryRoles(ctx context.Context, bh BackgroundExec, account *TenantInfo, roleSetOfCurrentUser *btree.Set[int64]) error {
	var err error
	var sql string

	var erArray []ExecResult
	var roleId int64

	if account.GetUseSecondaryRole() {
		sql = getSqlForRoleIdOfUserId(int(account.GetUserID()))
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			return err
		}

		if execResultArrayHasData(erArray) {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				roleId, err = erArray[0].GetInt64(i, 0)
				if err != nil {
					return err
				}
				roleSetOfCurrentUser.Insert(roleId)
			}
		}
	}
	return err
}

// determineUserCanGrantRolesToOthersInternal decides if the user can grant roles to other users or roles
// the same as the grant/revoke privilege, role with inputted transaction and BackgroundExec
func determineUserCanGrantRolesToOthersInternal(ctx context.Context, bh BackgroundExec, ses *Session, fromRoles []*tree.Role) (bool, error) {
	//step1: normalize the names of roles and users
	var err error
	err = normalizeNamesOfRoles(fromRoles)
	if err != nil {
		return false, err
	}

	//step2: decide the current user
	account := ses.GetTenantInfo()

	//step3: check the link: roleX -> roleA -> .... -> roleZ -> the current user. Every link has the with_grant_option.
	var vr *verifiedRole
	var ret = true
	var granted bool
	//the temporal set of roles during the execution
	var tempRoleSet *btree.Set[int64]
	var sql string
	//the set of roles the (k+1) th iteration during the execution
	roleSetOfKPlusOneThIteration := &btree.Set[int64]{}
	//the set of roles the k th iteration during the execution
	roleSetOfKthIteration := &btree.Set[int64]{}
	//the set of roles of the current user that executes this statement or function
	roleSetOfCurrentUser := &btree.Set[int64]{}
	//the set of roles visited by traversal algorithm
	roleSetOfVisited := &btree.Set[int64]{}
	verifiedFromRoles := make([]*verifiedRole, len(fromRoles))

	//step 1 : add the primary role
	roleSetOfCurrentUser.Insert(int64(account.GetDefaultRoleID()))

	for i, role := range fromRoles {
		sql = getSqlForRoleIdOfRole(role.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			goto handleFailed
		}
		if vr == nil {
			err = moerr.NewInternalError("there is no role %s", role.UserName)
			goto handleFailed
		}
		verifiedFromRoles[i] = vr
	}

	//step 2: The Set R2 {the roleid granted to the userid}
	//If the user uses the all secondary roles, the secondary roles needed to be loaded
	err = loadAllSecondaryRoles(ctx, bh, account, roleSetOfCurrentUser)
	if err != nil {
		goto handleFailed
	}

	for _, role := range verifiedFromRoles {
		//if it is the role in use, do the check
		if roleSetOfCurrentUser.Contains(role.id) {
			//check the direct relation between role and user
			granted, err = isRoleGrantedToUserWGO(ctx, bh, role.id, int64(account.GetUserID()))
			if err != nil {
				goto handleFailed
			}
			if granted {
				continue
			}
		}

		roleSetOfKthIteration.Clear()
		roleSetOfVisited.Clear()
		roleSetOfKthIteration.Insert(role.id)

		riResult := goOn
		//It is kind of level traversal
		for roleSetOfKthIteration.Len() != 0 && riResult == goOn {
			roleSetOfKPlusOneThIteration.Clear()
			for _, ri := range roleSetOfKthIteration.Keys() {
				tempRoleSet, err = getRoleSetThatRoleGrantedToWGO(ctx, bh, ri, roleSetOfVisited, roleSetOfKPlusOneThIteration)
				if err != nil {
					goto handleFailed
				}

				if setIsIntersected(tempRoleSet, roleSetOfCurrentUser) {
					riResult = successDone
					break
				}
			}

			//swap Rk,R(k+1)
			roleSetOfKthIteration, roleSetOfKPlusOneThIteration = roleSetOfKPlusOneThIteration, roleSetOfKthIteration
		}

		if riResult != successDone {
			//fail
			ret = false
			break
		}
	}
	return ret, err

handleFailed:
	return false, err
}

// determineUserCanGrantRoleToOtherUsers decides if the user can grant roles to other users or roles
// the same as the grant/revoke privilege, role.
func determineUserCanGrantRolesToOthers(ctx context.Context, ses *Session, fromRoles []*tree.Role) (bool, error) {
	//step1: normalize the names of roles and users
	var err error
	var ret bool
	err = normalizeNamesOfRoles(fromRoles)
	if err != nil {
		return false, err
	}

	//step2: decide the current user
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	ret, err = determineUserCanGrantRolesToOthersInternal(ctx, bh, ses, fromRoles)
	if err != nil {
		goto handleFailed
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return ret, err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return false, rbErr
	}
	return false, err
}

// isRoleGrantedToUserWGO verifies the role has been granted to the user with with_grant_option = true.
// Algorithm 1
func isRoleGrantedToUserWGO(ctx context.Context, bh BackgroundExec, roleId, UserId int64) (bool, error) {
	var err error

	var erArray []ExecResult
	sql := getSqlForCheckUserGrantWGO(roleId, UserId)
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}

	return false, nil
}

// getRoleSetThatRoleGrantedToWGO returns all the roles that the role has been granted to with with_grant_option = true.
// Algorithm 2
func getRoleSetThatRoleGrantedToWGO(ctx context.Context, bh BackgroundExec, roleId int64, RVisited, RkPlusOne *btree.Set[int64]) (*btree.Set[int64], error) {
	var err error

	var erArray []ExecResult
	var id int64
	rset := &btree.Set[int64]{}
	sql := getSqlForCheckRoleGrantWGO(roleId)
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			id, err = erArray[0].GetInt64(i, 0)
			if err != nil {
				return nil, err
			}
			if !RVisited.Contains(id) {
				RVisited.Insert(id)
				RkPlusOne.Insert(id)
				rset.Insert(id)
			}
		}
	}

	return rset, err
}

// authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase decides the user has the privilege of executing the statement with object type account
func authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ctx context.Context, ses *Session, stmt tree.Statement) (bool, error) {
	var err error
	var ok, yes bool
	priv := ses.GetPrivilege()
	if priv.objectType() != objectTypeAccount && priv.objectType() != objectTypeDatabase { //do nothing
		return true, nil
	}
	ok, err = determineUserHasPrivilegeSet(ctx, ses, priv, stmt)
	if err != nil {
		return false, err
	}

	//for GrantRole statement, check with_grant_option
	if !ok && priv.kind == privilegeKindInherit {
		grant := stmt.(*tree.Grant)
		grantRole := grant.GrantRole
		yes, err = determineUserCanGrantRolesToOthers(ctx, ses, grantRole.Roles)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
	}
	//for Create User statement with default role.
	//TODO:

	return ok, nil
}

// authenticateUserCanExecuteStatementWithObjectTypeTable decides the user has the privilege of executing the statement with object type table
func authenticateUserCanExecuteStatementWithObjectTypeTable(ctx context.Context, ses *Session, stmt tree.Statement, p *plan2.Plan) (bool, error) {
	priv := determinePrivilegeSetOfStatement(stmt)
	if priv.objectType() == objectTypeTable {
		arr := extractPrivilegeTipsFromPlan(p)
		if len(arr) == 0 {
			return true, nil
		}
		convertPrivilegeTipsToPrivilege(priv, arr)
		ok, err := determineUserHasPrivilegeSet(ctx, ses, priv, stmt)
		if err != nil {
			return false, err
		}
		return ok, nil
	}
	return true, nil
}

// formSqlFromGrantPrivilege makes the sql for querying the database.
func formSqlFromGrantPrivilege(ctx context.Context, ses *Session, gp *tree.GrantPrivilege, priv *tree.Privilege) (string, error) {
	tenant := ses.GetTenantInfo()
	sql := ""
	var privType PrivilegeType
	var err error
	privType, err = convertAstPrivilegeTypeToPrivilegeType(priv.Type, gp.ObjType)
	if err != nil {
		return "", err
	}
	switch gp.ObjType {
	case tree.OBJECT_TYPE_TABLE:
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			sql = getSqlForCheckWithGrantOptionForTableDatabaseStar(int64(tenant.GetDefaultRoleID()), privType, ses.GetDatabaseName())
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			sql = getSqlForCheckWithGrantOptionForTableStarStar(int64(tenant.GetDefaultRoleID()), privType)
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
			sql = getSqlForCheckWithGrantOptionForTableDatabaseStar(int64(tenant.GetDefaultRoleID()), privType, gp.Level.DbName)
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
			sql = getSqlForCheckWithGrantOptionForTableDatabaseTable(int64(tenant.GetDefaultRoleID()), privType, gp.Level.DbName, gp.Level.TabName)
		case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
			sql = getSqlForCheckWithGrantOptionForTableDatabaseTable(int64(tenant.GetDefaultRoleID()), privType, ses.GetDatabaseName(), gp.Level.TabName)
		default:
			return "", moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
		}
	case tree.OBJECT_TYPE_DATABASE:
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			sql = getSqlForCheckWithGrantOptionForDatabaseStar(int64(tenant.GetDefaultRoleID()), privType)
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			sql = getSqlForCheckWithGrantOptionForDatabaseStarStar(int64(tenant.GetDefaultRoleID()), privType)
		case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
			//in the syntax, we can not distinguish the table name from the database name.
			sql = getSqlForCheckWithGrantOptionForDatabaseDB(int64(tenant.GetDefaultRoleID()), privType, gp.Level.TabName)
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE:
			sql = getSqlForCheckWithGrantOptionForDatabaseDB(int64(tenant.GetDefaultRoleID()), privType, gp.Level.DbName)
		default:
			return "", moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
		}
	case tree.OBJECT_TYPE_ACCOUNT:
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			sql = getSqlForCheckWithGrantOptionForAccountStar(int64(tenant.GetDefaultRoleID()), privType)
		default:
			return "", moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
		}
	default:
		return "", moerr.NewInternalError("object type %v is unsupported", gp.ObjType)
	}
	return sql, nil
}

// getRoleSetThatPrivilegeGrantedToWGO gets all roles that the privilege granted to with with_grant_option = true
// The algorithm 3
func getRoleSetThatPrivilegeGrantedToWGO(ctx context.Context, bh BackgroundExec, privType PrivilegeType) (*btree.Set[int64], error) {
	var err error

	var erArray []ExecResult
	var id int64
	rset := &btree.Set[int64]{}
	sql := getSqlForCheckRoleHasPrivilegeWGO(int64(privType))
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			id, err = erArray[0].GetInt64(i, 0)
			if err != nil {
				return nil, err
			}
			rset.Insert(id)
		}
	}

	return rset, err
}

// setIsIntersected decides the A is intersecting the B.
func setIsIntersected(A, B *btree.Set[int64]) bool {
	if A.Len() > B.Len() {
		A, B = B, A
	}
	iter := A.Iter()
	for x := iter.First(); x; x = iter.Next() {
		if B.Contains(iter.Key()) {
			return true
		}
	}
	return false
}

// determineUserCanGrantPrivilegesToOthers decides the privileges can be granted to others.
func determineUserCanGrantPrivilegesToOthers(ctx context.Context, ses *Session, gp *tree.GrantPrivilege) (bool, error) {
	//step1: normalize the names of roles and users
	var err error
	//step2: decide the current user
	account := ses.GetTenantInfo()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//step3: check the link: roleX -> roleA -> .... -> roleZ -> the current user. Every link has the with_grant_option.
	var ret = true
	var privType PrivilegeType
	//the temporal set of roles during the execution
	var tempRoleSet *btree.Set[int64]
	//the set of roles that the privilege granted to with WGO=true
	var roleSetOfPrivilegeGrantedToWGO *btree.Set[int64]
	//the set of roles the (k+1) th iteration during the execution
	roleSetOfKPlusOneThIteration := &btree.Set[int64]{}
	//the set of roles the k th iteration during the execution
	roleSetOfKthIteration := &btree.Set[int64]{}
	//the set of roles visited by traversal algorithm
	roleSetOfVisited := &btree.Set[int64]{}
	//the set of roles of the current user that executes this statement or function
	roleSetOfCurrentUser := &btree.Set[int64]{}

	roleSetOfCurrentUser.Insert(int64(account.GetDefaultRoleID()))

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step 2: The Set R2 {the roleid granted to the userid}
	//If the user uses the all secondary roles, the secondary roles needed to be loaded
	err = loadAllSecondaryRoles(ctx, bh, account, roleSetOfCurrentUser)
	if err != nil {
		goto handleFailed
	}

	for _, priv := range gp.Privileges {
		privType, err = convertAstPrivilegeTypeToPrivilegeType(priv.Type, gp.ObjType)
		if err != nil {
			goto handleFailed
		}

		//call the algorithm 3.
		roleSetOfPrivilegeGrantedToWGO, err = getRoleSetThatPrivilegeGrantedToWGO(ctx, bh, privType)
		if err != nil {
			goto handleFailed
		}

		if setIsIntersected(roleSetOfPrivilegeGrantedToWGO, roleSetOfCurrentUser) {
			continue
		}

		riResult := goOn
		for _, rx := range roleSetOfPrivilegeGrantedToWGO.Keys() {
			roleSetOfKthIteration.Clear()
			roleSetOfVisited.Clear()
			roleSetOfKthIteration.Insert(rx)

			//It is kind of level traversal
			for roleSetOfKthIteration.Len() != 0 && riResult == goOn {
				roleSetOfKPlusOneThIteration.Clear()
				for _, ri := range roleSetOfKthIteration.Keys() {
					tempRoleSet, err = getRoleSetThatRoleGrantedToWGO(ctx, bh, ri, roleSetOfVisited, roleSetOfKPlusOneThIteration)
					if err != nil {
						goto handleFailed
					}

					if setIsIntersected(tempRoleSet, roleSetOfCurrentUser) {
						riResult = successDone
						break
					}
				}

				//swap Rk,R(k+1)
				roleSetOfKthIteration, roleSetOfKPlusOneThIteration = roleSetOfKPlusOneThIteration, roleSetOfKthIteration
			}

			if riResult == successDone {
				break
			}
		}
		if riResult != successDone {
			ret = false
			break
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return ret, err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return false, rbErr
	}
	return false, err
}

func convertAstPrivilegeTypeToPrivilegeType(priv tree.PrivilegeType, ot tree.ObjectType) (PrivilegeType, error) {
	var privType PrivilegeType
	switch priv {
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_ACCOUNT:
		privType = PrivilegeTypeCreateAccount
	case tree.PRIVILEGE_TYPE_STATIC_DROP_ACCOUNT:
		privType = PrivilegeTypeDropAccount
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_ACCOUNT:
		privType = PrivilegeTypeAlterAccount
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_USER:
		privType = PrivilegeTypeCreateUser
	case tree.PRIVILEGE_TYPE_STATIC_DROP_USER:
		privType = PrivilegeTypeDropUser
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_USER:
		privType = PrivilegeTypeAlterUser
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_ROLE:
		privType = PrivilegeTypeCreateRole
	case tree.PRIVILEGE_TYPE_STATIC_DROP_ROLE:
		privType = PrivilegeTypeDropRole
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_ROLE:
		privType = PrivilegeTypeAlterRole
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_DATABASE:
		privType = PrivilegeTypeCreateDatabase
	case tree.PRIVILEGE_TYPE_STATIC_DROP_DATABASE:
		privType = PrivilegeTypeDropDatabase
	case tree.PRIVILEGE_TYPE_STATIC_SHOW_DATABASES:
		privType = PrivilegeTypeShowDatabases
	case tree.PRIVILEGE_TYPE_STATIC_CONNECT:
		privType = PrivilegeTypeConnect
	case tree.PRIVILEGE_TYPE_STATIC_MANAGE_GRANTS:
		privType = PrivilegeTypeManageGrants
	case tree.PRIVILEGE_TYPE_STATIC_ALL:
		switch ot {
		case tree.OBJECT_TYPE_ACCOUNT:
			privType = PrivilegeTypeAccountAll
		case tree.OBJECT_TYPE_DATABASE:
			privType = PrivilegeTypeDatabaseAll
		case tree.OBJECT_TYPE_TABLE:
			privType = PrivilegeTypeTableAll
		default:
			return 0, moerr.NewInternalError(`the object type "%s" do not support the privilege "%s"`, ot.String(), priv.ToString())
		}
	case tree.PRIVILEGE_TYPE_STATIC_OWNERSHIP:
		switch ot {
		case tree.OBJECT_TYPE_DATABASE:
			privType = PrivilegeTypeDatabaseOwnership
		case tree.OBJECT_TYPE_TABLE:
			privType = PrivilegeTypeTableOwnership
		default:
			return 0, moerr.NewInternalError(`the object type "%s" do not support the privilege "%s"`, ot.String(), priv.ToString())
		}
	case tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES:
		privType = PrivilegeTypeShowTables
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLE:
		privType = PrivilegeTypeCreateTable
	case tree.PRIVILEGE_TYPE_STATIC_DROP_TABLE:
		privType = PrivilegeTypeDropTable
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_VIEW:
		privType = PrivilegeTypeCreateView
	case tree.PRIVILEGE_TYPE_STATIC_DROP_VIEW:
		privType = PrivilegeTypeDropView
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_VIEW:
		privType = PrivilegeTypeAlterView
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_TABLE:
		privType = PrivilegeTypeAlterTable
	case tree.PRIVILEGE_TYPE_STATIC_SELECT:
		privType = PrivilegeTypeSelect
	case tree.PRIVILEGE_TYPE_STATIC_INSERT:
		privType = PrivilegeTypeInsert
	case tree.PRIVILEGE_TYPE_STATIC_REPLACE:
		privType = PrivilegeTypeReplace
	case tree.PRIVILEGE_TYPE_STATIC_UPDATE:
		privType = PrivilegeTypeUpdate
	case tree.PRIVILEGE_TYPE_STATIC_DELETE:
		privType = PrivilegeTypeDelete
	case tree.PRIVILEGE_TYPE_STATIC_INDEX:
		privType = PrivilegeTypeIndex
	case tree.PRIVILEGE_TYPE_STATIC_EXECUTE:
		privType = PrivilegeTypeExecute
	case tree.PRIVILEGE_TYPE_STATIC_TRUNCATE:
		privType = PrivilegeTypeTruncate
	case tree.PRIVILEGE_TYPE_STATIC_REFERENCE:
		privType = PrivilegeTypeReference
	case tree.PRIVILEGE_TYPE_STATIC_VALUES:
		privType = PrivilegeTypeValues
	default:
		return 0, moerr.NewInternalError("unsupported privilege type %s", priv.ToString())
	}
	return privType, nil
}

// authenticateUserCanExecuteStatementWithObjectTypeNone decides the user has the privilege of executing the statement with object type none
func authenticateUserCanExecuteStatementWithObjectTypeNone(ctx context.Context, ses *Session, stmt tree.Statement) (bool, error) {
	priv := ses.GetPrivilege()
	if priv.objectType() != objectTypeNone { //do nothing
		return true, nil
	}
	tenant := ses.GetTenantInfo()

	if priv.privilegeKind() == privilegeKindNone { // do nothing
		return true, nil
	} else if priv.privilegeKind() == privilegeKindSpecial { //GrantPrivilege, RevokePrivilege

		checkGrantPrivilege := func(g *tree.GrantPrivilege) (bool, error) {
			//in the version 0.6, only the moAdmin and accountAdmin can grant the privilege.
			if tenant.IsAdminRole() {
				return true, nil
			}
			return determineUserCanGrantPrivilegesToOthers(ctx, ses, g)
		}

		checkRevokePrivilege := func() (bool, error) {
			//in the version 0.6, only the moAdmin and accountAdmin can revoke the privilege.
			return tenant.IsAdminRole(), nil
		}

		switch gp := stmt.(type) {
		case *tree.Grant:
			if gp.Typ == tree.GrantTypePrivilege {
				yes, err := checkGrantPrivilege(&gp.GrantPrivilege)
				if err != nil {
					return yes, err
				}
				if yes {
					return yes, nil
				}
			}
		case *tree.Revoke:
			if gp.Typ == tree.RevokeTypePrivilege {
				return checkRevokePrivilege()
			}
		case *tree.GrantPrivilege:
			yes, err := checkGrantPrivilege(gp)
			if err != nil {
				return yes, err
			}
			if yes {
				return yes, nil
			}
		case *tree.RevokePrivilege:
			return checkRevokePrivilege()
		}
	}

	return false, nil
}

// checkSysExistsOrNot checks the SYS tenant exists or not.
func checkSysExistsOrNot(ctx context.Context, bh BackgroundExec, pu *config.ParameterUnit) (bool, error) {
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

	erArray, err = getResultSet(bh)
	if err != nil {
		return false, err
	}
	if len(erArray) != 1 {
		return false, moerr.NewInternalError("it must have result set")
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		_, err = erArray[0].GetString(i, 0)
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

	erArray, err = getResultSet(bh)
	if err != nil {
		return false, err
	}
	if len(erArray) != 1 {
		return false, moerr.NewInternalError("it must have result set")
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		tableName, err = erArray[0].GetString(i, 0)
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

// InitSysTenant initializes the tenant SYS before any tenants and accepting any requests
// during the system is booting.
func InitSysTenant(ctx context.Context) error {
	var err error
	var exists bool
	pu := config.GetParameterUnit(ctx)

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
	ctx = context.WithValue(ctx, defines.UserIDKey{}, uint32(rootID))
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, uint32(moAdminRoleID))

	mp, err := mpool.NewMPool("init_system_tenant", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)
	bh := NewBackgroundHandler(ctx, mp, pu)
	defer bh.Close()

	//USE the mo_catalog
	err = bh.Exec(ctx, "use mo_catalog;")
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	exists, err = checkSysExistsOrNot(ctx, bh, pu)
	if err != nil {
		goto handleFailed
	}

	if !exists {
		err = createTablesInMoCatalog(ctx, bh, tenant, pu)
		if err != nil {
			goto handleFailed
		}

		err = createTablesInInformationSchema(ctx, bh, tenant, pu)
		if err != nil {
			goto handleFailed
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return err
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// createTablesInMoCatalog creates catalog tables in the database mo_catalog.
func createTablesInMoCatalog(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, pu *config.ParameterUnit) error {
	var err error
	var initMoAccount string
	var initDataSqls []string
	if !tenant.IsSysTenant() {
		return moerr.NewInternalError("only sys tenant can execute the function")
	}

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//create tables for the tenant
	for _, sql := range createSqls {
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant
	//step 1: add new tenant entry to the mo_account
	initMoAccount = fmt.Sprintf(initMoAccountFormat, sysAccountID, sysAccountName, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), sysAccountComments)
	addSqlIntoSet(initMoAccount)

	//step 2:add new role entries to the mo_role

	initMoRole1 := fmt.Sprintf(initMoRoleFormat, moAdminRoleID, moAdminRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user

	initMoUser1 := fmt.Sprintf(initMoUserFormat, rootID, rootHost, rootName, rootPassword, rootStatus, types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType, rootCreatorID, rootOwnerRoleID, rootDefaultRoleID)
	initMoUser2 := fmt.Sprintf(initMoUserFormat, dumpID, dumpHost, dumpName, dumpPassword, dumpStatus, types.CurrentTimestamp().String2(time.UTC, 0), dumpExpiredTime, dumpLoginType, dumpCreatorID, dumpOwnerRoleID, dumpDefaultRoleID)
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

	//fill the mo_account, mo_role, mo_user, mo_role_privs, mo_user_grant
	for _, sql := range initDataSqls {
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}
	}

handleFailed:
	return err
}

// createTablesInInformationSchema creates the database information_schema and the views or tables.
func createTablesInInformationSchema(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, pu *config.ParameterUnit) error {
	err := bh.Exec(ctx, "create database if not exists information_schema;")
	if err != nil {
		return err
	}
	return err
}

func checkTenantExistsOrNot(ctx context.Context, bh BackgroundExec, pu *config.ParameterUnit, userName string) (bool, error) {
	var sqlForCheckTenant string

	var erArray []ExecResult
	var err error
	sqlForCheckTenant = getSqlForCheckTenant(userName)
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sqlForCheckTenant)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

// InitGeneralTenant initializes the application level tenant
func InitGeneralTenant(ctx context.Context, ses *Session, ca *tree.CreateAccount) error {
	var err error
	var exists bool
	var newTenant *TenantInfo
	tenant := ses.GetTenantInfo()
	pu := config.GetParameterUnit(ctx)

	if !(tenant.IsSysTenant() && tenant.IsMoAdminRole()) {
		return moerr.NewInternalError("tenant %s user %s role %s do not have the privilege to create the new account", tenant.GetTenant(), tenant.GetUser(), tenant.GetDefaultRole())
	}

	//normalize the name
	ca.Name, err = normalizeName(ca.Name)
	if err != nil {
		return err
	}

	ca.AuthOption.AdminName, err = normalizeName(ca.AuthOption.AdminName)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(tenant.GetTenantID()))
	ctx = context.WithValue(ctx, defines.UserIDKey{}, uint32(tenant.GetUserID()))
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, uint32(tenant.GetDefaultRoleID()))

	mp, err := mpool.NewMPool("init_general_tenant", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)

	bh := NewBackgroundHandler(ctx, mp, pu)
	defer bh.Close()

	//USE the mo_catalog
	err = bh.Exec(ctx, "use mo_catalog;")
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	exists, err = checkTenantExistsOrNot(ctx, bh, pu, ca.Name)
	if err != nil {
		goto handleFailed
	}

	if exists {
		if !ca.IfNotExists { //do nothing
			err = moerr.NewInternalError("the tenant %s exists", ca.Name)
			goto handleFailed
		}
	} else {
		newTenant, err = createTablesInMoCatalogOfGeneralTenant(ctx, bh, tenant, pu, ca)
		if err != nil {
			goto handleFailed
		}

		err = createTablesInInformationSchemaOfGeneralTenant(ctx, bh, tenant, pu, newTenant)
		if err != nil {
			goto handleFailed
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return err
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// createTablesInMoCatalogOfGeneralTenant creates catalog tables in the database mo_catalog.
func createTablesInMoCatalogOfGeneralTenant(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, pu *config.ParameterUnit, ca *tree.CreateAccount) (*TenantInfo, error) {
	var err error
	var initMoAccount string

	var erArray []ExecResult
	var newTenantID int64
	var newUserId int64
	var comment = ""
	var initDataSqls []string
	var initMoRole1 string
	var initMoRole2 string
	var initMoUser1 string
	var initMoUserGrant1 string
	var initMoUserGrant2 string
	var newTenant *TenantInfo
	var name, password, status string
	var newTenantCtx context.Context

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	if nameIsInvalid(ca.Name) {
		err = moerr.NewInternalError("the account name is invalid")
		goto handleFailed
	}

	if nameIsInvalid(ca.AuthOption.AdminName) {
		err = moerr.NewInternalError("the admin name is invalid")
		goto handleFailed
	}

	//!!!NOTE : Insert into mo_account with original context.
	// Other operations with a new context with new tenant info
	//step 1: add new tenant entry to the mo_account
	if ca.Comment.Exist {
		comment = ca.Comment.Comment
	}

	initMoAccount = fmt.Sprintf(initMoAccountWithoutIDFormat, ca.Name, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), comment)
	//execute the insert
	err = bh.Exec(ctx, initMoAccount)
	if err != nil {
		goto handleFailed
	}

	//query the tenant id
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, getSqlForCheckTenant(ca.Name))
	if err != nil {
		goto handleFailed
	}

	erArray, err = getResultSet(bh)
	if err != nil {
		goto handleFailed
	}

	if execResultArrayHasData(erArray) {
		newTenantID, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			goto handleFailed
		}
	} else {
		err = moerr.NewInternalError("get the id of tenant %s failed", ca.Name)
		goto handleFailed
	}

	newTenant = &TenantInfo{
		Tenant:        ca.Name,
		User:          ca.AuthOption.AdminName,
		DefaultRole:   publicRoleName,
		TenantID:      uint32(newTenantID),
		UserID:        uint32(newUserId),
		DefaultRoleID: publicRoleID,
	}

	//with new tenant
	newTenantCtx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(newTenantID))
	newUserId = dumpID + 1
	newTenantCtx = context.WithValue(newTenantCtx, defines.UserIDKey{}, uint32(newUserId))
	newTenantCtx = context.WithValue(newTenantCtx, defines.RoleIDKey{}, uint32(publicRoleID))

	//create tables for the tenant
	for _, sql := range createSqls {
		//only the SYS tenant has the table mo_account
		if strings.HasPrefix(sql, "create table mo_account") {
			continue
		}
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			goto handleFailed
		}
	}

	//initialize the default data of tables for the tenant

	//step 2:add new role entries to the mo_role
	initMoRole1 = fmt.Sprintf(initMoRoleFormat, accountAdminRoleID, accountAdminRoleName, tenant.GetUserID(), tenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 = fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, tenant.GetUserID(), tenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user
	if ca.AuthOption.IdentifiedType.Typ != tree.AccountIdentifiedByPassword {
		err = moerr.NewInternalError("only support password verification now")
		goto handleFailed
	}
	name = ca.AuthOption.AdminName
	password = ca.AuthOption.IdentifiedType.Str
	if len(password) == 0 {
		err = moerr.NewInternalError("password is empty string")
		goto handleFailed
	}
	status = rootStatus
	//TODO: fix the status of user or account
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = "suspend"
		}
	}
	//the first user id in the general tenant
	initMoUser1 = fmt.Sprintf(initMoUserFormat, newUserId, rootHost, name, password, status,
		types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
		tenant.GetUserID(), tenant.GetDefaultRoleID(), accountAdminRoleID)
	addSqlIntoSet(initMoUser1)

	//step4: add new entries to the mo_role_privs
	//accountadmin role
	for _, t := range entriesOfAccountAdminForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			accountAdminRoleID, accountAdminRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			tenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
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
			tenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//step5: add new entries to the mo_user_grant
	initMoUserGrant1 = fmt.Sprintf(initMoUserGrantFormat, accountAdminRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant1)
	initMoUserGrant2 = fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant2)

	//fill the mo_role, mo_user, mo_role_privs, mo_user_grant, mo_role_grant
	for _, sql := range initDataSqls {
		bh.ClearExecResultSet()
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			goto handleFailed
		}
	}

handleFailed:
	return newTenant, err
}

// createTablesInInformationSchemaOfGeneralTenant creates the database information_schema and the views or tables.
func createTablesInInformationSchemaOfGeneralTenant(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, pu *config.ParameterUnit, newTenant *TenantInfo) error {
	//with new tenant
	//TODO: when we have the auto_increment column, we need new strategy.
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(newTenant.GetTenantID()))
	ctx = context.WithValue(ctx, defines.UserIDKey{}, uint32(newTenant.GetUserID()))
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, uint32(newTenant.GetDefaultRoleID()))

	var err error
	sqls := make([]string, 0, len(sysview.InitInformationSchemaSysTables)+len(sysview.InitMysqlSysTables)+4)

	sqls = append(sqls, "create database information_schema;")
	sqls = append(sqls, "use information_schema;")
	sqls = append(sqls, sysview.InitInformationSchemaSysTables...)
	sqls = append(sqls, "create database mysql;")
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

func checkUserExistsOrNot(ctx context.Context, pu *config.ParameterUnit, tenantName string) (bool, error) {
	mp, err := mpool.NewMPool("check_user_exists", 0, mpool.NoFixed)
	if err != nil {
		return false, err
	}
	defer mpool.DeleteMPool(mp)

	sqlForCheckUser := getSqlForPasswordOfUser(tenantName)
	erArray, err := executeSQLInBackgroundSession(ctx, mp, pu, sqlForCheckUser)
	if err != nil {
		return false, err
	}

	if !execResultArrayHasData(erArray) {
		return false, nil
	}

	return true, nil
}

// InitUser creates new user for the tenant
func InitUser(ctx context.Context, tenant *TenantInfo, cu *tree.CreateUser) error {
	var err error
	var exists int
	var erArray []ExecResult
	var newUserId int64
	var host string
	var newRoleId int64
	var status string

	err = normalizeNamesOfUsers(cu.Users)
	if err != nil {
		return err
	}

	if cu.Role != nil {
		err = normalizeNameOfRole(cu.Role)
		if err != nil {
			return err
		}
	}

	pu := config.GetParameterUnit(ctx)
	mp, err := mpool.NewMPool("init_user", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)

	bh := NewBackgroundHandler(ctx, mp, pu)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//TODO: get role and the id of role
	newRoleId = publicRoleID
	if cu.Role != nil {
		sqlForRoleIdOfRole := getSqlForRoleIdOfRole(cu.Role.UserName)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sqlForRoleIdOfRole)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}
		if !execResultArrayHasData(erArray) {
			err = moerr.NewInternalError("there is no role %s", cu.Role.UserName)
			goto handleFailed
		}
		newRoleId, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			goto handleFailed
		}

		from := &verifiedRole{
			typ:  roleType,
			name: cu.Role.UserName,
		}

		for _, user := range cu.Users {
			to := &verifiedRole{
				typ:  userType,
				name: user.Username,
			}
			err = verifySpecialRolesInGrant(tenant, from, to)
			if err != nil {
				goto handleFailed
			}
		}
	}

	//TODO: get password_option or lock_option. there is no field in mo_user to store it.
	status = userStatusUnlock
	if cu.MiscOpt != nil {
		if _, ok := cu.MiscOpt.(*tree.UserMiscOptionAccountLock); ok {
			status = userStatusLock
		}
	}

	for _, user := range cu.Users {
		//dedup with user
		sql := getSqlForPasswordOfUser(user.Username)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}
		exists = 0
		if execResultArrayHasData(erArray) {
			exists = 1
		}

		//dedup with the role
		if exists == 0 {
			sqlForRoleIdOfRole := getSqlForRoleIdOfRole(user.Username)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForRoleIdOfRole)
			if err != nil {
				goto handleFailed
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				goto handleFailed
			}
			if execResultArrayHasData(erArray) {
				exists = 2
			}
		}

		if exists != 0 {
			if cu.IfNotExists { //do nothing
				continue
			}
			if exists == 1 {
				err = moerr.NewInternalError("the user %s exists", user.Username)
			} else if exists == 2 {
				err = moerr.NewInternalError("there is a role with the same name as the user")
			}

			goto handleFailed
		}

		if user.AuthOption == nil {
			err = moerr.NewInternalError("the user %s misses the auth_option", user.Username)
			goto handleFailed
		}

		if user.AuthOption.Typ != tree.AccountIdentifiedByPassword {
			err = moerr.NewInternalError("only support password verification now")
			goto handleFailed
		}

		password := user.AuthOption.Str
		if len(password) == 0 {
			err = moerr.NewInternalError("password is empty string")
			goto handleFailed
		}

		//TODO: get comment or attribute. there is no field in mo_user to store it.
		host = user.Hostname
		if len(user.Hostname) == 0 || user.Hostname == "%" {
			host = rootHost
		}
		initMoUser1 := fmt.Sprintf(initMoUserWithoutIDFormat, host, user.Username, password, status,
			types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
			tenant.GetUserID(), tenant.GetDefaultRoleID(), newRoleId)

		bh.ClearExecResultSet()
		err = bh.Exec(ctx, initMoUser1)
		if err != nil {
			goto handleFailed
		}

		//query the id
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, getSqlForPasswordOfUser(user.Username))
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(bh)
		if err != nil {
			goto handleFailed
		}

		if !execResultArrayHasData(erArray) {
			err = moerr.NewInternalError("get the id of user %s failed", user.Username)
			goto handleFailed
		}
		newUserId, err = erArray[0].GetInt64(0, 0)
		if err != nil {
			goto handleFailed
		}

		initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, newRoleId, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
		err = bh.Exec(ctx, initMoUserGrant1)
		if err != nil {
			goto handleFailed
		}

		//if it is not public role, just insert the record for public
		if newRoleId != publicRoleID {
			initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
			err = bh.Exec(ctx, initMoUserGrant2)
			if err != nil {
				goto handleFailed
			}
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// InitRole creates the new role
func InitRole(ctx context.Context, tenant *TenantInfo, cr *tree.CreateRole) error {
	var err error
	var exists int
	var erArray []ExecResult
	err = normalizeNamesOfRoles(cr.Roles)
	if err != nil {
		return err
	}
	pu := config.GetParameterUnit(ctx)

	mp, err := mpool.NewMPool("init_role", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	bh := NewBackgroundHandler(ctx, mp, pu)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	for _, r := range cr.Roles {
		exists = 0
		if isPredefinedRole(r.UserName) {
			exists = 3
		} else {
			//dedup with role
			sqlForRoleIdOfRole := getSqlForRoleIdOfRole(r.UserName)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForRoleIdOfRole)
			if err != nil {
				goto handleFailed
			}

			erArray, err = getResultSet(bh)
			if err != nil {
				goto handleFailed
			}
			if execResultArrayHasData(erArray) {
				exists = 1
			}

			//dedup with user
			if exists == 0 {
				sql := getSqlForPasswordOfUser(r.UserName)
				bh.ClearExecResultSet()
				err = bh.Exec(ctx, sql)
				if err != nil {
					goto handleFailed
				}

				erArray, err = getResultSet(bh)
				if err != nil {
					goto handleFailed
				}
				if execResultArrayHasData(erArray) {
					exists = 2
				}
			}
		}

		if exists != 0 {
			if cr.IfNotExists {
				continue
			}
			if exists == 1 {
				err = moerr.NewInternalError("the role %s exists", r.UserName)
			} else if exists == 2 {
				err = moerr.NewInternalError("there is a user with the same name as the role %s", r.UserName)
			} else if exists == 3 {
				err = moerr.NewInternalError("can not use the name %s. it is the name of the predefined role", r.UserName)
			}

			goto handleFailed
		}

		initMoRole := fmt.Sprintf(initMoRoleWithoutIDFormat, r.UserName, tenant.GetUserID(), tenant.GetDefaultRoleID(),
			types.CurrentTimestamp().String2(time.UTC, 0), "")
		err = bh.Exec(ctx, initMoRole)
		if err != nil {
			goto handleFailed
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}
