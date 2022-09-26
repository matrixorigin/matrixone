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
	"math/rand"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
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

func (ti *TenantInfo) GetDefaultRoleID() uint32 {
	return ti.DefaultRoleID
}

func (ti *TenantInfo) SetDefaultRoleID(id uint32) {
	ti.DefaultRoleID = id
}

func (ti *TenantInfo) IsSysTenant() bool {
	return ti.GetTenant() == GetDefaultTenant()
}

func (ti *TenantInfo) IsDefaultRole() bool {
	return ti.GetDefaultRole() == GetDefaultRole()
}

func (ti *TenantInfo) IsMoAdminRole() bool {
	return ti.GetDefaultRole() == moAdminRoleName
}

func (ti *TenantInfo) IsAdminRole() bool {
	return ti.GetDefaultRoleID() == moAdminRoleID || ti.GetDefaultRoleID() == accountAdminRoleID
}

func GetDefaultTenant() string {
	return sysAccountName
}

func GetDefaultRole() string {
	return moAdminRoleName
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
				Tenant:      strings.ToLower(tenant),
				User:        strings.ToLower(user),
				DefaultRole: GetDefaultRole(),
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
				Tenant:      strings.ToLower(tenant),
				User:        strings.ToLower(user),
				DefaultRole: strings.ToLower(role),
			}, nil
		}
	}
}

const (
	//	createMoUserIndex      = 0
	createMoAccountIndex = 1
	//createMoRoleIndex      = 2
	//createMoUserGrantIndex = 3
	//createMoRoleGrantIndex = 4
	//createMoRolePrivIndex  = 5
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
	userStatusLock   = "lock"
	userStatusUnlock = "unlock"

	rootID            = 0
	rootHost          = "NULL"
	rootName          = "root"
	rootPassword      = "111"
	rootStatus        = userStatusUnlock
	rootExpiredTime   = "NULL"
	rootLoginType     = "PASSWORD"
	rootCreatorID     = rootID
	rootOwnerRoleID   = moAdminRoleID
	rootDefaultRoleID = moAdminRoleID

	dumpID            = 1
	dumpHost          = "NULL"
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
	PrivilegeTypeUpdate
	PrivilegeTypeTruncate
	PrivilegeTypeDelete
	PrivilegeTypeReference
	PrivilegeTypeIndex //include create/alter/drop index
	PrivilegeTypeTableAll
	PrivilegeTypeTableOwnership
	PrivilegeTypeExecute
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
	case PrivilegeTypeTableAll:
		return "table all"
	case PrivilegeTypeTableOwnership:
		return "table ownership"
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
		"mo_user":       0,
		"mo_account":    0,
		"mo_role":       0,
		"mo_user_grant": 0,
		"mo_role_grant": 0,
		"mo_role_privs": 0,
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
		`create table mo_role_privs(
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
	}

	initMoAccountFormat = `insert into mo_catalog.mo_account(
				account_id,
				account_name,
				status,
				created_time,
				comments) values (%d,"%s","%s","%s","%s");`
	initMoRoleFormat = `insert into mo_catalog.mo_role(
				role_id,
				role_name,
				creator,
				owner,
				created_time,
				comments
			) values (%d,"%s",%d,%d,"%s","%s");`
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
    		) values(%d,%s,"%s","%s","%s","%s",%s,"%s",%d,%d,%d);`
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

var (
	//privilege verification
	checkTenantFormat = `select account_id,account_name from mo_catalog.mo_account where account_name = "%s";`

	getPasswordOfUserFormat = `select user_id,authentication_string,default_role from mo_catalog.mo_user where user_name = "%s";`

	checkRoleExistsFormat = `select role_id from mo_catalog.mo_role where role_id = %d and role_name = "%s";`

	roleIdOfRoleFormat = `select role_id from mo_catalog.mo_role where role_name = "%s";`

	//operations on the mo_user_grant
	getRoleOfUserFormat = `select r.role_id from  mo_catalog.mo_role r, mo_catalog.mo_user_grant ug where ug.role_id = r.role_id and ug.user_id = %d and r.role_name = "%s";`

	getRoleIdOfUserIdFormat = `select role_id,with_grant_option from mo_catalog.mo_user_grant where user_id = %d;`

	checkUserGrantFormat = `select role_id,user_id,with_grant_option from mo_catalog.mo_user_grant where role_id = %d and user_id = %d;`

	updateUserGrantFormat = `update mo_catalog.mo_user_grant set granted_time = "%s", with_grant_option = %v where role_id = %d and user_id = %d;`

	insertUserGrantFormat = `insert into mo_catalog.mo_user_grant(role_id,user_id,granted_time,with_grant_option) values (%d,%d,"%s",%v);`

	deleteUserGrantFormat = `delete from mo_catalog.mo_user_grant where role_id = %d and user_id = %d;`

	//operations on the mo_role_grant
	checkRoleGrantFormat = `select granted_id,grantee_id,with_grant_option from mo_catalog.mo_role_grant where granted_id = %d and grantee_id = %d;`

	updateRoleGrantFormat = `update mo_catalog.mo_role_grant set operation_role_id = %d, operation_user_id = %d, granted_time = "%s", with_grant_option = %v where granted_id = %d and grantee_id = %d;`

	insertRoleGrantFormat = `insert mo_catalog.mo_role_grant(granted_id,grantee_id,operation_role_id,operation_user_id,granted_time,with_grant_option) values (%d,%d,%d,%d,"%s",%v);`

	deleteRoleGrantFormat = `delete from mo_catalog.mo_role_grant where granted_id = %d and grantee_id = %d;`

	getAllStuffRoleGrantFormat = `select granted_id,grantee_id,with_grant_option from mo_catalog.mo_role_grant;`

	getInheritedRoleIdOfRoleIdFormat = `select granted_id,with_grant_option from mo_catalog.mo_role_grant where grantee_id = %d;`

	checkRoleHasPrivilegeFormat = `select role_id,with_grant_option from mo_catalog.mo_role_privs where role_id = %d and obj_type = "%s" and obj_id = %d and privilege_id = %d;`

	updateRolePrivsFormat = `update mo_catalog.mo_role_privs set operation_user_id = %d, granted_time = "%s", with_grant_option = %v where role_id = %d and obj_type = "%s" and obj_id = %d and privilege_id = %d;`

	insertRolePrivsFormat = `insert into mo_catalog.mo_role_privs(role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,granted_time,with_grant_option) 
								values (%d,"%s","%s",%d,%d,"%s","%s",%d,"%s",%v);`

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

func getSqlForRoleIdOfRole(roleName string) string {
	return fmt.Sprintf(roleIdOfRoleFormat, roleName)
}

func getSqlForRoleOfUser(userID int, roleName string) string {
	return fmt.Sprintf(getRoleOfUserFormat, userID, roleName)
}

func getSqlForRoleIdOfUserId(userId int) string {
	return fmt.Sprintf(getRoleIdOfUserIdFormat, userId)
}

func getSqlForCheckUserGrant(roleId, userId int64) string {
	return fmt.Sprintf(checkUserGrantFormat, roleId, userId)
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

func getSqlForUpdateRolePrivs(userId int64, timestamp string, withGrantOption bool, roleId int64, objType objectType, objId, privilegeId int64) string {
	return fmt.Sprintf(updateRolePrivsFormat, userId, timestamp, withGrantOption, roleId, objType, objId, privilegeId)
}

func getSqlForInsertRolePrivs(roleId int64, roleName, objType string, objId, privilegeId int64, privilegeName, privilegeLevel string, operationUserId int64, grantedTime string, withGrantOption bool) string {
	return fmt.Sprintf(insertRolePrivsFormat, roleId, roleName, objType, objId, privilegeId, privilegeName, privilegeLevel, operationUserId, grantedTime, withGrantOption)
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
}

func (p *privilege) objectType() objectType {
	return p.objType
}

func (p *privilege) privilegeKind() privilegeKind {
	return p.kind
}

// privilegeEntry denotes the entry of the privilege that appears in the table mo_role_privs
type privilegeEntry struct {
	privilegeId     PrivilegeType
	privilegeLevel  privilegeLevelType
	objType         objectType
	objId           int
	withGrantOption bool
	//for object type table
	databaseName string
	tableName    string
}

var (
	//initial privilege entries
	privilegeEntriesMap = map[PrivilegeType]privilegeEntry{
		PrivilegeTypeCreateAccount:     {PrivilegeTypeCreateAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", ""},
		PrivilegeTypeDropAccount:       {PrivilegeTypeDropAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", ""},
		PrivilegeTypeAlterAccount:      {PrivilegeTypeAlterAccount, privilegeLevelStar, objectTypeAccount, objectIDAll, false, "", ""},
		PrivilegeTypeCreateUser:        {PrivilegeTypeCreateUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeDropUser:          {PrivilegeTypeDropUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeAlterUser:         {PrivilegeTypeAlterUser, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeCreateRole:        {PrivilegeTypeCreateRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeDropRole:          {PrivilegeTypeDropRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeAlterRole:         {PrivilegeTypeAlterRole, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeCreateDatabase:    {PrivilegeTypeCreateDatabase, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeDropDatabase:      {PrivilegeTypeDropDatabase, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeShowDatabases:     {PrivilegeTypeShowDatabases, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeConnect:           {PrivilegeTypeConnect, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeManageGrants:      {PrivilegeTypeManageGrants, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeAccountAll:        {PrivilegeTypeAccountAll, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeAccountOwnership:  {PrivilegeTypeAccountOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeUserOwnership:     {PrivilegeTypeUserOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeRoleOwnership:     {PrivilegeTypeRoleOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeShowTables:        {PrivilegeTypeShowTables, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeCreateObject:      {PrivilegeTypeCreateObject, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeCreateTable:       {PrivilegeTypeCreateTable, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeCreateView:        {PrivilegeTypeCreateView, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeDropObject:        {PrivilegeTypeDropObject, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeDropTable:         {PrivilegeTypeDropTable, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeDropView:          {PrivilegeTypeDropView, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeAlterObject:       {PrivilegeTypeAlterObject, privilegeLevelDatabase, objectTypeDatabase, objectIDAll, true, "", ""},
		PrivilegeTypeDatabaseAll:       {PrivilegeTypeDatabaseAll, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeDatabaseOwnership: {PrivilegeTypeDatabaseOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeSelect:            {PrivilegeTypeSelect, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeInsert:            {PrivilegeTypeInsert, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeUpdate:            {PrivilegeTypeUpdate, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeTruncate:          {PrivilegeTypeTruncate, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeDelete:            {PrivilegeTypeDelete, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeReference:         {PrivilegeTypeReference, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeIndex:             {PrivilegeTypeIndex, privilegeLevelTable, objectTypeTable, objectIDAll, true, "", ""},
		PrivilegeTypeTableAll:          {PrivilegeTypeTableAll, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeTableOwnership:    {PrivilegeTypeTableOwnership, privilegeLevelStar, objectTypeAccount, objectIDAll, true, "", ""},
		PrivilegeTypeExecute:           {PrivilegeTypeExecute, privilegeLevelRoutine, objectTypeFunction, objectIDAll, true, "", ""},
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
		PrivilegeTypeAlterRole,
		PrivilegeTypeCreateDatabase,
		PrivilegeTypeDropDatabase,
		PrivilegeTypeShowDatabases,
		PrivilegeTypeConnect,
		PrivilegeTypeManageGrants,
		PrivilegeTypeAccountAll,
		PrivilegeTypeAccountOwnership,
		PrivilegeTypeUserOwnership,
		PrivilegeTypeRoleOwnership,
		PrivilegeTypeShowTables,
		PrivilegeTypeCreateObject,
		PrivilegeTypeCreateTable,
		PrivilegeTypeCreateView,
		PrivilegeTypeDropObject,
		PrivilegeTypeDropTable,
		PrivilegeTypeDropView,
		PrivilegeTypeAlterObject,
		PrivilegeTypeDatabaseAll,
		PrivilegeTypeDatabaseOwnership,
		PrivilegeTypeSelect,
		PrivilegeTypeInsert,
		PrivilegeTypeUpdate,
		PrivilegeTypeTruncate,
		PrivilegeTypeDelete,
		PrivilegeTypeReference,
		PrivilegeTypeIndex,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
		PrivilegeTypeExecute,
	}

	//the initial entries of mo_role_privs for the role 'accountadmin'
	entriesOfAccountAdminForMoRolePrivsFor = []PrivilegeType{
		PrivilegeTypeCreateUser,
		PrivilegeTypeDropUser,
		PrivilegeTypeAlterUser,
		PrivilegeTypeCreateRole,
		PrivilegeTypeDropRole,
		PrivilegeTypeAlterRole,
		PrivilegeTypeCreateDatabase,
		PrivilegeTypeDropDatabase,
		PrivilegeTypeShowDatabases,
		PrivilegeTypeConnect,
		PrivilegeTypeManageGrants,
		PrivilegeTypeAccountAll,
		PrivilegeTypeAccountOwnership,
		PrivilegeTypeUserOwnership,
		PrivilegeTypeRoleOwnership,
		PrivilegeTypeShowTables,
		PrivilegeTypeCreateObject,
		PrivilegeTypeCreateTable,
		PrivilegeTypeCreateView,
		PrivilegeTypeDropObject,
		PrivilegeTypeDropTable,
		PrivilegeTypeDropView,
		PrivilegeTypeAlterObject,
		PrivilegeTypeDatabaseAll,
		PrivilegeTypeDatabaseOwnership,
		PrivilegeTypeSelect,
		PrivilegeTypeInsert,
		PrivilegeTypeUpdate,
		PrivilegeTypeTruncate,
		PrivilegeTypeDelete,
		PrivilegeTypeReference,
		PrivilegeTypeIndex,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
		PrivilegeTypeExecute,
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

type verifiedRole struct {
	typ  verifiedRoleType
	name string
	id   int64
}

// verifyRoleFunc gets result set from mo_role_grant or mo_user_grant
func verifyRoleFunc(ctx context.Context, bh BackgroundExec, sql, name string, typ verifiedRoleType) (*verifiedRole, error) {
	var err error
	var rsset []ExecResult
	var roleId int64
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	results := bh.GetExecResultSet()
	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return nil, err
	}

	if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
		roleId, err = rsset[0].GetInt64(0, 0)
		if err != nil {
			return nil, err
		}
		return &verifiedRole{typ, name, roleId}, nil
	}
	return nil, nil
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

// doGrantPrivilege accomplishes the GrantPrivilege statement
func doGrantPrivilege(ctx context.Context, ses *Session, gp *tree.GrantPrivilege) error {
	pu := ses.Pu
	account := ses.GetTenantInfo()
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var err error
	var rsset []ExecResult
	var roleId int64
	var privType PrivilegeType
	var objType objectType
	var privLevel privilegeLevelType
	var objId int64
	var id int64

	getDatabaseOrTableId := func(ctx context.Context, isDb bool, dbName, tableName string) (int64, error) {
		var sql string
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

		results := bh.GetExecResultSet()
		rsset, err = convertIntoResultSet(results)
		if err != nil {
			return 0, err
		}

		if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
			id, err = rsset[0].GetInt64(0, 0)
			if err != nil {
				return 0, err
			}
			return id, nil
		}
		if isDb {
			return 0, moerr.NewInternalError("there is no database %s", dbName)
		} else {
			return 0, moerr.NewInternalError("there is no table %s in database %s", tableName, dbName)
		}
	}

	//Get primary keys
	//step 1: get role_id
	verifiedRoles := make([]*verifiedRole, len(gp.Roles))

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	for i, role := range gp.Roles {
		sql := getSqlForRoleIdOfRole(role.UserName)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		results := bh.GetExecResultSet()
		rsset, err = convertIntoResultSet(results)
		if err != nil {
			goto handleFailed
		}

		if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
			for j := uint64(0); j < rsset[0].GetRowCount(); j++ {
				roleId, err = rsset[0].GetInt64(j, 0)
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

	//step 2: get obj_type, privilege_level
	//step 3: get obj_id
	switch gp.ObjType {
	case tree.OBJECT_TYPE_TABLE:
		objType = objectTypeTable
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId, err = getDatabaseOrTableId(ctx, true, ses.GetDatabaseName(), "")
			if err != nil {
				goto handleFailed
			}
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			privLevel = privilegeLevelStarStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
			privLevel = privilegeLevelDatabaseStar
			objId, err = getDatabaseOrTableId(ctx, true, gp.Level.DbName, "")
			if err != nil {
				goto handleFailed
			}
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
			privLevel = privilegeLevelDatabaseTable
			objId, err = getDatabaseOrTableId(ctx, false, gp.Level.DbName, gp.Level.TabName)
			if err != nil {
				goto handleFailed
			}
		case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
			privLevel = privilegeLevelTable
			objId, err = getDatabaseOrTableId(ctx, false, ses.GetDatabaseName(), gp.Level.TabName)
			if err != nil {
				goto handleFailed
			}
		default:
			err = moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
			goto handleFailed
		}
	case tree.OBJECT_TYPE_DATABASE:
		objType = objectTypeDatabase
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
			privLevel = privilegeLevelStarStar
			objId = objectIDAll
		case tree.PRIVILEGE_LEVEL_TYPE_DATABASE:
			privLevel = privilegeLevelDatabase
			objId, err = getDatabaseOrTableId(ctx, true, gp.Level.DbName, "")
			if err != nil {
				goto handleFailed
			}
		default:
			err = moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType.ToString(), gp.Level.Level)
			goto handleFailed
		}
	case tree.OBJECT_TYPE_ACCOUNT:
		objType = objectTypeAccount
		switch gp.Level.Level {
		case tree.PRIVILEGE_LEVEL_TYPE_STAR:
			privLevel = privilegeLevelStar
			objId = objectIDAll
		default:
			err = moerr.NewInternalError("in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
			goto handleFailed
		}
	default:
		err = moerr.NewInternalError("object type %v is unsupported", gp.ObjType)
		goto handleFailed
	}

	//step 4: get privilege_id
	//step 5: check exists
	//step 6: update or insert
	for _, priv := range gp.Privileges {
		privType = convertAstPrivilegeTypeToPrivilegeType(priv.Type)
		//check the match between the privilegeScope and the objectType
		switch privType.Scope() {
		case PrivilegeScopeSys, PrivilegeScopeAccount, PrivilegeScopeUser, PrivilegeScopeRole:
			if objType != objectTypeAccount {
				err = moerr.NewInternalError("the privilege %s can not be granted to the object type account", priv.Type.ToString())
				goto handleFailed
			}
		case PrivilegeScopeDatabase:
			if objType != objectTypeDatabase {
				err = moerr.NewInternalError("the privilege %s can not be granted to the object type database", priv.Type.ToString())
				goto handleFailed
			}
		case PrivilegeScopeTable:
			if objType != objectTypeTable {
				err = moerr.NewInternalError("the privilege %s can not be granted to the object type table", priv.Type.ToString())
				goto handleFailed
			}
		case PrivilegeScopeRoutine:
			if objType != objectTypeFunction {
				err = moerr.NewInternalError("the privilege %s can not be granted to the object type function", priv.Type.ToString())
				goto handleFailed
			}
		}
		for _, role := range verifiedRoles {
			sql := getSqlForCheckRoleHasPrivilege(role.id, objType, objId, int64(privType))
			//check exists
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				goto handleFailed
			}

			results := bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(results)
			if err != nil {
				goto handleFailed
			}

			//choice 1 : update the record
			//choice 2 : inset new record
			choice := 1
			if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
				for j := uint64(0); j < rsset[0].GetRowCount(); j++ {
					_, err = rsset[0].GetInt64(j, 0)
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
	pu := ses.Pu
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var err error

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
	for i, role := range rr.Roles {
		sql := getSqlForRoleIdOfRole(role.UserName)
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			goto handleFailed
		}
		verifiedFromRoles[i] = vr
		if vr == nil {
			if !rr.IfExists { //when the "IF EXISTS" is set, just skip the check
				err = moerr.NewInternalError("there is no role %s", role.UserName)
				goto handleFailed
			}
		}
	}

	//step2 : check Users are real Users or Roles,  exists or not
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
			if vr == nil {
				err = moerr.NewInternalError("there is no role or user %s", user.Username)
				goto handleFailed
			}
			verifiedToRoles[i] = vr
		}
	}

	//step3 : process Revoke role from role
	//step4 : process Revoke role from user
	for _, from := range verifiedFromRoles {
		if from == nil { //Under "IF EXISTS"
			continue
		}
		for _, to := range verifiedToRoles {
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

// doGrantRole accomplishes the GrantRole statement
func doGrantRole(ctx context.Context, ses *Session, gr *tree.GrantRole) error {
	pu := ses.Pu
	account := ses.GetTenantInfo()
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var rsset []ExecResult
	var err error
	var withGrantOption int64

	//step1 : check Roles exists or not
	var vr *verifiedRole
	var needLoadMoRoleGrant bool
	var grantedId, granteeId int64

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
			//check user
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

		results := bh.GetExecResultSet()
		rsset, err = convertIntoResultSet(results)
		if err != nil {
			goto handleFailed
		}

		if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
			for j := uint64(0); j < rsset[0].GetRowCount(); j++ {
				//column grantedId
				grantedId, err = rsset[0].GetInt64(j, 0)
				if err != nil {
					goto handleFailed
				}

				//column granteeId
				granteeId, err = rsset[0].GetInt64(j, 1)
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

			results := bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(results)
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
			if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
				for j := uint64(0); j < rsset[0].GetRowCount(); j++ {
					withGrantOption, err = rsset[0].GetInt64(j, 2)
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
	switch st := stmt.(type) {
	case *tree.CreateAccount:
		typs = append(typs, PrivilegeTypeCreateAccount)
	case *tree.DropAccount:
		typs = append(typs, PrivilegeTypeDropAccount)
	case *tree.AlterAccount:
		typs = append(typs, PrivilegeTypeAlterAccount)
	case *tree.CreateUser:
		typs = append(typs, PrivilegeTypeCreateUser, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
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
	case *tree.ShowDatabases:
		typs = append(typs, PrivilegeTypeShowDatabases, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
	case *tree.Use:
		if st.IsUseRole() {
			objType = objectTypeNone
			kind = privilegeKindNone
		} else {
			typs = append(typs, PrivilegeTypeConnect, PrivilegeTypeAccountAll /*, PrivilegeTypeAccountOwnership*/)
		}
	case *tree.ShowTables, *tree.ShowCreateTable, *tree.ShowColumns, *tree.ShowCreateView, *tree.ShowCreateDatabase:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeShowTables, PrivilegeTypeDatabaseAll /*PrivilegeTypeDatabaseOwnership*/)
	case *tree.CreateTable:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeCreateTable, PrivilegeTypeCreateObject, PrivilegeTypeDatabaseAll /* PrivilegeTypeDatabaseOwnership*/)
	case *tree.CreateView:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeCreateView, PrivilegeTypeCreateObject, PrivilegeTypeDatabaseAll /* PrivilegeTypeDatabaseOwnership*/)
	case *tree.DropTable:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeDropTable, PrivilegeTypeDropObject, PrivilegeTypeDatabaseAll /*PrivilegeTypeDatabaseOwnership*/)
	case *tree.DropView:
		objType = objectTypeDatabase
		typs = append(typs, PrivilegeTypeDropView, PrivilegeTypeDropObject, PrivilegeTypeDatabaseAll /*PrivilegeTypeDatabaseOwnership*/)
	case *tree.Select:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeSelect, PrivilegeTypeTableAll /*PrivilegeTypeTableOwnership*/)
	case *tree.Insert, *tree.Load, *tree.Import:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeInsert, PrivilegeTypeTableAll /*PrivilegeTypeTableOwnership*/)
	case *tree.Update:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeUpdate, PrivilegeTypeTableAll /*PrivilegeTypeTableOwnership*/)
	case *tree.Delete:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeDelete, PrivilegeTypeTableAll /*PrivilegeTypeTableOwnership*/)
	case *tree.CreateIndex, *tree.DropIndex, *tree.ShowIndex:
		objType = objectTypeTable
		typs = append(typs, PrivilegeTypeIndex)
	case *tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables, *tree.ShowStatus, *tree.ShowTarget:
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
	default:
		panic(fmt.Sprintf("does not have the privilege definition of the statement %s", stmt))
	}

	entries := make([]privilegeEntry, len(typs))
	for i, typ := range typs {
		entries[i] = privilegeEntriesMap[typ]
	}
	return &privilege{kind, objType, entries, special}
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
				appendPot(privilegeTips{
					t,
					node.ObjRef.GetSchemaName(),
					node.ObjRef.GetObjName(),
				})
			} else if node.NodeType == plan.Node_INSERT { //insert select
				appendPot(privilegeTips{
					PrivilegeTypeInsert,
					node.ObjRef.GetSchemaName(),
					node.ObjRef.GetObjName(),
				})
			} else if node.NodeType == plan.Node_DELETE {
				appendPot(privilegeTips{
					PrivilegeTypeDelete,
					node.ObjRef.GetSchemaName(),
					node.ObjRef.GetObjName(),
				})
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

	entries := make([]privilegeEntry, 0, len(arr))
	for _, tips := range arr {
		e := privilegeEntriesMap[tips.typ]
		e.databaseName = tips.databaseName
		e.tableName = tips.tableName
		entries = append(entries, e)

		dedup[pair{tips.databaseName, tips.tableName}] = 1
	}

	//predefined privilege : tableAll, ownership
	predefined := []PrivilegeType{PrivilegeTypeTableAll /*,PrivilegeTypeTableOwnership*/}
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

// determineRoleSetSatisfyPrivilegeSet decides the privileges of role set can satisfy the requirement of the privilege set.
// The algorithm 2.
func determineRoleSetSatisfyPrivilegeSet(ctx context.Context, bh BackgroundExec, roleIds []int64, priv *privilege) (bool, error) {
	var rsset []ExecResult
	var sql string
	var err error
	//there is no privilege needs, just approve
	if len(priv.entries) == 0 {
		return true, nil
	}
	for _, roleId := range roleIds {
		for _, entry := range priv.entries {
			if entry.privilegeId == PrivilegeTypeAccountOwnership || entry.privilegeId == PrivilegeTypeUserOwnership || entry.privilegeId == PrivilegeTypeTableOwnership {
				if roleId == moAdminRoleID || roleId == accountAdminRoleID {
					//in the version 0.6, only the moAdmin and accountAdmin have the owner right.
					return true, nil
				}
			}

			if entry.objType == objectTypeTable {
				if roleId == moAdminRoleID || roleId == accountAdminRoleID {
					//NOTE: admin role can operate the mo_catalog.*
					//TODO: how to fix general role access the mo_catalog.*
					if entry.databaseName == "mo_catalog" {
						return true, nil
					}
				}
			}

			sql, err = getSqlFromPrivilegeEntry(roleId, entry)
			if err != nil {
				return false, err
			}

			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				return false, err
			}
			results := bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(results)
			if err != nil {
				return false, err
			}

			if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
				return true, nil
			}
		}

	}
	return false, nil
}

// determinePrivilegesOfUserSatisfyPrivilegeSet decides the privileges of user can satisfy the requirement of the privilege set
// The algorithm 1.
func determinePrivilegesOfUserSatisfyPrivilegeSet(ctx context.Context, ses *Session, priv *privilege, stmt tree.Statement) (bool, error) {
	setR := &btree.Set[int64]{}
	tenant := ses.GetTenantInfo()
	pu := ses.Pu

	//step 1: The Set R1 {default role id}
	setR.Insert((int64)(tenant.GetDefaultRoleID()))
	//TODO: call the algorithm 2.
	//step 2: The Set R2 {the roleid granted to the userid}
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var rsset []ExecResult

	sqlForRoleIdOfUserId := getSqlForRoleIdOfUserId(int(tenant.GetUserID()))
	err := bh.Exec(ctx, sqlForRoleIdOfUserId)
	if err != nil {
		return false, err
	}

	results := bh.GetExecResultSet()
	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return false, err
	}

	if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
		for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
			roleId, err := rsset[0].GetInt64(i, 0)
			if err != nil {
				return false, err
			}
			setR.Insert(roleId)
		}
	}

	setVisited := &btree.Set[int64]{}
	setRList := make([]int64, 0, setR.Len())
	//init setVisited = setR
	setR.Scan(func(roleId int64) bool {
		setVisited.Insert(roleId)
		setRList = append(setRList, roleId)
		return true
	})
	//TODO: call the algorithm 2.
	//If the result of the algorithm 2 is true, Then return true;
	yes, err := determineRoleSetSatisfyPrivilegeSet(ctx, bh, setRList, priv)
	if err != nil {
		return false, err
	}
	if yes {
		return true, nil
	}
	/*
		step 3: !!!NOTE all roleid in setR has been processed by the algorithm 2.
		setVisited is the set of all roleid that has been processed.
		setVisited = setR;
		For {
			For roleA in setR {
				Find the peer roleB in the table mo_role_grant(granted_id,grantee_id) with grantee_id = roleA;
				If roleB is not in setVisited, Then add roleB into setR';
					add roleB into setVisited;
			}

			If setR' is empty, Then return false;
			//TODO: call the algorithm 2.
			If the result of the algorithm 2 is true, Then return true;
			setR = setR';
			setR' = {};
		}
	*/
	for {
		setRPlus := make([]int64, 0, len(setRList))
		quit := false
		select {
		case <-ctx.Done():
			quit = true
		default:
		}
		if quit {
			break
		}

		//get roleB of roleA
		for _, roleA := range setRList {
			sqlForInheritedRoleIdOfRoleId := getSqlForInheritedRoleIdOfRoleId(roleA)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForInheritedRoleIdOfRoleId)
			if err != nil {
				return false, moerr.NewInternalError("get inherited role id of the role id. error:%v", err)
			}

			results = bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(results)
			if err != nil {
				return false, err
			}

			if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
				for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
					roleB, err := rsset[0].GetInt64(i, 0)
					if err != nil {
						return false, err
					}

					if !setVisited.Contains(roleB) {
						setVisited.Insert(roleB)
						setRPlus = append(setRPlus, roleB)
					}
				}
			}
		}

		//no more roleB, it is done
		if len(setRPlus) == 0 {
			return false, nil
		}

		//TODO: call the algorithm 2.
		//If the result of the algorithm 2 is true, Then return true;
		yes, err := determineRoleSetSatisfyPrivilegeSet(ctx, bh, setRPlus, priv)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
		setRList = setRPlus
	}

	return false, nil
}

// determineRoleHasWithGrantOption decides all roleIds have the with_grant_option = true
func determineRoleHasWithGrantOption(ctx context.Context, ses *Session, roles []*tree.Role) (bool, error) {
	tenant := ses.GetTenantInfo()
	pu := ses.Pu

	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var rsset []ExecResult

	//step1 : get roleIds from name
	dedupNames := &btree.Set[string]{}
	for _, role := range roles {
		dedupNames.Insert(role.UserName)
	}

	dedup := &btree.Map[int64, int64]{}
	for _, name := range dedupNames.Keys() {
		sql := getSqlForRoleIdOfRole(name)
		bh.ClearExecResultSet()
		err := bh.Exec(ctx, sql)
		if err != nil {
			return false, err
		}

		results := bh.GetExecResultSet()
		rsset, err = convertIntoResultSet(results)
		if err != nil {
			return false, err
		}

		if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
			for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
				roleId, err := rsset[0].GetInt64(i, 0)
				if err != nil {
					return false, err
				}

				dedup.Set(roleId, 0)
			}
		}
	}

	//step2 : get all role id the user has.
	sqlForRoleIdOfUserId := getSqlForRoleIdOfUserId(int(tenant.GetUserID()))
	bh.ClearExecResultSet()
	err := bh.Exec(ctx, sqlForRoleIdOfUserId)
	if err != nil {
		return false, err
	}

	results := bh.GetExecResultSet()
	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return false, err
	}

	if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
		for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
			roleId, err := rsset[0].GetInt64(i, 0)
			if err != nil {
				return false, err
			}

			grantOpt, err := rsset[0].GetInt64(i, 1)
			if err != nil {
				return false, err
			}

			if grantOpt != 0 { //with_grant_option = true
				if _, exists := dedup.Get(roleId); exists {
					dedup.Set(roleId, grantOpt)
				}
			}
		}
	}

	all := true
	//if all roleId have the with_grant_option = true, it is done
	dedup.Scan(func(k, v int64) bool {
		if v == 0 {
			all = false
			return false
		}
		return true
	})

	if all {
		return all, nil
	}

	//step3 : check mo_role_grant
	sqlForInherited := getSqlForInheritedRoleIdOfRoleId(int64(tenant.GetDefaultRoleID()))
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sqlForInherited)
	if err != nil {
		return false, err
	}

	results = bh.GetExecResultSet()
	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return false, err
	}

	if len(rsset) != 0 && rsset[0].GetRowCount() != 0 {
		for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
			grantedId, err := rsset[0].GetInt64(i, 0)
			if err != nil {
				return false, err
			}

			grantOpt, err := rsset[0].GetInt64(i, 1)
			if err != nil {
				return false, err
			}

			if grantOpt != 0 { //with_grant_option = true
				if _, exists := dedup.Get(grantedId); exists {
					dedup.Set(grantedId, grantOpt)
				}
			}
		}
	}

	all = true
	//if all roleId have the with_grant_option = true, it is done
	dedup.Scan(func(k, v int64) bool {
		if v == 0 {
			all = false
			return false
		}
		return true
	})

	return all, nil
}

// authenticatePrivilegeOfStatementWithObjectTypeAccountAndDatabase decides the user has the privilege of executing the statement with object type account
func authenticatePrivilegeOfStatementWithObjectTypeAccountAndDatabase(ctx context.Context, ses *Session, stmt tree.Statement) (bool, error) {
	priv := ses.GetPrivilege()
	if priv.objectType() != objectTypeAccount && priv.objectType() != objectTypeDatabase { //do nothing
		return true, nil
	}
	ok, err := determinePrivilegesOfUserSatisfyPrivilegeSet(ctx, ses, priv, stmt)
	if err != nil {
		return false, err
	}

	//for GrantRole statement, check with_grant_option
	if !ok && priv.kind == privilegeKindInherit {
		grantRole := stmt.(*tree.GrantRole)
		yes, err := determineRoleHasWithGrantOption(ctx, ses, grantRole.Roles)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
	}
	return ok, nil
}

// authenticatePrivilegeOfStatementWithObjectTypeTable decides the user has the privilege of executing the statement with object type table
func authenticatePrivilegeOfStatementWithObjectTypeTable(ctx context.Context, ses *Session, stmt tree.Statement, p *plan2.Plan) (bool, error) {
	priv := determinePrivilegeSetOfStatement(stmt)
	if priv.objectType() == objectTypeTable {
		arr := extractPrivilegeTipsFromPlan(p)
		convertPrivilegeTipsToPrivilege(priv, arr)
		ok, err := determinePrivilegesOfUserSatisfyPrivilegeSet(ctx, ses, priv, stmt)
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
	privType := convertAstPrivilegeTypeToPrivilegeType(priv.Type)
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

// determinePrivilegeHasWithGrantOption decides all privileges have the with_grant_option = true
func determinePrivilegeHasWithGrantOption(ctx context.Context, ses *Session, gp *tree.GrantPrivilege) (bool, error) {
	pu := ses.Pu
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var rsset []ExecResult

	/*
		object_type and privilege_level
			Table :
				*.* : all tables of all databases
				db.*
				db.table
				table
			Database:
				*  : all databases
				*.* : all objects of all databases
				db;
			Function:
				*.*：all functions of all databases
				db.routine
			Account:
				*: only
	*/
	//Fields' source
	//role_id : tenant.defaultRoleId
	//obj_type : GrantPrivilege.ObjType
	//obj_id :
	//  object_type + privilege_level => obj_id
	//	Table : mo_database join mo_tables join mo_role_privs
	//		*.* : => 0
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = 0
			and rp.obj_type = "table"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="*.*"
			and rp.with_grant_option = true;
	*/
	//		db.* => 0
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = 0
			and rp.obj_type = "table"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="d.*"
			and d.datname = DB
			and rp.with_grant_option = true;
	*/
	//		db.table => tableId
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = t.rel_id
			and rp.obj_type = "table"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="d.t"
			and d.datname = DB
			and t.relname = X
			and rp.with_grant_option = true;
	*/
	//		table => tableId
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = t.rel_id
			and rp.obj_type = "table"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="t"
			and d.datname = CURRENT_DATABASE
			and t.relname = X
			and rp.with_grant_option = true;
	*/
	//	Database: mo_database join mo_role_privs
	//		*  : => 0
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = 0
			and rp.obj_type = "database"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="*"
			and rp.with_grant_option = true;
	*/
	//		*.* : => 0
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = 0
			and rp.obj_type = "database"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="*.*"
			and rp.with_grant_option = true;
	*/
	//		db; => databaseId
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = d.dat_id
			and rp.obj_type = "database"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="d"
			and  d.datname = DB
			and rp.with_grant_option = true;
	*/
	//	Function:
	//		*.*：=> 0
	//		db.routine => routineId
	//	Account:
	//		*: => 0
	/*
		select rp.privilege_id,rp.with_grant_option
		from mo_database d, mo_tables t, mo_role_privs rp
		where d.dat_id = t.reldatabase_id
			and rp.obj_id = 0
			and rp.obj_type = "account"
			and rp.role_id = ROLE_ID
			and rp.privilege_id = SELECT_ID
			and rp.privilege_level="**"
			and rp.with_grant_option = true;
	*/
	//privilege_level :GrantPrivilege.privilegeLevel

	//For every privilege, Do the check.
	for _, p := range gp.Privileges {
		sql, err := formSqlFromGrantPrivilege(ctx, ses, gp, p)
		if err != nil {
			return false, err
		}
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return false, err
		}

		results := bh.GetExecResultSet()
		rsset, err = convertIntoResultSet(results)
		if err != nil {
			return false, err
		}

		if len(rsset) == 0 || rsset[0].GetRowCount() == 0 {
			return false, nil
		}
	}
	return true, nil
}

func convertAstPrivilegeTypeToPrivilegeType(priv tree.PrivilegeType) PrivilegeType {
	switch priv {
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_ACCOUNT:
		return PrivilegeTypeCreateAccount
	case tree.PRIVILEGE_TYPE_STATIC_DROP_ACCOUNT:
		return PrivilegeTypeDropAccount
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_ACCOUNT:
		return PrivilegeTypeAlterAccount
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_USER:
		return PrivilegeTypeCreateUser
	case tree.PRIVILEGE_TYPE_STATIC_DROP_USER:
		return PrivilegeTypeDropUser
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_USER:
		return PrivilegeTypeAlterUser
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_ROLE:
		return PrivilegeTypeCreateRole
	case tree.PRIVILEGE_TYPE_STATIC_DROP_ROLE:
		return PrivilegeTypeDropRole
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_ROLE:
		return PrivilegeTypeAlterRole
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_DATABASE:
		return PrivilegeTypeCreateDatabase
	case tree.PRIVILEGE_TYPE_STATIC_DROP_DATABASE:
		return PrivilegeTypeDropDatabase
	case tree.PRIVILEGE_TYPE_STATIC_SHOW_DATABASES:
		return PrivilegeTypeShowDatabases
	case tree.PRIVILEGE_TYPE_STATIC_CONNECT:
		return PrivilegeTypeConnect
	case tree.PRIVILEGE_TYPE_STATIC_MANAGE_GRANTS:
		return PrivilegeTypeManageGrants
	case tree.PRIVILEGE_TYPE_STATIC_ALL:
		return PrivilegeTypeDatabaseAll //TODO: database ? table ? account ?
	case tree.PRIVILEGE_TYPE_STATIC_OWNERSHIP:
		return PrivilegeTypeDatabaseOwnership //TODO
	case tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES:
		return PrivilegeTypeShowTables
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLE:
		return PrivilegeTypeCreateTable
	case tree.PRIVILEGE_TYPE_STATIC_DROP_TABLE:
		return PrivilegeTypeDropTable
	case tree.PRIVILEGE_TYPE_STATIC_CREATE_VIEW:
		return PrivilegeTypeCreateView
	case tree.PRIVILEGE_TYPE_STATIC_DROP_VIEW:
		return PrivilegeTypeDropView
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_VIEW:
		return PrivilegeTypeAlterView
	case tree.PRIVILEGE_TYPE_STATIC_ALTER_TABLE:
		return PrivilegeTypeAlterTable
	case tree.PRIVILEGE_TYPE_STATIC_SELECT:
		return PrivilegeTypeSelect
	case tree.PRIVILEGE_TYPE_STATIC_INSERT:
		return PrivilegeTypeInsert
	case tree.PRIVILEGE_TYPE_STATIC_UPDATE:
		return PrivilegeTypeUpdate
	case tree.PRIVILEGE_TYPE_STATIC_DELETE:
		return PrivilegeTypeDelete
	case tree.PRIVILEGE_TYPE_STATIC_INDEX:
		return PrivilegeTypeIndex
	case tree.PRIVILEGE_TYPE_STATIC_EXECUTE:
		return PrivilegeTypeExecute
	default:
		panic(fmt.Sprintf("unsupported privilege type %s", priv.ToString()))
	}
}

// authenticatePrivilegeOfStatementWithObjectTypeNone decides the user has the privilege of executing the statement with object type none
func authenticatePrivilegeOfStatementWithObjectTypeNone(ctx context.Context, ses *Session, stmt tree.Statement) (bool, error) {
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
			return determinePrivilegeHasWithGrantOption(ctx, ses, g)
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
func checkSysExistsOrNot(ctx context.Context, pu *config.ParameterUnit) (bool, error) {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	var rsset []ExecResult

	dbSql := "show databases;"
	bh.ClearExecResultSet()
	err := bh.Exec(ctx, dbSql)
	if err != nil {
		return false, err
	}

	results := bh.GetExecResultSet()
	if len(results) != 1 {
		panic("it must have result set")
	}

	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return false, err
	}

	for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
		_, err := rsset[0].GetString(i, 0)
		if err != nil {
			return false, err
		}
	}

	bh.ClearExecResultSet()

	sql := "show tables from mo_catalog;"
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	results = bh.GetExecResultSet()
	if len(results) != 1 {
		panic("it must have result set")
	}

	rsset, err = convertIntoResultSet(results)
	if err != nil {
		return false, err
	}

	tableNames := []string{}
	for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
		tableName, err := rsset[0].GetString(i, 0)
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
	if !tenant.IsSysTenant() {
		return moerr.NewInternalError("only sys tenant can execute the function")
	}

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//USE the mo_catalog
	addSqlIntoSet("use mo_catalog;")

	//BEGIN the transaction
	addSqlIntoSet("begin;")

	//create tables for the tenant
	for _, sql := range createSqls {
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant
	//step 1: add new tenant entry to the mo_account
	initMoAccount = fmt.Sprintf(initMoAccountFormat, sysAccountID, sysAccountName, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), sysAccountComments)
	addSqlIntoSet(initMoAccount)

	//step 2:add new role entries to the mo_role

	initMoRole1 := fmt.Sprintf(initMoRoleFormat, moAdminRoleID, moAdminRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), moAdminRoleComment)
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), publicRoleComment)
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
	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant1)
	addSqlIntoSet(initMoUserGrant2)
	initMoUserGrant4 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, dumpID, types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant4)

	addSqlIntoSet("commit;")

	//fill the mo_account, mo_role, mo_user, mo_role_privs, mo_user_grant
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	for _, sql := range initDataSqls {
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}
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

// createTablesInInformationSchema creates the database information_schema and the views or tables.
func createTablesInInformationSchema(ctx context.Context, tenant *TenantInfo, pu *config.ParameterUnit) error {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	err := bh.Exec(ctx, "create database information_schema;")
	if err != nil {
		return err
	}
	return err
}

func checkTenantExistsOrNot(ctx context.Context, pu *config.ParameterUnit, userName string) (bool, error) {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)

	sqlForCheckTenant := getSqlForCheckTenant(userName)
	rsset, err := executeSQLInBackgroundSession(ctx, guestMMu, pu.Mempool, pu, sqlForCheckTenant)
	if err != nil {
		return false, err
	}

	if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
		return false, nil
	}

	return true, nil
}

// InitGeneralTenant initializes the application level tenant
func InitGeneralTenant(ctx context.Context, tenant *TenantInfo, ca *tree.CreateAccount) error {
	var err error
	var exists bool
	pu := config.GetParameterUnit(ctx)

	if !(tenant.IsSysTenant() && tenant.IsMoAdminRole()) {
		return moerr.NewInternalError("tenant %s user %s role %s do not have the privilege to create the new account", tenant.GetTenant(), tenant.GetUser(), tenant.GetDefaultRole())
	}

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, tenant.GetTenantID())
	ctx = context.WithValue(ctx, defines.UserIDKey{}, tenant.GetUserID())
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())

	exists, err = checkTenantExistsOrNot(ctx, pu, ca.Name)
	if err != nil {
		return err
	}

	if exists {
		if ca.IfNotExists { //do nothing
			return nil
		}
		return moerr.NewInternalError("the tenant %s exists", ca.Name)
	}

	var newTenant *TenantInfo
	newTenant, err = createTablesInMoCatalogOfGeneralTenant(ctx, tenant, pu, ca)
	if err != nil {
		return err
	}

	err = createTablesInInformationSchemaOfGeneralTenant(ctx, tenant, pu, newTenant)
	if err != nil {
		return err
	}

	return nil
}

// createTablesInMoCatalogOfGeneralTenant creates catalog tables in the database mo_catalog.
func createTablesInMoCatalogOfGeneralTenant(ctx context.Context, tenant *TenantInfo, pu *config.ParameterUnit, ca *tree.CreateAccount) (*TenantInfo, error) {
	var err error
	var initMoAccount string
	var initDataSqls []string

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//USE the mo_catalog
	addSqlIntoSet("use mo_catalog;")

	//BEGIN the transaction
	addSqlIntoSet("begin;")

	//!!!NOTE : Insert into mo_account with original context.
	// Other operations with a new context with new tenant info
	//step 1: add new tenant entry to the mo_account
	//TODO: use auto increment
	comment := ""
	if ca.Comment.Exist {
		comment = ca.Comment.Comment
	}
	newTenantID := uint32(rand.Int31())
	initMoAccount = fmt.Sprintf(initMoAccountFormat, newTenantID, ca.Name, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), comment)

	insertIntoMoAccountSqlIdx := len(initDataSqls)
	addSqlIntoSet(initMoAccount)

	//create tables for the tenant
	for i, sql := range createSqls {
		//only the SYS tenant has the table mo_account
		if i == createMoAccountIndex {
			continue
		}
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant

	//step 2:add new role entries to the mo_role

	initMoRole1 := fmt.Sprintf(initMoRoleFormat, accountAdminRoleID, accountAdminRoleName, tenant.GetUserID(), tenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), accountAdminRoleComment)
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, tenant.GetUserID(), tenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), publicRoleComment)
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user
	//TODO:use auto_increment column for the userid
	if ca.AuthOption.IdentifiedType.Typ != tree.AccountIdentifiedByPassword {
		return nil, moerr.NewInternalError("only support password verification now")
	}
	name := ca.AuthOption.AdminName
	password := ca.AuthOption.IdentifiedType.Str
	if len(password) == 0 {
		return nil, moerr.NewInternalError("password is empty string")
	}
	status := rootStatus
	//TODO: fix the status of user or account
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = "suspend"
		}
	}
	newUserId := uint32(rand.Int31())
	initMoUser1 := fmt.Sprintf(initMoUserFormat, newUserId, rootHost, name, password, status,
		types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
		tenant.GetUserID(), tenant.GetDefaultRoleID(), publicRoleID)
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

	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant2)
	addSqlIntoSet("commit;")

	//with new tenant
	//TODO: when we have the auto_increment column, we need new strategy.
	newTenantCtx := context.WithValue(ctx, defines.TenantIDKey{}, newTenantID)
	newTenantCtx = context.WithValue(newTenantCtx, defines.UserIDKey{}, newUserId)
	newTenantCtx = context.WithValue(newTenantCtx, defines.RoleIDKey{}, uint32(publicRoleID))

	newTenant := &TenantInfo{
		Tenant:        ca.Name,
		User:          ca.AuthOption.AdminName,
		DefaultRole:   publicRoleName,
		TenantID:      newTenantID,
		UserID:        newUserId,
		DefaultRoleID: publicRoleID,
	}

	//fill the mo_account, mo_role, mo_user, mo_role_privs, mo_user_grant
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	for i, sql := range initDataSqls {
		inputCtx := ctx
		if insertIntoMoAccountSqlIdx != i {
			inputCtx = newTenantCtx
		}
		err = bh.Exec(inputCtx, sql)
		if err != nil {
			goto handleFailed
		}
	}

	return newTenant, err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return nil, rbErr
	}
	return newTenant, err
}

// createTablesInInformationSchemaOfGeneralTenant creates the database information_schema and the views or tables.
func createTablesInInformationSchemaOfGeneralTenant(ctx context.Context, tenant *TenantInfo, pu *config.ParameterUnit, newTenant *TenantInfo) error {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	//with new tenant
	//TODO: when we have the auto_increment column, we need new strategy.
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(newTenant.GetTenantID()))
	ctx = context.WithValue(ctx, defines.UserIDKey{}, uint32(newTenant.GetUserID()))
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, uint32(newTenant.GetDefaultRoleID()))
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()
	err := bh.Exec(ctx, "create database information_schema;")
	if err != nil {
		return err
	}
	return err
}

func checkUserExistsOrNot(ctx context.Context, pu *config.ParameterUnit, tenantName string) (bool, error) {
	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)

	sqlForCheckUser := getSqlForPasswordOfUser(tenantName)
	rsset, err := executeSQLInBackgroundSession(ctx, guestMMu, pu.Mempool, pu, sqlForCheckUser)
	if err != nil {
		return false, err
	}

	if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
		return false, nil
	}

	return true, nil
}

// InitUser creates new user for the tenant
func InitUser(ctx context.Context, tenant *TenantInfo, cu *tree.CreateUser) error {
	var err error
	var exists bool
	var rsset []ExecResult
	pu := config.GetParameterUnit(ctx)

	var initUserSqls []string

	appendSql := func(sql string) {
		initUserSqls = append(initUserSqls, sql)
	}

	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()

	//TODO: get role and the id of role
	newRoleId := publicRoleID
	if cu.Role != nil {
		if strings.ToLower(cu.Role.UserName) != publicRoleName {
			sqlForRoleIdOfRole := getSqlForRoleIdOfRole(cu.Role.UserName)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForRoleIdOfRole)
			if err != nil {
				return err
			}
			values := bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(values)
			if err != nil {
				return err
			}
			if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
				return moerr.NewInternalError("there is no role %s", cu.Role.UserName)
			}
			roleId, err := rsset[0].GetInt64(0, 0)
			if err != nil {
				return err
			}
			newRoleId = int(roleId)
		}
	}

	//TODO: get password_option or lock_option. there is no field in mo_user to store it.
	status := userStatusUnlock
	if cu.MiscOpt != nil {
		if _, ok := cu.MiscOpt.(*tree.UserMiscOptionAccountLock); ok {
			status = userStatusLock
		}
	}

	appendSql("begin;")

	for _, user := range cu.Users {
		exists, err = checkUserExistsOrNot(ctx, pu, user.Username)
		if err != nil {
			return err
		}

		if exists {
			if cu.IfNotExists { //do nothing
				continue
			}
			return moerr.NewInternalError("the user %s exists", user.Username)
		}

		if user.AuthOption == nil {
			return moerr.NewInternalError("the user %s misses the auth_option", user.Username)
		}

		if user.AuthOption.Typ != tree.AccountIdentifiedByPassword {
			return moerr.NewInternalError("only support password verification now")
		}

		password := user.AuthOption.Str
		if len(password) == 0 {
			return moerr.NewInternalError("password is empty string")
		}

		//TODO: get comment or attribute. there is no field in mo_user to store it.
		//TODO: to get the user id from the auto_increment table
		newUserId := rand.Int31()
		initMoUser1 := fmt.Sprintf(initMoUserFormat, newUserId, rootHost, user.Username, password, status,
			types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
			tenant.GetUserID(), tenant.GetDefaultRoleID(), newRoleId)

		appendSql(initMoUser1)

		initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, newRoleId, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
		appendSql(initMoUserGrant1)

		//if it is not public role, just insert the record for public
		if newRoleId != publicRoleID {
			initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
			appendSql(initMoUserGrant2)
		}
	}

	appendSql("commit;")

	//fill the mo_user
	for _, sql := range initUserSqls {
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}
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
	var exists bool
	var rsset []ExecResult
	pu := config.GetParameterUnit(ctx)

	var initRoleSqls []string

	appendSql := func(sql string) {
		initRoleSqls = append(initRoleSqls, sql)
	}

	guestMMu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	bh := NewBackgroundHandler(ctx, guestMMu, pu.Mempool, pu)
	defer bh.Close()

	appendSql("begin;")

	for _, r := range cr.Roles {
		if strings.ToLower(r.UserName) == publicRoleName {
			exists = true
		} else {
			sqlForRoleIdOfRole := getSqlForRoleIdOfRole(r.UserName)
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlForRoleIdOfRole)
			if err != nil {
				return err
			}
			values := bh.GetExecResultSet()
			rsset, err = convertIntoResultSet(values)
			if err != nil {
				return err
			}
			if len(rsset) >= 1 && rsset[0].GetRowCount() >= 1 {
				exists = true
			}
		}

		if exists {
			if cr.IfNotExists {
				continue
			}
			return moerr.NewInternalError("the role %s exists", r.UserName)
		}

		newRoleId := rand.Int31()
		initMoRole := fmt.Sprintf(initMoRoleFormat, newRoleId, r.UserName, tenant.GetUserID(), tenant.GetDefaultRoleID(),
			types.CurrentTimestamp().String2(time.UTC, 0), "")
		appendSql(initMoRole)
	}

	appendSql("commit;")

	//fill the mo_user
	for _, sql := range initRoleSqls {
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}
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
