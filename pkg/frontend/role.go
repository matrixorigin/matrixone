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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// handleCreateRole creates the new role
func handleCreateRole(ctx context.Context, ses FeSession, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ctx context.Context, ses FeSession, dr *tree.DropRole) error {
	return doDropRole(ctx, ses.(*Session), dr)
}

// handleGrantRole grants the role
func handleGrantRole(ctx context.Context, ses FeSession, gr *tree.GrantRole) error {
	return doGrantRole(ctx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ctx context.Context, ses FeSession, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ctx context.Context, ses FeSession, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, ses, gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ctx context.Context, ses FeSession, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, ses, rp)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ctx context.Context, ses FeSession, sr *tree.SetRole) error {
	return doSwitchRole(ctx, ses.(*Session), sr)
}

// InitRole creates the new role
func InitRole(ctx context.Context, ses *Session, tenant *TenantInfo, cr *tree.CreateRole) (err error) {
	var exists int
	var erArray []ExecResult
	var sql string
	err = normalizeNamesOfRoles(ctx, cr.Roles)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	for _, r := range cr.Roles {
		exists = 0
		if isPredefinedRole(r.UserName) {
			exists = 3
		} else {
			//dedup with role
			sql, err = getSqlForRoleIdOfRole(ctx, r.UserName)
			if err != nil {
				return err
			}
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
				exists = 1
			}

			//dedup with user
			if exists == 0 {
				sql, err = getSqlForPasswordOfUser(ctx, r.UserName)
				if err != nil {
					return err
				}
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
					exists = 2
				}
			}
		}

		if exists != 0 {
			if cr.IfNotExists {
				continue
			}
			if exists == 1 {
				err = moerr.NewInternalError(ctx, "the role %s exists", r.UserName)
			} else if exists == 2 {
				err = moerr.NewInternalError(ctx, "there is a user with the same name as the role %s", r.UserName)
			} else if exists == 3 {
				err = moerr.NewInternalError(ctx, "can not use the name %s. it is the name of the predefined role", r.UserName)
			}

			return err
		}

		initMoRole := fmt.Sprintf(initMoRoleWithoutIDFormat, r.UserName, tenant.GetUserID(), tenant.GetDefaultRoleID(),
			types.CurrentTimestamp().String2(time.UTC, 0), "")
		err = bh.Exec(ctx, initMoRole)
		if err != nil {
			return err
		}
	}
	return err
}

func doGrantPrivilegeImplicitly(_ context.Context, ses *Session, stmt tree.Statement) error {
	var err error
	var sql string
	tenantInfo := ses.GetTenantInfo()
	if tenantInfo == nil || tenantInfo.IsAdminRole() {
		return err
	}
	currentRole := tenantInfo.GetDefaultRole()

	// 1.first change to moadmin/accountAdmin
	var tenantCtx context.Context
	tenantInfo = ses.GetTenantInfo()
	// if is system account
	if tenantInfo.IsSysTenant() {
		tenantCtx = defines.AttachAccount(ses.GetRequestContext(), uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	} else {
		tenantCtx = defines.AttachAccount(ses.GetRequestContext(), tenantInfo.GetTenantID(), tenantInfo.GetUserID(), uint32(accountAdminRoleID))
	}

	// 2.grant database privilege
	switch st := stmt.(type) {
	case *tree.CreateDatabase:
		sql = getSqlForGrantOwnershipOnDatabase(string(st.Name), currentRole)
	case *tree.CreateTable:
		// get database name
		var dbName string
		if len(st.Table.SchemaName) == 0 {
			dbName = ses.GetDatabaseName()
		} else {
			dbName = string(st.Table.SchemaName)
		}
		// get table name
		tableName := string(st.Table.ObjectName)
		sql = getSqlForGrantOwnershipOnTable(dbName, tableName, currentRole)
	}

	bh := ses.GetBackgroundExec(tenantCtx)
	defer bh.Close()

	err = bh.Exec(tenantCtx, sql)
	if err != nil {
		return err
	}

	return err
}

func doRevokePrivilegeImplicitly(_ context.Context, ses *Session, stmt tree.Statement) error {
	var err error
	var sql string
	tenantInfo := ses.GetTenantInfo()
	if tenantInfo == nil || tenantInfo.IsAdminRole() {
		return err
	}
	currentRole := tenantInfo.GetDefaultRole()

	// 1.first change to moadmin/accountAdmin
	var tenantCtx context.Context
	tenantInfo = ses.GetTenantInfo()
	// if is system account
	if tenantInfo.IsSysTenant() {
		tenantCtx = defines.AttachAccount(ses.GetRequestContext(), uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	} else {
		tenantCtx = defines.AttachAccount(ses.GetRequestContext(), tenantInfo.GetTenantID(), tenantInfo.GetUserID(), uint32(accountAdminRoleID))
	}

	// 2.grant database privilege
	switch st := stmt.(type) {
	case *tree.DropDatabase:
		sql = getSqlForRevokeOwnershipFromDatabase(string(st.Name), currentRole)
	case *tree.DropTable:
		// get database name
		var dbName string
		if len(st.Names[0].SchemaName) == 0 {
			dbName = ses.GetDatabaseName()
		} else {
			dbName = string(st.Names[0].SchemaName)
		}
		// get table name
		tableName := string(st.Names[0].ObjectName)
		sql = getSqlForRevokeOwnershipFromTable(dbName, tableName, currentRole)
	}

	bh := ses.GetBackgroundExec(tenantCtx)
	defer bh.Close()

	err = bh.Exec(tenantCtx, sql)
	if err != nil {
		return err
	}

	return err
}

func doCheckRole(ctx context.Context, ses *Session) error {
	var err error
	tenantInfo := ses.GetTenantInfo()
	currentAccount := tenantInfo.GetTenant()
	currentRole := tenantInfo.GetDefaultRole()
	if currentAccount == sysAccountName {
		if currentRole != moAdminRoleName {
			err = moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
		}
	} else if currentRole != accountAdminRoleName {
		err = moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
	}
	return err
}

// doSetSecondaryRoleAll set the session role of the user with smallness role_id
func doSetSecondaryRoleAll(ctx context.Context, ses *Session) (err error) {
	var sql string
	var userId uint32
	var erArray []ExecResult
	var roleId int64
	var roleName string

	account := ses.GetTenantInfo()
	// get current user_id
	userId = account.GetUserID()

	// init role_id and role_name
	roleId = publicRoleID
	roleName = publicRoleName

	// step1:get all roles expect public
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	sql = getSqlForgetUserRolesExpectPublicRole(publicRoleID, userId)
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
		roleId, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}

		roleName, err = erArray[0].GetString(ctx, 0, 1)
		if err != nil {
			return err
		}
	}

	// step2 : switch the default role and role id;
	account.SetDefaultRoleID(uint32(roleId))
	account.SetDefaultRole(roleName)

	return err
}

// doSwitchRole accomplishes the Use Role and Use Secondary Role statement
func doSwitchRole(ctx context.Context, ses *Session, sr *tree.SetRole) (err error) {
	var sql string
	var erArray []ExecResult
	var roleId int64

	account := ses.GetTenantInfo()

	if sr.SecondaryRole {
		//use secondary role all or none
		switch sr.SecondaryRoleType {
		case tree.SecondaryRoleTypeAll:
			doSetSecondaryRoleAll(ctx, ses)
			account.SetUseSecondaryRole(true)
		case tree.SecondaryRoleTypeNone:
			account.SetUseSecondaryRole(false)
		}
	} else if sr.Role != nil {
		err = normalizeNameOfRole(ctx, sr.Role)
		if err != nil {
			return err
		}

		//step1 : check the role exists or not;

		switchRoleFunc := func() (rtnErr error) {
			bh := ses.GetBackgroundExec(ctx)
			defer bh.Close()

			rtnErr = bh.Exec(ctx, "begin;")
			defer func() {
				rtnErr = finishTxn(ctx, bh, rtnErr)
			}()
			if rtnErr != nil {
				return rtnErr
			}

			sql, rtnErr = getSqlForRoleIdOfRole(ctx, sr.Role.UserName)
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
				roleId, rtnErr = erArray[0].GetInt64(ctx, 0, 0)
				if rtnErr != nil {
					return rtnErr
				}
			} else {
				return moerr.NewInternalError(ctx, "there is no role %s", sr.Role.UserName)
			}

			//step2 : check the role has been granted to the user or not
			sql = getSqlForCheckUserGrant(roleId, int64(account.GetUserID()))
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
				return moerr.NewInternalError(ctx, "the role %s has not be granted to the user %s", sr.Role.UserName, account.GetUser())
			}
			return rtnErr
		}

		err = switchRoleFunc()
		if err != nil {
			return err
		}

		//step3 : switch the default role and role id;
		account.SetDefaultRoleID(uint32(roleId))
		account.SetDefaultRole(sr.Role.UserName)
		//then, reset secondary role to none
		account.SetUseSecondaryRole(false)

		return err
	}

	return err
}

// doDropRole accomplishes the DropRole statement
func doDropRole(ctx context.Context, ses *Session, dr *tree.DropRole) (err error) {
	var vr *verifiedRole
	var sql string
	account := ses.GetTenantInfo()
	err = normalizeNamesOfRoles(ctx, dr.Roles)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	//put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	//step1: check roles exists or not.
	//handle "IF EXISTS"
	for _, role := range dr.Roles {
		sql, err = getSqlForRoleIdOfRole(ctx, role.UserName)
		if err != nil {
			return err
		}
		vr, err = verifyRoleFunc(ctx, bh, sql, role.UserName, roleType)
		if err != nil {
			return err
		}

		if vr == nil {
			if !dr.IfExists { //when the "IF EXISTS" is set, just skip it.
				return moerr.NewInternalError(ctx, "there is no role %s", role.UserName)
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
			return moerr.NewInternalError(ctx, "can not delete the role %s", vr.name)
		}

		sqls := getSqlForDeleteRole(vr.id)
		for _, sqlx := range sqls {
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sqlx)
			if err != nil {
				return err
			}
		}
	}

	return err
}

// doRevokePrivilege accomplishes the RevokePrivilege statement
func doRevokePrivilege(ctx context.Context, ses FeSession, rp *tree.RevokePrivilege) (err error) {
	var vr *verifiedRole
	var objType objectType
	var privLevel privilegeLevelType
	var objId int64
	var privType PrivilegeType
	var sql string
	err = normalizeNamesOfRoles(ctx, rp.Roles)
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
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	//handle "IF EXISTS"
	//step 1: check roles. exists or not.
	for i, user := range rp.Roles {
		//check Revoke privilege on xxx yyy from moadmin(accountadmin)
		if account.IsNameOfAdminRoles(user.UserName) {
			return moerr.NewInternalError(ctx, "the privilege can not be revoked from the role %s", user.UserName)
		}
		sql, err = getSqlForRoleIdOfRole(ctx, user.UserName)
		if err != nil {
			return err
		}
		vr, err = verifyRoleFunc(ctx, bh, sql, user.UserName, roleType)
		if err != nil {
			return err
		}
		verifiedRoles[i] = vr
		if vr == nil {
			if !rp.IfExists { //when the "IF EXISTS" is set, just skip it.
				return moerr.NewInternalError(ctx, "there is no role %s", user.UserName)
			}
		}
	}

	//get the object type
	objType, err = convertAstObjectTypeToObjectType(ctx, rp.ObjType)
	if err != nil {
		return err
	}

	//check the privilege and the object type
	for i, priv := range rp.Privileges {
		privType, err = convertAstPrivilegeTypeToPrivilegeType(ctx, priv.Type, rp.ObjType)
		if err != nil {
			return err
		}
		//check the match between the privilegeScope and the objectType
		err = matchPrivilegeTypeWithObjectType(ctx, privType, objType)
		if err != nil {
			return err
		}
		checkedPrivilegeTypes[i] = privType
	}

	//step 2: decide the object type , the object id and the privilege_level
	privLevel, objId, err = checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, rp.ObjType, *rp.Level)
	if err != nil {
		return err
	}

	//step 3: delete the granted privilege
	for _, privType = range checkedPrivilegeTypes {
		for _, role := range verifiedRoles {
			if role == nil {
				continue
			}
			if privType == PrivilegeTypeConnect && isPublicRole(role.name) {
				return moerr.NewInternalError(ctx, "the privilege %s can not be revoked from the role %s", privType, role.name)
			}
			sql = getSqlForDeleteRolePrivs(role.id, objType.String(), objId, int64(privType), privLevel.String())
			bh.ClearExecResultSet()
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}
	}
	return err
}
