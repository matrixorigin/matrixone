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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// handleCreateUser creates the user for the tenant
func handleCreateUser(ctx context.Context, ses FeSession, cu *tree.CreateUser) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ctx context.Context, ses FeSession, du *tree.DropUser) error {
	return doDropUser(ctx, ses.(*Session), du)
}

func handleAlterUser(ctx context.Context, ses FeSession, au *tree.AlterUser) error {
	return doAlterUser(ctx, ses.(*Session), au)
}

// InitUser creates new user for the tenant
func InitUser(ctx context.Context, ses *Session, tenant *TenantInfo, cu *tree.CreateUser) (err error) {
	var exists int
	var erArray []ExecResult
	var newUserId int64
	var host string
	var newRoleId int64
	var status string
	var sql string
	var mp *mpool.MPool

	err = normalizeNamesOfUsers(ctx, cu.Users)
	if err != nil {
		return err
	}

	if cu.Role != nil {
		err = normalizeNameOfRole(ctx, cu.Role)
		if err != nil {
			return err
		}
	}

	mp, err = mpool.NewMPool("init_user", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	//TODO: get role and the id of role
	newRoleId = publicRoleID
	if cu.Role != nil {
		sql, err = getSqlForRoleIdOfRole(ctx, cu.Role.UserName)
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
		if !execResultArrayHasData(erArray) {
			return moerr.NewInternalError(ctx, "there is no role %s", cu.Role.UserName)
		}
		newRoleId, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
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
			err = verifySpecialRolesInGrant(ctx, tenant, from, to)
			if err != nil {
				return err
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
		sql, err = getSqlForPasswordOfUser(ctx, user.Username)
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
		exists = 0
		if execResultArrayHasData(erArray) {
			exists = 1
		}

		//dedup with the role
		if exists == 0 {
			sql, err = getSqlForRoleIdOfRole(ctx, user.Username)
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

		if exists != 0 {
			if cu.IfNotExists { //do nothing
				continue
			}
			if exists == 1 {
				err = moerr.NewInternalError(ctx, "the user %s exists", user.Username)
			} else if exists == 2 {
				err = moerr.NewInternalError(ctx, "there is a role with the same name as the user")
			}

			return err
		}

		if user.AuthOption == nil {
			return moerr.NewInternalError(ctx, "the user %s misses the auth_option", user.Username)
		}

		if user.AuthOption.Typ != tree.AccountIdentifiedByPassword {
			return moerr.NewInternalError(ctx, "only support password verification now")
		}

		password := user.AuthOption.Str
		if len(password) == 0 {
			return moerr.NewInternalError(ctx, "password is empty string")
		}

		//encryption the password
		encryption := HashPassWord(password)

		//TODO: get comment or attribute. there is no field in mo_user to store it.
		host = user.Hostname
		if len(user.Hostname) == 0 || user.Hostname == "%" {
			host = rootHost
		}
		initMoUser1 := fmt.Sprintf(initMoUserWithoutIDFormat, host, user.Username, encryption, status,
			types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
			tenant.GetUserID(), tenant.GetDefaultRoleID(), newRoleId)

		bh.ClearExecResultSet()
		err = bh.Exec(ctx, initMoUser1)
		if err != nil {
			return err
		}

		//query the id
		bh.ClearExecResultSet()
		sql, err = getSqlForPasswordOfUser(ctx, user.Username)
		if err != nil {
			return err
		}
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			return err
		}

		if !execResultArrayHasData(erArray) {
			return moerr.NewInternalError(ctx, "get the id of user %s failed", user.Username)
		}
		newUserId, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}

		initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, newRoleId, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
		err = bh.Exec(ctx, initMoUserGrant1)
		if err != nil {
			return err
		}

		//if it is not public role, just insert the record for public
		if newRoleId != publicRoleID {
			initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newUserId, types.CurrentTimestamp().String2(time.UTC, 0), true)
			err = bh.Exec(ctx, initMoUserGrant2)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func doAlterUser(ctx context.Context, ses *Session, au *tree.AlterUser) (err error) {
	var sql string
	var vr *verifiedRole
	var user *tree.User
	var userName string
	var hostName string
	var password string
	var erArray []ExecResult
	var encryption string
	account := ses.GetTenantInfo()
	currentUser := account.User

	//1.authenticate the actions
	if au.Role != nil {
		return moerr.NewInternalError(ctx, "not support alter role")
	}
	if au.MiscOpt != nil {
		return moerr.NewInternalError(ctx, "not support password or lock operation")
	}
	if au.CommentOrAttribute.Exist {
		return moerr.NewInternalError(ctx, "not support alter comment or attribute")
	}
	if len(au.Users) != 1 {
		return moerr.NewInternalError(ctx, "can only alter one user at a time")
	}

	err = normalizeNamesOfUsers(ctx, au.Users)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	user = au.Users[0]
	userName = user.Username
	hostName = user.Hostname
	password = user.AuthOption.Str
	if len(password) == 0 {
		return moerr.NewInternalError(ctx, "password is empty string")
	}
	//put it into the single transaction
	err = bh.Exec(ctx, "begin")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	if user.AuthOption == nil {
		return moerr.NewInternalError(ctx, "Operation ALTER USER failed for '%s'@'%s', alter Auth is nil", userName, hostName)
	}

	if user.AuthOption.Typ != tree.AccountIdentifiedByPassword {
		return moerr.NewInternalError(ctx, "Operation ALTER USER failed for '%s'@'%s', only support alter Auth by identified by", userName, hostName)
	}

	//check the user exists or not
	sql, err = getSqlForPasswordOfUser(ctx, userName)
	if err != nil {
		return err
	}
	vr, err = verifyRoleFunc(ctx, bh, sql, userName, roleType)
	if err != nil {
		return err
	}

	if vr == nil {
		//If Exists :
		// false : return an error
		// true : return and  do nothing
		if !au.IfExists {
			return moerr.NewInternalError(ctx, "Operation ALTER USER failed for '%s'@'%s', user does't exist", user.Username, user.Hostname)
		} else {
			return err
		}
	}

	//if the user is admin user with the role moadmin or accountadmin,
	//the user can be altered
	//otherwise only general user can alter itself
	if account.IsSysTenant() {
		sql, err = getSqlForCheckUserHasRole(ctx, currentUser, moAdminRoleID)
	} else {
		sql, err = getSqlForCheckUserHasRole(ctx, currentUser, accountAdminRoleID)
	}
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

	//encryption the password
	encryption = HashPassWord(password)

	if execResultArrayHasData(erArray) {
		sql, err = getSqlForUpdatePasswordOfUser(ctx, encryption, userName)
		if err != nil {
			return err
		}
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	} else {
		if currentUser != userName {
			return moerr.NewInternalError(ctx, "Operation ALTER USER failed for '%s'@'%s', don't have the privilege to alter", userName, hostName)
		}
		sql, err = getSqlForUpdatePasswordOfUser(ctx, encryption, userName)
		if err != nil {
			return err
		}
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

// doDropUser accomplishes the DropUser statement
func doDropUser(ctx context.Context, ses *Session, du *tree.DropUser) (err error) {
	var vr *verifiedRole
	var sql string
	var sqls []string
	var erArray []ExecResult
	account := ses.GetTenantInfo()
	err = normalizeNamesOfUsers(ctx, du.Users)
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

	//step1: check users exists or not.
	//handle "IF EXISTS"
	for _, user := range du.Users {
		sql, err = getSqlForPasswordOfUser(ctx, user.Username)
		if err != nil {
			return err
		}
		vr, err = verifyRoleFunc(ctx, bh, sql, user.Username, roleType)
		if err != nil {
			return err
		}

		if vr == nil {
			if !du.IfExists { //when the "IF EXISTS" is set, just skip it.
				return moerr.NewInternalError(ctx, "there is no user %s", user.Username)
			}
		}

		if vr == nil {
			continue
		}

		//if the user is admin user with the role moadmin or accountadmin,
		//the user can not be deleted.
		if account.IsSysTenant() {
			sql, err = getSqlForCheckUserHasRole(ctx, user.Username, moAdminRoleID)
		} else {
			sql, err = getSqlForCheckUserHasRole(ctx, user.Username, accountAdminRoleID)
		}
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
			return moerr.NewInternalError(ctx, "can not delete the user %s", user.Username)
		}

		//step2 : delete mo_user
		//step3 : delete mo_user_grant
		sqls = getSqlForDeleteUser(vr.id)
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
