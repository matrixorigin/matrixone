// Copyright 2026 Matrix Origin
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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type setDefaultRoleUser struct {
	userName string
	userHost string
	userID   int64

	hasSelectedRole bool
	isAdmin         bool
}

type setDefaultRoleUserSpec struct {
	username string
	hostname string
}

// setDefaultRoleTargetsAreCurrentUser recognizes only the narrow self-service
// case. The writer still resolves the names to user IDs and confirms that the
// authenticated user ID is the target before persisting anything.
func setDefaultRoleTargetsAreCurrentUser(
	ctx context.Context,
	ses *Session,
	stmt *tree.SetDefaultRole,
) (bool, error) {
	if ses == nil || stmt == nil || len(stmt.Users) == 0 || ses.GetTenantInfo() == nil {
		return false, nil
	}

	currentUser, err := normalizeName(ctx, ses.GetTenantInfo().GetUser())
	if err != nil {
		return false, err
	}
	for _, user := range stmt.Users {
		if user == nil || user.AuthOption != nil {
			return false, nil
		}
		username, err := normalizeName(ctx, user.Username)
		if err != nil {
			return false, err
		}
		if !strings.EqualFold(username, currentUser) {
			return false, nil
		}
	}
	return true, nil
}

func setDefaultRoleName(ctx context.Context, stmt *tree.SetDefaultRole) (string, error) {
	if stmt == nil {
		return "", moerr.NewInternalError(ctx, "SET DEFAULT ROLE statement is missing")
	}

	switch stmt.Type {
	case tree.SET_DEFAULT_ROLE_TYPE_NONE:
		if len(stmt.Roles) != 0 {
			return "", moerr.NewInternalError(ctx, "SET DEFAULT ROLE NONE cannot include a role list")
		}
		return publicRoleName, nil
	case tree.SET_DEFAULT_ROLE_TYPE_ALL:
		return "", moerr.NewInternalError(ctx, "SET DEFAULT ROLE ALL is not supported; MatrixOne stores one default role per user")
	case tree.SET_DEFAULT_ROLE_TYPE_NORMAL:
		if len(stmt.Roles) != 1 || stmt.Roles[0] == nil {
			return "", moerr.NewInternalError(ctx, "SET DEFAULT ROLE supports exactly one role")
		}
		return normalizeName(ctx, stmt.Roles[0].UserName)
	default:
		return "", moerr.NewInternalError(ctx, "invalid SET DEFAULT ROLE type")
	}
}

func normalizeSetDefaultRoleUsers(ctx context.Context, users []*tree.User) ([]setDefaultRoleUserSpec, error) {
	if len(users) == 0 {
		return nil, moerr.NewInternalError(ctx, "SET DEFAULT ROLE requires at least one user")
	}

	specs := make([]setDefaultRoleUserSpec, 0, len(users))
	for _, user := range users {
		if user == nil {
			return nil, moerr.NewInternalError(ctx, "SET DEFAULT ROLE contains an empty user")
		}
		if user.AuthOption != nil {
			return nil, moerr.NewInternalError(ctx, "SET DEFAULT ROLE does not support user authentication options")
		}
		username, err := normalizeName(ctx, user.Username)
		if err != nil {
			return nil, err
		}
		hostname := strings.TrimSpace(user.Hostname)
		if hostname == "" {
			return nil, moerr.NewInternalError(ctx, "SET DEFAULT ROLE requires a user host")
		}
		specs = append(specs, setDefaultRoleUserSpec{
			username: username,
			hostname: hostname,
		})
	}
	return specs, nil
}

func setDefaultRoleLookupNames(specs []setDefaultRoleUserSpec, currentUser string) []string {
	seen := make(map[string]struct{}, len(specs)+1)
	names := make([]string, 0, len(specs)+1)
	add := func(name string) {
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, spec := range specs {
		add(spec.username)
	}
	add(currentUser)
	sort.Slice(names, func(i, j int) bool {
		lowerI, lowerJ := strings.ToLower(names[i]), strings.ToLower(names[j])
		if lowerI != lowerJ {
			return lowerI < lowerJ
		}
		return names[i] < names[j]
	})
	return names
}

func querySetDefaultRoleResult(ctx context.Context, bh BackgroundExec, sql string) (ExecResult, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	if len(results) != 1 || results[0] == nil {
		return nil, moerr.NewInternalError(ctx, "SET DEFAULT ROLE catalog query returned an invalid result")
	}
	return results[0], nil
}

func setDefaultRoleUserLookupSQL(username string) string {
	return fmt.Sprintf(
		"select user_id, user_name, user_host from mo_catalog.mo_user where user_name = %s order by user_id for update;",
		quoteSQLStringLiteral(username),
	)
}

func setDefaultRoleMembershipLookupSQL(userID int64, roleIDs []int64) string {
	roleList := make([]string, len(roleIDs))
	for i, roleID := range roleIDs {
		roleList[i] = fmt.Sprintf("%d", roleID)
	}
	return fmt.Sprintf(
		"select role_id from mo_catalog.mo_user_grant where user_id = %d and role_id in (%s) order by role_id for update;",
		userID, strings.Join(roleList, ","),
	)
}

func doSetDefaultRole(ctx context.Context, ses *Session, stmt *tree.SetDefaultRole) (err error) {
	if ses == nil || ses.GetTenantInfo() == nil {
		return moerr.NewInternalError(ctx, "SET DEFAULT ROLE requires an account session")
	}

	roleName, err := setDefaultRoleName(ctx, stmt)
	if err != nil {
		return err
	}
	specs, err := normalizeSetDefaultRoleUsers(ctx, stmt.Users)
	if err != nil {
		return err
	}
	account := ses.GetTenantInfo()
	currentUser, err := normalizeName(ctx, account.GetUser())
	if err != nil {
		return err
	}
	currentUserID := int64(account.GetUserID())
	if currentUserID < 0 {
		return moerr.NewInternalError(ctx, "SET DEFAULT ROLE cannot resolve the current user")
	}

	// Lock target user rows in a deterministic name order. Include the current
	// principal in that order so concurrent cross-user updates cannot take the
	// caller and target user locks in opposite orders.
	lookupNames := setDefaultRoleLookupNames(specs, currentUser)

	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{
		forcePessimisticRC:         true,
		cancelTxnCreateWithRequest: true,
	})
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	roleSQL := fmt.Sprintf(
		"select role_id, role_name from mo_catalog.mo_role where role_name = %s order by role_id for update;",
		quoteSQLStringLiteral(roleName),
	)
	roleRows, err := querySetDefaultRoleResult(ctx, bh, roleSQL)
	if err != nil {
		return err
	}
	if roleRows == nil || roleRows.GetRowCount() == 0 {
		return moerr.NewInternalErrorf(ctx, "there is no role %s", roleName)
	}
	if roleRows.GetRowCount() != 1 {
		return moerr.NewInternalErrorf(ctx, "role %s resolves to multiple catalog rows", roleName)
	}
	roleID, err := roleRows.GetInt64(ctx, 0, 0)
	if err != nil {
		return err
	}
	resolvedRoleName, err := roleRows.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}
	if roleID < 0 || roleID > int64(^uint32(0)) || !strings.EqualFold(resolvedRoleName, roleName) {
		return moerr.NewInternalErrorf(ctx, "invalid catalog role for SET DEFAULT ROLE %s", roleName)
	}

	usersByName := make(map[string]*setDefaultRoleUser, len(lookupNames))
	usersByID := make(map[int64]*setDefaultRoleUser, len(lookupNames))
	for _, username := range lookupNames {
		userRows, queryErr := querySetDefaultRoleResult(ctx, bh, setDefaultRoleUserLookupSQL(username))
		if queryErr != nil {
			return queryErr
		}
		if userRows == nil || userRows.GetRowCount() == 0 {
			if strings.EqualFold(username, currentUser) {
				return moerr.NewInternalError(ctx, "the current user no longer exists in the account catalog")
			}
			return moerr.NewInternalErrorf(ctx, "there is no user %s", username)
		}
		if userRows.GetRowCount() != 1 {
			return moerr.NewInternalErrorf(ctx, "user %s resolves to multiple catalog rows", username)
		}
		userID, queryErr := userRows.GetInt64(ctx, 0, 0)
		if queryErr != nil {
			return queryErr
		}
		resolvedUserName, queryErr := userRows.GetString(ctx, 0, 1)
		if queryErr != nil {
			return queryErr
		}
		userHost, queryErr := userRows.GetString(ctx, 0, 2)
		if queryErr != nil {
			return queryErr
		}
		if userID < 0 || userID > int64(^uint32(0)) || !strings.EqualFold(resolvedUserName, username) {
			return moerr.NewInternalErrorf(ctx, "invalid catalog user for SET DEFAULT ROLE target %s", username)
		}
		if strings.EqualFold(username, currentUser) && userID != currentUserID {
			return moerr.NewInternalError(ctx, "the current user identity changed; retry SET DEFAULT ROLE")
		}
		lockedUser := &setDefaultRoleUser{
			userName: resolvedUserName,
			userHost: userHost,
			userID:   userID,
		}
		usersByName[username] = lockedUser
		usersByID[userID] = lockedUser
	}
	currentUserRow := usersByID[currentUserID]
	if currentUserRow == nil || currentUserRow.userID != currentUserID {
		return moerr.NewInternalError(ctx, "SET DEFAULT ROLE cannot confirm the current user identity")
	}

	targetsByID := make(map[int64]*setDefaultRoleUser, len(specs))
	for _, spec := range specs {
		user := usersByName[spec.username]
		if user == nil || !strings.EqualFold(user.userName, spec.username) {
			return moerr.NewInternalErrorf(ctx, "there is no user %s", spec.username)
		}
		if spec.hostname != "%" && spec.hostname != user.userHost {
			return moerr.NewInternalErrorf(ctx, "user host %s does not match the account catalog for %s", spec.hostname, spec.username)
		}
		targetsByID[user.userID] = usersByID[user.userID]
	}

	adminRoleID := int64(accountAdminRoleID)
	if account.IsSysTenant() {
		adminRoleID = int64(moAdminRoleID)
	}
	roleIDsToCheck := []int64{roleID}
	if adminRoleID != roleID {
		roleIDsToCheck = append(roleIDsToCheck, adminRoleID)
		sort.Slice(roleIDsToCheck, func(i, j int) bool { return roleIDsToCheck[i] < roleIDsToCheck[j] })
	}

	lockedUserIDs := make([]int64, 0, len(usersByID))
	for userID := range usersByID {
		lockedUserIDs = append(lockedUserIDs, userID)
	}
	sort.Slice(lockedUserIDs, func(i, j int) bool { return lockedUserIDs[i] < lockedUserIDs[j] })
	for _, userID := range lockedUserIDs {
		grantRows, queryErr := querySetDefaultRoleResult(ctx, bh, setDefaultRoleMembershipLookupSQL(userID, roleIDsToCheck))
		if queryErr != nil {
			return queryErr
		}
		user := usersByID[userID]
		if grantRows == nil {
			continue
		}
		for row := uint64(0); row < grantRows.GetRowCount(); row++ {
			grantedRoleID, queryErr := grantRows.GetInt64(ctx, row, 0)
			if queryErr != nil {
				return queryErr
			}
			if grantedRoleID == roleID {
				user.hasSelectedRole = true
			}
			if grantedRoleID == adminRoleID {
				user.isAdmin = true
			}
			if grantedRoleID != roleID && grantedRoleID != adminRoleID {
				return moerr.NewInternalError(ctx, "SET DEFAULT ROLE membership query returned an unexpected role")
			}
		}
	}

	currentUserIsAdmin := currentUserRow.isAdmin
	targetIDs := make([]int64, 0, len(targetsByID))
	for userID, target := range targetsByID {
		if !target.hasSelectedRole {
			return moerr.NewInternalErrorf(ctx, "the role %s has not be granted to the user %s", roleName, target.userName)
		}
		if target.isAdmin && userID != currentUserID && !currentUserIsAdmin {
			return moerr.NewInternalErrorf(ctx, "cannot change the default role of administrator user %s", target.userName)
		}
		targetIDs = append(targetIDs, userID)
	}
	sort.Slice(targetIDs, func(i, j int) bool { return targetIDs[i] < targetIDs[j] })

	for _, userID := range targetIDs {
		bh.ClearExecResultSet()
		if err = bh.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_user set default_role = %d where user_id = %d;", roleID, userID)); err != nil {
			return err
		}
	}
	return nil
}
