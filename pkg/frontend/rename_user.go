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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"math"
	"sort"
	"strings"
)

type accountUserBinding struct {
	id         int64
	name, host string
}

func queryAccountUsers(ctx context.Context, bh BackgroundExec, predicate string, lock bool) ([]accountUserBinding, error) {
	sql := "select user_id, user_name, user_host from mo_catalog.mo_user where " + predicate + " order by user_id"
	if lock {
		sql += " for update"
	}
	sql += ";"
	r, err := querySetDefaultRoleResult(ctx, bh, sql)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	var rows []accountUserBinding
	for i := uint64(0); i < r.GetRowCount(); i++ {
		id, e := r.GetInt64(ctx, i, 0)
		if e != nil {
			return nil, e
		}
		name, e := r.GetString(ctx, i, 1)
		if e != nil {
			return nil, e
		}
		host, e := r.GetString(ctx, i, 2)
		if e != nil {
			return nil, e
		}
		rows = append(rows, accountUserBinding{id, name, host})
	}
	return rows, nil
}
func lockAccountUserBindings(ctx context.Context, bh BackgroundExec, users []accountUserBinding) error {
	users = append([]accountUserBinding(nil), users...)
	sort.Slice(users, func(i, j int) bool { return users[i].id < users[j].id })
	for _, u := range users {
		rows, err := queryAccountUsers(ctx, bh, fmt.Sprintf("user_id = %d", u.id), true)
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].name != u.name {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
	}
	return nil
}
func lockVerifiedUserBindings(ctx context.Context, bh BackgroundExec, roles []*verifiedRole) error {
	var users []accountUserBinding
	for _, r := range roles {
		if r == nil || r.typ != userType {
			continue
		}
		rows, e := queryAccountUsers(ctx, bh, "user_name = "+quoteSQLStringLiteral(r.name), false)
		if e != nil {
			return e
		}
		if len(rows) != 1 || rows[0].id != r.id {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		users = append(users, rows[0])
	}
	return lockAccountUserBindings(ctx, bh, users)
}
func authenticatedSpecialUser(t *TenantInfo) bool {
	if t == nil || t.GetTenantID() != sysAccountID || !t.IsSysTenant() || t.GetUserID() != math.MaxUint32 {
		return false
	}
	ok, _, _ := isSpecialUser(t.GetUser())
	return ok
}
func canonicalRoutineDefiner(ctx context.Context, bh BackgroundExec, t *TenantInfo) (string, error) {
	if t == nil {
		return "", moerr.NewInternalError(ctx, "routine requires creator identity")
	}
	if authenticatedSpecialUser(t) {
		id, e := defines.GetAccountId(ctx)
		if e != nil {
			return "", e
		}
		if id == sysAccountID {
			return t.GetUser(), nil
		}
	}
	rows, e := queryAccountUsers(ctx, bh, fmt.Sprintf("user_id = %d", t.GetUserID()), true)
	if e != nil {
		return "", e
	}
	if len(rows) != 1 {
		return "", moerr.NewInternalError(ctx, "routine creator no longer exists")
	}
	return rows[0].name, nil
}
func protectedRenameUser(name string) bool {
	switch strings.ToLower(name) {
	case "root", "dump", "internal", "mo_logger", "proxy_user":
		return true
	}
	ok, _, _ := isSpecialUser(name)
	return ok
}

// validateRenameUserDestination preserves the existing login/management grammar.
// It applies only to destinations: legacy catalog names must remain renameable
// away through the safely quoted source lookup. Do not loosen shared SQL guards.
func validateRenameUserDestination(ctx context.Context, name string) error {
	if err := inputNameIsInvalid(ctx, name); err != nil {
		return err
	}
	if strings.ContainsAny(name, "\\?\x00") || strings.Contains(name, "%3A") {
		return moerr.NewInvalidInput(ctx, "RENAME USER destination cannot reliably roundtrip through login")
	}
	return nil
}

func doRenameUser(ctx context.Context, ses *Session, stmt *tree.RenameUser) (err error) {
	if ses == nil || ses.GetTenantInfo() == nil || stmt == nil || len(stmt.Pairs) == 0 {
		return moerr.NewInternalError(ctx, "RENAME USER requires account and pairs")
	}
	account := ses.GetTenantInfo()
	ctx = defines.AttachAccountId(ctx, account.GetTenantID())
	for _, p := range stmt.Pairs {
		for _, u := range []*tree.User{p.From, p.To} {
			if u == nil || u.AuthOption != nil {
				return moerr.NewInvalidInput(ctx, "invalid RENAME USER specification")
			}
			u.Username, err = normalizeName(ctx, u.Username)
			if err != nil {
				return err
			}
			if protectedRenameUser(u.Username) {
				return moerr.NewInvalidInput(ctx, "cannot rename protected user")
			}
		}
		// Validate every pair before acquiring an executor or starting a txn.
		if err = validateRenameUserDestination(ctx, p.To.Username); err != nil {
			return err
		}
	}
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true, cancelTxnCreateWithRequest: true, internalExecution: true})
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() { err = finishTxn(ctx, bh, err) }()
	var users []accountUserBinding
	for _, p := range stmt.Pairs {
		for _, u := range []*tree.User{p.From, p.To} {
			rows, e := queryAccountUsers(ctx, bh, "user_name = "+quoteSQLStringLiteral(u.Username), false)
			if e != nil {
				return e
			}
			users = append(users, rows...)
		}
	}
	current, e := queryAccountUsers(ctx, bh, fmt.Sprintf("user_id = %d", account.GetUserID()), false)
	if e != nil {
		return e
	}
	if len(current) != 1 {
		return moerr.NewInternalError(ctx, "authenticated user no longer exists")
	}
	users = append(users, current[0])
	if err = lockAccountUserBindings(ctx, bh, users); err != nil {
		return err
	}
	locked := make(map[int64]bool)
	for _, u := range users {
		locked[u.id] = true
	}
	admin, e := querySetDefaultRoleResult(defines.AttachAccountId(ctx, sysAccountID), bh, fmt.Sprintf("select admin_name from mo_catalog.mo_account where account_id = %d;", account.GetTenantID()))
	if e != nil {
		return e
	}
	if admin == nil || admin.GetRowCount() != 1 {
		return moerr.NewInternalError(ctx, "account no longer exists")
	}
	adminName, e := admin.GetString(ctx, 0, 0)
	if e != nil {
		return e
	}
	for _, p := range stmt.Pairs {
		src, e := queryAccountUsers(ctx, bh, "user_name = "+quoteSQLStringLiteral(p.From.Username), false)
		if e != nil {
			return e
		}
		if len(src) != 1 {
			return moerr.NewInvalidInput(ctx, "RENAME USER source does not exist")
		}
		u := src[0]
		if protectedRenameUser(u.name) {
			return moerr.NewInvalidInput(ctx, "cannot rename protected user")
		}
		if !locked[u.id] {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		for _, spec := range []*tree.User{p.From, p.To} {
			if spec.Hostname != "" && spec.Hostname != "%" && spec.Hostname != u.host {
				return moerr.NewInvalidInput(ctx, "RENAME USER cannot change host")
			}
		}
		dst, e := queryAccountUsers(ctx, bh, "user_name = "+quoteSQLStringLiteral(p.To.Username), false)
		if e != nil {
			return e
		}
		if len(dst) != 0 {
			return moerr.NewInvalidInput(ctx, "RENAME USER destination already exists")
		}
		if strings.EqualFold(u.name, adminName) || strings.EqualFold(p.To.Username, adminName) {
			return moerr.NewInvalidInput(ctx, "cannot rename administrator")
		}
		isAdmin, e := userIsAdministrator(ctx, bh, u.id, account)
		if e != nil {
			return e
		}
		if isAdmin {
			return moerr.NewInvalidInput(ctx, "cannot rename administrator")
		}
		for _, table := range []string{"mo_user_defined_function", "mo_stored_procedure"} {
			r, e := querySetDefaultRoleResult(ctx, bh, fmt.Sprintf("select definer from mo_catalog.%s where definer in (%s,%s) limit 1;", table, quoteSQLStringLiteral(u.name), quoteSQLStringLiteral(p.To.Username)))
			if e != nil {
				return e
			}
			if r != nil && r.GetRowCount() > 0 {
				return moerr.NewInvalidInput(ctx, "RENAME USER has stored routine DEFINER references")
			}
		}
		if err = bh.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_user set user_name = %s where user_id = %d;", quoteSQLStringLiteral(p.To.Username), u.id)); err != nil {
			return err
		}
	}
	return nil
}

func loginUserStateSQL(id int64, expiry bool) string {
	fields := "status, login_attempts, lock_time"
	if expiry {
		fields = "password_last_changed"
	}
	return fmt.Sprintf("select %s from mo_catalog.mo_user where user_id = %d;", fields, id)
}
func executeLoginUserRead(ctx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
	rows, err := executeSQLInBackgroundSession(ctx, bh, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 || rows[0] == nil || rows[0].GetRowCount() != 1 {
		return nil, markAuthenticationRejected(moerr.NewInternalError(ctx, "authenticated user no longer exists"))
	}
	return rows, nil
}
