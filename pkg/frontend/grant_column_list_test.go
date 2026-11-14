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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// countColumnQualifiedPrivileges counts the privileges of a parsed statement
// that carry a column qualification. The production parser must preserve it
// all the way into the AST the privilege owner consumes.
func countColumnQualifiedPrivileges(privileges []*tree.Privilege) int {
	n := 0
	for _, p := range privileges {
		if len(p.ColumnList) > 0 {
			n++
		}
	}
	return n
}

func parseOneGrantOrRevoke(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, "parser must accept the column-qualified statement")
	return stmt
}

func newSessionForPrivilegeOwner(t *testing.T, stmt tree.Statement) *Session {
	t.Helper()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	priv := determinePrivilegeSetOfStatement(stmt)
	ses := newSes(priv, ctrl)
	return ses
}

// Test_doGrantPrivilegeRejectsColumnQualifiedPrivileges proves that a parsed
// column-qualified privilege list is rejected by the privilege owner before
// any role lookup or privilege mutation runs: a column-restricted grant must
// never silently degrade into a full-object grant (inherited by every user
// through PUBLIC), no partial grant may survive a mixed privilege list, and a
// user grantee now fails with the explicit unsupported-column diagnostic
// before the role-only namespace resolution.
func Test_doGrantPrivilegeRejectsColumnQualifiedPrivileges(t *testing.T) {
	queries := []struct {
		name string
		sql  string
	}{
		{"public grantee", "grant select(c_pub) on table bh1.t to public"},
		{"named role grantee", "grant select(c_pub) on table bh1.t to r1"},
		{"user grantee gets column diagnostic", "grant select(c_pub) on table bh1.t to u_colv"},
		{"with grant option", "grant select(c_pub) on table bh1.t to public with grant option"},
		{"mixed list rejects whole statement", "grant select, insert(c_pub) on table bh1.t to r1"},
	}
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseOneGrantOrRevoke(t, tc.sql)
			grant, ok := stmt.(*tree.Grant)
			require.True(t, ok)
			require.Equal(t, tree.GrantTypePrivilege, grant.Typ)
			gp := &grant.GrantPrivilege
			require.NotEmpty(t, gp.Privileges)
			require.Greater(t, countColumnQualifiedPrivileges(gp.Privileges), 0,
				"production parser must retain the column list for the owner")

			ses := newSessionForPrivilegeOwner(t, gp)
			bh := &backgroundExecTest{}

			err := doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, gp, bh)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
				"column-qualified grant must fail with the checked not-supported diagnostic, got %v", err)
			require.Empty(t, bh.executedSQLs,
				"rejection must precede every role lookup and privilege mutation")
		})
	}
}

// Test_doRevokePrivilegeRejectsColumnQualifiedPrivileges proves a parsed
// column-qualified REVOKE is rejected before any revocation: the column list
// cannot name the whole-object grants that would actually be removed, so no
// over-revoke SQL may run.
func Test_doRevokePrivilegeRejectsColumnQualifiedPrivileges(t *testing.T) {
	queries := []struct {
		name string
		sql  string
	}{
		{"role grantee", "revoke select(c_pub) on table bh1.t from r1"},
		{"mixed list rejects whole statement", "revoke select, update(c_pub) on table bh1.t from r1"},
	}
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseOneGrantOrRevoke(t, tc.sql)
			revoke, ok := stmt.(*tree.Revoke)
			require.True(t, ok)
			require.Equal(t, tree.RevokeTypePrivilege, revoke.Typ)
			rp := &revoke.RevokePrivilege
			require.NotEmpty(t, rp.Privileges)
			require.Greater(t, countColumnQualifiedPrivileges(rp.Privileges), 0,
				"production parser must retain the column list for the owner")

			ses := newSessionForPrivilegeOwner(t, rp)
			bh := &backgroundExecTest{}

			err := doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, rp, bh)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
				"column-qualified revoke must fail with the checked not-supported diagnostic, got %v", err)
			require.Empty(t, bh.executedSQLs,
				"rejection must precede every role lookup and revocation")
		})
	}
}

// Test_doGrantPrivilegePlainPrivilegeListStillGrants is the nearest control:
// unqualified privilege lists keep the normal grant flow — the role-only
// object-grantee namespace and the whole-object privilege statements must be
// untouched by the rejection guard.
func Test_doGrantPrivilegePlainPrivilegeListStillGrants(t *testing.T) {
	ctx := context.Background()
	stmt := parseOneGrantOrRevoke(t, "grant select on table *.* to r1")
	grant, ok := stmt.(*tree.Grant)
	require.True(t, ok)
	gp := &grant.GrantPrivilege
	require.Equal(t, 0, countColumnQualifiedPrivileges(gp.Privileges))

	ses := newSessionForPrivilegeOwner(t, gp)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil
	sql, err := getSqlForRoleIdOfRole(ctx, "r1")
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForRoleIdOfRole([][]interface{}{{int64(5)}})
	privType, err := convertAstPrivilegeTypeToPrivilegeType(ctx, gp.Privileges[0].Type, gp.ObjType)
	require.NoError(t, err)
	checkSQL := getSqlForCheckRoleHasPrivilege(int64(5), objectTypeTable, objectIDAll, int64(privType))
	bh.sql2result[checkSQL] = newMrsForCheckRoleHasPrivilege([][]interface{}{})

	err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, gp, bh)
	require.NoError(t, err)
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.NotEmpty(t, bh.executedSQLs)
	require.Contains(t, bh.executedSQLs[len(bh.executedSQLs)-1], "mo_role_privs")
}

// Test_doRevokePrivilegePlainPrivilegeListStillRevokes is the nearest
// control: an ordinary whole-object REVOKE still completes the full
// revocation flow through the same owner.
func Test_doRevokePrivilegePlainPrivilegeListStillRevokes(t *testing.T) {
	ctx := context.Background()
	stmt := parseOneGrantOrRevoke(t, "revoke select on table *.* from r1")
	revoke, ok := stmt.(*tree.Revoke)
	require.True(t, ok)
	rp := &revoke.RevokePrivilege
	require.Equal(t, 0, countColumnQualifiedPrivileges(rp.Privileges))

	ses := newSessionForPrivilegeOwner(t, rp)
	bh := &backgroundExecTest{}
	bh.init()
	sql, err := getSqlForRoleIdOfRole(ctx, "r1")
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForRoleIdOfRole([][]interface{}{{int64(5)}})

	err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, rp, bh)
	require.NoError(t, err)
	require.NotEmpty(t, bh.executedSQLs)
	require.Contains(t, bh.executedSQLs[len(bh.executedSQLs)-1], "mo_role_privs")
}

// Test_grantsNameAdmissionBoundaries witnesses the name-admission asymmetry
// that frames the SHOW GRANTS quote tests: creation routes reject quote,
// backtick, double-quote, space and tab role and user names (the
// InitRole/InitUser flows call getSqlForRoleIdOfRole /
// getSqlForPasswordOfUser -> inputNameIsInvalid), but SHOW GRANTS itself
// performs no name admission (the frontend only fills defaults for
// *tree.ShowGrants), and InitUser applies no charset check to hostnames.
// Stored role/user rows therefore passed the admission below, while SHOW
// statement arguments remain free-form text the renderer must quote.
func Test_grantsNameAdmissionBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, bad := range []string{"r'1", "r`1", `r"1`, "r 1", "r\t1"} {
		require.Error(t, inputNameIsInvalid(ctx, bad), "creation-route admission must reject %q", bad)
		_, err := getSqlForRoleIdOfRole(ctx, bad)
		require.Error(t, err, "role lookup must reject %q", bad)
		_, err = getSqlForPasswordOfUser(ctx, bad)
		require.Error(t, err, "user lookup must reject %q", bad)
	}
	name, err := normalizeName(ctx, "r-1.x_2")
	require.NoError(t, err)
	require.Equal(t, "r-1.x_2", name)
}
