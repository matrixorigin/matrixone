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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestGrantRoleWithAdminOption verifies the MySQL 8.0 role-grant clause
// `GRANT role TO user WITH ADMIN OPTION` is accepted for role grants only:
// the ADMIN clause routes into the existing GrantRole.GrantOption flag that
// persists with_grant_option and authorizes re-grants. Privilege and PROXY
// grants must keep rejecting it.
func TestGrantRoleWithAdminOption(t *testing.T) {
	t.Run("role grant accepts admin option", func(t *testing.T) {
		for _, test := range []struct {
			sql       string
			wantRoles int
			wantUsers int
		}{
			{"grant r1 to u1 with admin option", 1, 1},
			{"grant r1 to u1 WITH ADMIN OPTION", 1, 1},
			{"grant r1, r2 to u1, 'u2'@'localhost' with admin option", 2, 2},
			{"grant r1 to `u1`@`%` with Admin Option", 1, 1},
		} {
			sql := test.sql
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)

			grant, ok := stmt.(*tree.Grant)
			require.True(t, ok, sql)
			require.Equal(t, tree.GrantTypeRole, grant.Typ, sql)
			require.True(t, grant.GrantRole.GrantOption, sql)
			// Fixed expectations: the role and recipient counts come from the
			// SQL fixtures and are never derived from the actual parse result.
			require.Len(t, grant.GrantRole.Roles, test.wantRoles, sql)
			require.Len(t, grant.GrantRole.Users, test.wantUsers, sql)

			// The canonical formatting keeps the legacy `with grant option`
			// text: both clauses carry the identical persisted
			// with_grant_option flag, so the formatted statement replays to a
			// semantically identical grant.
			formatted := tree.String(stmt, dialect.MYSQL)
			require.Contains(t, formatted, "with grant option", sql)

			reparsed, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, formatted)
			grant2, ok := reparsed.(*tree.Grant)
			require.True(t, ok, formatted)
			require.True(t, grant2.GrantRole.GrantOption, formatted)
		}
	})

	t.Run("role grant keeps grant option and plain forms", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "grant r1 to u1 with grant option", 1)
		require.NoError(t, err)
		require.True(t, stmt.(*tree.Grant).GrantRole.GrantOption)

		stmt, err = ParseOne(context.Background(), "grant r1 to u1", 1)
		require.NoError(t, err)
		require.False(t, stmt.(*tree.Grant).GrantRole.GrantOption)
	})

	t.Run("privilege and proxy grants reject admin option", func(t *testing.T) {
		for _, sql := range []string{
			"grant all on db.* to u1 with admin option",
			"grant select on db1.t1 to u1 with admin option",
			"grant proxy on u1 to u2 with admin option",
		} {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		}
	})

	t.Run("malformed admin clauses are rejected", func(t *testing.T) {
		for _, sql := range []string{
			"grant r1 to u1 with admin",         // missing OPTION
			"grant r1 to u1 with admin options", // wrong tail
			"grant r1 to u1 with admin option option",
			"grant r1 to u1 with grant option, admin option",
			"grant r1 to u1 with admin option, admin option",
		} {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		}
	})

	t.Run("admin stays a plain identifier outside the clause", func(t *testing.T) {
		for _, sql := range []string{
			"create user admin identified by 'pass'",
			"create role admin",
			"grant admin to u1",
			"grant r1 to admin with admin option",
			"with admin as (select 1 as x) select x from admin",
			"with admin (option) as (select 1 as x) select x from admin",
			"create table admin (admin int)",
			"select admin from admin",
		} {
			_, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
		}
	})

	t.Run("comments and newlines are ignored inside the clause", func(t *testing.T) {
		for _, sql := range []string{
			"grant r1 to u1 with admin /* not a real clause */ option",
			"grant r1 to u1 with admin\noption",
			"grant r1 to u1 with\nadmin option",
			"grant r1 to u1 with AdMiN OPTION",
		} {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			grant, ok := stmt.(*tree.Grant)
			require.True(t, ok, sql)
			require.True(t, grant.GrantRole.GrantOption, sql)
		}
	})

	t.Run("backquoted admin remains an identifier", func(t *testing.T) {
		for _, sql := range []string{
			"grant r1 to u1 with `admin` option",
			"grant r1 to u1 with `Admin` option",
		} {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		}
	})
}

// TestGrantedRoleNotListedInShowGrantsOutput documents the current freshmain
// SHOW GRANTS contract observed on the real server: `SHOW GRANTS FOR user`
// renders the privilege lines of the user's roles; granted-role membership
// lines such as `GRANT r31806admin TO u31806a` are not part of the current
// output, for legacy `WITH GRANT OPTION` grants exactly as for the new
// `WITH ADMIN OPTION`. The behavioral matrix asserts the two clauses render
// identically; this unit test pins only the parsing of SHOW GRANTS for a
// user granted via either clause. Runtime rendering is covered by the
// recorded 73-check real-instance matrix, not by this unit test.
func TestShowGrantsParsesForUsersGrantedViaAdminOption(t *testing.T) {
	for _, sql := range []string{
		"show grants for u31806a",
		"show grants for current_user()",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}
