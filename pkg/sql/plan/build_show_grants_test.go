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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Test_buildShowGrantsResolvesGrantedObjectScope proves the rewritten SHOW
// GRANTS query resolves the granted object's real identity from the system
// catalog (logical relation ID for table/view grants, database ID for
// db-bound grants) instead of echoing the stored privilege-level type token.
func Test_buildShowGrantsResolvesGrantedObjectScope(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	p, err := runOneStmt(mock, t, "show grants for ROLE role1")
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.GetQuery())

	scanned := map[string]bool{}
	for _, node := range p.GetQuery().GetNodes() {
		if td := node.GetTableDef(); td != nil {
			scanned[td.GetName()] = true
		}
	}

	require.True(t, scanned["mo_role_privs"],
		"the rewritten query must still enumerate mo_role_privs")
	require.True(t, scanned["mo_tables"],
		"table/view grants must be resolved against the system catalog mo_tables (stored obj_id is the logical relation id), got %v", scanned)
	require.True(t, scanned["mo_database"],
		"db-bound grants (d.*, current-DB *, database d) must be resolved against mo_database, got %v", scanned)
}

// The contract tests below name adversarial values (quote / backtick)
// deliberately: creation routes reject such role/user names
// (getSqlForRoleIdOfRole / getSqlForPasswordOfUser -> inputNameIsInvalid in
// pkg/frontend), so stored role/user rows passed that admission, but SHOW
// GRANTS statement arguments themselves get no name admission (the frontend
// only fills defaults), and stored hostnames are not charset-checked at
// creation. Renderer behavior for these arguments is therefore a reachable
// public surface and must stay a quoted context.

// Test_buildShowGrantsRoleSQLScopeContract pins the scope-resolution
// contract of the SHOW GRANTS FOR ROLE rewrite: id-domain separation by
// object type and privilege level, current-account bounding,
// backtick-doubled identifier quoting for the resolved object names, the
// preserved legacy fallbacks (no scope for account grants, stored token
// for global/unresolvable scopes), canonical sqlquote quoting of the role
// name in every interpolation context, and a deterministic output order.
func Test_buildShowGrantsRoleSQLScopeContract(t *testing.T) {
	sql := buildShowGrantsRoleSQL(7, "role'1")

	require.Contains(t, sql,
		"case p.obj_type when 'account' then '' else case")
	require.Contains(t, sql,
		"when rt.relname is not null then concat('`', replace(rt.reldatabase, '`', '``'), '`.`', replace(rt.relname, '`', '``'), '`')")
	require.Contains(t, sql,
		"when dbt.datname is not null and p.obj_type = 'database' then concat('`', replace(dbt.datname, '`', '``'), '`')")
	require.Contains(t, sql,
		"when dbt.datname is not null then concat('`', replace(dbt.datname, '`', '``'), '`.*')")
	require.Contains(t, sql,
		"left join mo_catalog.mo_tables as rt on rt.account_id = 7 and rt.rel_logical_id = p.obj_id and p.obj_type in ('table','view') and p.privilege_level in ('d.t','t')")
	require.Contains(t, sql,
		"left join mo_catalog.mo_database as dbt on dbt.account_id = 7 and dbt.dat_id = p.obj_id and "+
			"((p.obj_type in ('table','view') and p.privilege_level in ('d.*','*')) or (p.obj_type = 'database' and p.privilege_level = 'd'))")
	// Display literal, alias identifier and equality literal are all built
	// by the canonical sqlquote helpers, never raw interpolation.
	require.Contains(t, sql,
		"end,   "+sqlquote.String(" `role'1`")+")")
	require.Contains(t, sql,
		"as "+sqlquote.Ident("Grants for role'1")+" from  mo_catalog.mo_role_privs as p ")
	require.Contains(t, sql,
		"where p.role_name = "+sqlquote.String("role'1")+" order by p.role_id, p.privilege_id, p.obj_type, p.obj_id, p.privilege_level;")
}

// Test_buildShowGrantsUserSQLScopeContract pins the SHOW GRANTS FOR user
// rewrite: the user-membership inner joins stay explicit, the scope
// resolution is the same as for roles, user and host appear only through
// sqlquote-quoted literals (equality and display) and a sqlquote
// identifier alias, and the output order stays deterministic.
func Test_buildShowGrantsUserSQLScopeContract(t *testing.T) {
	sql := buildShowGrantsUserSQL(9, "u'1", "local'host")

	require.Contains(t, sql,
		"as "+sqlquote.Ident("Grants for u'1@localhost")+" from mo_catalog.mo_user as u "+
			"join mo_catalog.mo_user_grant as g on g.user_id = u.user_id "+
			"join mo_catalog.mo_role_privs as p on p.role_id = g.role_id "+
			"left join mo_catalog.mo_tables as rt on rt.account_id = 9 and rt.rel_logical_id = p.obj_id")
	require.Contains(t, sql,
		"left join mo_catalog.mo_database as dbt on dbt.account_id = 9 and dbt.dat_id = p.obj_id")
	require.Contains(t, sql,
		"case p.obj_type when 'account' then '' else case")
	require.Contains(t, sql,
		"end,   "+sqlquote.String(" `u'1`")+", '@', "+sqlquote.String("`local'host`")+")")
	require.Contains(t, sql,
		"where u.user_name = "+sqlquote.String("u'1")+
			" and u.user_host = "+sqlquote.String("local'host")+" order by p.role_id, p.privilege_id, p.obj_type, p.obj_id, p.privilege_level;")
}

// Test_buildShowGrantsRawShowArgumentsStillBuild proves unadmitted SHOW
// GRANTS statement arguments cannot break the rewritten SQL: a role name
// containing a backtick still builds a full plan, and a quoted username /
// hostname pair still parses. The mock catalog has no mo_user_grant table,
// so the user form asserts statement parse rather than a full build.
func Test_buildShowGrantsRawShowArgumentsStillBuild(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	p, err := runOneStmt(mock, t, "show grants for ROLE `r``1`")
	require.NoError(t, err, "a backtick in the SHOW role argument must not break the rewrite")
	require.NotNil(t, p.GetQuery())

	stmts, err := parsers.ParseWithSQLMode(context.Background(), dialect.MYSQL, buildShowGrantsUserSQL(9, `u"1`, "h`o"), 0, "")
	require.NoError(t, err, "quoted username/hostname arguments must not break the production parser")
	require.Len(t, stmts, 1)
}
