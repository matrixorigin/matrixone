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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIsHiddenIndexTableName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{catalog.SecondaryIndexTableNamePrefix + "b6b4_x", true},
		{catalog.UniqueIndexTableNamePrefix + "b6b4_x", true},
		{catalog.FullTextIndexTableNamePrefix + "ft_docs", true},
		{catalog.HnswIndexTableNamePrefix + "v_meta", true},
		{catalog.IvfpqIndexTableNamePrefix + "v_meta", true},
		{"__mo_index_secondary_b6b4_x", true},
		{"__mo_index_unique_B6B4_X", true},
		{"__MO_INDEX_SECONDARY_UP", true},
		{"mo_tables", false},
		{"base", false},
		// "__mo_index_idx_col" is the standard hidden-table column name; as a
		// table identifier it still carries the reserved prefix, so it is
		// rejected (column references never reach this check).
		{"__mo_index_idx_col", true},
		{"x__mo_index_secondary_a", false},
		{"__mo_index", false},
		{"", false},
		{catalog.MOAutoIncrTable, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, isHiddenIndexTableName(c.name), "name %q", c.name)
	}

	// the reserved prefixes come from the catalog constants
	assert.True(t, isHiddenIndexTableName(catalog.PrefixIndexTableName+"anything"))
}

func parseOneGuardTestStmt(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, "parse %q", sql)
	return stmt
}

func TestFindHiddenIndexTableRefInStatement(t *testing.T) {
	t.Parallel()
	hidden := "__mo_index_secondary_b6b4_x"
	hiddenUnique := "__mo_index_unique_b6b4_y"

	blocked := []struct {
		name string
		sql  string
	}{
		{"plain select", "select * from " + hidden},
		{"db qualified select", "select * from db1." + hidden},
		{"3-part select", "select * from db1." + hidden + " i join db1.base b on i.__mo_index_pri_col = b.x"},
		{"backtick quoted", "select * from `__mo_index_secondary_b6b4_x`"},
		{"mixed case", "select * from `__MO_INDEX_Secondary_B6B4_X`"},
		{"scalar subquery in select list", "select (select max(__mo_index_idx_col) from " + hidden + ") from base"},
		{"where in subquery", "select x from base where y in (select __mo_index_idx_col from " + hidden + ")"},
		{"where exists subquery", "select x from base where exists (select 1 from " + hiddenUnique + " u where u.__mo_index_pri_col = base.x)"},
		{"union", "select x from base union all select __mo_index_pri_col from " + hidden},
		{"cte body", "with c as (select * from " + hidden + ") select * from c"},
		{"subquery table in from", "select * from (select * from " + hidden + ") tmp"},
		{"join subquery table", "select * from base b, (select * from " + hiddenUnique + ") x where b.x = x.__mo_index_pri_col"},
		{"join condition subquery", "select * from base a join base2 b on a.y = (select max(__mo_index_idx_col) from " + hidden + ")"},
		{"case expr subquery", "select case when (select count(*) from " + hiddenUnique + ") > 0 then 1 else 0 end from base"},
		{"serial value subquery", "select serial_extract((select serial(count(*)) from " + hidden + "), 0 as bigint)"},
		{"serial index subquery", "select serial_extract(serial(1), (select 0 from " + hidden + " limit 1) as bigint)"},
		{"insert target", "insert into " + hidden + " values (1, 2)"},
		{"insert target db qualified", "insert into db1." + hiddenUnique + " values (1)"},
		{"insert select source", "insert into base select * from " + hidden},
		{"insert on duplicate subquery", "insert into base values (1) on duplicate key update y = (select max(__mo_index_idx_col) from " + hidden + ")"},
		{"update target", "update " + hidden + " set __mo_index_pri_col = 1"},
		{"update from hidden", "update base b join " + hidden + " x on b.x = x.__mo_index_pri_col set b.y = 1"},
		{"update set subquery", "update base set y = (select max(__mo_index_idx_col) from " + hidden + ")"},
		{"delete target", "delete from " + hidden + " where __mo_index_idx_col = 1"},
		{"delete join referencing hidden", "delete b from base b join " + hiddenUnique + " x on b.x = x.__mo_index_pri_col"},
		{"replace target", "replace into " + hidden + " values (1)"},
		{"replace select source", "replace into base select * from " + hidden},
		{"load data target", "load data infile '/tmp/x.csv' into table " + hidden},
		{"truncate", "truncate table " + hidden},
		{"drop single", "drop table if exists " + hidden},
		{"drop multi", "drop table base, " + hiddenUnique},
		{"alter target", "alter table " + hidden + " add column c int"},
		{"alter rename from hidden", "alter table " + hidden + " rename to evicted"},
		{"alter rename to hidden", "alter table base rename to " + hiddenUnique},
		{"rename table from hidden", "rename table " + hidden + " to evicted"},
		{"rename table to hidden", "rename table base to " + hidden},
		{"create under prefix", "create table " + hidden + " (a int)"},
		{"create like hidden", "create table t2 like " + hidden},
		{"create as select hidden", "create table t2 as select * from " + hidden},
		{"create view name", "create view " + hidden + " as select 1"},
		{"create view source", "create view v as select * from " + hiddenUnique},
		{"alter view source", "alter view v as select * from " + hiddenUnique},
		{"aggregate ordering subquery", "select percentile_disc(0.5) within group (order by (select count(*) from " + hidden + ")) from base"},
		{"window ordering subquery", "select row_number() over (order by (select count(*) from " + hidden + ")) from base"},
		{"window partition subquery", "select count(*) over (partition by (select count(*) from " + hidden + ")) from base"},
		{"named window subquery", "select row_number() over w from base window w as (order by (select count(*) from " + hidden + "))"},
		{"update ordering subquery", "update base set y=1 order by (select count(*) from " + hidden + ") limit 1"},
		{"delete ordering subquery", "delete from base order by (select count(*) from " + hidden + ") limit 1"},
		{"table function subquery", "select * from generate_series(1, (select count(*) from " + hidden + ")) g"},
		{"create index on hidden", "create index i on " + hidden + " (__mo_index_idx_col)"},
		{"drop index on hidden", "drop index i on " + hidden},
		{"show create table", "show create table " + hidden},
		{"show create table db", "show create table db1." + hiddenUnique},
		{"show create view", "show create view " + hidden},
		{"show columns", "show columns from " + hidden},
		{"show columns from db", "show columns from " + hidden + " from db1"},
		{"desc", "desc " + hidden},
		{"show index", "show index from " + hidden},
		{"analyze", "analyze table " + hidden + "(x)"},
		{"explain", "explain select * from " + hidden},
		{"explain analyze", "explain analyze select * from " + hiddenUnique},
		{"explain physical", "explain phyplan select * from " + hidden},
		{"grant on hidden", "grant select on table db1." + hidden + " to u1"},
		{"grant table level", "grant select on table " + hiddenUnique + " to u1"},
		{"revoke on hidden", "revoke select on table db1." + hidden + " from u1"},
		{"merge source", "merge into base t using " + hidden + " s on t.x = s.__mo_index_pri_col when matched then update set y = 1"},
		{"paren select", "select * from ((select x from base) union (select __mo_index_pri_col from " + hidden + ")) z"},
	}
	for _, c := range blocked {
		t.Run("blocked/"+c.name, func(t *testing.T) {
			stmt := parseOneGuardTestStmt(t, c.sql)
			defer stmt.Free()
			finder := &hiddenIndexTableRefFinder{}
			finder.scanStatement(stmt)
			assert.True(t, finder.found, "must catch %q", c.sql)
			assert.True(t, isHiddenIndexTableName(finder.table), "caught name %q for %q", finder.table, c.sql)
		})
	}

	allowed := []struct {
		name string
		sql  string
	}{
		{"plain select", "select * from base"},
		{"column with internal name", "select __mo_index_idx_col, x from base"},
		{"serial extraction", "select serial_extract(serial(1), 0 as bigint)"},
		{"subquery on base tables", "select x from base where y in (select y from base2)"},
		{"cte on base tables", "with c as (select * from base2) select * from c"},
		{"insert values", "insert into base values (1)"},
		{"insert select base", "insert into base select * from base2"},
		{"update base", "update base set y = 1 where x > 0"},
		{"delete base", "delete from base where x in (select x from base2)"},
		{"create ordinary", "create table ord_t (a int)"},
		{"alter view on base", "alter view v as select * from base"},
		{"aggregate ordering on base", "select percentile_disc(0.5) within group (order by x) from base"},
		{"window ordering on base", "select row_number() over (order by x) from base"},
		{"window partition on base", "select count(*) over (partition by x) from base"},
		{"named window on base", "select row_number() over w from base window w as (order by x)"},
		{"update ordering on base", "update base set y=1 order by x limit 1"},
		{"delete ordering on base", "delete from base order by x limit 1"},
		{"table function on base", "select * from generate_series(1,3) g"},
		{"explain physical base", "explain phyplan select * from base"},
		{"create index on base", "create index idx1 on base (y)"},
		{"drop index on base", "drop index idx1 on base"},
		{"show create base", "show create table base"},
		{"show columns base", "show columns from base"},
		{"show index base", "show index from base"},
		{"analyze base", "analyze table base(x)"},
		{"explain base", "explain select * from base"},
		{"explain analyze base", "explain analyze select * from base"},
		{"grant on base", "grant select on table db1.base to u1"},
		{"revoke on base", "revoke select on table db1.base from u1"},
		{"grant wildcard", "grant select on table db1.* to u1"},
		{"show create catalog", "show create table mo_catalog.mo_tables"},
		{"select from mo_indexes", "select index_table_name from mo_catalog.mo_indexes"},
	}
	for _, c := range allowed {
		t.Run("allowed/"+c.name, func(t *testing.T) {
			stmt := parseOneGuardTestStmt(t, c.sql)
			defer stmt.Free()
			finder := &hiddenIndexTableRefFinder{}
			finder.scanStatement(stmt)
			assert.False(t, finder.found, "must not flag %q", c.sql)
		})
	}
}

func TestDirectHiddenIndexTableAccessAllowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// no session context: not a client statement
	assert.True(t, directHiddenIndexTableAccessAllowed(nil))

	newRealUserSession := func(t *testing.T, tenant *TenantInfo) *Session {
		ses := newTestSession(t, ctrl)
		t.Cleanup(func() { ses.Close() })
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(tenant)
		return ses
	}

	t.Run("internal session bypasses", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(false)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"})
		assert.True(t, directHiddenIndexTableAccessAllowed(ses))
	})

	t.Run("ordinary user denied", func(t *testing.T) {
		ses := newRealUserSession(t, &TenantInfo{Tenant: "acc1", DefaultRole: "developer"})
		assert.False(t, directHiddenIndexTableAccessAllowed(ses))
	})

	t.Run("tenant account admin denied", func(t *testing.T) {
		ses := newRealUserSession(t, &TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"})
		assert.False(t, directHiddenIndexTableAccessAllowed(ses))
	})

	t.Run("sys tenant moadmin allowed", func(t *testing.T) {
		ses := newRealUserSession(t, &TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName})
		assert.True(t, directHiddenIndexTableAccessAllowed(ses))
	})

	t.Run("sys tenant non moadmin denied", func(t *testing.T) {
		ses := newRealUserSession(t, &TenantInfo{Tenant: sysAccountName, DefaultRole: "developer"})
		assert.False(t, directHiddenIndexTableAccessAllowed(ses))
	})

	t.Run("escape hatch on allows ordinary user", func(t *testing.T) {
		ses := newRealUserSession(t, &TenantInfo{Tenant: "acc1", DefaultRole: "developer"})
		assert.False(t, directHiddenIndexTableAccessAllowed(ses))
		require.NoError(t, ses.SetSessionSysVar(context.Background(), allowHiddenIndexTableAccessVariable, int64(1)))
		assert.True(t, directHiddenIndexTableAccessAllowed(ses))
		require.NoError(t, ses.SetSessionSysVar(context.Background(), allowHiddenIndexTableAccessVariable, int64(0)))
		assert.False(t, directHiddenIndexTableAccessAllowed(ses))
	})
}

func TestVerifyRealUserStatementAgainstHiddenIndexTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hidden := "__mo_index_secondary_b6b4_x"

	t.Run("ordinary user rejected", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "developer"})

		stmt := parseOneGuardTestStmt(t, "select * from "+hidden)
		defer stmt.Free()
		err := verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "do not have privilege to access the hidden internal table")
		assert.Contains(t, err.Error(), hidden)
	})

	t.Run("ordinary user dml rejected", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "developer"})

		for _, sql := range []string{
			"insert into db1." + hidden + " values (1)",
			"show create table " + hidden,
			"drop table " + hidden,
		} {
			stmt := parseOneGuardTestStmt(t, sql)
			err := verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt)
			require.Error(t, err, "sql %q", sql)
			stmt.Free()
		}
	})

	t.Run("ordinary user base table ok", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "developer"})

		stmt := parseOneGuardTestStmt(t, "select * from base where y in (select y from base2)")
		defer stmt.Free()
		assert.NoError(t, verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt))
	})

	t.Run("internal session allowed", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(false)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"})

		stmt := parseOneGuardTestStmt(t, "insert into "+hidden+" values (1)")
		defer stmt.Free()
		assert.NoError(t, verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt))
	})

	t.Run("sys moadmin allowed", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName})

		stmt := parseOneGuardTestStmt(t, "select * from "+hidden)
		defer stmt.Free()
		assert.NoError(t, verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt))
	})

	t.Run("escape hatch allows ordinary user", func(t *testing.T) {
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.SetFromRealUser(true)
		ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "developer"})
		require.NoError(t, ses.SetSessionSysVar(context.Background(), allowHiddenIndexTableAccessVariable, int64(1)))

		stmt := parseOneGuardTestStmt(t, "delete from "+hidden)
		defer stmt.Free()
		assert.NoError(t, verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), ses, stmt))
	})

	t.Run("nil statement", func(t *testing.T) {
		assert.NoError(t, verifyRealUserStatementAgainstHiddenIndexTables(context.Background(), nil, nil))
	})
}

// TestAuthenticateUserCanExecuteStatementHiddenIndexTable exercises the guard
// through the statement authentication entry point, the same path every
// client statement takes before execution.
func TestAuthenticateUserCanExecuteStatementHiddenIndexTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "developer"})

	stmt := parseOneGuardTestStmt(t, "select * from db1.__mo_index_unique_b6b4_y")
	defer stmt.Free()
	_, err := authenticateUserCanExecuteStatement(context.Background(), ses, stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hidden internal table")
}

func TestStoredSQLHiddenIndexCallerPolicy(t *testing.T) {
	ctx := context.Background()
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"})
	// A nested background session has no real-user identity of its own. Its
	// routine must retain the outer caller instead of gaining internal access.
	parent := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	child := &backSession{parentBackSession: parent}
	callCtx := context.WithValue(ctx, storedSQLCallerKey{}, storedSQLCaller(ctx, ses))
	require.Same(t, ses, storedSQLCaller(callCtx, child))
	const hidden = "__mo_index_secondary_probe"
	prepared := parseOneGuardTestStmt(t, "select * from "+hidden)
	t.Cleanup(prepared.Free)
	require.ErrorContains(t, verifyStoredSQLAgainstHiddenIndexTables(callCtx, prepared), "hidden internal table")
	require.NoError(t, verifyStoredSQLAgainstHiddenIndexTables(ctx, prepared), "maintenance is not a stored SQL call")

	for _, tc := range []struct {
		name string
		run  func(*Interpreter) error
	}{
		{"read", func(i *Interpreter) error { _, err := i.executeSQL("select * from " + hidden); return err }},
		{"write", func(i *Interpreter) error { _, err := i.executeSQL("delete from " + hidden); return err }},
		{"condition", func(i *Interpreter) error {
			_, err := i.EvalCond("(select count(*) from " + hidden + ") > 0")
			return err
		}},
		{"starlark SQL boundary", func(i *Interpreter) error { return i.execStoredSQL(i.ctx, "select * from "+hidden) }},
		{"declaration expression", func(i *Interpreter) error {
			stmt := parseOneGuardTestStmt(t, "select (select count(*) from "+hidden+")")
			defer stmt.Free()
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			_, err := i.evaluateStoredProcedureExpr(expr, nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			back := &backgroundExecTest{}
			back.init()
			interpreter := &Interpreter{ctx: callCtx, ses: child, bh: back}
			err := tc.run(interpreter)
			require.ErrorContains(t, err, "hidden internal table")
			require.Empty(t, back.currentSql, "denial must precede background execution")
		})
	}

	back := &backgroundExecTest{}
	back.init()
	interpreter := &Interpreter{ctx: callCtx, ses: child, bh: back}
	require.NoError(t, interpreter.execStoredSQL(callCtx, "insert into base values (1)"))
	require.Equal(t, "insert into base values (1)", back.currentSql)
	require.Error(t, interpreter.execStoredSQL(callCtx, "select * from"))
	// Maintenance/catalog callers still use the unwrapped executor.
	require.NoError(t, back.Exec(callCtx, "delete from "+hidden))

	for _, tc := range []struct {
		name   string
		real   bool
		tenant *TenantInfo
		hatch  int64
	}{
		{"debug", true, &TenantInfo{Tenant: "acc1", DefaultRole: "developer"}, 1},
		{"system admin", true, &TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName}, 0},
		{"internal", false, &TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses.SetFromRealUser(tc.real)
			ses.SetTenantInfo(tc.tenant)
			require.NoError(t, ses.SetSessionSysVar(ctx, allowHiddenIndexTableAccessVariable, tc.hatch))
			require.NoError(t, interpreter.execStoredSQL(callCtx, "select * from "+hidden))
			require.NoError(t, verifyStoredSQLAgainstHiddenIndexTables(callCtx, prepared))
			require.Equal(t, "select * from "+hidden, back.currentSql)
		})
	}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "acc1", DefaultRole: "accountadmin"})
	require.NoError(t, ses.SetSessionSysVar(ctx, allowHiddenIndexTableAccessVariable, int64(0)))
	require.ErrorContains(t, interpreter.execStoredSQL(callCtx, "select * from "+hidden), "hidden internal table")
}
