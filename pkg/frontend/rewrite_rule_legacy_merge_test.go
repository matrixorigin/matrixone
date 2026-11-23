// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestRoleRuleMergeAfterAddingRuleToLegacyTarget(t *testing.T) {
	ctx := context.WithValue(t.Context(), defines.TenantIDKey{}, uint32(0))
	ses := newSes(&privilege{}, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.sesSysVars.Set("lower_case_table_names", int64(1))
	ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
	bh := &backgroundExecTest{}
	bh.init()
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)

	parse := func(sql string) tree.Statement {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		return stmt
	}

	roleSQL, err := getSqlForRoleIdOfRole(ctx, "later")
	require.NoError(t, err)
	bh.sql2result[roleSQL] = newMrsForRoleIdOfRole([][]interface{}{{int64(20)}})
	targetSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "t")
	require.NoError(t, err)
	bh.sql2result[targetSQL] = newMrsForCheckDatabaseTable([][]interface{}{{int64(100)}})
	bh.sql2result["select rule_name from mo_catalog.mo_role_rule where role_id = 20"] = newMrsForSqlForShowDatabases(nil)
	add := parse("alter role later add rule 'select id from db.t where id = 2' on table db.` t`").(*tree.AlterRoleAddRule)
	require.NoError(t, handleAlterRoleAddRule(ses, &ExecCtx{reqCtx: ctx, ses: ses}, add))

	// The primary role already owns the spelling stored by an earlier ADD.
	// Replay the actual committed INSERT for the newly added inherited rule.
	catalogRows := [][]interface{}{{int64(10), "db. t", "select id from db.t where id = 1"}}
	for _, sql := range bh.executedSQLs {
		if !strings.HasPrefix(sql, "insert into mo_catalog.mo_role_rule ") {
			continue
		}
		insert := parse(sql).(*tree.Insert)
		row := insert.Rows.Select.(*tree.ValuesClause).Rows[0]
		catalogRows = append(catalogRows, []interface{}{
			int64(20), row[1].(*tree.NumVal).String(), row[2].(*tree.NumVal).String(),
		})
	}
	require.Len(t, catalogRows, 2)
	require.Equal(t, "commit;", bh.executedSQLs[len(bh.executedSQLs)-1])
	bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId(
		[][]interface{}{{int64(20), false}})
	bh.sql2result[getSqlForRoleRulesOfRoleIDs([]int64{10, 20})] = newMrsForRewriteRules(catalogRows)

	const query = "select id from db.t"
	policy, err := captureRewritePolicy(ctx, ses)
	require.NoError(t, err)
	rewritten, err := policy.rewrite(ctx, query, "")
	require.NoError(t, err)
	stmt := parse(rewritten)
	require.NoError(t, parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
		ctx, []tree.Statement{stmt}, rewritten, "", 1))
	rules := stmt.(*tree.Select).RewriteOption.Rewrites["db.t"]
	require.Len(t, rules, 1)
	filter := rules[0].Stmt.(*tree.Select).Select.(*tree.SelectClause).Where.Expr
	require.IsType(t, &tree.OrExpr{}, filter, "stored keys %q and %q, rewrite %s",
		catalogRows[0][1], catalogRows[1][1], rewritten)
	or := filter.(*tree.OrExpr)
	for i, operand := range []tree.Expr{or.Left, or.Right} {
		equal := operand.(*tree.ParenExpr).Expr.(*tree.ComparisonExpr)
		require.Equal(t, tree.EQUAL, equal.Op)
		require.Equal(t, "id", equal.Left.(*tree.UnresolvedName).ColName())
		value, ok := equal.Right.(*tree.NumVal).Int64()
		require.True(t, ok)
		require.Equal(t, int64(i+1), value)
	}
}

func TestLoadRuleCacheLegacyTargetComparison(t *testing.T) {
	const left = "select id from source.t where id = 1"
	const right = "select id from source.t where id = 2"
	for _, tc := range []struct {
		name string
		mode int64
		want map[string]string
	}{
		{"case sensitive", 0, map[string]string{"DB.T": left, "db.t": right}},
		{"case insensitive", 2, map[string]string{
			"db.t": "select id from source.t where (id = 1) or (id = 2)",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSes(&privilege{}, gomock.NewController(t))
			t.Cleanup(ses.Close)
			ses.sesSysVars.Set("lower_case_table_names", tc.mode)
			ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
			bh := &backgroundExecTest{}
			bh.init()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)
			bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{{20, false}})
			bh.sql2result[getSqlForRoleRulesOfRoleIDs([]int64{10, 20})] = newMrsForRewriteRules([][]interface{}{
				{10, "DB. T", left},
				{20, "db.t", right},
			})

			rules, _, err := loadRuleCache(t.Context(), ses, tc.mode)
			require.NoError(t, err)
			for target, rule := range rules {
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rule, tc.mode)
				require.NoError(t, err)
				rules[target] = tree.String(stmt, dialect.MYSQL)
				stmt.Free()
			}
			require.Equal(t, tc.want, rules)
		})
	}
}

func TestLoadRuleCacheRejectsInvalidLegacyRules(t *testing.T) {
	for _, tc := range []struct {
		name   string
		roleID int64
	}{
		{"same role", 20},
		{"earlier role", 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSes(&privilege{}, gomock.NewController(t))
			t.Cleanup(ses.Close)
			ses.sesSysVars.Set("lower_case_table_names", int64(1))
			ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
			bh := &backgroundExecTest{}
			bh.init()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)
			bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{{20, false}})
			bh.sql2result[getSqlForRoleRulesOfRoleIDs([]int64{10, 20})] = newMrsForRewriteRules([][]interface{}{
				{tc.roleID, "db. t", "delete from db.t"},
				{20, "db.t", "select id from db.t where id = 2"},
			})

			policy, err := captureRewritePolicy(t.Context(), ses)
			require.ErrorContains(t, err, "only accept SELECT-like statements")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError))
			require.Nil(t, policy)
			require.Nil(t, ses.ruleCache)
			require.Equal(t, "rollback;", bh.executedSQLs[len(bh.executedSQLs)-1])
		})
	}
}
