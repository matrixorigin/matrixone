// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func parseCTASRewriteTestStatement(t *testing.T) *tree.CreateTable {
	t.Helper()
	const sql = `/*+ {"rewrites":{"db1.t":"select * from db1.t where tenant_id = 1 and marker = '__mo_query'"}} */ create table db1.copy as select * from db1.t`
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	})
	require.NoError(t, parsers.AddRewriteHints(ctx, stmts, sql))
	return stmts[0].(*tree.CreateTable)
}

func TestCTASRewriteOptionCarriesEffectiveAST(t *testing.T) {
	ctas := parseCTASRewriteTestStatement(t)
	option := ctas.AsSource.RewriteOption
	require.NotNil(t, option)
	require.Contains(t, option.Rewrites, "db1.t")
	rule := option.Rewrites["db1.t"]
	require.Equal(t, "db1", rule.DbName)
	require.Equal(t, "t", rule.TableName)
	body := tree.StringWithOpts(rule.Stmt, dialect.MYSQL, tree.WithSingleQuoteString())
	require.Contains(t, body, "tenant_id = 1")
	require.Contains(t, body, "marker = '__mo_query'")

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"insert into db1.copy select * from db1.t", 1)
	require.NoError(t, err)
	defer stmt.Free()
	attachRewriteOptionToStatement(stmt, option)
	insert := stmt.(*tree.Insert)
	require.Same(t, option, insert.Rows.RewriteOption)
	require.Equal(t, "insert into db1.copy select * from db1.t", tree.String(stmt, dialect.MYSQL))
}

func TestCTASPopulateCarriesEffectiveRewriteOption(t *testing.T) {
	injectedErr := errors.New("填充失败")
	for _, tc := range []struct {
		name   string
		temp   bool
		policy bool
		err    error
	}{
		{name: "regular", policy: true},
		{name: "temporary", temp: true, policy: true},
		{name: "error restores context", policy: true, err: injectedErr},
		{name: "without policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseCTASRewriteTestStatement(t)
			option := stmt.AsSource.RewriteOption
			if !tc.policy {
				stmt = &tree.CreateTable{AsSource: &tree.Select{}}
				option = nil
			}
			// 本分支没有 runtime 变量删除 API，替换并恢复整个隔离 runtime，
			// 不遗留 executor，也不为测试创建无关自增服务或文件服务。
			previousRuntime := moruntime.ServiceRuntime("")
			rt := moruntime.DefaultRuntime()
			moruntime.SetupServiceBasedRuntime("", rt)
			t.Cleanup(func() { moruntime.SetupServiceBasedRuntime("", previousRuntime) })
			ctx := defines.AttachAccountId(context.Background(), 0)
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			proc := process.NewTopProcess(ctx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer proc.Free()
			proc.Session = &testInternalExecutorSession{}
			c := NewCompile("test", "db1", "", "", "", nil, proc, stmt, false, nil, time.Now())
			defer c.Release()
			c.pn = &planpb.Plan{}
			internalExecutor := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, internalExecutor)
			sql := "insert into `db1`.`copy` select * from db1.t"
			wantSQL := sql
			if tc.temp {
				wantSQL = "insert into `db1`.`physical_copy` select * from db1.t"
			}
			internalExecutor.EXPECT().Exec(gomock.Any(), wantSQL, gomock.Any()).DoAndReturn(
				func(inner context.Context, _ string, _ executor.Options) (executor.Result, error) {
					if option == nil {
						require.Nil(t, getInternalExecutorRewriteOption(inner))
					} else {
						require.Same(t, option, getInternalExecutorRewriteOption(inner))
					}
					require.Same(t, proc.Session, getInternalExecutorSession(inner))
					require.True(t, needInternalExecutorPrivilegeCheck(inner))
					return executor.Result{AffectedRows: 3}, tc.err
				})
			err := c.populateCreatedTable(&planpb.CreateTable{CreateAsSelectSql: sql},
				tc.temp, "db1", "copy", "physical_copy")
			require.ErrorIs(t, err, tc.err)
			require.Same(t, ctx, proc.Ctx)
			require.Nil(t, getInternalExecutorRewriteOption(proc.Ctx))
			require.False(t, needInternalExecutorPrivilegeCheck(proc.Ctx))
			if tc.err == nil {
				require.Equal(t, uint64(3), c.getAffectedRows())
			} else {
				require.Zero(t, c.getAffectedRows())
			}
			// 非 CTAS 不调用内部执行器，也不留下前一条语句的规则。
			require.NoError(t, c.populateCreatedTable(&planpb.CreateTable{}, false, "db1", "copy", "copy"))
			require.Same(t, ctx, proc.Ctx)
		})
	}
}

func TestCTASRewriteOptionWithoutSource(t *testing.T) {
	for _, stmt := range []tree.Statement{nil, &tree.Select{}, &tree.CreateTable{}} {
		c := &Compile{stmt: stmt}
		require.Nil(t, c.ctasRewriteOption())
	}
}
