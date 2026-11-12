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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
)

func parseCTASRewriteTestStatement(t *testing.T) *tree.CreateTable {
	t.Helper()
	const sql = `/*+ {"rewrites":{"dst_db.t":"select * from dst_db.t where note = 'a\\b' and marker = '__mo_query'"},"remapdb":{"src_db":"dst_db"}} */ create table dst_db.copy as select * from dst_db.t`
	ctx := context.Background()
	stmts, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, sql, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	})
	require.NoError(t, parsers.AddRewriteHintsWithSQLMode(ctx, stmts, sql, "NO_BACKSLASH_ESCAPES"))
	return stmts[0].(*tree.CreateTable)
}

func TestCTASRewriteOptionCarriesEffectiveAST(t *testing.T) {
	ctas := parseCTASRewriteTestStatement(t)
	option := ctas.AsSource.RewriteOption
	require.NotNil(t, option)
	require.Equal(t, map[string]string{"src_db": "dst_db"}, option.RemapDb)
	require.Contains(t, option.Rewrites, "dst_db.t")
	require.NotContains(t, option.Rewrites, "src_db.t")
	body := tree.StringWithOpts(option.Rewrites["dst_db.t"][0].Stmt, dialect.MYSQL, tree.WithSingleQuoteString())
	require.Contains(t, body, `note = 'a\\b'`)
	require.Contains(t, body, "__mo_query")

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"insert into dst_db.copy select * from dst_db.t", 1)
	require.NoError(t, err)
	defer stmt.Free()
	attachRewriteOptionToStatement(stmt, option)
	insert := stmt.(*tree.Insert)
	require.Same(t, option, insert.Rows.RewriteOption)
	require.Equal(t, "insert into dst_db.copy select * from dst_db.t", tree.String(stmt, dialect.MYSQL))
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
			proc := testutil.NewProcess(t)
			defer proc.Free()
			ctx := proc.Ctx
			proc.Session = &testInternalExecutorSession{}
			c := NewCompile("test", "dst_db", "", "", "", nil, proc, stmt, false, nil, time.Now())
			defer c.Release()
			c.pn = &planpb.Plan{}
			internalExecutor := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, internalExecutor)
			t.Cleanup(func() {
				if hadPrevious {
					rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, internalExecutor)
				}
			})
			sql := "insert into `dst_db`.`copy` select * from dst_db.t"
			wantSQL := sql
			if tc.temp {
				wantSQL = "insert into `dst_db`.`physical_copy` select * from dst_db.t"
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
				tc.temp, "dst_db", "copy", "physical_copy")
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
			require.NoError(t, c.populateCreatedTable(&planpb.CreateTable{}, false, "dst_db", "copy", "copy"))
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
