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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestPreparedCTASPreservesExplicitHints(t *testing.T) {
	ctx := context.Background()
	const sql = `/*+ {"rewrites":{"db1.source":"select amount from db1.source where marker = '__mo_query'"}} */ create table db1.snapshot as select * from db1.source`
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	ctas := stmt.(*tree.CreateTable)
	ses := &Session{}
	ses.ruleCache = map[string]string{}
	ses.rewriteEnabled.Store(true)
	require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
	require.Equal(t, "select amount from db1.source where marker = '__mo_query'", ctas.AsSource.RewriteOption.Rewrites["db1.source"].SQL)
	ses.rewriteEnabled.Store(false)
	require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
	require.Nil(t, ctas.AsSource.RewriteOption)
}

func TestPreparedCTASRewriteUsesCurrentRules(t *testing.T) {
	ctx := context.Background()
	const sql = "create table db1.snapshot as (select * from db1.source)"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	ctas := stmt.(*tree.CreateTable)
	ses := &Session{}
	ses.rewriteEnabled.Store(true)
	for _, rule := range []string{
		"select amount from db1.source where tenant_id = 1 and marker = '__mo_query'",
		"select * from db1.source where tenant_id = 2",
	} {
		ses.ruleCache = map[string]string{"db1.source": rule}
		require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
		require.Len(t, ctas.AsSource.RewriteOption.Rewrites, 1)
		require.Equal(t, rule, ctas.AsSource.RewriteOption.Rewrites["db1.source"].SQL)
	}
	// 角色变宽或关闭开关后，不能保留上一轮较窄角色的旧投影。
	ses.ruleCache = map[string]string{}
	require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
	require.Nil(t, ctas.AsSource.RewriteOption)
	ses.ruleCache = map[string]string{"db1.source": "select amount from db1.source"}
	require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
	ses.rewriteEnabled.Store(false)
	require.NoError(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
	require.Nil(t, ctas.AsSource.RewriteOption)
	// 错误策略必须拒绝执行，不能以无策略继续准备。
	ses.rewriteEnabled.Store(true)
	ses.ruleCache = map[string]string{"db1.source": "delete from db1.source"}
	require.Error(t, applyPreparedCTASRewrite(ctx, ses, ctas, sql))
}

type preparedASTOwnerProbe struct {
	tree.Statement
	freed int
}

func (p *preparedASTOwnerProbe) Free() { p.freed++ }

func TestBinaryPreparedWrapperBorrowsASTOnEarlyFailure(t *testing.T) {
	stmt := &preparedASTOwnerProbe{}
	p := &plan.Plan{Plan: &plan.Plan_Dcl{Dcl: &plan.DataControl{
		Control: &plan.DataControl_Prepare{Prepare: &plan.Prepare{Plan: &plan.Plan{}}},
	}}}
	ec := &ExecCtx{input: &UserInput{stmt: stmt, preparePlan: p, isBinaryProtExecute: true}}
	wrappers, err := GetComputationWrapper(ec, "", "", nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, wrappers, 1)
	wrappers[0].(*TxnComputationWrapper).Free()
	require.Zero(t, stmt.freed, "执行前失败只能释放 wrapper，不能释放 prepared owner 的 AST")
	stmt.Free()
	require.Equal(t, 1, stmt.freed)
}

func TestPreparedCTASErrorsKeepHandle(t *testing.T) {
	ctx := context.Background()
	old, err := parsers.ParseOne(ctx, dialect.MYSQL, "create table db1.copy as select * from db1.source", 1)
	require.NoError(t, err)
	defer old.Free()
	oldPlan := &plan.Plan{}
	ses := &Session{}
	ses.rewriteEnabled.Store(true)
	ses.ruleCache = map[string]string{"db1.source": "delete from db1.source"}
	for _, sql := range []string{
		"select (",
		"select 1",
		"create table db1.copy (id int)",
		"create table db1.copy as select * from db1.source",
	} {
		p := &PrepareStmt{Sql: sql, PrepareStmt: old, PreparePlan: oldPlan}
		require.Error(t, refreshPreparedCTAS(ctx, ses, p))
		require.Same(t, old, p.PrepareStmt)
		require.Same(t, oldPlan, p.PreparePlan)
		require.NotNil(t, old.(*tree.CreateTable).AsSource)
	}
	// SQL PREPARE 拒绝多语句，并且错误规则不能发布 prepared handle。
	for _, sql := range []string{"select 1; select 2", "create table db1.copy as select * from db1.source"} {
		p, err := doPrepareString(ses, &ExecCtx{reqCtx: ctx}, &tree.PrepareString{Name: "bad_ctas", Sql: sql})
		require.Error(t, err)
		require.Nil(t, p)
	}
}

func TestRefreshPreparedCTASLeavesOtherStatementsAlone(t *testing.T) {
	for _, stmt := range []tree.Statement{nil, &tree.Select{}, &tree.CreateTable{}} {
		p := &PrepareStmt{PrepareStmt: stmt}
		require.NoError(t, refreshPreparedCTAS(context.Background(), nil, p))
		require.Equal(t, stmt, p.PrepareStmt)
	}
}
