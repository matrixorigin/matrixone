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

func TestDirectPreparedCTASSourceSQL(t *testing.T) {
	ctx := context.Background()
	const source = "create table db1.copy as select '__mo_query;from', 'secret_access_key=original' from db1.source"
	for _, prefix := range []string{
		"prepare stmt from ",
		"/* from misleading */ prepare `from` FROM\n",
		`/*+ {"rewrites":{"db1.source":"select * from db1.source"}} */ prepare stmt from `,
		"-- from misleading\n prepare stmt /* from misleading */ from ",
	} {
		got, err := directPreparedCTASSourceSQL(ctx, prefix+source)
		require.NoError(t, err)
		require.Equal(t, source, got)
	}
	for _, sql := range []string{"", "select 1", "prepare stmt", "prepare stmt from"} {
		_, err := directPreparedCTASSourceSQL(ctx, sql)
		require.Error(t, err)
	}
	// 与日志相同的切分索引，但不能使用其脱敏结果；内层分号不分割语句。
	batch := "select 1; /* from misleading */ prepare stmt from " + source + "; execute stmt"
	parts := parsers.SplitSqlBySemicolon(batch)
	require.Len(t, parts, 3)
	got, err := directPreparedCTASSourceSQL(ctx, parts[1])
	require.NoError(t, err)
	require.Equal(t, source, got)
	ec := &ExecCtx{rawSQLOfStmt: source}
	ec.Close()
	require.Empty(t, ec.rawSQLOfStmt)
}

func TestDirectPreparedCTASExecutableCommentSource(t *testing.T) {
	ctx := context.Background()
	const source = "create table db1.copy as select 'marker*/tail;from' as marker"
	for _, sql := range []string{
		"/*! prepare p from " + source + " */",
		"/*!50700 prepare p from " + source + " */",
		"/*! prepare p from */ " + source,
		"/*! prepare p */ from " + source,
	} {
		got, err := directPreparedCTASSourceSQL(ctx, sql)
		require.NoError(t, err)
		require.Equal(t, source, got, "只去掉包装边界，保留字符串中的关闭符和分号")
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, got, 1)
		require.NoError(t, err)
		stmt.Free()
	}
	_, err := directPreparedCTASSourceSQL(ctx, "/*! prepare p from "+source)
	require.Error(t, err, "无关闭边界必须拒绝")
}

func TestPreparedCTASTargetDatabaseBinding(t *testing.T) {
	for _, tc := range []struct{ table, database, want string }{
		{"copy", "a", "a"},
		{"b.copy", "a", "b"},
		{"copy", "", ""},
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "create table "+tc.table+" as select 1", 1)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			ctas := stmt.(*tree.CreateTable)
			bindPreparedCTASTargetDatabase(ctas, tc.database)
			require.Equal(t, tc.want, string(ctas.Table.SchemaName))
			require.Equal(t, tc.want != "", ctas.Table.ExplicitSchema)
			if tc.want != "" {
				bindPreparedCTASTargetDatabase(ctas, "changed")
				require.Equal(t, tc.want, string(ctas.Table.SchemaName))
			}
		}()
	}
}

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
