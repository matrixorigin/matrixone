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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestCTASRewriteOptionCarriesEffectiveAST(t *testing.T) {
	ctx := context.Background()
	outerSQL := `/*+ {"rewrites":{"db2.t1":"select * from db2.t1 where note = 'a\\b'"},"remapdb":{"db1":"db2"}} */ create table db2.copy as select * from db2.t1`
	stmts, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, outerSQL, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.NoError(t, parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
		ctx, stmts, outerSQL, "NO_BACKSLASH_ESCAPES", 1))
	createTable, ok := stmts[0].(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, createTable.AsSource.RewriteOption)
	option := createTable.AsSource.RewriteOption
	require.Equal(t, map[string]string{"db1": "db2"}, option.RemapDb)
	require.Contains(t, option.Rewrites, "db2.t1")
	require.NotContains(t, option.Rewrites, "db1.t1")
	require.Len(t, option.Rewrites["db2.t1"], 1)
	require.Contains(t, tree.StringWithOpts(
		option.Rewrites["db2.t1"][0].Stmt,
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
	), `note = 'a\\b'`)

	generated := "insert into `db2`.`copy` select * from db2.t1"
	inner, err := parsers.Parse(ctx, dialect.MYSQL, generated, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range inner {
			stmt.Free()
		}
	}()
	attachRewriteOptionToStatement(inner[0], option)
	insert, ok := inner[0].(*tree.Insert)
	require.True(t, ok)
	require.Same(t, option, insert.Rows.RewriteOption)
}

func TestCTASPopulateCarriesEffectiveRewriteOption(t *testing.T) {
	ctx := context.Background()
	outerSQL := `/*+ {"rewrites":{"dst_db.t":"select * from dst_db.t where note = '__mo_query'"},"remapdb":{"src_db":"dst_db"}} */ create table dst_db.copy as select * from dst_db.t`
	stmts, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, outerSQL, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.NoError(t, parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
		ctx, stmts, outerSQL, "NO_BACKSLASH_ESCAPES", 1))
	createTable, ok := stmts[0].(*tree.CreateTable)
	require.True(t, ok)
	option := createTable.AsSource.RewriteOption
	require.NotNil(t, option)
	require.Contains(t, option.Rewrites, "dst_db.t")
	require.Contains(t, tree.StringWithOpts(
		option.Rewrites["dst_db.t"][0].Stmt,
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
	), "__mo_query")

	proc := newPlanTestProcess(t)
	proc.Ctx = ctx
	c := NewCompile("test", "dst_db", outerSQL, "", "", nil, proc, createTable, false, nil, time.Now())
	defer c.Release()
	c.pn = &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}}}
	internalExecutor := &recordingInternalSQLExecutor{mocker: func(string) (executor.Result, error) {
		return executor.Result{}, nil
	}}
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

	qry := &planpb.CreateTable{CreateAsSelectSql: "insert into `dst_db`.`copy` select * from dst_db.t"}
	require.NoError(t, c.populateCreatedTable(qry, false, "dst_db", "copy", "copy"))
	require.Len(t, internalExecutor.contexts, 1)
	require.Same(t, option, getInternalExecutorRewriteOption(internalExecutor.contexts[0]))
}
