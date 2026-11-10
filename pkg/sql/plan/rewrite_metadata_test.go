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

package plan

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestRewriteVisibleColumns(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		expected  map[string]struct{}
		all       bool
		supported bool
	}{
		{
			name:      "projection",
			sql:       "select id, amount from db.controlled where tenant = 1",
			expected:  map[string]struct{}{"id": {}, "amount": {}},
			supported: true,
		},
		{
			name:      "qualified projection",
			sql:       "select db.controlled.id, controlled.amount from db.controlled",
			expected:  map[string]struct{}{"id": {}, "amount": {}},
			supported: true,
		},
		{
			name:      "matching alias",
			sql:       "select id as id, amount from db.controlled",
			expected:  map[string]struct{}{"id": {}, "amount": {}},
			supported: true,
		},
		{
			name: "renamed alias fails closed",
			sql:  "select id as identifier from db.controlled",
		},
		{
			name: "implicit database projection fails closed",
			sql:  "select id from controlled",
		},
		{
			name: "implicit database star fails closed",
			sql:  "select * from controlled",
		},
		{
			name: "implicit database alias fails closed",
			sql:  "select a.id from controlled as a",
		},
		{
			name: "implicit database qualified star fails closed",
			sql:  "select controlled.* from controlled",
		},
		{
			name: "different database fails closed",
			sql:  "select id from other.controlled",
		},
		{
			name:      "star",
			sql:       "select * from db.controlled where tenant = 1",
			all:       true,
			supported: true,
		},
		{
			name: "historical star fails closed",
			sql:  "select * from db.controlled {snapshot='s'}",
		},
		{
			name: "historical projection fails closed",
			sql:  "select id from db.controlled {timestamp='2026-01-01 00:00:00'}",
		},
		{
			name:      "source alias",
			sql:       "select a.id from db.controlled as a",
			expected:  map[string]struct{}{"id": {}},
			supported: true,
		},
		{
			name:      "source alias star",
			sql:       "select a.* from db.controlled as a",
			all:       true,
			supported: true,
		},
		{
			name:      "qualified star",
			sql:       "select controlled.* from db.controlled",
			all:       true,
			supported: true,
		},
		{
			name: "invalid column qualifier",
			sql:  "select absent.secret from db.controlled",
		},
		{
			name: "invalid database qualifier",
			sql:  "select other.controlled.secret from db.controlled",
		},
		{
			name: "original qualifier hidden by alias",
			sql:  "select controlled.secret from db.controlled as a",
		},
		{
			name: "invalid star qualifier",
			sql:  "select absent.* from db.controlled",
		},
		{
			name: "star followed by invalid column",
			sql:  "select *, absent.secret from db.controlled",
		},
		{
			name: "star followed by invalid star",
			sql:  "select *, absent.* from db.controlled",
		},
		{
			name: "star followed by computed expression",
			sql:  "select *, id+1 from db.controlled",
		},
		{
			name: "grouped star fails closed",
			sql:  "select * from db.controlled group by id",
		},
		{
			name: "computed expression fails closed",
			sql:  "select id + 1 from db.controlled",
		},
		{
			name: "join fails closed",
			sql:  "select controlled.id from db.controlled join db.other on controlled.id = other.id",
		},
		{
			name: "cte fails closed",
			sql:  "with controlled as (select id from db.controlled) select * from controlled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parseRewriteSelect(t, test.sql)
			names, all, supported := rewriteVisibleColumns(stmt, "db", "controlled", 1)
			require.Equal(t, test.expected, names)
			require.Equal(t, test.all, all)
			require.Equal(t, test.supported, supported)
		})
	}
}

func TestRewriteVisibleColumnsChainIntersects(t *testing.T) {
	inner := parseRewriteSelect(t, "select id, tenant, amount from db.controlled")
	outer := parseRewriteSelect(t, "select id, amount from db.controlled")
	left, leftAll, leftOK := rewriteVisibleColumns(inner, "db", "controlled", 1)
	right, rightAll, rightOK := rewriteVisibleColumns(outer, "db", "controlled", 1)
	require.True(t, leftOK && rightOK)
	require.False(t, leftAll || rightAll)
	require.Equal(t, map[string]struct{}{"id": {}, "amount": {}}, intersectRewriteColumnNames(left, right))
}

func TestApplyRewriteMetadataVisibilityFailClosed(t *testing.T) {
	builder, ctx := newRewriteMetadataTestContext(t)
	ctx.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
		"db.controlled": {{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select id + 1 from db.controlled")}},
	}}
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
	filtered, err := builder.applyRewriteMetadataVisibility(child, ctx, INFORMATION_SCHEMA, informationSchemaColumnsTable)
	require.NoError(t, err)
	require.NotEqual(t, child, filtered)
	require.Equal(t, planpb.Node_FILTER, builder.qry.Nodes[filtered].NodeType)
	require.Len(t, builder.qry.Nodes[filtered].FilterList, 1)
	require.True(t, builder.qry.Nodes[filtered].NotCacheable)
	require.False(t, evalRewriteMetadataPredicate(t, builder, builder.qry.Nodes[filtered].FilterList[0], "db", "controlled", "id"))
}

func TestApplyRewriteMetadataVisibilityAllowsProjectionAndIntersects(t *testing.T) {
	builder, ctx := newRewriteMetadataTestContext(t)
	ctx.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
		"db.controlled": {
			{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select id, tenant, amount from db.controlled")},
			{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select id, amount from db.controlled")},
		},
	}}
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
	filtered, err := builder.applyRewriteMetadataVisibility(child, ctx, "INFORMATION_SCHEMA", "COLUMNS")
	require.NoError(t, err)
	expr := builder.qry.Nodes[filtered].FilterList[0]
	require.True(t, evalRewriteMetadataPredicate(t, builder, expr, "db", "controlled", "id"))
	require.True(t, evalRewriteMetadataPredicate(t, builder, expr, "db", "controlled", "amount"))
	require.False(t, evalRewriteMetadataPredicate(t, builder, expr, "db", "controlled", "tenant"))
	require.False(t, evalRewriteMetadataPredicate(t, builder, expr, "db", "controlled", "secret"))
	require.True(t, evalRewriteMetadataPredicate(t, builder, expr, "db", "other", "secret"))
}

func TestApplyRewriteMetadataVisibilityEmptyIntersectionIsDenyAll(t *testing.T) {
	builder, ctx := newRewriteMetadataTestContext(t)
	ctx.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
		"db.controlled": {
			{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select id from db.controlled")},
			{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select amount from db.controlled")},
		},
	}}
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
	filtered, err := builder.applyRewriteMetadataVisibility(child, ctx, INFORMATION_SCHEMA, informationSchemaColumnsTable)
	require.NoError(t, err)
	require.False(t, evalRewriteMetadataPredicate(t, builder, builder.qry.Nodes[filtered].FilterList[0], "db", "controlled", "id"))
}

func TestRewriteMetadataHelpers(t *testing.T) {
	rewrite := &tree.Rewrite{DbName: "db", TableName: "table"}
	require.Equal(t, "db", func() string { db, _ := rewriteTargetName("key", rewrite); return db }())
	db, table := rewriteTargetName("db.table", &tree.Rewrite{})
	require.Equal(t, "db", db)
	require.Equal(t, "table", table)
	db, table = rewriteTargetName("invalid", nil)
	require.Empty(t, db)
	require.Empty(t, table)
	require.True(t, isSystemDatabase("MO_CATALOG"))
	require.False(t, isSystemDatabase("application"))
	require.Empty(t, intersectRewriteColumnNames(nil, map[string]struct{}{"id": {}}))
	require.Empty(t, intersectRewriteColumnNames(map[string]struct{}{"id": {}}, nil))

	ctx := &BindContext{projects: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int32)}}}}
	require.Equal(t, int32(types.T_int32), rewriteMetadataOutputType(ctx, 0).Id)
	require.Equal(t, makePlan2StringConstExprWithType("").Typ.Id, rewriteMetadataOutputType(&BindContext{}, 0).Id)
	combined, err := combineRewriteMetadataExprs(context.Background(), "or", nil)
	require.NoError(t, err)
	require.Nil(t, combined)
	combined, err = combineRewriteMetadataExprs(context.Background(), "or", []*planpb.Expr{makePlan2BoolConstExprWithType(true)})
	require.NoError(t, err)
	require.True(t, combined.GetLit().GetBval())
}

func newRewriteMetadataTestContext(t *testing.T) (*QueryBuilder, *BindContext) {
	t.Helper()
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, true, false)
	ctx := NewBindContext(builder, nil)
	ctx.projectTag = 7
	ctx.headings = []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"}
	textType := planpb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}
	ctx.results = []*planpb.Expr{{Typ: textType}, {Typ: textType}, {Typ: textType}}
	return builder, ctx
}

// Execute the bound predicate through the production expression evaluator.
// Substitute only input catalog cells, keeping every comparison/function intact.
func evalRewriteMetadataPredicate(t *testing.T, builder *QueryBuilder, expr *planpb.Expr, schema, table, column string) bool {
	t.Helper()
	input := []string{schema, table, column}
	bound := DeepCopyExpr(expr)
	var replace func(*planpb.Expr)
	replace = func(e *planpb.Expr) {
		if col := e.GetCol(); col != nil {
			*e = *makePlan2StringConstExprWithType(input[col.ColPos])
			return
		}
		if f := e.GetF(); f != nil {
			for _, arg := range f.Args {
				replace(arg)
			}
		}
	}
	replace(bound)
	proc := builder.compCtx.GetProcess()
	executor, err := colexec.NewExpressionExecutor(proc, bound)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsNull(0))
	return vector.GetFixedAtWithTypeCheck[bool](result, 0)
}

type rewriteMetadataModeCompiler struct {
	*MockCompilerContext
	mode int64
}

func (c *rewriteMetadataModeCompiler) GetLowerCaseTableNames() int64 { return c.mode }

func TestRewriteMetadataIdentifierIdentity(t *testing.T) {
	for _, mode := range []int64{0, 1, 2} {
		t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
			compiler := &rewriteMetadataModeCompiler{NewMockCompilerContext(false, newPlanTestProcess(t)), mode}
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, true, false)
			_, ctx := newRewriteMetadataTestContext(t)
			ctx.lower = 1 // Stored view mode must not override the caller mode.
			sql := "select id from DbCase.Controlled"
			stmts, err := mysql.Parse(context.Background(), sql, mode)
			require.NoError(t, err)
			ctx.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
				"DbCase.Controlled": {{DbName: "DbCase", TableName: "Controlled", Stmt: stmts[0]}},
			}}
			child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
			filtered, err := builder.applyRewriteMetadataVisibility(child, ctx, INFORMATION_SCHEMA, informationSchemaColumnsTable)
			require.NoError(t, err)
			expr := builder.qry.Nodes[filtered].FilterList[0]
			require.True(t, evalRewriteMetadataPredicate(t, builder, expr, "DbCase", "Controlled", "id"))
			require.False(t, evalRewriteMetadataPredicate(t, builder, expr, "DbCase", "Controlled", "secret"))
			require.Equal(t, mode == 0, evalRewriteMetadataPredicate(t, builder, expr, "dbcase", "controlled", "secret"))
			require.True(t, evalRewriteMetadataPredicate(t, builder, expr, "DbCase", "Unrelated", "secret"))
		})
	}
}

func parseRewriteSelect(t *testing.T, sql string) *tree.Select {
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	selectStmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)
	return selectStmt
}
