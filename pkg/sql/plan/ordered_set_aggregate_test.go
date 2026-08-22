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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func findAggregateByName(query *planpb.Query, name string) *planpb.Function {
	for _, node := range query.Nodes {
		for _, expr := range node.AggList {
			if fn := expr.GetF(); fn != nil && strings.EqualFold(fn.Func.GetObjName(), name) {
				return fn
			}
		}
	}
	return nil
}

func TestBuildOrderedSetAggregates(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name       string
		sql        string
		desc       byte
		wantOrder  bool
		wantMedian bool
	}{
		{name: "continuous", sql: "select percentile_cont(0.5) within group (order by a) from select_test.bind_select"},
		{name: "discrete descending", sql: "select percentile_disc(0.5) within group (order by a desc) from select_test.bind_select", desc: 1},
		{name: "group concat within group", sql: "select group_concat(a) within group (order by b desc) from select_test.bind_select", wantOrder: true},
		{name: "median within group", sql: "select median(a) within group (order by a desc) from select_test.bind_select", wantMedian: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			queryPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)

			name := "percentile_cont"
			if strings.Contains(tc.sql, "percentile_disc") {
				name = "percentile_disc"
			} else if strings.Contains(tc.sql, "group_concat") {
				name = "group_concat"
			} else if strings.Contains(tc.sql, "median") {
				name = "median"
			}
			fn := findAggregateByName(queryPlan.GetQuery(), name)
			require.NotNil(t, fn)
			if tc.wantOrder {
				require.NotEqual(t, planpb.AggregateConfigType_AGG_CONFIG_NONE, fn.AggConfigType)
				require.NotEmpty(t, fn.AggConfig)
				return
			}
			if tc.wantMedian {
				require.Len(t, fn.Args, 1)
				return
			}
			require.Len(t, fn.Args, 2)
			require.Equal(t, tc.desc, fn.AggConfig[0])
		})
	}
}

func TestBuildOrderedSetPercentileRejectsNonConstant(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(b) within group (order by a) from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "percentile argument of percentile_cont must be a non-null constant")
}

func TestBuildMedianWithinGroupRejectsDifferentOrderExpression(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select median(a) within group (order by b) from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "median requires the WITHIN GROUP ORDER BY expression to match")
}

func TestBuildMedianWithinGroupRejectsWindowForm(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select median(a) within group (order by a) over () from select_test.bind_select", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.Error(t, err)
}

func TestBindMedianWithinGroupRejectsInvalidShape(t *testing.T) {
	binder := &HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}
	for _, tc := range []struct {
		name string
		expr *tree.FuncExpr
		want string
	}{
		{
			name: "multiple value expressions",
			expr: &tree.FuncExpr{Exprs: tree.Exprs{nil, nil}},
			want: "median requires exactly one value expression",
		},
		{
			name: "missing order expression",
			expr: &tree.FuncExpr{Exprs: tree.Exprs{nil}},
			want: "median requires exactly one WITHIN GROUP ORDER BY expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := binder.bindMedianWithinGroupAgg(NameMedian, tc.expr, 0, false)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestBuildOrderedSetPercentileRejectsInvalidWithinGroupShape(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "missing within group",
			sql:  "select percentile_cont(0.5) from select_test.bind_select",
			want: "percentile_cont requires WITHIN GROUP",
		},
		{
			name: "multiple order expressions",
			sql:  "select percentile_cont(0.5) within group (order by a, b) from select_test.bind_select",
			want: "percentile_cont requires exactly one WITHIN GROUP ORDER BY expression",
		},
		{
			name: "null percentile",
			sql:  "select percentile_cont(null) within group (order by a) from select_test.bind_select",
			want: "percentile argument of percentile_cont must be a non-null constant",
		},
		{
			name: "non numeric order expression",
			sql:  "select percentile_cont(0.5) within group (order by n_name) from nation",
			want: "",
		},
		{
			name: "maximum width decimal continuous interpolation",
			sql:  "select percentile_cont(0.5) within group (order by cast(a as decimal(38,0))) from select_test.bind_select",
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			if tc.want == "" {
				require.Error(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestBuildOrderedSetPercentileRejectsWindowForm(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(0.5) within group (order by a) over () from select_test.bind_select", 1)
	require.NoError(t, err)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "ordered-set percentile window functions")
}
