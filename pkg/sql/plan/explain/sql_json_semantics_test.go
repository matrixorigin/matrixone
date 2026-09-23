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

package explain

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	planpkg "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestSQLJSONPlannerLiteralVectorDifferentials(t *testing.T) {
	// Exercise parse, bind, optimize and constant-fold before rendering. The
	// typed assertion ensures the public SQL actually reaches LiteralVec.
	for _, tc := range []struct {
		name, column, first, second, wantFirst, wantSecond string
	}{
		{"datetime_microseconds", "cast(n_name as datetime(6))",
			"cast('2024-01-02 03:04:05.123456' as datetime(6)), cast('2024-02-01' as datetime(6))",
			"cast('2024-01-02 03:04:05.123457' as datetime(6)), cast('2024-02-01' as datetime(6))",
			"03:04:05.123456", "03:04:05.123457"},
		{"timestamp_microseconds", "cast(n_name as timestamp(6))",
			"cast('2024-01-02 03:04:05.123456' as timestamp(6)), cast('2024-02-01' as timestamp(6))",
			"cast('2024-01-02 03:04:05.123457' as timestamp(6)), cast('2024-02-01' as timestamp(6))",
			"03:04:05.123456", "03:04:05.123457"},
		{"decimal_scale", "cast(n_nationkey as decimal(10,2))",
			"cast(1.20 as decimal(10,2)), cast(12.00 as decimal(10,2))",
			"cast(1.21 as decimal(10,2)), cast(12.00 as decimal(10,2))", "1.20", "1.21"},
		{"string_boundaries", "n_name", "'a b', 'c'", "'a', 'b c'", `"a b"`, `"b c"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			render := func(list string) string {
				sql := fmt.Sprintf("select n_name from nation where %s in (%s)", tc.column, list)
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				compiler := planpkg.NewMockCompilerContext(true)
				compiler.GetProcess().GetSessionInfo().TimeZone = time.UTC
				query, err := planpkg.NewBaseOptimizer(compiler).Optimize(stmt, false)
				require.NoError(t, err)
				var vectors int
				var visit func(*plan.Expr)
				visit = func(e *plan.Expr) {
					if e == nil {
						return
					}
					if e.GetVec() != nil {
						vectors++
					}
					if f := e.GetF(); f != nil {
						for _, a := range f.Args {
							visit(a)
						}
					}
					if l := e.GetList(); l != nil {
						for _, a := range l.List {
							visit(a)
						}
					}
				}
				for _, n := range query.Nodes {
					for _, e := range n.FilterList {
						visit(e)
					}
					for _, e := range n.ProjectList {
						visit(e)
					}
				}
				require.Positive(t, vectors, "SQL must reach the folded LiteralVec path")
				data, err := BuildSQLJSONPlan(t.Context(), query)
				require.NoError(t, err)
				var document struct {
					MatrixOne sqlJSONMatrixOne `json:"matrixone"`
				}
				require.NoError(t, json.Unmarshal(data, &document))
				var filters []string
				for _, n := range document.MatrixOne.Nodes {
					filters = append(filters, n.Filter)
				}
				return strings.Join(filters, "\n")
			}
			a, b := render(tc.first), render(tc.second)
			require.NotEqual(t, a, b)
			require.Contains(t, a, tc.wantFirst)
			require.Contains(t, b, tc.wantSecond)
		})
	}
}

func TestSQLJSONPlannerLiteralVectorBeyondDisplayLimit(t *testing.T) {
	values := make([]string, 17)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	render := func() string {
		nodes := buildPlannerSQLJSONNodes(t, "select n_name from nation where n_nationkey in ("+strings.Join(values, ",")+")")
		var filters []string
		for _, n := range nodes {
			filters = append(filters, n.Filter)
		}
		return strings.Join(filters, "\n")
	}
	a := render()
	values[16] = "18"
	b := render()
	require.NotEqual(t, a, b)
	require.Contains(t, a, "17")
	require.Contains(t, b, "18")
	require.NotContains(t, a, "... 17 values")
}

func TestSQLJSONPlannerAggregateDistinctControls(t *testing.T) {
	agg := func(distinct string) string {
		nodes := buildPlannerSQLJSONNodes(t, "select sum("+distinct+" n_regionkey), sum(n_nationkey) from nation")
		for _, n := range nodes {
			if n.Aggregate != "" {
				return n.Aggregate
			}
		}
		t.Fatal("aggregate node missing")
		return ""
	}
	a, b := agg("distinct"), agg("")
	require.Contains(t, a, "DISTINCT")
	require.NotContains(t, b, "DISTINCT")
	require.Equal(t, strings.ReplaceAll(a, "DISTINCT ", ""), b)
}

func TestSQLJSONPlannerSampleControls(t *testing.T) {
	sample := func(group, expr string) sqlJSONNode {
		nodes := buildPlannerSQLJSONNodes(t, "select "+group+", sample("+expr+", 2 rows) from nation group by "+group)
		for _, n := range nodes {
			if strings.Contains(strings.ToLower(n.Operator), "sample") {
				return n
			}
		}
		t.Fatal("sample node missing")
		return sqlJSONNode{}
	}
	first := sample("n_regionkey", "n_nationkey + 1")
	otherGroup := sample("n_name", "n_nationkey + 1")
	otherExpr := sample("n_regionkey", "n_nationkey + 2")
	require.NotEqual(t, first.GroupBy, otherGroup.GroupBy)
	require.NotEqual(t, first.Aggregate, otherExpr.Aggregate)
	require.Contains(t, first.Aggregate, "+ 1")
	require.Contains(t, otherExpr.Aggregate, "+ 2")
}

func TestSQLJSONPlannerSamplePolicyDifferentials(t *testing.T) {
	for _, tc := range []struct {
		policy, count, mode string
	}{
		{"1 rows", "sample_rows=1", "sample_using_row=false"},
		{"100 rows", "sample_rows=100", "sample_using_row=false"},
		{"1 rows, 'row'", "sample_rows=1", "sample_using_row=true"},
		{"12.5 percent", "sample_percent=12.5", "sample_using_row=true"},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			nodes := buildPlannerSQLJSONNodes(t, "select sample(n_nationkey, "+tc.policy+") from nation")
			var found bool
			for _, node := range nodes {
				if strings.Contains(strings.ToLower(node.Operator), "sample") {
					found = true
					require.Contains(t, node.Expressions, tc.count)
					require.Contains(t, node.Expressions, tc.mode)
				}
			}
			require.True(t, found, "public SQL must reach SAMPLE")
		})
	}
}

func TestSQLJSONPlannerVectorLiteral(t *testing.T) {
	for _, value := range []string{"[1,1,1]", "[2,2,2]"} {
		t.Run(value, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "select cast('"+value+"' as vecf32(3))", 1)
			require.NoError(t, err)
			query, err := planpkg.NewBaseOptimizer(planpkg.NewMockCompilerContext(true)).Optimize(stmt, false)
			require.NoError(t, err)
			var found bool
			for _, node := range query.Nodes {
				for _, expr := range node.ProjectList {
					if lit := expr.GetLit(); lit != nil {
						if v, ok := lit.Value.(*plan.Literal_VecVal); ok {
							found = true
							require.Len(t, v.VecVal, 12)
							text, err := sqlJSONExpr(t.Context(), expr, &ExplainOptions{Format: EXPLAIN_FORMAT_JSON})
							require.NoError(t, err)
							require.Equal(t, fmt.Sprintf("0x%X", []byte(v.VecVal)), text)
						}
					}
				}
			}
			require.True(t, found, "optimizer must construct Literal_VecVal, not a synthetic literal")
			data, err := BuildSQLJSONPlan(t.Context(), query)
			require.NoError(t, err)
			require.True(t, json.Valid(data))
		})
	}
}
