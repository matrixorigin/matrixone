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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/stretchr/testify/require"
)

func TestPreparedNumericContextUsesInsertValuesTarget(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       planpb.Type
		paramCount int
	}{
		{
			name: "binary arithmetic",
			sql:  "insert into constraint_test.emp (sal) values (? + ?)",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name: "mod function",
			sql:  "insert into constraint_test.emp (sal) values (mod(?, ?))",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name: "double sibling overrides decimal target",
			sql:  "insert into constraint_test.emp (sal) values ((? + ?) + cast(1 as double))",
			want: planpb.Type{Id: int32(types.T_float64)},
		},
		{
			name:       "numeric function",
			sql:        "insert into constraint_test.emp (sal) values (abs(?))",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "parenthesized numeric function",
			sql:        "insert into constraint_test.emp (sal) values (((abs(?))))",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "cast wrapped numeric function",
			sql:        "insert into constraint_test.emp (sal) values (cast(abs(?) as decimal(7, 2)))",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "numeric function inside arithmetic",
			sql:        "insert into constraint_test.emp (sal) values (abs(?) + 0)",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "replace numeric function",
			sql:        "replace into constraint_test.emp (sal) values (abs(?))",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "insert set numeric function",
			sql:        "insert into constraint_test.emp set sal = abs(?)",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name:       "replace set numeric function",
			sql:        "replace into constraint_test.emp set sal = abs(?)",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			paramCount := test.paramCount
			if paramCount == 0 {
				paramCount = 2
			}
			require.Len(t, paramTypes, paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, test.want.Id, typ.Id)
				if test.want.Id == int32(types.T_decimal64) {
					require.Equal(t, test.want, typ)
				}
			}
		})
	}
}

func TestNumericValuesFunctionWithoutParamsKeepsOwnArgDomain(t *testing.T) {
	tests := []string{
		"insert into constraint_test.emp (sal) values (length(100000.5))",
		"insert into constraint_test.emp (sal) values (char_length(100000.5))",
		"replace into constraint_test.emp (sal) values (length(100000.5))",
		"insert into constraint_test.emp set sal = char_length(100000.5)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(t, err)
		})
	}
}

func TestPreparedNumericReturningFunctionKeepsIndependentArgDomain(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (empno) values (field(?, ?, ?))", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	found := false
	check := func(expr *planpb.Expr) {
		walkPlanExpr(expr, func(item *planpb.Expr) {
			fn := item.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "field" {
				return
			}
			found = true
			require.Len(t, fn.Args, 3)
			for _, arg := range fn.Args {
				require.Equal(t, int32(types.T_text), arg.Typ.Id)
			}
		})
	}
	for _, node := range queryPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			check(expr)
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, data := range col.Data {
					check(data.Expr)
				}
			}
		}
	}
	require.True(t, found)
}

func TestPreparedNumericContextUsesInsertSelectTarget(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       types.T
		paramCount int
	}{
		{
			name:       "direct select",
			sql:        "insert into constraint_test.emp (sal) select ? + ?",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "dynamic abs inherits target",
			sql:        "insert into constraint_test.emp (sal) select abs(?)",
			want:       types.T_decimal64,
			paramCount: 1,
		},
		{
			name:       "parenthesized dynamic abs inherits target",
			sql:        "insert into constraint_test.emp (sal) select (abs(?))",
			want:       types.T_decimal64,
			paramCount: 1,
		},
		{
			name:       "dynamic floor inherits target",
			sql:        "insert into constraint_test.emp (sal) select floor(?)",
			want:       types.T_decimal64,
			paramCount: 1,
		},
		{
			name:       "parenthesized select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "union select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ? union all select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name:       "double sibling overrides target",
			sql:        "insert into constraint_test.emp (sal) select (? + ?) + cast(1 as double)",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "case double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"case when 1 = 1 then cast(1 as double) else 0 end",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "if double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"if(1 = 1, cast(-100000 as double), 0)",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "coalesce double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"coalesce(cast(-100000 as double), 0)",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "ifnull double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"ifnull(cast(-100000 as double), 0)",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "nullif double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"nullif(cast(-100000 as double), 0)",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "abs double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"abs(cast(-100000 as double))",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "abs nested arithmetic double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"abs(? + cast(-100000 as double))",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "floor double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"floor(cast(-100000 as double))",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "power double result overrides target",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"power(cast(-100000 as double), 1)",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name:       "dynamic sqrt fixes approximate domain",
			sql:        "insert into constraint_test.emp (sal) select ? + sqrt(?)",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name:       "dynamic power fixes approximate domain",
			sql:        "insert into constraint_test.emp (sal) select ? + power(?, ?)",
			want:       types.T_float64,
			paramCount: 3,
		},
		{
			name: "derived dynamic sqrt propagates approximate domain",
			sql: "insert into constraint_test.emp (sal) " +
				"select ? + sqrt(d.x) from (select ? + ? as x) d",
			want:       types.T_float64,
			paramCount: 3,
		},
		{
			name: "cte dynamic power propagates approximate domain",
			sql: "insert into constraint_test.emp (sal) " +
				"with c as (select ? + ? as x) select ? + power(c.x, ?) from c",
			want:       types.T_float64,
			paramCount: 4,
		},
		{
			name:       "dynamic arithmetic inside abs keeps target",
			sql:        "insert into constraint_test.emp (sal) select abs(? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived arithmetic inside abs propagates target",
			sql: "insert into constraint_test.emp (sal) " +
				"select abs(d.x + 0) from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "cte unary inside abs propagates target",
			sql: "insert into constraint_test.emp (sal) " +
				"with c as (select ? + ? as x) select abs(-c.x) from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "schema qualified table does not shadow cte lineage",
			sql: "insert into constraint_test.emp (empno, sal) " +
				"with emp as (select ? + ? as x) " +
				"select p.empno, q.x from constraint_test.emp p cross join emp q",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "long cte chain propagates target",
			sql: "insert into constraint_test.emp (sal) " +
				"with c1 as (select ? + ? as x), " +
				"c2 as (select x from c1), c3 as (select x from c2), " +
				"c4 as (select x from c3), c5 as (select x from c4), " +
				"c6 as (select x from c5), c7 as (select x from c6), " +
				"c8 as (select x from c7) select x from c8",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "aggregate double result overrides target",
			sql: "insert into constraint_test.emp (sal) " +
				"select ? + avg(cast(100000 as double)) from nation",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "window double result overrides target",
			sql: "insert into constraint_test.emp (sal) " +
				"select ? + avg(cast(100000 as double)) over () from nation",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "union common double domain overrides target",
			sql: "insert into constraint_test.emp (sal) " +
				"(select ? + ? union all select cast(0 as double))",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "intersect common double domain overrides target",
			sql: "insert into constraint_test.emp (sal) " +
				"(select ? + ? intersect select cast(0 as double))",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "minus common double domain overrides target",
			sql: "insert into constraint_test.emp (sal) " +
				"(select ? + ? minus select cast(0 as double))",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "nested non numeric function exposes only final numeric result",
			sql: "insert into constraint_test.emp (sal) select ? + " +
				"length(concat(cast(1 as decimal(30, 2)), ''))",
			want:       types.T_decimal64,
			paramCount: 1,
		},
		{
			name: "case result arithmetic keeps target",
			sql: "insert into constraint_test.emp (sal) select " +
				"case when 1 = 1 then ? + ? else 0 end",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "group by position",
			sql:        "insert into constraint_test.emp (sal) select ? + ? from constraint_test.emp group by 1",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "scalar subquery",
			sql:        "insert into constraint_test.emp (sal) select (select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "scalar subquery inside arithmetic",
			sql:        "insert into constraint_test.emp (sal) select (select ? + ?) + 0",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "scalar subquery follows double sibling domain",
			sql:        "insert into constraint_test.emp (sal) select (select ? + ?) + cast(1 as double)",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name:       "outer parameter follows scalar subquery double domain",
			sql:        "insert into constraint_test.emp (sal) select (select cast(1 as double)) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar derived values double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select d.x from (values row(cast(-100000 as double))) as d(x)) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar parenthesized values double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select d.x from ((values row(cast(-100000 as double)))) as d(x)) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar derived column double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (select cast(1 as double) as x) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows nested scalar derived column double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select d.x from (select x from (select cast(1 as double) as x) s) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar derived star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (select * from (select cast(1 as double) as x) s) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar qualified star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (select s.* from (select cast(1 as double) as x) s) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar cte column double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (with c as (select cast(1 as double) as x) select x from c)) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar cte star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (with c as (select cast(1 as double) as x), " +
				"d as (select * from c) select x from d)) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar qualified cte star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(with c as (select cast(1 as double) as x) select c.* from c) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar merged join star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (select * from (select 1 as a) l join " +
				"(select 1 as a, cast(1 as double) as x) r using (a)) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name: "outer parameter follows scalar natural join star double domain",
			sql: "insert into constraint_test.emp (sal) select " +
				"(select x from (select * from (select 1 as a) l natural join " +
				"(select 1 as a, cast(1 as double) as x) r) d) + ?",
			want:       types.T_float64,
			paramCount: 1,
		},
		{
			name:       "outer parameter keeps decimal target with scalar integer subquery",
			sql:        "insert into constraint_test.emp (sal) select (select 1) + ?",
			want:       types.T_decimal64,
			paramCount: 1,
		},
		{
			name:       "scalar subquery inside unary arithmetic",
			sql:        "insert into constraint_test.emp (sal) select -(select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "scalar subquery inside mod",
			sql:        "insert into constraint_test.emp (sal) select mod((select ? + ?), 1)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived table passthrough",
			sql:        "insert into constraint_test.emp (sal) select x from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived arithmetic passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.x + 0 from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived dynamic abs passthrough",
			sql:        "insert into constraint_test.emp (sal) select abs(d.x) from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived values alias passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.x from (values row(? + ?)) as d(x)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived values default column passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.column_0 from (values row(? + ?)) as d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived parenthesized values passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.x from ((values row(? + ?))) as d(x)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived values union passthrough",
			sql: "insert into constraint_test.emp (sal) select d.x " +
				"from ((values row(? + ?)) union all select ? + ?) as d(x)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "derived values multiple rows passthrough",
			sql: "insert into constraint_test.emp (sal) select d.x " +
				"from (values row(? + ?), row(? + ?)) as d(x)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "derived values target column only",
			sql: "insert into constraint_test.emp (sal) select d.x " +
				"from (values row(1, ? + ?)) as d(ignored, x)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived if root passthrough",
			sql: "insert into constraint_test.emp (sal) select if(1 = 1, d.x, 0) " +
				"from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived case root passthrough",
			sql: "insert into constraint_test.emp (sal) select " +
				"case when 1 = 1 then d.x else 0 end from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived arithmetic double sibling overrides target",
			sql: "insert into constraint_test.emp (sal) select d.x + cast(0 as double) " +
				"from (select ? + ? as x) d",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name:       "unnamed sibling does not block derived passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.x from (select ? + ? as x, 1) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "physical source advances unqualified star target",
			sql: "insert into constraint_test.emp (empno, ename, job, mgr, sal) " +
				"select * from nation cross join (select ? + ? as v) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "nested derived table passthrough",
			sql:        "insert into constraint_test.emp (sal) select x from (select x from (select ? + ? as x) d1) d2",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "nested derived star passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.x from (select * from (select ? + ? as x) s) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "nested derived merged star passthrough",
			sql: "insert into constraint_test.emp (sal) select d.v from " +
				"(select * from (select 1 as a) x join (select 1 as a, ? + ? as v) y using (a)) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "cte passthrough",
			sql:        "insert into constraint_test.emp (sal) with c as (select ? + ? as x) select x from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "cte arithmetic passthrough",
			sql: "insert into constraint_test.emp (sal) with c as (select ? + ? as x) " +
				"select x + 0 from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "cte dynamic abs passthrough",
			sql: "insert into constraint_test.emp (sal) with c as (select ? + ? as x) " +
				"select abs(c.x) from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "cte coalesce root passthrough",
			sql: "insert into constraint_test.emp (sal) with c as (select ? + ? as x) " +
				"select coalesce(c.x, 0) from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "aliased cte passthrough",
			sql:        "insert into constraint_test.emp (sal) with c(y) as (select ? + ?) select q.y from c q",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived column alias passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.y from (select ? + ?) d(y)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived qualified star passthrough",
			sql:        "insert into constraint_test.emp (sal, comm) select d.* from (select ? + ? as x, ? + ? as y) d",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "derived union passthrough",
			sql: "insert into constraint_test.emp (sal) select d.x from " +
				"(select ? + ? as x union all select ? + ?) d",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "chained cte passthrough",
			sql: "insert into constraint_test.emp (sal) " +
				"with c as (select ? + ? as x), d as (select x from c) select x from d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "chained cte star passthrough",
			sql: "insert into constraint_test.emp (sal) " +
				"with c as (select ? + ? as x), d as (select * from c) select d.x from d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "derived conflicting targets fall back",
			sql: "insert into constraint_test.emp (sal, empno) " +
				"select d.x, d.x from (select ? + ? as x) d",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "derived values conflicting targets fall back",
			sql: "insert into constraint_test.emp (sal, empno) " +
				"select d.x, d.x from (values row(? + ?)) as d(x)",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "derived conflicting targets reverse order fall back",
			sql: "insert into constraint_test.emp (empno, sal) " +
				"select d.x, d.x from (select ? + ? as x) d",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "cte conflicting targets fall back",
			sql: "insert into constraint_test.emp (sal, empno) " +
				"with c as (select ? + ? as x) select c.x, c.x from c",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "cte conflict stays ambiguous across three aliases",
			sql: "insert into constraint_test.emp (sal, empno, mgr) " +
				"with c as (select ? + ? as x) select a.x, b.x, d.x " +
				"from c a cross join c b cross join c d",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name: "derived identical targets propagate",
			sql: "insert into constraint_test.emp (sal, comm) " +
				"select d.x, d.x from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "recursive cte seed target",
			sql: "insert into constraint_test.emp (sal) with recursive c(x) as " +
				"(select ? + ? union all select x from c where x < 1) select x from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name: "recursive cte member target",
			sql: "insert into constraint_test.emp (sal) with recursive c(x) as " +
				"(select 0 union all select ? + ? from c where x < 1) select x from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, test.paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
				if test.want == types.T_decimal64 {
					require.Equal(t, int32(7), typ.Width)
					require.Equal(t, int32(2), typ.Scale)
				}
			}
		})
	}
}

func TestPreparedNumericFunctionControlArgUsesOverloadType(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal) select round(?, ?)",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Equal(t, planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}, paramTypes[1])
	require.Equal(t, int32(types.T_int64), paramTypes[2].Id)
}

func TestSeedNumericSourceTargetKeepsAmbiguousPositionEmpty(t *testing.T) {
	decimal72 := planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	decimal74 := planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 4}
	source := numericProjectionSourceInfo{
		targets:         make([]planpb.Type, 1),
		targetAmbiguous: make([]bool, 1),
	}

	seedNumericSourceTarget(&source, 0, decimal72)
	require.Equal(t, decimal72, source.targets[0])
	seedNumericSourceTarget(&source, 0, decimal72)
	require.Equal(t, decimal72, source.targets[0])

	seedNumericSourceTarget(&source, 0, decimal74)
	require.Equal(t, planpb.Type{}, source.targets[0])
	require.True(t, source.targetAmbiguous[0])

	seedNumericSourceTarget(&source, 0, decimal72)
	require.Equal(t, planpb.Type{}, source.targets[0])
}

func TestStoreNumericTableProjectionTargetsKeepsConflictAmbiguous(t *testing.T) {
	decimal72 := planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	uint32Type := planpb.Type{Id: int32(types.T_uint32)}
	targets := make(map[string][]planpb.Type)
	ambiguous := make(map[string][]bool)

	storeNumericTableProjectionTargets(targets, ambiguous, "c", []planpb.Type{decimal72})
	storeNumericTableProjectionTargets(targets, ambiguous, "c", []planpb.Type{uint32Type})
	require.Equal(t, planpb.Type{}, targets["c"][0])
	require.True(t, ambiguous["c"][0])

	storeNumericTableProjectionTargets(targets, ambiguous, "c", []planpb.Type{uint32Type})
	require.Equal(t, planpb.Type{}, targets["c"][0])
	require.True(t, ambiguous["c"][0])
}

func TestPreparedNumericContextUsesClusterTableStarVisibility(t *testing.T) {
	tests := []struct {
		name      string
		accountID uint32
		sql       string
	}{
		{
			name:      "tenant unqualified star hides account id",
			accountID: 1,
			sql: "insert into constraint_test.emp (empno, sal) " +
				"select * from cluster_numeric_src cross join (select ? + ? as x) d",
		},
		{
			name:      "tenant qualified star hides account id",
			accountID: 1,
			sql: "insert into constraint_test.emp (empno, sal) " +
				"select s.*, d.* from cluster_numeric_src s cross join (select ? + ? as x) d",
		},
		{
			name:      "system star keeps account id",
			accountID: catalog.System_Account,
			sql: "insert into constraint_test.emp (empno, mgr, sal) " +
				"select * from cluster_numeric_src cross join (select ? + ? as x) d",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			optimizer.ctxt.GetAccountIdFunc = func() (uint32, error) {
				return test.accountID, nil
			}
			optimizer.ctxt.tables["cluster_numeric_src"] = &planpb.TableDef{
				Name:      "cluster_numeric_src",
				TableType: catalog.SystemClusterRel,
				Cols: []*planpb.ColDef{
					{Name: "visible_col", Typ: planpb.Type{Id: int32(types.T_uint32)}},
					{Name: catalog.ExternalFilePath, Typ: planpb.Type{Id: int32(types.T_varchar)}},
					{Name: util.GetClusterTableAttributeName(), Typ: planpb.Type{Id: int32(types.T_uint32)}},
				},
			}
			optimizer.ctxt.objects["cluster_numeric_src"] = &planpb.ObjectRef{ObjName: "cluster_numeric_src"}

			stmt, err := mysql.ParseOne(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)
			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, 2)
			for _, typ := range paramTypes {
				require.Equal(t, int32(types.T_decimal64), typ.Id)
				require.Equal(t, int32(7), typ.Width)
				require.Equal(t, int32(2), typ.Scale)
			}
		})
	}
}

func TestPreparedNumericContextUsesClusterTableStarVisibilityInScalarLineage(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	optimizer.ctxt.GetAccountIdFunc = func() (uint32, error) {
		return 1, nil
	}
	optimizer.ctxt.tables["cluster_numeric_src"] = &planpb.TableDef{
		Name:      "cluster_numeric_src",
		TableType: catalog.SystemClusterRel,
		Cols: []*planpb.ColDef{
			{Name: "visible_col", Typ: planpb.Type{Id: int32(types.T_uint32)}},
			{Name: catalog.ExternalFilePath, Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: util.GetClusterTableAttributeName(), Typ: planpb.Type{Id: int32(types.T_uint32)}},
		},
	}
	optimizer.ctxt.objects["cluster_numeric_src"] = &planpb.ObjectRef{ObjName: "cluster_numeric_src"}

	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal) select "+
			"(select x from (select * from cluster_numeric_src cross join "+
			"(select cast(1 as double) as x) d) q(a, x)) + ?",
		1,
	)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 1)
	for _, typ := range paramTypes {
		require.Equal(t, int32(types.T_float64), typ.Id)
	}
}

func TestPreparedNumericContextDoesNotPropagateThroughExistsSubquery(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal) select (select ? + ? where exists(select ? + ?))",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 4)
	for _, pos := range []int32{1, 2} {
		require.Equal(t, planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}, paramTypes[pos])
	}
	for _, pos := range []int32{3, 4} {
		require.Equal(t, int32(types.T_float64), paramTypes[pos].Id)
	}
}

func TestPreparedNumericContextKeepsIndependentGroupByParameters(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal) select ? + ? from nation group by ? + ?",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 4)
	for _, pos := range []int32{1, 2} {
		require.Equal(t, planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}, paramTypes[pos])
	}
	for _, pos := range []int32{3, 4} {
		require.Equal(t, int32(types.T_float64), paramTypes[pos].Id)
	}
}

func TestPreparedNumericContextReusesGroupByAliasParameters(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal) select ? + ? as x from nation group by x",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 2)
	for _, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(7), typ.Width)
		require.Equal(t, int32(2), typ.Scale)
	}
}

func TestResolveNumericScalarColumnIsScopeSafe(t *testing.T) {
	doubleScan := numericAstTypedOperand(planpb.Type{Id: int32(types.T_float64)})
	sources := []numericScalarSource{
		{alias: "a", cols: []string{"x"}, types: []numericAstTypeScan{doubleScan}, known: true},
		{alias: "b", cols: []string{"x"}, types: []numericAstTypeScan{doubleScan}, known: true},
	}

	_, ok := resolveNumericScalarColumn(sources, tree.NewUnresolvedName(tree.NewCStr("x", 0)))
	require.False(t, ok)

	scan, ok := resolveNumericScalarColumn(
		sources,
		tree.NewUnresolvedName(tree.NewCStr("a", 0), tree.NewCStr("x", 0)),
	)
	require.True(t, ok)
	require.Len(t, scan.strong, 1)
	require.Equal(t, int32(types.T_float64), scan.strong[0].Id)

	sources[1] = numericScalarSource{alias: "b"}
	_, ok = resolveNumericScalarColumn(sources, tree.NewUnresolvedName(tree.NewCStr("x", 0)))
	require.False(t, ok)
}

func TestPreparedNumericContextHandlesMergedJoinStarColumns(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       types.T
		paramCount int
	}{
		{
			name: "using join unqualified star",
			sql: "insert into constraint_test.emp (empno, sal) select * " +
				"from (select 1 as a) x join (select 1 as a, ? + ? as v) y using (a)",
			want: types.T_decimal64,
		},
		{
			name: "natural join unqualified star",
			sql: "insert into constraint_test.emp (empno, sal) select * " +
				"from (select 1 as a) x natural join (select 1 as a, ? + ? as v) y",
			want: types.T_decimal64,
		},
		{
			name: "using join qualified star",
			sql: "insert into constraint_test.emp (empno, sal) select y.* " +
				"from (select 1 as a) x join (select 1 as a, ? + ? as v) y using (a)",
			want: types.T_decimal64,
		},
		{
			name: "on join unqualified star",
			sql: "insert into constraint_test.emp (empno, mgr, sal) select * " +
				"from (select 1 as a) x join (select 1 as b, ? + ? as v) y on x.a = y.b",
			want: types.T_decimal64,
		},
		{
			name: "right using join chooses right merged column",
			sql: "insert into constraint_test.emp (sal, empno) select * " +
				"from (select 1 as a) x right join (select ? + ? as a, 1 as v) y using (a)",
			want: types.T_decimal64,
		},
		{
			name: "full using join propagates both merged arms",
			sql: "insert into constraint_test.emp (sal, empno) select * " +
				"from (select ? + ? as a) x full join (select ? + ? as a, 1 as v) y using (a)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "nested using joins preserve star order",
			sql: "insert into constraint_test.emp (empno, mgr, sal) select * from " +
				"((select 1 as a) x join (select 1 as a, 1 as b) y using (a)) " +
				"join (select 1 as b, ? + ? as v) z using (b)",
			want: types.T_decimal64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmt, err := mysql.ParseOne(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			paramCount := test.paramCount
			if paramCount == 0 {
				paramCount = 2
			}
			require.Len(t, paramTypes, paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
				if test.want == types.T_decimal64 {
					require.Equal(t, int32(7), typ.Width)
					require.Equal(t, int32(2), typ.Scale)
				}
			}
		})
	}
}

func TestPreparedNumericContextUsesSampleSuffixTarget(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal, deptno, comm) "+
			"select ? + ?, sample(n_nationkey, 1 rows), ? + ? from nation",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 4)
	for pos, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(7), typ.Width, "parameter %d has the wrong width", pos)
		require.Equal(t, int32(2), typ.Scale, "parameter %d has the wrong scale", pos)
	}
}

func TestPreparedNumericContextUsesLegacyInsertSelectTarget(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into ext(v) select ? + ? from nation",
		1,
	)
	require.NoError(t, err)

	insertStmt := stmt.(*tree.Insert)
	tableDef := &planpb.TableDef{
		Name:      "ext",
		TableType: catalog.SystemExternalRel,
		Cols: []*planpb.ColDef{{
			Name: "v",
			Typ:  planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		}},
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, optimizer.CurrentContext(), true, false)
	bindCtx := NewBindContext(builder, nil)
	info := &dmlSelectInfo{tblInfo: &dmlTableInfo{
		tableDefs: []*planpb.TableDef{tableDef},
		objRef:    []*planpb.ObjectRef{{ObjName: "ext"}},
	}}

	_, _, _, err = initInsertStmt(builder, bindCtx, insertStmt, info)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, &Plan{Plan: &planpb.Plan_Query{Query: builder.qry}})
	require.Len(t, paramTypes, 2)
	for _, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(7), typ.Width)
		require.Equal(t, int32(2), typ.Scale)
	}
}

func collectUniquePlanParamTypes(t *testing.T, queryPlan *Plan) map[int32]planpb.Type {
	t.Helper()
	result := make(map[int32]planpb.Type)
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.FilterList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.GroupBy {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.AggList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.OnUpdateExprs {
			collectExprParamTypesByPos(t, expr, result)
		}
		if dedup := node.DedupJoinCtx; dedup != nil {
			for _, expr := range dedup.UpdateColExprList {
				collectExprParamTypesByPos(t, expr, result)
			}
		}
		if rowset := node.RowsetData; rowset != nil {
			for _, col := range rowset.Cols {
				for _, data := range col.Data {
					collectExprParamTypesByPos(t, data.Expr, result)
				}
			}
		}
	}
	return result
}

func collectExprParamTypesByPos(t *testing.T, expr *planpb.Expr, result map[int32]planpb.Type) {
	t.Helper()
	collectExprEffectiveParamPlanTypesByPos(expr, planpb.Type{}, func(pos int32, typ planpb.Type) {
		if previous, ok := result[pos]; ok {
			require.Equal(t, previous.Id, typ.Id, "parameter %d has inconsistent type id", pos)
			require.Equal(t, previous.Width, typ.Width, "parameter %d has inconsistent width", pos)
			require.Equal(t, previous.Scale, typ.Scale, "parameter %d has inconsistent scale", pos)
			return
		}
		result[pos] = typ
	})
}

func collectExprEffectiveParamPlanTypesByPos(
	expr *planpb.Expr,
	inherited planpb.Type,
	collect func(int32, planpb.Type),
) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		typ := inherited
		if typ.Id == 0 {
			typ = expr.Typ
		}
		collect(param.Pos, typ)
		return
	}
	if fn := expr.GetF(); fn != nil {
		childType := inherited
		if fn.Func != nil && fn.Func.ObjName == "cast" {
			childType = expr.Typ
		} else if childType.Id == 0 && types.T(expr.Typ.Id).ToType().IsNumeric() {
			// Numeric operators carry the resolved width and scale on their result;
			// prepared children inherit that effective operation type.
			childType = expr.Typ
		}
		for _, arg := range fn.Args {
			collectExprEffectiveParamPlanTypesByPos(arg, childType, collect)
		}
	}
}

func TestPreparedNonNumericAssignmentKeepsTargetType(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "update varchar assignment",
			sql:  "update constraint_test.emp set ename = ? where empno = 1",
			want: types.T_text,
		},
		{
			name: "insert values varchar assignment",
			sql:  "insert into constraint_test.emp (empno, ename) values (1, ?)",
			want: types.T_text,
		},
		{
			name: "insert select varchar assignment",
			sql:  "insert into constraint_test.emp (empno, ename) select 1, ?",
			want: types.T_text,
		},
		{
			name: "on duplicate key update date assignment",
			sql: "insert into constraint_test.emp (empno, hiredate) values (1, '2024-01-01') " +
				"on duplicate key update hiredate = ?",
			want: types.T_date,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, 1)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
			}
		})
	}
}

func TestNumericAssignmentTargetKeepsGroupedProjection(t *testing.T) {
	// a numeric assignment target must not re-bind a projection that is itself a
	// GROUP BY key against the raw scan columns; it should resolve to the grouped
	// column and plan without a "must appear in the GROUP BY clause" error.
	tests := []string{
		"insert into constraint_test.emp (sal) select empno + deptno from constraint_test.emp group by empno + deptno",
		"insert into constraint_test.emp (sal) select empno + deptno as s from constraint_test.emp group by empno + deptno",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(t, err)
		})
	}
}

func TestParameterizedGroupingSetsKeepDistinctPositions(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(
		optimizer.CurrentContext().GetContext(),
		"select count(*) from nation group by grouping sets "+
			"((n_regionkey + ?), (n_nationkey + ?))",
		1,
	)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	require.Len(t, collectUniquePlanParamTypes(t, queryPlan), 2)

	seen := make(map[int]bool)
	for _, node := range queryPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_AGG || len(node.GroupingFlag) != 2 {
			continue
		}
		active := -1
		for pos, grouped := range node.GroupingFlag {
			if !grouped {
				continue
			}
			require.Equal(t, -1, active)
			active = pos
		}
		if active >= 0 {
			seen[active] = true
		}
	}
	require.Equal(t, map[int]bool{0: true, 1: true}, seen)
}

func TestNumericConditionalContextSkipsPredicateParams(t *testing.T) {
	tests := []string{
		"insert into constraint_test.emp (sal) select ? + " +
			"if(? = 1, cast(-100000 as double), 0)",
		"insert into constraint_test.emp (sal) select ? + " +
			"case when ? = 1 then cast(-100000 as double) else 0 end",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Equal(t, int32(types.T_float64), paramTypes[1].Id)
			require.Equal(t, int32(types.T_int64), paramTypes[2].Id)
		})
	}
}

func TestValuesExprIsFuncCall(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	sql := "insert into constraint_test.emp (sal) values " +
		"(1), (-1), (mod(1, 2)), ((mod(1, 2))), (abs(1)), ((abs(1))), (cast(abs(1) as double))"
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
	require.NoError(t, err)

	insertStmt, ok := stmts[0].(*tree.Insert)
	require.True(t, ok)
	valuesClause, ok := insertStmt.Rows.Select.(*tree.ValuesClause)
	require.True(t, ok)

	wants := []bool{false, false, false, false, true, true, true}
	require.Len(t, valuesClause.Rows, len(wants))
	for i, want := range wants {
		require.Equal(t, want, valuesExprIsFuncCall(valuesClause.Rows[i][0]), "row %d", i)
	}
}

func TestBindProjectionListWithSampleFunc(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	sql := "select n_name, sample(n_nationkey, 10 rows) from nation group by n_name"
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
	require.NoError(t, err)

	_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
	require.NoError(t, err)
}

func TestPreparedNumericContextUsesUpdateTarget(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		want         planpb.Type
		checkFlatten bool
		paramCount   int
	}{
		{
			name: "update decimal assignment",
			sql:  "update constraint_test.emp set sal = ? + ? where empno = 1",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name:       "update dynamic abs inherits target",
			sql:        "update constraint_test.emp set sal = abs(?) where empno = 1",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name: "update double sibling overrides target",
			sql:  "update constraint_test.emp set sal = (? + ?) + cast(1 as double) where empno = 1",
			want: planpb.Type{Id: int32(types.T_float64)},
		},
		{
			name: "update case double result overrides target",
			sql: "update constraint_test.emp set sal = ? + " +
				"case when 1 = 1 then cast(1 as double) else 0 end where empno = 1",
			want:       planpb.Type{Id: int32(types.T_float64)},
			paramCount: 1,
		},
		{
			name: "update case result arithmetic keeps target",
			sql: "update constraint_test.emp set sal = " +
				"case when 1 = 1 then ? + ? else 0 end where empno = 1",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 2,
		},
		{
			name:       "update parameter follows scalar subquery double domain",
			sql:        "update constraint_test.emp set sal = (select cast(1 as double)) + ? where empno = 1",
			want:       planpb.Type{Id: int32(types.T_float64)},
			paramCount: 1,
		},
		{
			name: "update parameter follows scalar derived column double domain",
			sql: "update constraint_test.emp set sal = " +
				"(select x from (select cast(1 as double) as x) d) + ? where empno = 1",
			want:       planpb.Type{Id: int32(types.T_float64)},
			paramCount: 1,
		},
		{
			name: "update parameter follows scalar derived star double domain",
			sql: "update constraint_test.emp set sal = " +
				"(select x from (select * from (select cast(1 as double) as x) s) d) + ? " +
				"where empno = 1",
			want:       planpb.Type{Id: int32(types.T_float64)},
			paramCount: 1,
		},
		{
			name: "on duplicate key update decimal assignment",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = ? + ?",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name: "on duplicate key update dynamic abs inherits target",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = abs(?)",
			want:       planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			paramCount: 1,
		},
		{
			name: "on duplicate key update scalar subquery",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select ? + ?)",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update case double result overrides target",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = ? + " +
				"case when 1 = 1 then cast(1 as double) else 0 end",
			want:       planpb.Type{Id: int32(types.T_float64)},
			paramCount: 1,
		},
		{
			name: "on duplicate key update case result arithmetic keeps target",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = case when 1 = 1 then ? + ? else 0 end",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
			paramCount:   2,
		},
		{
			name: "on duplicate key update scalar subquery with from",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select ? + ? from nation limit 1)",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update scalar subquery inside arithmetic",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select ? + ?) + 0",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update scalar subquery follows double sibling domain",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select ? + ?) + cast(1 as double)",
			want:         planpb.Type{Id: int32(types.T_float64)},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update parameter follows scalar subquery double domain",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select cast(1 as double)) + ?",
			want:         planpb.Type{Id: int32(types.T_float64)},
			checkFlatten: true,
			paramCount:   1,
		},
		{
			name: "on duplicate key update parameter follows scalar derived column double domain",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = " +
				"(select x from (select cast(1 as double) as x) d) + ?",
			want:         planpb.Type{Id: int32(types.T_float64)},
			checkFlatten: true,
			paramCount:   1,
		},
		{
			name: "on duplicate key update parameter follows scalar derived star double domain",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = " +
				"(select x from (select * from (select cast(1 as double) as x) s) d) + ?",
			want:         planpb.Type{Id: int32(types.T_float64)},
			checkFlatten: true,
			paramCount:   1,
		},
		{
			name: "on duplicate key update scalar subquery inside unary arithmetic",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = -(select ? + ?)",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update scalar subquery inside mod",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = mod((select ? + ?), 1)",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
		},
		{
			name: "on duplicate key update multiple numeric assignments",
			sql: "insert into constraint_test.emp (empno, sal, comm) values (1, 1, 1) " +
				"on duplicate key update sal = (select ? + ?) + 0, comm = ? + ?",
			want:         planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
			checkFlatten: true,
			paramCount:   4,
		},
		{
			name: "update from derived source",
			sql: "update constraint_test.emp set sal = d.x " +
				"from (select ? + ? as x) d where emp.empno = 1",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name: "update from derived conditional root",
			sql: "update constraint_test.emp set sal = if(1 = 1, d.x, 0) " +
				"from (select ? + ? as x) d where emp.empno = 1",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
		{
			name: "update from derived dynamic abs root",
			sql: "update constraint_test.emp set sal = abs(d.x) " +
				"from (select ? + ? as x) d where emp.empno = 1",
			want: planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			paramCount := test.paramCount
			if paramCount == 0 {
				paramCount = 2
			}
			require.Len(t, paramTypes, paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, test.want.Id, typ.Id)
				if test.want.Id == int32(types.T_decimal64) {
					require.Equal(t, test.want.Width, typ.Width)
					require.Equal(t, test.want.Scale, typ.Scale)
				}
			}
			if test.checkFlatten {
				dedupCount := 0
				updateExprCount := 0
				for _, node := range queryPlan.GetQuery().Nodes {
					if node.DedupJoinCtx == nil {
						continue
					}
					dedupCount++
					for _, expr := range node.DedupJoinCtx.UpdateColExprList {
						updateExprCount++
						require.False(t, exprContainsSubqueryRef(expr))
					}
				}
				require.Positive(t, dedupCount)
				require.Positive(t, updateExprCount)
			}
		})
	}
}

func exprContainsSubqueryRef(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetSub() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsSubqueryRef(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsSubqueryRef(item) {
				return true
			}
		}
	}
	return false
}

func walkPlanExpr(expr *planpb.Expr, visit func(*planpb.Expr)) {
	if expr == nil {
		return
	}
	visit(expr)
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			walkPlanExpr(arg, visit)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			walkPlanExpr(item, visit)
		}
	}
}
