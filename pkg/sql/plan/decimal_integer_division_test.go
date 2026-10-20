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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestDecimalIntegerDivSchemaBinding(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		oid         types.T
	}{
		{"folded_narrow", "select cast(5.678 as decimal(10,3)) div cast(2 as signed) as q", types.T_int32},
		{"wide", "select cast(5.678 as decimal(18,3)) div cast(2 as signed) as q", types.T_int64},
		{"unsigned", "select cast(5.678 as decimal(10,3)) div cast(2 as unsigned) as q", types.T_uint32},
		{"derived", "select q from (select cast(n_nationkey as decimal(10,3)) div 2 as q from nation) s", types.T_int32},
		{"unrelated_bigint", "select cast(2 as signed) as q", types.T_int64},
		{"semantic_wrapper", "select cast(n_nationkey as decimal(10,3)) div 2 + 1 as q from nation", types.T_int64},
	} {
		for _, kind := range []string{"table", "view"} {
			t.Run(kind+"_"+tc.name, func(t *testing.T) {
				p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "create "+kind+" decimal_div_test as "+tc.query)
				require.NoError(t, err)
				var cols []*ColDef
				if kind == "table" {
					cols = p.GetDdl().GetCreateTable().TableDef.Cols
				} else {
					cols = p.GetDdl().GetCreateView().TableDef.Cols
				}
				require.NotEmpty(t, cols)
				require.Equal(t, int32(tc.oid), cols[0].Typ.Id)
			})
		}
	}
}

func TestDecimalIntegerDivViewConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		oid         types.T
	}{
		{"signed", "select cast(n_nationkey as decimal(10,3)) div 2 as q from nation", types.T_int32},
		{"unsigned", "select cast(n_nationkey as decimal(10,3)) div cast(2 as unsigned) as q from nation", types.T_uint32},
		{"folded", "select cast(5.678 as decimal(10,3)) div 2 as q", types.T_int32},
		{"derived", "select q from (select cast(n_nationkey as decimal(10,3)) div 2 as q from nation) s", types.T_int32},
		{"cte", "with s as (select cast(n_nationkey as decimal(10,3)) div 2 as q from nation) select q from s", types.T_int32},
		{"ordered", "select cast(n_nationkey as decimal(10,3)) div 2 as q from nation order by n_regionkey limit 1", types.T_int32},
		{"wide", "select cast(n_nationkey as decimal(18,3)) div 2 as q from nation", types.T_int64},
		{"unrelated_bigint", "select cast(n_nationkey as signed) as q from nation", types.T_int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			// Markerless views with stale physical columns must derive the same
			// completed boundary from their SQL as newly created views.
			f.addView(t, "div_source_view", tc.query)
			f.addView(t, "div_nested_view", "select q from div_source_view")
			for _, source := range []string{"div_source_view", "div_nested_view"} {
				for _, kind := range []string{"table", "view"} {
					t.Run(kind+"_"+source, func(t *testing.T) {
						stmts, err := mysql.Parse(t.Context(), "create "+kind+" div_consumer as select q from "+source, 1)
						require.NoError(t, err)
						defer stmts[0].Free()
						p, err := BuildPlan(f.compiler, stmts[0], false)
						require.NoError(t, err)
						var cols []*ColDef
						if kind == "table" {
							cols = p.GetDdl().GetCreateTable().TableDef.Cols
						} else {
							cols = p.GetDdl().GetCreateView().TableDef.Cols
						}
						require.NotEmpty(t, cols)
						require.Equal(t, int32(tc.oid), cols[0].Typ.Id)
					})
				}
			}
		})
	}
}

func TestDecimalIntegerDivViewBoundaryPlan(t *testing.T) {
	for _, tc := range []struct {
		name, peer string
		arithmetic types.T
		schema     types.T
	}{
		{"signed", "2", types.T_int64, types.T_int32},
		{"unsigned", "cast(2 as unsigned)", types.T_uint64, types.T_uint32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			query := "select cast(n_nationkey as decimal(10,3)) div " + tc.peer + " as q from nation"
			f.addView(t, "div_boundary_view", query)
			for _, source := range []string{"div_boundary_view", "(" + query + ") s"} {
				stmts, err := mysql.Parse(t.Context(), "select q from "+source, 1)
				require.NoError(t, err)
				func() {
					defer stmts[0].Free()
					p, err := BuildPlan(f.compiler, stmts[0], false)
					require.NoError(t, err)
					div := findPlanFunctionExpr(p, "div")
					require.NotNil(t, div)
					require.Equal(t, int32(tc.arithmetic), div.Typ.Id)
					want := tc.arithmetic
					if source == "div_boundary_view" {
						want = tc.schema
						var boundaryCast *Expr
						for _, node := range p.GetQuery().Nodes {
							for _, expr := range node.ProjectList {
								if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" && expr.Typ.Id == int32(want) {
									boundaryCast = expr
								}
							}
						}
						require.NotNil(t, boundaryCast, "the view must convert the vector, not relabel DIV")
						require.Equal(t, int32(tc.arithmetic), boundaryCast.GetF().Args[0].Typ.Id)
					}
					cols := GetResultColumnsFromPlan(p)
					require.Len(t, cols, 1)
					require.Equal(t, int32(want), cols[0].Typ.Id)
				}()
			}
		})
	}
}

func TestDecimalIntegerDivPersistedFloor(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "prepare decimal_div_floor from 'select cast(? as decimal(10,3)) div cast(2 as unsigned)'")
	require.NoError(t, err)
	expr := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "div")
	require.NotNil(t, expr)
	floor, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion121, floor)
}
