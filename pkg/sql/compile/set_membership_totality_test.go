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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

func TestSetMembershipInputsAreTotal(t *testing.T) {
	newQuery := func() *plan.Query {
		typ := types.T_int64.ToType().PlanType()
		scan := func() *plan.Node {
			return &plan.Node{
				NodeType:    plan.Node_TABLE_SCAN,
				TableDef:    &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}}},
				ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)},
			}
		}
		return &plan.Query{Steps: []int32{2}, Nodes: []*plan.Node{
			scan(), scan(),
			{NodeType: plan.Node_INTERSECT_ALL, Children: []int32{0, 1}, ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}},
		}}
	}
	for _, tc := range []struct {
		name string
		edit func(*plan.Query)
		want bool
	}{
		{"independent scans", nil, true},
		{"shared arm", func(q *plan.Query) { q.Nodes[2].Children[1] = 0 }, false},
		{"shared step", func(q *plan.Query) { q.Steps = append(q.Steps, 0) }, false},
		{"cycle", func(q *plan.Query) { q.Nodes[0].Children = []int32{2} }, false},
		{"invalid column", func(q *plan.Query) { q.Nodes[0].ProjectList[0].GetCol().ColPos = 2 }, false},
		{"different column type", func(q *plan.Query) { q.Nodes[0].TableDef.Cols[0].Typ.Id = int32(types.T_uint64) }, false},
		{"different arm type", func(q *plan.Query) {
			q.Nodes[1].ProjectList[0].Typ.Id = int32(types.T_uint64)
			q.Nodes[1].TableDef.Cols[0].Typ.Id = int32(types.T_uint64)
		}, false},
		{"branch limit", func(q *plan.Query) { q.Nodes[0].Limit = plan2.MakePlan2Int64ConstExprWithType(1) }, false},
		{"block metadata columns", func(q *plan.Query) {
			q.Nodes[0].BlockFilterList = []*plan.Expr{plan2.MakePlan2BoolConstExprWithType(true)}
		}, false},
		{"deferred literal", func(q *plan.Query) {
			literal := plan2.MakePlan2Int64ConstExprWithType(1)
			literal.GetLit().Src = plan2.MakePlan2Int64ConstExprWithType(2)
			q.Nodes[0].ProjectList[0] = literal
		}, false},
		{"fallible physical key", func(q *plan.Query) {
			q.Nodes[2].PhysicalEqualityKeyList = []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "divide"}}}}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := newQuery()
			if tc.edit != nil {
				tc.edit(query)
			}
			require.Equal(t, tc.want, setMembershipInputsAreTotal(query, 2))
		})
	}
	t.Run("invalid source in predicate", func(t *testing.T) {
		query := newQuery()
		predicate, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
			plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0),
			plan2.MakePlan2Int64ConstExprWithType(1),
		})
		require.NoError(t, err)
		query.Nodes[0].FilterList = []*plan.Expr{predicate}
		require.True(t, setMembershipInputsAreTotal(query, 2))
		predicate.GetF().Args[1].GetLit().StringSource = ^uint32(0)
		require.False(t, setMembershipInputsAreTotal(query, 2))
	})
	t.Run("invalid source in physical key", func(t *testing.T) {
		query := newQuery()
		literal := plan2.MakePlan2Int64ConstExprWithType(1)
		query.Nodes[2].PhysicalEqualityKeyList = []*plan.Expr{literal}
		require.True(t, setMembershipInputsAreTotal(query, 2))
		literal.GetLit().StringSource = ^uint32(0)
		require.False(t, setMembershipInputsAreTotal(query, 2))
	})
}

func TestSetMembershipRejectedExpressionIsNotReported(t *testing.T) {
	previous := errutil.GetReportErrorFunc()
	var reports atomic.Int32
	errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
	defer errutil.SetErrorReporter(previous)

	typ := types.T_int64.ToType().PlanType()
	input := []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}
	invalid := plan2.GetColExpr(typ, 0, 1)
	require.False(t, setMembershipExpressionsMatchInputs([]*plan.Expr{invalid}, [][]*plan.Expr{input}))
	require.Zero(t, reports.Load(), "an optimizer fallback must not report an internal error")
}
