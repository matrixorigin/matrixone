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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type equalityBoundContext struct {
	CompilerContext
	rows  uint64
	total uint64
	calls int
}

func (c *equalityBoundContext) BoundEqualityRows(*plan.TableDef, *plan.ColDef, *plan.Expr) (uint64, uint64) {
	c.calls++
	return c.rows, c.total
}

func TestIndexedEqualityUsesOnlyCheapObservedBounds(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar)}
	node := &plan.Node{
		ObjRef:      &plan.ObjectRef{Obj: 100001},
		BindingTags: []int32{1},
		TableDef: &plan.TableDef{TblId: 100001, TableType: catalog.SystemOrdinaryRel,
			Cols:          []*plan.ColDef{{Name: "group_id", Typ: typ}, {Name: "other", Typ: typ}},
			Indexes:       []*plan.IndexDef{{TableExist: true, Parts: []string{"group_id", "other"}}},
			Name2ColIndex: map[string]int32{"group_id": 0, "other": 1},
		},
	}
	eq := func(col int32) *plan.Expr {
		constant := makePlan2StringConstExprWithType("rare")
		constant.Typ = typ
		column := GetColExpr(typ, 1, col)
		column.GetCol().Name = node.TableDef.Cols[col].Name
		return &plan.Expr{Selectivity: 0.5, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{column, constant},
		}}}
	}
	ctx := &equalityBoundContext{rows: 8000, total: 100000}
	builder := &QueryBuilder{compCtx: ctx}
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, eq(1), 100000))
	require.Zero(t, ctx.calls)
	ambiguous := eq(0)
	node.TableDef.Cols[0].Name = "prefix.group_id"
	node.TableDef.Indexes[0].Parts[0] = "prefix.group_id"
	ambiguous.GetF().Args[0].GetCol().Name = "prefix.group_id"
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, ambiguous, 100000))
	require.Zero(t, ctx.calls)
	node.TableDef.Cols[0].Name = "group_id"
	node.TableDef.Indexes[0].Parts[0] = "group_id"
	node.TableDef.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"group_id:3"}`
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, eq(0), 100000))
	require.Zero(t, ctx.calls)
	node.TableDef.Indexes[0].IndexAlgoParams = ""
	node.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "other", Names: []string{"other"}}
	node.FilterList = []*plan.Expr{eq(0), eq(1)}
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, node.FilterList[0], 100000))
	require.Zero(t, ctx.calls)
	node.FilterList = nil
	node.TableDef.Pkey = nil
	expr := eq(0)
	require.Equal(t, 0.08, builder.tightenIndexedEquality(node, expr, 100000))
	require.Equal(t, 0.08, builder.tightenIndexedEquality(node, expr, 100000))
	require.Equal(t, 1, ctx.calls, "recalculation must reuse the observation")

	ctx.rows = 90000
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, eq(0), 1000000), "snapshot coverage limits the effect of stale table size")
	ctx.rows = 100000
	require.Equal(t, 0.5, builder.tightenIndexedEquality(node, eq(0), 1000000), "stale table size alone must not tighten the estimate")
	ctx.rows, ctx.total = 0, 0
	for range 5 {
		require.Equal(t, 0.5, builder.tightenIndexedEquality(node, eq(0), 100000))
	}
	require.Equal(t, equalityRowBoundAttempts, ctx.calls)
}
