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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCoveringIndexRangeBounds(t *testing.T) {
	for _, tc := range []struct {
		name, sql, index, bound string
	}{
		{"forced aggregate", "select count(distinct a) from index_hint_t force index(idx_a) where a between 1000 and 1005", "idx_a", "prefix_between"},
		{"forced open lower", "select count(distinct a) from index_hint_t force index(idx_a) where a > 1000", "idx_a", "prefix_in_range"},
		{"forced closed upper", "select count(distinct a) from index_hint_t force index(idx_a) where a <= 1005", "idx_a", "prefix_in_range"},
		{"composite covering", "select sum(b) from index_hint_t force index(idx_ab) where a=0 and b between 1000 and 1005", "idx_ab", "prefix_between"},
		{"composite open suffix", "select sum(b) from index_hint_t force index(idx_ab) where a=0 and b > 1000", "idx_ab", "prefix_in_range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			var names []string
			for _, node := range reachableIndexOrderNodes(p) {
				if !node.IndexScanInfo.IsIndexScan || node.IndexScanInfo.IndexName != tc.index {
					continue
				}
				for _, filter := range node.FilterList {
					if fn := filter.GetF(); fn != nil && fn.Func != nil {
						names = append(names, fn.Func.ObjName)
					}
				}
			}
			require.Contains(t, names, tc.bound, "index scan must constrain the serialized key")
		})
	}
}

func TestCoveringRangeRequiresTotalFilters(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int32)}
	column := GetColExpr(intType, 10, 0)
	literal := makePlan2Int32ConstExprWithType(1)
	comparison := func(value *planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{column, value})
		require.NoError(t, err)
		return expr
	}
	dynamic := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	throwing := &planpb.Expr{Typ: intType, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "cast"}, Args: []*planpb.Expr{literal},
	}}}
	for _, tc := range []struct {
		name  string
		value *planpb.Expr
		want  bool
	}{
		{"literal", literal, true},
		{"parameter", dynamic, false},
		{"function may throw", throwing, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &planpb.Node{BindingTags: []int32{10}, FilterList: []*planpb.Expr{comparison(tc.value)}}
			require.Equal(t, tc.want, indexRangeFiltersAreTotal(node))
		})
	}
}

func TestCoveringIndexRangeFlagDomain(t *testing.T) {
	for _, tc := range []struct {
		name, sql, want string
	}{
		{"valid", "select count(distinct a) from index_hint_t force index(idx_a) where in_range(a,1,4,0)", "prefix_in_range"},
		{"out of range", "select count(distinct a) from index_hint_t force index(idx_a) where in_range(a,1,4,4)", "in_range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			var names []string
			for _, node := range reachableIndexOrderNodes(p) {
				if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.IndexName == "idx_a" {
					for _, filter := range node.FilterList {
						if fn := filter.GetF(); fn != nil && fn.Func != nil {
							names = append(names, fn.Func.ObjName)
						}
					}
				}
			}
			require.Contains(t, names, tc.want)
		})
	}
}

func TestCoveringIndexRangeCombinesOpposingBounds(t *testing.T) {
	for _, tc := range []struct {
		name, sql, index string
		parts            int
	}{
		{"leading", "select count(distinct a) from index_hint_t force index(idx_a) where a>=1 and a<4", "idx_a", 1},
		{"suffix", "select sum(b) from index_hint_t force index(idx_ab) where a=0 and b>=1 and b<4", "idx_ab", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			var bound *planpb.Function
			for _, node := range reachableIndexOrderNodes(p) {
				if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.IndexName == tc.index {
					for _, filter := range node.FilterList {
						if fn := filter.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "prefix_in_range" {
							bound = fn
						}
					}
				}
			}
			require.NotNil(t, bound, "both comparisons should drive one serialized range")
			require.Len(t, bound.Args, 4)
			for _, endpoint := range bound.Args[1:3] {
				require.NotNil(t, endpoint.GetF())
				require.Equal(t, "serial_full", endpoint.GetF().Func.ObjName)
				require.Len(t, endpoint.GetF().Args, tc.parts)
			}
			flag, ok := bound.Args[3].GetLit().Value.(*planpb.Literal_U8Val)
			require.True(t, ok)
			require.EqualValues(t, 2, flag.U8Val)
		})
	}
}

func TestCoveringIndexRangeKeepsExistingBoundedLookup(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	p, err := runOneStmt(mock, t, "select sum(b) from index_hint_t force index(idx_ab) where a=0 and b between 1000 and 1005 and b>=0 and b<9999")
	require.NoError(t, err)
	for _, node := range reachableIndexOrderNodes(p) {
		if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.IndexName == "idx_ab" {
			require.NotEmpty(t, node.FilterList)
			require.Equal(t, "prefix_between", node.FilterList[0].GetF().Func.ObjName)
			return
		}
	}
	t.Fatal("expected a covering index scan")
}
