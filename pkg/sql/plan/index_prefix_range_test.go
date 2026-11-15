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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSecondaryIndexEqualityPrefixWithIntegerRange(t *testing.T) {
	for _, tc := range []struct {
		name, op, want, params                                      string
		rangeSel                                                    float64
		otherColumn, otherBinding, floatRange, unique, disableBlock bool
		dynamicPrefix, dynamicRange                                 bool
	}{
		{name: "between", op: "between", want: "prefix_between"},
		{name: "zero_range_cost", op: "between", want: "prefix_between"},
		{name: "dynamic_prefix_cost", op: "between", want: "prefix_between", dynamicPrefix: true},
		{name: "dynamic_range_cost", op: "between", want: "prefix_between", dynamicRange: true},
		{name: "unknown_range_cost", op: "between", want: "prefix_between", rangeSel: -1},
		{name: "nan_range_cost", op: "between", want: "prefix_between", rangeSel: math.NaN()},
		{name: "infinite_range_cost", op: "between", want: "prefix_between", rangeSel: math.Inf(1)},
		{name: "block_filter_disabled", op: "between", want: "prefix_between", disableBlock: true},
		{name: "open_range", op: "in_range", want: "prefix_in_range"},
		{name: "lower_open", op: ">", want: "prefix_in_range"},
		{name: "upper_closed", op: "<=", want: "prefix_in_range"},
		{name: "unindexed_column", op: "between", otherColumn: true},
		{name: "foreign_binding", op: "between", otherBinding: true},
		{name: "float_range", op: "between", floatRange: true},
		{name: "truncated_part", op: "between", params: `{"prefix_lengths":"b:4"}`},
		{name: "malformed_metadata", op: "between", params: `{broken`},
		{name: "unique_encoding", op: "between", unique: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			if tc.disableBlock {
				builder.optimizerHints = &OptimizerHints{blockFilter: 2}
			}
			scanTag, indexTag := int32(10), int32(20)
			intType := planpb.Type{Id: int32(types.T_int32)}
			rangeType := intType
			if tc.floatRange {
				rangeType.Id = int32(types.T_float64)
			}
			table := &planpb.TableDef{
				Name: "t", Cols: []*planpb.ColDef{
					{Name: "id", Typ: intType}, {Name: "a", Typ: intType},
					{Name: "b", Typ: rangeType}, {Name: "c", Typ: rangeType},
				},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2, "c": 3},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			}
			idx := &planpb.IndexDef{IndexName: "ix_ab", Parts: []string{"a", "b", catalog.CreateAlias("id")},
				IndexAlgoParams: tc.params, TableExist: true, Unique: tc.unique}
			eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
				GetColExpr(intType, scanTag, 1), makePlan2Int32ConstExprWithType(7),
			})
			require.NoError(t, err)
			colPos, rel := int32(2), scanTag
			if tc.otherColumn {
				colPos = 3
			}
			if tc.otherBinding {
				rel = 99
			}
			args := []*planpb.Expr{GetColExpr(rangeType, rel, colPos)}
			if tc.floatRange {
				args = append(args, MakePlan2Float64ConstExprWithType(1))
				if tc.op == "between" {
					args = append(args, MakePlan2Float64ConstExprWithType(3))
				}
			} else {
				args = append(args, makePlan2Int32ConstExprWithType(1))
				if tc.op == "between" || tc.op == "in_range" {
					args = append(args, makePlan2Int32ConstExprWithType(3))
				}
			}
			if tc.op == "in_range" {
				args = append(args, MakePlan2Uint8ConstExprWithType(3))
			}
			rangeExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), tc.op, args)
			require.NoError(t, err)
			if tc.dynamicPrefix {
				eq.GetF().Args[1] = &planpb.Expr{Typ: intType, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			}
			if tc.dynamicRange {
				rangeExpr.GetF().Args[1] = &planpb.Expr{Typ: rangeType, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}}
			}
			eq.Selectivity = 0.01
			rangeExpr.Selectivity = 0.25
			if tc.rangeSel != 0 {
				rangeExpr.Selectivity = tc.rangeSel
			}
			scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{scanTag},
				TableDef: table, FilterList: []*planpb.Expr{eq, rangeExpr}}
			idxTable := makeTestIndexTableDef()
			idxScan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{indexTag}, TableDef: idxTable,
				FilterList: []*planpb.Expr{builder.replaceEqualCondition(idx, scan.FilterList, []int32{0}, indexTag, idxTable)}}
			if tc.name == "zero_range_cost" {
				rangeExpr.Selectivity = 0
			}
			original := DeepCopyExprList(scan.FilterList)
			if tc.name == "between" {
				// An access range cannot take ownership of the estimate until
				// the decoded SQL range has actually been published.
				colMap, err := builder.regularIndexColumnMap(idx, scan, indexTag, idxTable)
				require.NoError(t, err)
				require.Empty(t, builder.secondaryIndexPrefixRanges(idx, scan, idxScan, []int32{0}, colMap))
			}
			builder.applyExtraFiltersOnIndex(idx, scan, idxScan, []int32{0})
			require.Len(t, scan.FilterList, len(original))
			for i := range original {
				before, err := original[i].Marshal()
				require.NoError(t, err)
				after, err := scan.FilterList[i].Marshal()
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
			var found *planpb.Expr
			for _, filter := range idxScan.BlockFilterList {
				if fn := filter.GetF(); fn != nil && (fn.Func.ObjName == "prefix_between" || fn.Func.ObjName == "prefix_in_range") {
					found = filter
				}
			}
			if tc.want == "" || tc.disableBlock {
				require.Nil(t, found)
				return
			}
			require.NotNil(t, found)
			require.Equal(t, tc.want, found.GetF().Func.ObjName)
			require.Equal(t, tc.want, idxScan.FilterList[0].GetF().Func.ObjName)
			// The decoded range filter, not the access prefix, owns the
			// range selectivity so the scan estimates it only once.
			require.InDelta(t, 0.01, idxScan.FilterList[0].Selectivity, 1e-12)
			require.Equal(t, indexTag, found.GetF().Args[0].GetCol().RelPos)
			for _, bound := range found.GetF().Args[1:3] {
				require.Equal(t, "serial_full", bound.GetF().Func.ObjName)
			}
		})
	}
}
