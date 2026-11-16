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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Test the cumulative un-compacted tuple wire budget, including duplicate rows
// and the vector envelope. These synthetic equality literals put that internal
// boundary exactly at 80 MiB without requiring a large IN payload.
func TestKeySetOriginalWireBudgetBoundary(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
	mp := proc.Mp()
	typ := types.T_varchar.ToType()
	typ.Width = 700000
	planType := makePlan2Type(&typ)
	idx := &IndexDef{Unique: true, Parts: []string{"a", "b"}}
	literal := &planpb.Expr{Typ: planType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: strings.Repeat("q", 650000)}}}}
	equal := &planpb.Expr{Selectivity: 1, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{GetColExpr(planType, 1, 0), literal}}}}
	sample := vector.NewVec(typ)
	defer sample.Free(mp)
	require.NoError(t, vector.AppendBytes(sample, []byte("aa"), false, mp))
	rowVec := vector.NewVec(typ)
	defer rowVec.Free(mp)
	rawSize := func(src *vector.Vector, row int) int {
		data, err := marshalKeySetElement(src, rowVec, row, mp)
		require.NoError(t, err)
		tuple, err := bindFuncExprAndConstFold(builder.GetContext(), proc, "serial", []*planpb.Expr{
			DeepCopyExpr(literal), {Typ: planType, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(len(data)), Data: data}}},
		})
		require.NoError(t, err)
		require.NotNil(t, tuple)
		require.NotNil(t, tuple.GetVec())
		return len(tuple.GetVec().Data)
	}
	targetPerRow := maxCompositePointBytes / maxCompositePoints
	adjust := targetPerRow - rawSize(sample, 0)
	require.Positive(t, adjust)
	literal.GetLit().Value.(*planpb.Literal_Sval).Sval += strings.Repeat("q", adjust)
	require.Equal(t, targetPerRow, rawSize(sample, 0))
	require.Equal(t, maxCompositePointBytes, targetPerRow*maxCompositePoints)
	for _, tc := range []struct {
		name     string
		last     string
		delta    int
		accepted bool
		distinct int
	}{
		{"below", "a", -1, true, 2},
		{"equal", "aa", 0, true, 1},
		{"above", "aaa", 1, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := vector.NewVec(typ)
			defer values.Free(mp)
			for row := 0; row < maxCompositePoints; row++ {
				value := "aa"
				if row == maxCompositePoints-1 {
					value = tc.last
				}
				require.NoError(t, vector.AppendBytes(values, []byte(value), false, mp))
			}
			require.Equal(t, targetPerRow+tc.delta, rawSize(values, maxCompositePoints-1))
			data, err := values.MarshalBinary()
			require.NoError(t, err)
			in := &planpb.Expr{Selectivity: .5, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{GetColExpr(planType, 1, 1), {Typ: planType, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(values.Length()), Data: data}}}}}}}
			got := builder.replaceKeySetCondition(idx, []*planpb.Expr{equal, in}, []int32{0, 1}, 7, mock.ctxt.tables["uk_hint_ab"])
			if !tc.accepted {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			count, ok := exactUniqueKeySetSize(got)
			require.True(t, ok)
			require.Equal(t, tc.distinct, count)
		})
	}
}
