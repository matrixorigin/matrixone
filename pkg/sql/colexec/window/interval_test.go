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

package window

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestWindowPrepareMaterializesIntervalBounds(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	raw := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
		plan2.MakePlan2StringConstExprWithType("second"),
	}}}}
	planned := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: raw}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
	original := planned.String()
	arg := makeWindowWithFrame(planned)
	defer func() {
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	for _, tc := range []struct {
		text *string
		want int64
		fail bool
	}{{stringPtr("1.5"), 1500000, false}, {nil, 0, true}, {stringPtr("-0.0000001"), 0, true}, {stringPtr("9223372036854.775808"), 0, true}, {stringPtr("0.000001"), 1, false}, {stringPtr("2"), 2000000, false}} {
		func() {
			params := setWindowPrepareParams(t, proc, tc.text)
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			err := arg.Prepare(proc) // no input batch is needed for validation
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, arg.ctr.runtimeFrames)
			} else {
				require.NoError(t, err)
				got := arg.ctr.runtimeFrames[0].Start.Val
				require.NotSame(t, raw, got)
				require.Equal(t, tc.want, got.GetList().List[0].GetLit().GetI64Val())
				require.Equal(t, int64(types.MicroSecond), got.GetList().List[1].GetLit().GetI64Val())
			}
			require.Equal(t, original, planned.String())
			arg.Reset(proc, false, nil)
		}()
	}
}

func TestNullIntervalRangeBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	magnitude := intervalInt64(0)
	magnitude.GetLit().Isnull = true
	offset := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{magnitude, intervalInt64(int64(types.Second))}}}}
	for _, typ := range []types.T{types.T_date, types.T_time, types.T_datetime, types.T_timestamp} {
		t.Run(typ.String(), func(t *testing.T) {
			// Zero-valued temporal keys suffice: the NULL arithmetic must be
			// decided before any date arithmetic or timestamp fold indexing.
			vec := vector.NewVec(typ.ToType())
			defer vec.Free(proc.Mp())
			for _, isNull := range []bool{false, true} {
				vec.CleanOnlyData()
				if typ == types.T_date {
					require.NoError(t, vector.AppendFixed(vec, types.Date(0), isNull, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(vec, int64(0), isNull, proc.Mp()))
				}
				for _, desc := range []bool{false, true} {
					for _, startNull := range []bool{false, true} {
						frame := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
						if startNull {
							frame.Start = &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: offset}
						} else {
							frame.End = &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: offset}
						}
						ctr := &container{desc: []bool{desc}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}}}
						left, right, selection, err := ctr.buildIntervalRows(proc, 0, 0, 1, frame)
						require.NoError(t, err)
						require.Nil(t, selection)
						require.Zero(t, left)
						if isNull {
							require.Equal(t, 1, right)
						} else {
							require.Zero(t, right)
						}
						require.False(t, boundedSlidingRangeFrame(frame, ctr.orderVecs))
						// Exercise the separate timestamp consumer directly too.
						selection, err = ctr.timestampRangeSelection(proc, time.UTC, 0, 0, 1, vec, frame)
						require.NoError(t, err)
						require.Nil(t, selection)
					}
				}
			}
		})
	}
}
