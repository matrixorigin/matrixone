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

package hashbuild

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRecoveryProjectionPreservesLogicalVarlenaMultiplicity(t *testing.T) {
	mp := mpool.MustNewZero()
	const rows = 4
	payload := strings.Repeat("x", 64)
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(payload), 1, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	require.Equal(t, 1, constant.Length(), "the value is physically stored once")
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = constant
	bat.SetRowCount(rows)

	selected, err := projectedSelectedRange(bat, 0, rows)
	require.NoError(t, err)
	require.Equal(t, uint64(rows*(types.VarlenaSize+len(payload))), selected)

	selected, err = projectedSelectedRange(bat, 2, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2*(types.VarlenaSize+len(payload))), selected)
}

func TestRecoveryProjectionTreatsBroadcastConstNullAsZeroPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("x", 64)), 1, mp,
	)
	require.NoError(t, err)
	constant.SetNull(0)
	source := batch.NewWithSize(1)
	source.Vecs[0] = constant
	source.SetRowCount(4)

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.True(t, window.Vecs[0].IsConstNull())
	require.Equal(t, 2, window.RowCount())

	projection, err := (&HashmapBuilder{}).projectRetainedRecovery(window)
	require.NoError(t, err)
	require.Equal(t, 2, projection.maxRows)
	require.Equal(t, uint64(2*types.VarlenaSize), projection.maxSelected)
	require.Equal(t, projection.maxSelected, projection.nextTailSelected)

	window.Clean(nil)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestRecoveryProjectionIgnoresReusableConstNullArea(t *testing.T) {
	mp := mpool.MustNewZero()
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("x", 64)), 1, mp,
	)
	require.NoError(t, err)
	require.NotEmpty(t, constant.GetArea())
	require.NoError(t, vector.SetConstNull(constant, 4, mp))
	require.True(t, constant.IsConstNull())
	require.NotEmpty(t, constant.GetArea(), "SetConstNull retains reusable area")
	source := batch.NewWithSize(1)
	source.Vecs[0] = constant
	source.SetRowCount(4)

	physical, selected, err := unionBatchAreaProjection(constant, 0, 4)
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, selected)

	projection, err := (&HashmapBuilder{}).projectRetainedRecovery(source)
	require.NoError(t, err)
	require.Equal(t, 4, projection.maxRows)
	require.Equal(t, uint64(4*types.VarlenaSize), projection.maxSelected)
	require.Equal(t, projection.maxSelected, projection.nextTailSelected)

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestRecoveryProjectionRejectsMissingPhysicalRows(t *testing.T) {
	for _, test := range []struct {
		name string
		rows int
		vec  func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "empty constant",
			rows: 1,
			vec: func(_ *testing.T, mp *mpool.MPool) *vector.Vector {
				return vector.NewConstNull(types.T_varchar.ToType(), 0, mp)
			},
		},
		{
			name: "short flat vector",
			rows: 2,
			vec: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				t.Helper()
				vec := vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(vec, []byte("value"), false, mp))
				return vec
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := batch.NewWithSize(1)
			source.Vecs[0] = test.vec(t, mp)
			source.SetRowCount(test.rows)

			_, err := projectedSelectedRange(source, 0, test.rows)
			require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)

			source.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestRecoveryProjectionUsesIncrementalPartialTail(t *testing.T) {
	mp := mpool.MustNewZero()
	tail := batch.NewWithSize(1)
	tail.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3}, nil, mp)
	tail.SetRowCount(3)
	defer tail.Clean(mp)
	source := batch.NewWithSize(1)
	source.Vecs[0] = testutil.MakeInt64Vector([]int64{4, 5, 6, 7}, nil, mp)
	source.SetRowCount(4)
	defer source.Clean(mp)

	builder := HashmapBuilder{}
	builder.Batches.Buf = []*batch.Batch{tail}
	builder.retainedSpillTailSelected = 3 * 8
	projection, err := builder.projectRetainedRecovery(source)
	require.NoError(t, err)
	require.Equal(t, 7, projection.maxRows)
	require.Equal(t, uint64(7*8), projection.maxSelected)
	require.Equal(t, uint64(7*8), projection.nextTailSelected)
}

func TestExpressionRecoveryIncludesReplacementOverlap(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454ConcatKey(t, proc)
	peak, err := expressionVectorPeak(proc, expr, 1024, false)
	require.NoError(t, err)
	recovery, err := expressionRecoveryBytes(proc, []*plan.Expr{expr}, 1024, false)
	require.NoError(t, err)
	require.Equal(t, 2*peak, recovery)
}

func TestExpressionRecoveryStringCastBound(t *testing.T) {
	for _, tc := range []struct {
		name     string
		source   types.T
		overload int32
	}{
		{"implicit char", types.T_char, 0},
		{"explicit varchar", types.T_varchar, 1},
		{"comparison char", types.T_char, 2},
		{"set varchar", types.T_varchar, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSpillTestHarness(t, 8<<20)
			t.Cleanup(func() {
				h.op.ctr.freeSpillExprExecs()
				require.NoError(t, h.op.releaseRecoveryCapacity(h.account, false))
				h.close(t)
			})
			source := &plan.Expr{
				Typ:  plan.Type{Id: int32(tc.source), Width: 10},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			}
			target := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				Expr: &plan.Expr_T{T: &plan.TargetType{}},
			}
			expr, err := plan2.BindFuncExprImplByPlanExpr(h.proc.Ctx, "cast", []*plan.Expr{source, target})
			require.NoError(t, err)
			expr.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, tc.overload)
			conditions := []*plan.Expr{expr}
			peak, err := expressionRecoveryBytes(h.proc, conditions, 8192, true)
			require.NoError(t, err)
			require.Less(t, peak, uint64(8<<20), "narrow string keys must not reserve GiB per batch")
			resultPeak, err := expressionResultPeak(expr, 8192)
			require.NoError(t, err)
			require.GreaterOrEqual(t, resultPeak, uint64(8192*(types.VarlenaSize+40)), "ten UTF-8 characters can occupy forty bytes")

			// Prove the real executor fits the admitted floor, including NULL and
			// an out-of-line four-byte UTF-8 value; no further admission is allowed.
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(h.proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.New(tc.source, 10, 0))
			values := []string{"12345", strings.Repeat("\U0001f600", 10), ""}
			for i := 0; i < 8192; i++ {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(values[i%len(values)]), i%len(values) == 2, h.proc.Mp()))
			}
			input.SetRowCount(8192)
			require.NoError(t, h.op.installRecoveryCapacity(h.generation))
			require.NoError(t, h.op.ctr.ensureRecoveryCapacity(h.proc, conditions,
				recoveryBatchProjection{maxRows: 8192, columns: 1}, nil))
			reserved, _ := h.op.ctr.recoveryCapacity.Snapshot()
			require.NoError(t, h.budget.UpdateAggregateCap(1))
			executors, err := h.op.ctr.initSpillExprExecs(h.proc, conditions)
			require.NoError(t, err)
			result, err := executors[0].Eval(h.proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, input.RowCount(), result.Length())
			for i := 0; i < input.RowCount(); i++ {
				require.Equal(t, i%len(values) == 2, result.IsNull(uint64(i)))
				if !result.IsNull(uint64(i)) {
					require.Equal(t, values[i%len(values)], result.GetStringAt(i))
				}
			}
			after, _ := h.op.ctr.recoveryCapacity.Snapshot()
			require.Equal(t, reserved, after)
			require.Zero(t, h.generation.RejectCount())

			// Unknown source widths and non-string conversions retain the fallback.
			for _, typ := range []plan.Type{
				{Id: int32(types.T_varchar)},
				{Id: int32(types.T_int64)},
				{Id: int32(types.T_text), Width: 10},
			} {
				expr.GetF().Args[0].Typ = typ
				fallback, err := expressionResultPeak(expr, 8192)
				require.NoError(t, err)
				want, err := expressionTypePeak(expr.Typ, 8192)
				require.NoError(t, err)
				require.Equal(t, want, fallback)
			}
		})
	}
}
