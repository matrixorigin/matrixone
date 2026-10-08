// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func decimal256Bound(t testing.TB, typ types.Type, coefficient types.Decimal256) *plan.Expr {
	t.Helper()
	mp := mpool.MustNewZero()
	v, err := vector.NewConstFixed(typ, coefficient, 1, mp)
	require.NoError(t, err)
	defer v.Free(mp)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: data}},
	}
}

func TestDecimal256RangeSearch(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 2)
	for _, desc := range []bool{false, true} {
		values := []int64{0, 125, 125, 300}
		if desc {
			values = []int64{300, 125, 125, 0}
		}
		v := vector.NewVec(typ)
		t.Cleanup(func() { v.Free(mp) })
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(v, types.Decimal256FromInt64(value), false, mp))
		}
		for row, value := range values {
			for _, offset := range []int64{0, 125, 400} {
				expr := decimal256Bound(t, typ, types.Decimal256FromInt64(offset))
				for _, add := range []bool{false, true} {
					key := value - offset
					if add {
						key = value + offset
					}
					// Independent linear integer oracle for the fixed small data.
					left, right := 0, 0
					for _, candidate := range values {
						if (!desc && candidate < key) || (desc && candidate > key) {
							left++
						}
						if (!desc && candidate <= key) || (desc && candidate >= key) {
							right++
						}
					}
					gotLeft, err := searchLeft(0, len(values), row, v, expr, add != desc, desc)
					require.NoError(t, err)
					require.Equal(t, left, gotLeft)
					gotRight, err := searchRight(0, len(values), row, v, expr, add == desc, desc)
					require.NoError(t, err)
					require.Equal(t, right, gotRight)
				}
			}
		}
	}
}

func TestDecimal256RangePhysicalOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 76, 0)
	maximum := types.Decimal256{B0_63: math.MaxUint64, B64_127: math.MaxUint64, B128_191: math.MaxUint64, B192_255: math.MaxInt64}
	minimum := types.Decimal256{B192_255: uint64(1) << 63}
	offset := decimal256Bound(t, typ, types.Decimal256FromInt64(1))
	for _, desc := range []bool{false, true} {
		for _, high := range []bool{false, true} {
			value := minimum
			if high {
				value = maximum
			}
			v, err := vector.NewConstFixed(typ, value, 3, mp)
			require.NoError(t, err)
			t.Cleanup(func() { v.Free(mp) })
			want := 0
			if high != desc {
				want = 3
			}
			left, err := searchLeft(0, 3, 1, v, offset, high != desc, desc)
			require.NoError(t, err)
			require.Equal(t, want, left)
			right, err := searchRight(0, 3, 1, v, offset, high == desc, desc)
			require.NoError(t, err)
			require.Equal(t, want, right)
		}
	}
}

func TestDecimal256RangeOffsetValidation(t *testing.T) {
	typ := types.New(types.T_decimal256, 65, 2)
	expr := decimal256Bound(t, typ, types.Decimal256FromInt64(125))
	got, err := decimal256RangeOffset(expr, expr.Typ)
	require.NoError(t, err)
	require.Equal(t, types.Decimal256FromInt64(125), got)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_, err := decimal256RangeOffset(expr, expr.Typ)
		if err != nil {
			panic(err)
		}
	}))
	for _, mutate := range []func(*plan.Expr){
		func(e *plan.Expr) { e.GetVec().Data = e.GetVec().Data[:len(e.GetVec().Data)-1] },
		func(e *plan.Expr) { e.GetVec().Len = 2 },
		func(e *plan.Expr) { e.Typ.Scale++ },
		func(e *plan.Expr) { e.Typ.Width++ },
		func(e *plan.Expr) { e.GetVec().Data[0] = 255 },
	} {
		bad := decimal256Bound(t, typ, types.Decimal256FromInt64(125))
		mutate(bad)
		_, err := decimal256RangeOffset(bad, expr.Typ)
		require.Error(t, err)
	}
	negative := decimal256Bound(t, typ, types.Decimal256FromInt64(-1))
	_, err = decimal256RangeOffset(negative, negative.Typ)
	require.Error(t, err)
}

func TestDecimal256RangeConstantPartitions(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 2)
	v, err := vector.NewConstFixed(typ, types.Decimal256FromInt64(125), 3, mp)
	require.NoError(t, err)
	defer v.Free(mp)
	ctr := &container{orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{v}}}}
	frame := &plan.FrameClause{Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: decimal256Bound(t, typ, types.Decimal256FromInt64(100))},
		End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: decimal256Bound(t, typ, types.Decimal256FromInt64(200))},
	}
	for row := range 3 {
		left, right, err := ctr.buildRangeInterval(row, 0, 3, frame)
		require.NoError(t, err)
		require.Equal(t, 3, left)
		require.Equal(t, 3, right)
	}
	frame.Start.Type = plan.FrameBound_PRECEDING
	frame.End.Type = plan.FrameBound_PRECEDING
	frame.End.Val = decimal256Bound(t, typ, types.Decimal256FromInt64(0))
	left, right, err := ctr.buildRangeInterval(1, 1, 2, frame)
	require.NoError(t, err)
	require.Equal(t, 1, left)
	require.Equal(t, 2, right)

	null := vector.NewConstNull(typ, 3, mp)
	defer null.Free(mp)
	ctr.orderVecs[0].Vec[0] = null
	left, right, err = ctr.buildRangeInterval(1, 1, 2, frame)
	require.NoError(t, err)
	require.Equal(t, 1, left)
	require.Equal(t, 2, right)
}

func TestDecimal256RangePrepareReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	typ := types.New(types.T_decimal256, 65, 2)
	planned := makePreparedRangeFrame(t, 0, 0, typ)
	arg := makeWindowWithFrame(planned)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	for _, tc := range []struct {
		value *string
		want  int64
		bad   bool
	}{
		{stringPtr("0"), 0, false},
		{stringPtr("1.25"), 125, false},
		{stringPtr("-1"), 0, true},
		{nil, 0, true},
		{stringPtr("invalid"), 0, true},
		{stringPtr("3"), 300, false},
	} {
		func() {
			params := setWindowPrepareParams(t, proc, tc.value)
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			err := arg.Prepare(proc)
			if tc.bad {
				require.Error(t, err)
				require.Nil(t, arg.ctr.runtimeFrames)
			} else {
				require.NoError(t, err)
				bound := arg.ctr.runtimeFrames[0].Start.Val
				got, err := decimal256RangeOffset(bound, bound.Typ)
				require.NoError(t, err)
				require.Equal(t, types.Decimal256FromInt64(tc.want), got)
			}
			requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
			arg.Reset(proc, false, nil)
		}()
	}
	constant := decimal256Bound(t, typ, types.Decimal256FromInt64(125))
	bound, err := materializeWindowBound(proc, &plan.FrameBound{Val: constant}, plan.FrameClause_RANGE)
	require.NoError(t, err)
	require.Same(t, constant, bound.Val)
}

func BenchmarkDecimal256RangeOffset(b *testing.B) {
	expr := decimal256Bound(b, types.New(types.T_decimal256, 65, 2), types.Decimal256FromInt64(125))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := decimal256RangeOffset(expr, expr.Typ); err != nil {
			b.Fatal(err)
		}
	}
}
