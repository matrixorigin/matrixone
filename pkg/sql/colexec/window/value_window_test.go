// Copyright 2021 Matrix Origin
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

package window

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// makeLagWindowSpec creates a WinSpecList entry for LAG(col0)
func makeLagWindowSpec() *plan.Expr {
	return makeValueWindowSpecWithName("lag", int32(types.T_int32))
}

func makeLeadWindowSpec() *plan.Expr {
	return makeValueWindowSpecWithName("lead", int32(types.T_int32))
}

func makeFirstValueWindowSpec() *plan.Expr {
	return makeValueWindowSpecWithName("first_value", int32(types.T_int32))
}

func makeLastValueWindowSpec() *plan.Expr {
	return makeValueWindowSpecWithName("last_value", int32(types.T_int32))
}

func makeNthValueWindowSpec() *plan.Expr {
	return makeValueWindowSpecWithName("nth_value", int32(types.T_int32))
}

func makeValueWindowSpecWithName(name string, typeId int32) *plan.Expr {
	f := &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_PRECEDING,
			UnBounded: true,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_FOLLOWING,
			UnBounded: true,
		},
	}
	return &plan.Expr{
		Typ: plan.Type{Id: typeId},
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				Name: name,
				WindowFunc: &plan.Expr{
					Typ: plan.Type{Id: typeId},
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{
								ObjName: name,
							},
						},
					},
				},
				Frame: f,
			},
		},
	}
}

func makeValueWindowAggExpr(name string) aggexec.AggFuncExecExpression {
	// Use a dummy agg ID — for WIN_VALUE functions, the AggFuncExec is never created.
	return aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColExpr(0)}, nil)
}

func makeInt32Batch(mp *mpool.MPool, vals []int32) *batch.Batch {
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil)
	bat.SetRowCount(len(vals))
	return bat
}

func makeVarcharBatch(mp *mpool.MPool, vals []string) *batch.Batch {
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = testutil.MakeVarcharVector(vals, nil)
	bat.SetRowCount(len(vals))
	return bat
}

func runValueWindowTest(t *testing.T, winSpec *plan.Expr, bat *batch.Batch, mp *mpool.MPool) *vector.Vector {
	proc := testutil.NewProcessWithMPool(t, "", mp)

	arg := &Window{
		WinSpecList: []*plan.Expr{winSpec},
		Types:       []types.Type{types.T_int32.ToType()},
		Aggs:        []aggexec.AggFuncExecExpression{makeValueWindowAggExpr("")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)

	err := arg.Prepare(proc)
	require.NoError(t, err)

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	// The result vector is the last column
	resultVec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
	// Dup so we can free the operator
	dup, err := resultVec.Dup(mp)
	require.NoError(t, err)

	arg.Free(proc, false, nil)
	proc.Free()
	return dup
}

func TestProcessValueFunc_Lag(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	result := runValueWindowTest(t, makeLagWindowSpec(), bat, mp)
	defer result.Free(mp)

	require.Equal(t, 4, result.Length())
	// LAG(col): first row should be NULL, rest should be previous value
	require.True(t, result.IsNull(0))
	require.Equal(t, int32(10), vector.MustFixedColNoTypeCheck[int32](result)[1])
	require.Equal(t, int32(20), vector.MustFixedColNoTypeCheck[int32](result)[2])
	require.Equal(t, int32(30), vector.MustFixedColNoTypeCheck[int32](result)[3])
}

func TestProcessValueFunc_Lead(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	result := runValueWindowTest(t, makeLeadWindowSpec(), bat, mp)
	defer result.Free(mp)

	require.Equal(t, 4, result.Length())
	require.Equal(t, int32(20), vector.MustFixedColNoTypeCheck[int32](result)[0])
	require.Equal(t, int32(30), vector.MustFixedColNoTypeCheck[int32](result)[1])
	require.Equal(t, int32(40), vector.MustFixedColNoTypeCheck[int32](result)[2])
	require.True(t, result.IsNull(3))
}

func TestProcessValueFunc_FirstValue(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	result := runValueWindowTest(t, makeFirstValueWindowSpec(), bat, mp)
	defer result.Free(mp)

	require.Equal(t, 4, result.Length())
	// FIRST_VALUE with unbounded frame: always the first row
	for i := 0; i < 4; i++ {
		require.Equal(t, int32(10), vector.MustFixedColNoTypeCheck[int32](result)[i])
	}
}

func TestProcessValueFunc_LastValue(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	result := runValueWindowTest(t, makeLastValueWindowSpec(), bat, mp)
	defer result.Free(mp)

	require.Equal(t, 4, result.Length())
	// LAST_VALUE with unbounded frame: always the last row
	for i := 0; i < 4; i++ {
		require.Equal(t, int32(40), vector.MustFixedColNoTypeCheck[int32](result)[i])
	}
}

func TestProcessValueFunc_NthValue(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	// NTH_VALUE defaults to n=1 (same as first_value) when no second arg
	result := runValueWindowTest(t, makeNthValueWindowSpec(), bat, mp)
	defer result.Free(mp)

	require.Equal(t, 4, result.Length())
	for i := 0; i < 4; i++ {
		require.Equal(t, int32(10), vector.MustFixedColNoTypeCheck[int32](result)[i])
	}
}

// TestProcessValueFunc_NthValueWithN tests nth_value(expr, 3) with unbounded frame.
func TestProcessValueFunc_NthValueWithN(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeNthValueWindowSpec()

	ctr := &container{bat: bat}
	nVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(3), 1, mp)
	require.NoError(t, err)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], nVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// nth_value(expr, 3) with unbounded frame: always the 3rd row = 30
	for i := 0; i < 4; i++ {
		require.Equal(t, int32(30), vals[i])
	}

	result.Free(mp)
	nVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_NthValueOutOfBounds tests nth_value with n exceeding frame size.
func TestProcessValueFunc_NthValueOutOfBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20})
	spec := makeNthValueWindowSpec()

	ctr := &container{bat: bat}
	nVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, mp)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], nVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Length())
	// n=5 exceeds frame size=2, all NULL
	require.True(t, result.IsNull(0))
	require.True(t, result.IsNull(1))

	result.Free(mp)
	nVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LeadWithOffset tests lead with offset=2.
func TestProcessValueFunc_LeadWithOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeLeadWindowSpec()

	ctr := &container{bat: bat}
	offsetVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, mp)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], offsetVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.Equal(t, int32(30), vals[0]) // lead(10, 2) → 30
	require.Equal(t, int32(40), vals[1]) // lead(20, 2) → 40
	require.True(t, result.IsNull(2))    // lead(30, 2) → NULL
	require.True(t, result.IsNull(3))    // lead(40, 2) → NULL

	result.Free(mp)
	offsetVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_NthValueWithFrame tests nth_value with explicit frame.
func TestProcessValueFunc_NthValueWithFrame(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeNthValueWindowSpec()
	w := spec.Expr.(*plan.Expr_W).W
	// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}},
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}},
		},
	}

	ctr := &container{bat: bat}
	nVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, mp)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], nVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Row 0: frame [0,2), nth(2) → index 1 → 20
	require.Equal(t, int32(20), vals[0])
	// Row 1: frame [0,3), nth(2) → index 1 → 20
	require.Equal(t, int32(20), vals[1])
	// Row 2: frame [1,4), nth(2) → index 2 → 30
	require.Equal(t, int32(30), vals[2])
	// Row 3: frame [2,4), nth(2) → index 3 → 40
	require.Equal(t, int32(40), vals[3])

	result.Free(mp)
	nVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestGetInt64FromVec(t *testing.T) {
	mp := mpool.MustNewZero()

	check := func(v *vector.Vector, expected int64) {
		val, ok := getInt64FromVec(v, 0)
		require.True(t, ok)
		require.Equal(t, expected, val)
		v.Free(mp)
	}

	check(testutil.MakeInt64Vector([]int64{5}, nil), 5)
	check(testutil.MakeInt32Vector([]int32{3}, nil), 3)
	check(testutil.MakeUint64Vector([]uint64{7}, nil), 7)
	check(testutil.MakeInt8Vector([]int8{2}, nil), 2)
	check(testutil.MakeInt16Vector([]int16{4}, nil), 4)
	check(testutil.MakeUint8Vector([]uint8{6}, nil), 6)
	check(testutil.MakeUint16Vector([]uint16{8}, nil), 8)
	check(testutil.MakeUint32Vector([]uint32{9}, nil), 9)

	// unsupported type → ok=false
	v11 := testutil.MakeVarcharVector([]string{"x"}, nil)
	_, ok := getInt64FromVec(v11, 0)
	require.False(t, ok)
	v11.Free(mp)

	// NULL value → ok=false
	v12 := testutil.MakeInt64Vector([]int64{0}, []uint64{0})
	_, ok = getInt64FromVec(v12, 0)
	require.False(t, ok)
	v12.Free(mp)
}

func TestAppendDefaultOrNull(t *testing.T) {
	mp := mpool.MustNewZero()

	// nil default → NULL
	result := vector.NewVec(types.T_int32.ToType())
	err := appendDefaultOrNull(result, nil, 0, mp)
	require.NoError(t, err)
	require.True(t, result.IsNull(0))
	result.Free(mp)

	// const default
	result2 := vector.NewVec(types.T_int32.ToType())
	defVec := testutil.MakeInt32Vector([]int32{99}, nil)
	err = appendDefaultOrNull(result2, defVec, 0, mp)
	require.NoError(t, err)
	require.Equal(t, int32(99), vector.MustFixedColNoTypeCheck[int32](result2)[0])
	result2.Free(mp)
	defVec.Free(mp)
}

// --- Tests for partition, frame, offset/default scenarios ---

// TestProcessValueFunc_LagWithPartition tests lag across partition boundaries.
// Simulates two partitions by manually setting ctr.ps.
func TestProcessValueFunc_LagWithPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	// Data: partition1=[10,20,30], partition2=[40,50] (already sorted)
	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40, 50})
	spec := makeLagWindowSpec()

	ctr := &container{bat: bat}
	// aggVecs: single vec for the expression column
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}
	// Partition boundaries: [0,3) and [3,5)
	ctr.ps = []int64{0, 3, 5}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 5, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Partition 1: [10,20,30]
	require.True(t, result.IsNull(0))    // lag(10) in partition1 → NULL
	require.Equal(t, int32(10), vals[1]) // lag(20) → 10
	require.Equal(t, int32(20), vals[2]) // lag(30) → 20
	// Partition 2: [40,50]
	require.True(t, result.IsNull(3))    // lag(40) in partition2 → NULL
	require.Equal(t, int32(40), vals[4]) // lag(50) → 40

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LeadWithPartition tests lead across partition boundaries.
func TestProcessValueFunc_LeadWithPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40, 50})
	spec := makeLeadWindowSpec()

	ctr := &container{bat: bat}
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}
	ctr.ps = []int64{0, 3, 5}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 5, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Partition 1: [10,20,30]
	require.Equal(t, int32(20), vals[0]) // lead(10) → 20
	require.Equal(t, int32(30), vals[1]) // lead(20) → 30
	require.True(t, result.IsNull(2))    // lead(30) → NULL (end of partition)
	// Partition 2: [40,50]
	require.Equal(t, int32(50), vals[3]) // lead(40) → 50
	require.True(t, result.IsNull(4))    // lead(50) → NULL

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LagWithOffset tests lag with offset=2.
func TestProcessValueFunc_LagWithOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeLagWindowSpec()

	ctr := &container{bat: bat}
	// aggVecs[0].Vec[0] = value column, Vec[1] = offset (const 2)
	offsetVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, mp)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], offsetVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.True(t, result.IsNull(0))    // lag(10, 2) → NULL
	require.True(t, result.IsNull(1))    // lag(20, 2) → NULL
	require.Equal(t, int32(10), vals[2]) // lag(30, 2) → 10
	require.Equal(t, int32(20), vals[3]) // lag(40, 2) → 20

	result.Free(mp)
	offsetVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LagWithDefault tests lag with offset=1 and default value.
func TestProcessValueFunc_LagWithDefault(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30})
	spec := makeLagWindowSpec()

	ctr := &container{bat: bat}
	offsetVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, mp)
	defaultVec := testutil.MakeInt32Vector([]int32{-1}, nil)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], offsetVec, defaultVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 3, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.Equal(t, int32(-1), vals[0]) // lag(10, 1, -1) → -1 (default)
	require.Equal(t, int32(10), vals[1]) // lag(20, 1, -1) → 10
	require.Equal(t, int32(20), vals[2]) // lag(30, 1, -1) → 20

	result.Free(mp)
	offsetVec.Free(mp)
	defaultVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_FirstValueWithFrame tests first_value with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING.
func TestProcessValueFunc_FirstValueWithFrame(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeValueWindowSpecWithName("first_value", int32(types.T_int32))
	// Override frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	w := spec.Expr.(*plan.Expr_W).W
	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val: &plan.Expr{
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}},
			},
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val: &plan.Expr{
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}},
			},
		},
	}

	ctr := &container{bat: bat}
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Row 0: frame [max(0,0-1), 0+1+1) = [0, 2) → first_value = 10
	require.Equal(t, int32(10), vals[0])
	// Row 1: frame [1-1, 1+1+1) = [0, 3) → first_value = 10
	require.Equal(t, int32(10), vals[1])
	// Row 2: frame [2-1, 2+1+1) = [1, 4) → first_value = 20
	require.Equal(t, int32(20), vals[2])
	// Row 3: frame [3-1, min(4,3+1+1)) = [2, 4) → first_value = 30
	require.Equal(t, int32(30), vals[3])

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LastValueWithFrame tests last_value with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING.
func TestProcessValueFunc_LastValueWithFrame(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeValueWindowSpecWithName("last_value", int32(types.T_int32))
	w := spec.Expr.(*plan.Expr_W).W
	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val: &plan.Expr{
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}},
			},
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val: &plan.Expr{
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}},
			},
		},
	}

	ctr := &container{bat: bat}
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Row 0: frame [0, 2) → last_value = 20
	require.Equal(t, int32(20), vals[0])
	// Row 1: frame [0, 3) → last_value = 30
	require.Equal(t, int32(30), vals[1])
	// Row 2: frame [1, 4) → last_value = 40
	require.Equal(t, int32(40), vals[2])
	// Row 3: frame [2, 4) → last_value = 40
	require.Equal(t, int32(40), vals[3])

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LagNonConstOffset tests lag with a non-const offset vector.
func TestProcessValueFunc_LagNonConstOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeLagWindowSpec()

	ctr := &container{bat: bat}
	// Non-const offset vector: [1, 2, 0, -1]
	offsetVec := testutil.MakeInt64Vector([]int64{1, 2, 0, -1}, nil)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], offsetVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.True(t, result.IsNull(0))    // lag(10, 1) → NULL (no prev)
	require.True(t, result.IsNull(1))    // lag(20, 2) → NULL (not enough rows)
	require.Equal(t, int32(30), vals[2]) // lag(30, 0) → 30 (self)
	require.True(t, result.IsNull(3))    // lag(40, -1) → NULL (negative offset)

	result.Free(mp)
	offsetVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LeadNonConstOffset tests lead with a non-const offset vector.
func TestProcessValueFunc_LeadNonConstOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeLeadWindowSpec()

	ctr := &container{bat: bat}
	offsetVec := testutil.MakeInt64Vector([]int64{2, 1, 0, -1}, nil)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], offsetVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.Equal(t, int32(30), vals[0]) // lead(10, 2) → 30
	require.Equal(t, int32(30), vals[1]) // lead(20, 1) → 30
	require.Equal(t, int32(30), vals[2]) // lead(30, 0) → 30 (self)
	require.True(t, result.IsNull(3))    // lead(40, -1) → NULL (negative offset)

	result.Free(mp)
	offsetVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_NthValueNonConst tests nth_value with a non-const n vector.
func TestProcessValueFunc_NthValueNonConst(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40})
	spec := makeNthValueWindowSpec()

	ctr := &container{bat: bat}
	// Non-const n vector: [1, 2, 0, 5] — 0 and 5 are invalid/out-of-bounds
	nVec := testutil.MakeInt64Vector([]int64{1, 2, 0, 5}, nil)
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0], nVec}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	require.Equal(t, int32(10), vals[0]) // nth_value(1) → 10
	require.Equal(t, int32(20), vals[1]) // nth_value(2) → 20
	require.True(t, result.IsNull(2))    // nth_value(0) → NULL (< 1)
	require.True(t, result.IsNull(3))    // nth_value(5) → NULL (out of bounds)

	result.Free(mp)
	nVec.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_FirstValueWithPartition tests first_value with partitions.
func TestProcessValueFunc_FirstValueWithPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40, 50})
	spec := makeFirstValueWindowSpec()

	ctr := &container{bat: bat}
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}
	ctr.ps = []int64{0, 3, 5}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 5, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Partition 1: [10,20,30] → first = 10
	require.Equal(t, int32(10), vals[0])
	require.Equal(t, int32(10), vals[1])
	require.Equal(t, int32(10), vals[2])
	// Partition 2: [40,50] → first = 40
	require.Equal(t, int32(40), vals[3])
	require.Equal(t, int32(40), vals[4])

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_LastValueWithPartition tests last_value with partitions.
func TestProcessValueFunc_LastValueWithPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	bat := makeInt32Batch(mp, []int32{10, 20, 30, 40, 50})
	spec := makeLastValueWindowSpec()

	ctr := &container{bat: bat}
	ctr.aggVecs = make([]group.ExprEvalVector, 1)
	ctr.aggVecs[0].Vec = []*vector.Vector{bat.Vecs[0]}
	ctr.ps = []int64{0, 3, 5}

	ap := &Window{WinSpecList: []*plan.Expr{spec}}

	result, err := ctr.processValueFunc(0, ap, proc)
	require.NoError(t, err)
	require.Equal(t, 5, result.Length())

	vals := vector.MustFixedColNoTypeCheck[int32](result)
	// Partition 1: [10,20,30] → last = 30
	require.Equal(t, int32(30), vals[0])
	require.Equal(t, int32(30), vals[1])
	require.Equal(t, int32(30), vals[2])
	// Partition 2: [40,50] → last = 50
	require.Equal(t, int32(50), vals[3])
	require.Equal(t, int32(50), vals[4])

	result.Free(mp)
	bat.Clean(mp)
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestProcessValueFunc_Varchar tests lag with varchar (varlen) type.
func TestProcessValueFunc_Varchar(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := makeVarcharBatch(mp, []string{"aaa", "bbb", "ccc"})

	spec := makeValueWindowSpecWithName("lag", int32(types.T_varchar))
	proc := testutil.NewProcessWithMPool(t, "", mp)

	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Types:       []types.Type{types.T_varchar.ToType()},
		Aggs:        []aggexec.AggFuncExecExpression{makeValueWindowAggExpr("")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)

	err := arg.Prepare(proc)
	require.NoError(t, err)

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	resultVec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
	require.Equal(t, 3, resultVec.Length())
	require.True(t, resultVec.IsNull(0))
	require.Equal(t, "aaa", resultVec.GetStringAt(1))
	require.Equal(t, "bbb", resultVec.GetStringAt(2))

	arg.Free(proc, false, nil)
	proc.Free()
}
