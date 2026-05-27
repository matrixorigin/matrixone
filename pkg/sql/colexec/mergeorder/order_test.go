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

package mergeorder

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type orderTestCase struct {
	arg   *MergeOrder
	types []types.Type
	proc  *process.Process
}

type int8Row struct {
	value  int8
	isNull bool
}

type pairRow struct {
	first  int8
	second int64
}

type countingColumnExecutor struct {
	col      int
	maxCalls int
	calls    int
}

func (e *countingColumnExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return e.EvalWithoutResultReusing(nil, batches, nil)
}

func (e *countingColumnExecutor) EvalWithoutResultReusing(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls > e.maxCalls {
		return nil, moerr.NewInternalErrorNoCtx("order executor reevaluated spilled keys")
	}
	return batches[0].Vecs[e.col], nil
}

func (e *countingColumnExecutor) ResetForNextQuery() {}
func (e *countingColumnExecutor) Free()              {}
func (e *countingColumnExecutor) IsColumnExpr() bool { return true }
func (e *countingColumnExecutor) TypeName() string   { return "countingColumnExecutor" }

type negatingInt8Executor struct {
	col   int
	calls int
}

func (e *negatingInt8Executor) Eval(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return e.EvalWithoutResultReusing(proc, batches, nil)
}

func (e *negatingInt8Executor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	e.calls++
	src := vector.MustFixedColWithTypeCheck[int8](batches[0].Vecs[e.col])
	values := make([]int8, len(src))
	for i := range src {
		values[i] = -src[i]
	}
	return testutil.NewVector(len(values), types.T_int8.ToType(), proc.Mp(), false, values), nil
}

func (e *negatingInt8Executor) ResetForNextQuery() {}
func (e *negatingInt8Executor) Free()              {}
func (e *negatingInt8Executor) IsColumnExpr() bool { return false }
func (e *negatingInt8Executor) TypeName() string   { return "negatingInt8Executor" }

func makeTestCases(t *testing.T) []orderTestCase {
	return []orderTestCase{
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1, types.T_int64), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}, {Expr: newExpression(1, types.T_int64), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	for tci, tc := range makeTestCases(t) {
		bats := []*batch.Batch{newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs), batch.EmptyBatch, newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs)}
		resetChildren(tc.arg, bats)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		var bat *batch.Batch
		for {
			ok, err := vm.Exec(tc.arg, tc.proc)
			if ok.Batch != nil {
				bat = ok.Batch
				continue
			}
			require.NoError(t, err)
			// do the result check
			if len(tc.arg.OrderBySpecs) > 0 {
				desc := tc.arg.OrderBySpecs[0].Flag&plan.OrderBySpec_DESC != 0
				index := tc.arg.OrderBySpecs[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos
				vec := bat.Vecs[index]
				if vec.GetType().Oid == types.T_int8 {
					i8c := vector.MustFixedColWithTypeCheck[int8](vec)
					if desc {
						for j := range i8c {
							if j > 0 {
								require.True(t, i8c[j] <= i8c[j-1], fmt.Sprintf("tc %d require desc, but get %v", tci, i8c))
							}
						}
					} else {
						for j := range i8c {
							if j > 0 {
								require.True(t, i8c[j] >= i8c[j-1])
							}
						}
					}
				} else if vec.GetType().Oid == types.T_int64 {
					i64c := vector.MustFixedColWithTypeCheck[int64](vec)
					if desc {
						for j := range i64c {
							if j > 0 {
								require.True(t, i64c[j] <= i64c[j-1])
							}
						}
					} else {
						for j := range i64c {
							if j > 0 {
								require.True(t, i64c[j] >= i64c[j-1])
							}
						}
					}
				}
			}

			break
		}
		tc.arg.Children[0].Free(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestOrderSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{1, 4, 7}),
		newValuesBatch(proc, []int8{2, 5, 8}),
		batch.EmptyBatch,
		newValuesBatch(proc, []int8{3, 6, 9}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{1, 2, 3, 4, 5, 6, 7, 8, 9}, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillMultiPass(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := make([]*batch.Batch, 0, spillMergeFanIn+8)
	for i := spillMergeFanIn + 8; i >= 1; i-- {
		bats = append(bats, newValuesBatch(proc, []int8{int8(i)}))
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	expected := make([]int8, 0, spillMergeFanIn+8)
	for i := 1; i <= spillMergeFanIn+8; i++ {
		expected = append(expected, int8(i))
	}
	require.Equal(t, expected, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillSkipsColumnKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	exec := &countingColumnExecutor{col: 0}
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	arg.ctr.executors = []colexec.ExpressionExecutor{exec}

	bats := make([]*batch.Batch, 0, spillMergeFanIn+8)
	for i := spillMergeFanIn + 8; i >= 1; i-- {
		bats = append(bats, newValuesBatch(proc, []int8{int8(i)}))
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	expected := make([]int8, 0, spillMergeFanIn+8)
	for i := 1; i <= spillMergeFanIn+8; i++ {
		expected = append(expected, int8(i))
	}
	require.Equal(t, expected, values)
	require.Equal(t, 0, exec.calls)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillPersistsComputedKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	exec := &negatingInt8Executor{col: 0}
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	arg.ctr.executors = []colexec.ExpressionExecutor{exec}

	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{3}),
		newValuesBatch(proc, []int8{1}),
		newValuesBatch(proc, []int8{2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{3, 2, 1}, values)
	require.Equal(t, 3, exec.calls)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillDesc(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: plan.OrderBySpec_DESC}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{9, 6, 3}),
		newValuesBatch(proc, []int8{8, 5, 2}),
		newValuesBatch(proc, []int8{7, 4, 1}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{9, 8, 7, 6, 5, 4, 3, 2, 1}, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillMultiKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{
			{Expr: newExpression(0, types.T_int8), Flag: 0},
			{Expr: newExpression(1, types.T_int64), Flag: plan.OrderBySpec_DESC},
		},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newPairBatch(proc, []int8{1, 2, 3}, []int64{10, 9, 8}),
		newPairBatch(proc, []int8{1, 2, 3}, []int64{7, 6, 5}),
		newPairBatch(proc, []int8{1, 2, 3}, []int64{4, 3, 2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectPairResults(t, arg, proc)
	require.Equal(t, []pairRow{
		{first: 1, second: 10},
		{first: 1, second: 7},
		{first: 1, second: 4},
		{first: 2, second: 9},
		{first: 2, second: 6},
		{first: 2, second: 3},
		{first: 3, second: 8},
		{first: 3, second: 5},
		{first: 3, second: 2},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillNullsLast(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: plan.OrderBySpec_NULLS_LAST}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newNullableValuesBatch(proc, []int8{1, 4, 0}, []uint64{2}),
		newNullableValuesBatch(proc, []int8{2, 5, 0}, []uint64{2}),
		newNullableValuesBatch(proc, []int8{3, 6, 0}, []uint64{2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectNullableInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8Row{
		{value: 1},
		{value: 2},
		{value: 3},
		{value: 4},
		{value: 5},
		{value: 6},
		{isNull: true},
		{isNull: true},
		{isNull: true},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillDescNullsFirst(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{{
			Expr: newExpression(0, types.T_int8),
			Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST,
		}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newNullableValuesBatch(proc, []int8{0, 9, 6}, []uint64{0}),
		newNullableValuesBatch(proc, []int8{0, 8, 5}, []uint64{0}),
		newNullableValuesBatch(proc, []int8{0, 7, 4}, []uint64{0}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectNullableInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8Row{
		{isNull: true},
		{isNull: true},
		{isNull: true},
		{value: 9},
		{value: 8},
		{value: 7},
		{value: 6},
		{value: 5},
		{value: 4},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestSpillAppendPolicy(t *testing.T) {
	var ctr container

	ctr.setSpillThreshold(8 * common.MiB)
	require.False(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(0), ctr.spillAppendTarget)

	ctr.setSpillThreshold(64 * common.MiB)
	require.True(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(32*common.MiB), ctr.spillAppendTarget)

	ctr.setSpillThreshold(2 * common.GiB)
	require.True(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(128*common.MiB), ctr.spillAppendTarget)
}

func TestCanAppendToActiveRun(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors: make([]colexec.ExpressionExecutor, 1),
	}
	defer func() {
		for i := range ctr.spillTailCols {
			if ctr.spillTailCols[i] != nil {
				ctr.spillTailCols[i].Free(proc.Mp())
			}
		}
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	tail := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	incomingEqual := testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false, []int8{5, 6})
	incomingLess := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{4})
	defer tail.Free(proc.Mp())
	defer incomingEqual.Free(proc.Mp())
	defer incomingLess.Free(proc.Mp())

	ctr.spillTailCols = []*vector.Vector{vector.NewOffHeapVecWithType(types.T_int8.ToType())}
	ctr.spillTailReady = false
	require.NoError(t, ctr.updateActiveRunTail(proc, []*vector.Vector{tail}, 0))

	require.True(t, ctr.canAppendToActiveRun([]*vector.Vector{incomingEqual}))
	require.False(t, ctr.canAppendToActiveRun([]*vector.Vector{incomingLess}))
}

func TestComputeDrainChunkFixedWidth(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_int64.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	reader := &spillRunReader{batch: src}
	reader.refreshDrainProfile()

	require.Equal(t, 100, computeDrainChunk(reader, 0))
	require.Equal(t, 1, computeDrainChunk(reader, maxBatchSizeToSend-1))
}

func TestComputeDrainChunkVarlen(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprintf("row-%d", i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_varchar.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	reader := &spillRunReader{batch: src}
	reader.refreshDrainProfile()

	chunk := computeDrainChunk(reader, 0)
	require.Greater(t, chunk, 0)
	require.LessOrEqual(t, chunk, maxVarlenDrainChunkRows)
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []orderTestCase{
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			bats := []*batch.Batch{newRandomBatch(tc.types, tc.proc, BenchmarkRows), batch.EmptyBatch, newRandomBatch(tc.types, tc.proc, BenchmarkRows)}
			resetChildren(tc.arg, bats)
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			for {
				ok, err := vm.Exec(tc.arg, tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}
		}
	}
}

func newTestCase(t testing.TB, ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	return orderTestCase{
		types: ts,
		proc:  proc,
		arg: &MergeOrder{
			OrderBySpecs: fs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
}

func newExpression(pos int32, typeID types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(typeID),
		},
	}
}

func newIntBatch(ts []types.Type, proc *process.Process, rows int64, fs []*plan.OrderBySpec) *batch.Batch {
	vs := make([]*vector.Vector, len(ts))
	for i, t := range ts {
		generateDescData := false
		for _, f := range fs {
			if f.Flag == plan.OrderBySpec_DESC {
				index := f.Expr.Expr.(*plan.Expr_Col).Col.ColPos
				if int(index) == i {
					generateDescData = true
				}
			}
		}

		if t.Oid == types.T_int8 {
			values := make([]int8, rows)
			if generateDescData {
				for j := range values {
					values[j] = int8(-j)
				}
			} else {
				for j := range values {
					values[j] = int8(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		} else if t.Oid == types.T_int64 {
			values := make([]int64, rows)
			if generateDescData {
				for j := range values {
					values[j] = int64(-j)
				}
			} else {
				for j := range values {
					values[j] = int64(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		}
	}
	zs := make([]int64, rows)
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors(vs, zs)
}

func newRandomBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func newValuesBatch(proc *process.Process, values []int8) *batch.Batch {
	vec := testutil.NewVector(len(values), types.T_int8.ToType(), proc.Mp(), false, values)
	zs := make([]int64, len(values))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{vec}, zs)
}

func newNullableValuesBatch(proc *process.Process, values []int8, nulls []uint64) *batch.Batch {
	vec := testutil.MakeInt8Vector(values, nulls, proc.Mp())
	zs := make([]int64, len(values))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{vec}, zs)
}

func newPairBatch(proc *process.Process, first []int8, second []int64) *batch.Batch {
	zs := make([]int64, len(first))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.MakeInt8Vector(first, nil, proc.Mp()),
		testutil.MakeInt64Vector(second, nil, proc.Mp()),
	}, zs)
}

func collectInt8Results(t *testing.T, arg *MergeOrder, proc *process.Process, col int) []int8 {
	var values []int8
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			values = append(values, vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[col])...)
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return values
}

func collectNullableInt8Results(t *testing.T, arg *MergeOrder, proc *process.Process, col int) []int8Row {
	var rows []int8Row
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			vec := result.Batch.Vecs[col]
			values := vector.MustFixedColWithTypeCheck[int8](vec)
			for i := range values {
				rows = append(rows, int8Row{
					value:  values[i],
					isNull: vec.GetNulls().Contains(uint64(i)),
				})
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return rows
}

func collectPairResults(t *testing.T, arg *MergeOrder, proc *process.Process) []pairRow {
	var rows []pairRow
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			first := vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[0])
			second := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
			for i := range first {
				rows = append(rows, pairRow{first: first[i], second: second[i]})
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return rows
}

func resetChildren(arg *MergeOrder, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
