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

package top

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type testCase struct {
	arg   *Top
	types []types.Type
	proc  *process.Process
}

type cancelOnDoneCheckContext struct {
	context.Context
	remaining int
	done      chan struct{}
}

func newCancelOnDoneCheckContext(parent context.Context, checks int) *cancelOnDoneCheckContext {
	return &cancelOnDoneCheckContext{
		Context:   parent,
		remaining: checks,
		done:      make(chan struct{}),
	}
}

func (ctx *cancelOnDoneCheckContext) Done() <-chan struct{} {
	if ctx.remaining > 0 {
		ctx.remaining--
		if ctx.remaining == 0 {
			close(ctx.done)
		}
	}
	return ctx.done
}

func (ctx *cancelOnDoneCheckContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func genTestCases(t *testing.T) []testCase {
	return []testCase{
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range genTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range genTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestTop(t *testing.T) {
	for _, tc := range genTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestTopCopiesNullVarlenaRow(t *testing.T) {
	tc := newTestCase(
		t,
		mpool.MustNewZero(),
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
		1,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}},
	)
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(bat.Vecs[0], []int64{2, 1}, nil, tc.proc.Mp())
	require.NoError(t, err)

	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	err = vector.AppendBytes(bat.Vecs[1], []byte("seed"), false, tc.proc.Mp())
	require.NoError(t, err)
	err = vector.AppendBytes(bat.Vecs[1], nil, true, tc.proc.Mp())
	require.NoError(t, err)
	ws := vector.MustFixedColNoTypeCheck[types.Varlena](bat.Vecs[1])
	ws[1].SetOffsetLen(25, 8)
	bat.SetRowCount(2)

	resetChildren(tc.arg, []*batch.Batch{bat, batch.EmptyBatch})
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])[0])
	require.True(t, result.Batch.Vecs[1].GetNulls().Contains(0))

	tc.arg.Free(tc.proc, false, nil)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestTopOrdersFloatNaNLastAndUsesSecondaryKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag plan.OrderBySpec_OrderByFlag
		want []int64
	}{
		{
			name: "ascending",
			want: []int64{10, 20, 1},
		},
		{
			name: "descending",
			flag: plan.OrderBySpec_DESC,
			want: []int64{20, 10, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCase := newTestCase(
				t,
				mpool.MustNewZero(),
				[]types.Type{types.T_float64.ToType(), types.T_int64.ToType()},
				3,
				[]*plan.OrderBySpec{
					{Expr: newExpression(0), Flag: tc.flag},
					{Expr: newExpression(1)},
				},
			)
			require.NoError(t, testCase.arg.Prepare(testCase.proc))

			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			for _, value := range []float64{
				math.Float64frombits(0x7ff8000000000002), 1, -1,
				math.Float64frombits(0x7ff8000000000001),
			} {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, testCase.proc.Mp()))
			}
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []int64{2, 20, 10, 1}, nil, testCase.proc.Mp()))
			bat.SetRowCount(4)
			resetChildren(testCase.arg, []*batch.Batch{bat, batch.EmptyBatch})

			result, err := vm.Exec(testCase.arg, testCase.proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			got := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
			require.Equal(t, tc.want, got)

			testCase.arg.Free(testCase.proc, false, nil)
			testCase.arg.GetChildren(0).Free(testCase.proc, false, nil)
			testCase.proc.Free()
			require.Zero(t, testCase.proc.Mp().CurrNB())
		})
	}
}

func TestTopSpill(t *testing.T) {
	limit := int64(topSpillThreshold + 1000)
	batchRows := 8192

	tcs := []testCase{
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
			[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
			[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: plan.OrderBySpec_DESC}}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType(), types.T_int32.ToType()}, limit,
			[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}, {Expr: newExpression(1), Flag: plan.OrderBySpec_DESC}}),
	}

	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		require.True(t, tc.arg.ctr.spilling)

		inputBats := []*batch.Batch{
			newBatch(tc.types, tc.proc, int64(batchRows)),
			newBatch(tc.types, tc.proc, int64(batchRows)),
			newBatch(tc.types, tc.proc, int64(batchRows)),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, inputBats)

		var totalRows int
		for {
			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			totalRows += result.Batch.RowCount()
		}
		require.Equal(t, int(limit), totalRows)

		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		inputBats = []*batch.Batch{
			newBatch(tc.types, tc.proc, int64(batchRows)),
			newBatch(tc.types, tc.proc, int64(batchRows)),
			newBatch(tc.types, tc.proc, int64(batchRows)),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, inputBats)

		totalRows = 0
		for {
			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			totalRows += result.Batch.RowCount()
		}
		require.Equal(t, int(limit), totalRows)

		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestTopSpillPrepareParamMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	src := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(vec, []byte("5"), false, mp))
	vec.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamInteger, vector.PrepareParamNone,
	})
	src.Vecs[0] = vec
	src.SetRowCount(2)
	defer src.Clean(mp)
	data, err := src.MarshalBinary()
	require.NoError(t, err)
	withMetadata, err := appendTopSpillPrepareParamMetadata(data, src)
	require.NoError(t, err)
	base, metadata, metadataRows, err := splitTopSpillPrepareParamMetadata(withMetadata)
	require.NoError(t, err)
	require.NotEqual(t, data, withMetadata)
	decoded := batch.NewWithSize(1)
	defer decoded.Clean(mp)
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(base, mp))
	require.NoError(t, restoreTopSpillPrepareParamMetadata(decoded, metadata, metadataRows, mp))
	require.Equal(t, vector.PrepareParamInteger, decoded.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamNone, decoded.Vecs[0].GetPrepareParamKindAt(1))

	bad := append([]byte(nil), withMetadata...)
	bad[len(bad)-1] = 0xff
	_, _, _, err = splitTopSpillPrepareParamMetadata(bad)
	require.Error(t, err)
}

func TestTopSpillEvalHonorsCancellationAfterInput(t *testing.T) {
	limit := int64(topSpillThreshold + 1)
	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.True(t, tc.arg.ctr.spilling)

	baseCtx := tc.proc.Ctx
	ctx, cancel := context.WithCancel(tc.proc.Ctx)
	tc.proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newBatch(tc.types, tc.proc, 8192),
		newBatch(tc.types, tc.proc, 8192),
		newBatch(tc.types, tc.proc, 8192),
	}).WithEndOfDataCallback(cancel)
	tc.arg.AppendChild(child)

	t.Cleanup(func() {
		tc.proc.Ctx = baseCtx
		tc.arg.Free(tc.proc, false, nil)
		child.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	})

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NotNil(t, tc.arg.ctr.spillFile)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	tc.arg.Reset(tc.proc, true, context.Canceled)
	require.Nil(t, tc.arg.ctr.spillFile)
	require.Nil(t, tc.arg.ctr.orderedRefs)
	child.Free(tc.proc, true, context.Canceled)

	tc.proc.Ctx = baseCtx
	require.NoError(t, tc.arg.Prepare(tc.proc))
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newBatch(tc.types, tc.proc, 4),
	})
	tc.arg.Children = nil
	tc.arg.AppendChild(child)

	var outputRows int
	for {
		result, execErr := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		outputRows += result.Batch.RowCount()
	}
	require.Equal(t, 4, outputRows)
}

func TestTopSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	limit := int64(topSpillThreshold + 1)
	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.True(t, tc.arg.ctr.spilling)

	baseCtx := tc.proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	tc.proc.Ctx = ctx
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{newBatch(tc.types, tc.proc, 32)}).
		WithBatchCallback(func(int) { cancel() })
	tc.arg.AppendChild(child)

	t.Cleanup(func() {
		tc.proc.Ctx = baseCtx
		tc.arg.Free(tc.proc, true, context.Canceled)
		child.Free(tc.proc, true, context.Canceled)
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	})

	result, err := vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Nil(t, tc.arg.ctr.spillFile)
	require.Empty(t, tc.arg.ctr.spillIndex)
}

func TestTopSpillBatchCancellationBeforeWrite(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	arg := &Top{}
	arg.ctr.n = 1
	src := newBatch([]types.Type{types.T_int64.ToType()}, proc, 32)
	analyzer := process.NewAnalyzer(0, false, false, "top-cancel-write")

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		arg.Free(proc, true, context.Canceled)
		src.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// The entry check passes. Cancellation is then observed after
	// serialization but before the first file write.
	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 2)
	err := arg.ctr.spillBatch(src, proc, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, arg.ctr.spillFile)
	info, statErr := arg.ctr.spillFile.Stat()
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
	require.Empty(t, arg.ctr.spillIndex)
}

func TestTopSpillBatchCancellationAfterWrite(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	arg := &Top{}
	arg.ctr.n = 1
	src := newBatch([]types.Type{types.T_int64.ToType()}, proc, 32)
	analyzer := process.NewAnalyzer(0, false, false, "top-cancel-after-write")

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		arg.Free(proc, true, context.Canceled)
		src.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// Entry and pre-write checks pass. The post-write boundary observes
	// cancellation before the spill index or heap state is published.
	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 3)
	err := arg.ctr.spillBatch(src, proc, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, arg.ctr.spillFile)
	info, statErr := arg.ctr.spillFile.Stat()
	require.NoError(t, statErr)
	require.Positive(t, info.Size())
	require.Empty(t, arg.ctr.spillIndex)
	require.Zero(t, arg.ctr.spillBatIdx)
}

func TestTopSpillEvalCancellationCheckpoints(t *testing.T) {
	tests := []struct {
		name          string
		cancelAtCheck int
	}{
		{name: "before evaluation", cancelAtCheck: 1},
		{name: "before spill read", cancelAtCheck: 2},
		{name: "before result publish", cancelAtCheck: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := &Top{}
			arg.ctr.n = 1
			arg.ctr.orderedRefs = []rowRef{{batchIdx: 0, rowIdx: 0}}
			analyzer := process.NewAnalyzer(0, false, false, "top-cancel-eval")
			src := newBatch([]types.Type{types.T_int64.ToType()}, proc, 1)
			require.NoError(t, arg.ctr.spillBatch(src, proc, analyzer))
			src.Clean(proc.Mp())
			baseCtx := proc.Ctx

			t.Cleanup(func() {
				proc.Ctx = baseCtx
				arg.Free(proc, true, context.Canceled)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})

			proc.Ctx = newCancelOnDoneCheckContext(baseCtx, test.cancelAtCheck)
			var result vm.CallResult
			done, err := arg.ctr.evalSpill(1, 1, proc, &result)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, done)
			require.Nil(t, result.Batch)
			require.Nil(t, arg.ctr.spillOutBat)
		})
	}
}

func TestTopSpillHeapCancellationCheckpoint(t *testing.T) {
	limit := int64(topSpillThreshold + 1)
	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.True(t, tc.arg.ctr.spilling)

	src := newBatch(tc.types, tc.proc, limit)
	tc.arg.ctr.n = len(src.Vecs)
	require.NoError(t, tc.arg.ctr.build(tc.arg, src, tc.proc, tc.arg.OpAnalyzer))
	src.Clean(tc.proc.Mp())
	require.Greater(t, len(tc.arg.ctr.sels), evalSpillChunkSize)
	originalRefs := len(tc.arg.ctr.sels)
	baseCtx := tc.proc.Ctx

	t.Cleanup(func() {
		tc.proc.Ctx = baseCtx
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	})

	// Entry and i=0 pass; cancellation is observed at i=8192 after a
	// partially completed heap-to-order transfer.
	tc.proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 3)
	var result vm.CallResult
	done, err := tc.arg.ctr.evalSpill(uint64(limit), tc.arg.ctr.n, tc.proc, &result)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, done)
	require.Nil(t, result.Batch)
	require.Len(t, tc.arg.ctr.orderedRefs, originalRefs)
	require.Len(t, tc.arg.ctr.sels, originalRefs-evalSpillChunkSize)

	tc.arg.Reset(tc.proc, true, context.Canceled)
	require.Nil(t, tc.arg.ctr.spillFile)
	require.Nil(t, tc.arg.ctr.orderedRefs)
}

func TestTopSpillInsufficientRows(t *testing.T) {
	limit := int64(topSpillThreshold + 1000)
	batchRows := 4096

	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, limit,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})

	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)

	inputBats := []*batch.Batch{
		newBatch(tc.types, tc.proc, int64(batchRows)),
		newBatch(tc.types, tc.proc, int64(batchRows)),
		batch.EmptyBatch,
	}
	resetChildren(tc.arg, inputBats)

	var totalRows int
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		totalRows += result.Batch.RowCount()
	}
	require.Equal(t, batchRows*2, totalRows)

	tc.arg.Free(tc.proc, false, nil)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestTopSpillMaxUint64LimitReturnsAllRows(t *testing.T) {
	batchRows := 7
	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType()}, 1,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})
	tc.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(^uint64(0))

	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	require.True(t, tc.arg.ctr.spilling)

	inputBats := []*batch.Batch{
		newBatch(tc.types, tc.proc, int64(batchRows)),
		batch.EmptyBatch,
	}
	resetChildren(tc.arg, inputBats)

	var totalRows int
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		totalRows += result.Batch.RowCount()
	}
	require.Equal(t, batchRows, totalRows)

	tc.arg.Free(tc.proc, false, nil)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func BenchmarkTop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []testCase{
			newTestCase(b, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase(b, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(b, err)

			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, BenchmarkRows),
				newBatch(tc.types, tc.proc, BenchmarkRows),
				batch.EmptyBatch,
			}
			resetChildren(tc.arg, bats)
			_, _ = vm.Exec(tc.arg, tc.proc)
			tc.arg.Free(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)
			tc.proc.Free()
		}
	}
}

func newTestCase(t testing.TB, m *mpool.MPool, ts []types.Type, limit int64, fs []*plan.OrderBySpec) testCase {
	return testCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool(t, "", m),
		arg: &Top{
			Fs:    fs,
			Limit: plan2.MakePlan2Uint64ConstExprWithType(uint64(limit)),
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

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{},
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Top, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
