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

package shuffle

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 8192 // default rows
)

// add unit tests for cases
type shuffleTestCase struct {
	arg   *Shuffle
	types []types.Type
	proc  *process.Process
}

type failingShuffleChild struct {
	*colexec.MockOperator
	err error
}

func (child *failingShuffleChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.CancelResult, child.err
}

type preparedContextShuffleChild struct {
	*colexec.MockOperator
	preparedCtx context.Context
	prepareErr  error
}

func (child *preparedContextShuffleChild) Prepare(proc *process.Process) error {
	child.preparedCtx = proc.Ctx
	if child.prepareErr != nil {
		return child.prepareErr
	}
	return child.MockOperator.Prepare(proc)
}

type blockingShuffleChild struct {
	*colexec.MockOperator
	started         chan struct{}
	exited          chan struct{}
	resetBeforeExit bool
}

func (child *blockingShuffleChild) Call(proc *process.Process) (vm.CallResult, error) {
	close(child.started)
	<-proc.Ctx.Done()
	if child.exited != nil {
		close(child.exited)
	}
	return vm.CancelResult, context.Cause(proc.Ctx)
}

func (child *blockingShuffleChild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if child.exited != nil {
		select {
		case <-child.exited:
		default:
			child.resetBeforeExit = true
		}
	}
	child.MockOperator.Reset(proc, pipelineFailed, err)
}

type handoffThenBlockingShuffleChild struct {
	*colexec.MockOperator
	bat     *batch.Batch
	blocked chan struct{}
	calls   int
}

func (child *handoffThenBlockingShuffleChild) Call(proc *process.Process) (vm.CallResult, error) {
	if child.calls == 0 {
		child.calls++
		result := vm.NewCallResult()
		result.Batch = child.bat
		return result, nil
	}
	close(child.blocked)
	<-proc.Ctx.Done()
	return vm.CancelResult, context.Cause(proc.Ctx)
}

func makeTestCases(t *testing.T) []shuffleTestCase {
	return []shuffleTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     5,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     4,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     8,
				ShuffleColMin: 1,
				ShuffleColMax: 8888888,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     5,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     3,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     8,
				ShuffleColMin: 1,
				ShuffleColMax: 999999,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &Shuffle{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         5,
				ShuffleRangeInt64: []int64{1, 100, 10000, 100000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &Shuffle{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         4,
				ShuffleRangeInt64: []int64{100, 10000, 100000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &Shuffle{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         8,
				ShuffleRangeInt64: []int64{1, 100, 10000, 100000, 200000, 300000, 400000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &Shuffle{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          5,
				ShuffleRangeUint64: []uint64{1, 100, 10000, 100000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &Shuffle{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          3,
				ShuffleRangeUint64: []uint64{100, 10000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &Shuffle{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          5,
				ShuffleRangeUint64: []uint64{1, 100, 10000, 100000},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     3,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     4,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     5,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     6,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     7,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     8,
			},
		},
	}
}

func TestFixedBucketShufflePreparesChildWithQueryScopedProducerContext(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	queryCtx := proc.Base.GetContextBase().BuildQueryCtx(proc.Ctx)
	proc.BuildPipelineContext(queryCtx)
	consumerCtx := proc.Ctx

	child := &preparedContextShuffleChild{MockOperator: colexec.NewMockOperator()}
	arg := NewArgument()
	arg.BucketNum = 1
	arg.CurrentShuffleIdx = 0
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(child)

	require.NoError(t, vm.Prepare(arg, proc))
	require.NotNil(t, child.preparedCtx)
	require.False(t, consumerCtx == child.preparedCtx)

	proc.Cancel(nil)
	select {
	case <-child.preparedCtx.Done():
		t.Fatal("consumer pipeline cancellation reached the producer child")
	default:
	}

	proc.ResetQueryContext()
	select {
	case <-child.preparedCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("query cancellation did not reach the producer child")
	}

	arg.Reset(proc, false, nil)
	child.Reset(proc, false, nil)
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShufflePrepareFailureCancelsPreparedProducerProcess(t *testing.T) {
	sentinel := errors.New("prepare failed")
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	queryCtx := proc.Base.GetContextBase().BuildQueryCtx(proc.Ctx)
	proc.BuildPipelineContext(queryCtx)

	child := &preparedContextShuffleChild{
		MockOperator: colexec.NewMockOperator(),
		prepareErr:   sentinel,
	}
	arg := NewArgument()
	arg.BucketNum = 1
	arg.CurrentShuffleIdx = 0
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(child)
	p := pipeline.NewMerge(arg)

	_, err := p.Run(proc)
	require.ErrorIs(t, err, sentinel)
	producerCtx := child.preparedCtx
	require.NotNil(t, producerCtx)
	p.Cleanup(proc, true, true, err)
	select {
	case <-producerCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("prepare failure leaked the producer process context")
	}

	arg.Free(proc, true, sentinel)
	child.Free(proc, true, sentinel)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func runShuffleCase(t *testing.T, tc shuffleTestCase, hasnull bool) {
	var result vm.CallResult
	var count int
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	resetChildren(tc.arg, getInputBats(tc, hasnull))
	for {
		result, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		count += result.Batch.RowCount()
	}
	require.Equal(t, 32*Rows, count)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.arg.Reset(tc.proc, false, nil)
}

func TestShuffleStandaloneDrainsAllBucketsAndPreservesRows(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		runShuffleCase(t, tc, true)
		// second run
		runShuffleCase(t, tc, false)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
	}
}

func TestShuffleDrainsBufferedRowsAfterChildExecStop(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()

	valueForBucket := func(target uint64) int64 {
		for value := int64(0); ; value++ {
			if plan2.SimpleInt64HashToRange(uint64(value), 2) == target {
				return value
			}
		}
	}
	bucket0 := valueForBucket(0)
	bucket1 := valueForBucket(1)
	newInput := func(values []int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.NewInt64Vector(len(values), types.T_int64.ToType(), mp, false, nil, values)
		bat.SetRowCount(len(values))
		return bat
	}

	source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInput([]int64{bucket0, bucket1, bucket0, bucket1}),
		newInput([]int64{bucket0, bucket0, bucket1, bucket1}),
	})
	limitArg := limit.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(6))
	limitArg.AppendChild(source)
	arg := NewArgument()
	arg.BucketNum = 2
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.ShuffleExpr = &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}
	arg.DrainAllBuckets = true
	arg.AppendChild(limitArg)
	defer func() {
		source.Free(proc, false, nil)
		limitArg.Reset(proc, false, nil)
		limitArg.Free(proc, false, nil)
		limitArg.Release()
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		arg.Release()
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	require.NoError(t, vm.Prepare(arg, proc))
	rows := 0
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil && !result.Batch.IsEmpty() {
			rows += result.Batch.RowCount()
		}
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
	}
	require.Equal(t, 6, rows)
}

func TestShuffleMetadata(t *testing.T) {
	arg := NewArgument()
	defer arg.Release()
	buf := new(bytes.Buffer)
	arg.String(buf)
	require.Equal(t, opName, arg.TypeName())
	require.Equal(t, opName, buf.String())
}

// TestEvalAndShuffle covers expression-based hash and range shuffles.
func TestEvalAndShuffle(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		tc.arg.ShuffleExpr = &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			Typ:  plan.Type{Id: int32(tc.types[0].Oid)},
		}
		runShuffleCase(t, tc, true)
		runShuffleCase(t, tc, false)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
	}
}

// TestEvalAndShuffleConst exercises the IsConst / IsConstNull branches of
// expression-based hash and range shuffles for every supported constant type.
func TestEvalAndShuffleConst(t *testing.T) {
	type constCase struct {
		name        string
		shuffleType plan.ShuffleType
		expr        *plan.Expr
		colType     types.Type
		bucketNum   int32
		colMin      int64
		colMax      int64
		rangeI64    []int64
		rangeU64    []uint64
	}
	mkInt64 := func(v int64) *plan.Expr { return plan2.MakePlan2Int64ConstExprWithType(v) }
	mkInt32 := func(v int32) *plan.Expr { return plan2.MakePlan2Int32ConstExprWithType(v) }
	mkInt16 := func(v int16) *plan.Expr { return plan2.MakePlan2Int16ConstExprWithType(v) }
	mkU64 := func(v uint64) *plan.Expr { return plan2.MakePlan2Uint64ConstExprWithType(v) }
	mkU32 := func(v uint32) *plan.Expr { return plan2.MakePlan2Uint32ConstExprWithType(v) }
	mkU16 := func(v uint16) *plan.Expr { return plan2.MakePlan2Uint16ConstExprWithType(v) }
	mkVarchar := func(s string) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}},
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: int32(len(s) + 8)},
		}
	}
	nullExpr := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}

	cases := []constCase{
		{"hash_int64", plan.ShuffleType_Hash, mkInt64(42), types.T_int64.ToType(), 8, 0, 0, nil, nil},
		{"hash_int32", plan.ShuffleType_Hash, mkInt32(42), types.T_int32.ToType(), 8, 0, 0, nil, nil},
		{"hash_int16", plan.ShuffleType_Hash, mkInt16(42), types.T_int16.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint64", plan.ShuffleType_Hash, mkU64(42), types.T_uint64.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint32", plan.ShuffleType_Hash, mkU32(42), types.T_uint32.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint16", plan.ShuffleType_Hash, mkU16(42), types.T_uint16.ToType(), 8, 0, 0, nil, nil},
		{"hash_varchar", plan.ShuffleType_Hash, mkVarchar("hello"), types.T_varchar.ToType(), 8, 0, 0, nil, nil},
		{"hash_const_null", plan.ShuffleType_Hash, nullExpr, types.T_int64.ToType(), 8, 0, 0, nil, nil},
		{"range_int64_minmax", plan.ShuffleType_Range, mkInt64(50), types.T_int64.ToType(), 8, 1, 1000000, nil, nil},
		{"range_int32_minmax", plan.ShuffleType_Range, mkInt32(50), types.T_int32.ToType(), 8, 1, 1000000, nil, nil},
		{"range_int16_minmax", plan.ShuffleType_Range, mkInt16(50), types.T_int16.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint64_minmax", plan.ShuffleType_Range, mkU64(50), types.T_uint64.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint32_minmax", plan.ShuffleType_Range, mkU32(50), types.T_uint32.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint16_minmax", plan.ShuffleType_Range, mkU16(50), types.T_uint16.ToType(), 8, 1, 1000000, nil, nil},
		{"range_varchar_minmax", plan.ShuffleType_Range, mkVarchar("abc"), types.T_varchar.ToType(), 8, 0, 1 << 60, nil, nil},
		{"range_int64_slice", plan.ShuffleType_Range, mkInt64(50), types.T_int64.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_int32_slice", plan.ShuffleType_Range, mkInt32(50), types.T_int32.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_int16_slice", plan.ShuffleType_Range, mkInt16(50), types.T_int16.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_uint64_slice", plan.ShuffleType_Range, mkU64(50), types.T_uint64.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_uint32_slice", plan.ShuffleType_Range, mkU32(50), types.T_uint32.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_uint16_slice", plan.ShuffleType_Range, mkU16(50), types.T_uint16.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_varchar_slice", plan.ShuffleType_Range, mkVarchar("abc"), types.T_varchar.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_const_null", plan.ShuffleType_Range, nullExpr, types.T_int64.ToType(), 8, 1, 1000000, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			arg := &Shuffle{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(c.shuffleType),
				BucketNum:          c.bucketNum,
				ShuffleColMin:      c.colMin,
				ShuffleColMax:      c.colMax,
				ShuffleRangeInt64:  c.rangeI64,
				ShuffleRangeUint64: c.rangeU64,
				ShuffleExpr:        c.expr,
			}
			require.NoError(t, arg.Prepare(proc))
			defer func() {
				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
			}()

			bat := testutil.NewBatch([]types.Type{c.colType}, false, 4, proc.Mp())
			defer bat.Clean(proc.Mp())
			out, err := arg.evalAndShuffle(bat, proc)
			require.NoError(t, err)
			require.NotNil(t, out)
			require.True(t, out.ShuffleIDX >= 0 && out.ShuffleIDX < c.bucketNum)
		})
	}
}

func TestEvalAndShuffleConstRoutesForeignBucketThroughPool(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum = 2
	arg.CurrentShuffleIdx = 1
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.ShuffleExpr = &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}
	arg.SetShufflePool(NewShufflePool(arg.BucketNum, 1, false))
	require.NoError(t, arg.Prepare(proc))
	defer func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	input := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, 4, mp)
	defer input.Clean(mp)
	output, err := arg.evalAndShuffle(input, proc)
	require.NoError(t, err)
	require.Nil(t, output)
	pooled := arg.ctr.shufflePool.getLastBatch(0)
	require.NotNil(t, pooled)
	require.Equal(t, int32(0), pooled.ShuffleIDX)
	require.Equal(t, input.RowCount(), pooled.RowCount())
	pooled.Clean(mp)
}

func TestEvalAndShuffleSingleBucketExpressionRoutesForeignBucketThroughPool(t *testing.T) {
	const value = int64(42)
	targetBucket := int32(plan2.SimpleInt64HashToRange(uint64(value), 2))
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum = 2
	arg.CurrentShuffleIdx = 1 - targetBucket
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.ShuffleExpr = &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}
	arg.SetShufflePool(NewShufflePool(arg.BucketNum, 1, false))
	require.NoError(t, arg.Prepare(proc))
	defer func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	input := batch.NewWithSize(1)
	var err error
	input.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), value, 4, mp)
	require.NoError(t, err)
	input.SetRowCount(4)
	defer input.Clean(mp)
	output, err := arg.evalAndShuffle(input, proc)
	require.NoError(t, err)
	require.Nil(t, output)
	pooled := arg.ctr.shufflePool.getLastBatch(targetBucket)
	require.NotNil(t, pooled)
	require.Equal(t, targetBucket, pooled.ShuffleIDX)
	require.Equal(t, input.RowCount(), pooled.RowCount())
	pooled.Clean(mp)
}

func TestShuffleResetAndFreeReleaseRuntimeState(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum = 2
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.ShuffleExpr = &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}

	require.NoError(t, arg.Prepare(proc))
	require.NotNil(t, arg.ctr.exprExec)
	require.True(t, arg.ctr.held)
	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.shufflePool)
	require.Nil(t, arg.ctr.sels)
	require.False(t, arg.ctr.held)
	require.NotNil(t, arg.ctr.exprExec)
	arg.Free(proc, false, nil)
	require.Nil(t, arg.ctr.exprExec)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestShuffleSharedPoolFixedBucketWorkersPreserveRows(t *testing.T) {
	const (
		bucketNum     = int32(2)
		rowsPerWorker = objectio.BlockMaxRows * 6
	)
	mp := mpool.MustNewZero()
	procs := []*process.Process{
		testutil.NewProcessWithMPool(t, "", mp),
		testutil.NewProcessWithMPool(t, "", mp),
	}
	pool := NewShufflePool(bucketNum, int32(len(procs)), false)
	args := make([]*Shuffle, len(procs))
	defer func() {
		for i, arg := range args {
			if arg != nil {
				arg.GetChildren(0).Free(procs[i], false, nil)
				arg.Reset(procs[i], false, nil)
				arg.Free(procs[i], false, nil)
				arg.Release()
			}
			procs[i].Free()
		}
		require.Equal(t, int64(0), mp.CurrNB())
	}()
	for i := range args {
		arg := NewArgument()
		arg.BucketNum = bucketNum
		arg.ShuffleColIdx = 0
		arg.ShuffleType = int32(plan.ShuffleType_Hash)
		arg.CurrentShuffleIdx = int32(i)
		arg.SetShufflePool(pool)

		values := make([]int64, rowsPerWorker)
		candidate := int64(i * rowsPerWorker * 4)
		for row := range values {
			target := uint64(row % int(bucketNum))
			for plan2.SimpleInt64HashToRange(uint64(candidate), uint64(bucketNum)) != target {
				candidate++
			}
			values[row] = candidate
			candidate++
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.NewInt64Vector(rowsPerWorker, types.T_int64.ToType(), mp, false, nil, values)
		input.SetRowCount(rowsPerWorker)
		arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		args[i] = arg
		require.NoError(t, arg.Prepare(procs[i]))
	}

	type workerResult struct {
		rows       int
		wrongBatch bool
		err        error
	}
	results := make(chan workerResult, len(args))
	for i := range args {
		go func(arg *Shuffle, proc *process.Process) {
			result := workerResult{}
			for proc.Ctx.Err() == nil {
				callResult, err := vm.Exec(arg, proc)
				if err != nil {
					result.err = err
					break
				}
				if callResult.Batch == nil || callResult.Status == vm.ExecStop {
					break
				}
				if callResult.Batch.IsEmpty() {
					continue
				}
				result.rows += callResult.Batch.RowCount()
				result.wrongBatch = result.wrongBatch || callResult.Batch.ShuffleIDX != arg.CurrentShuffleIdx
			}
			if result.err == nil && proc.Ctx.Err() != nil {
				result.err = proc.Ctx.Err()
			}
			results <- result
		}(args[i], procs[i])
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	workerResults := make([]workerResult, 0, len(args))
	timedOut := false
	for len(workerResults) < len(args) {
		select {
		case result := <-results:
			workerResults = append(workerResults, result)
		case <-timer.C:
			if timedOut {
				t.Fatal("fixed-bucket shuffle workers did not stop after cancellation")
			}
			for _, proc := range procs {
				proc.Cancel(context.DeadlineExceeded)
			}
			timedOut = true
			timer.Reset(time.Second)
		}
	}
	if timedOut {
		t.Fatal("fixed-bucket shuffle workers did not finish")
	}
	totalRows := 0
	for _, result := range workerResults {
		require.NoError(t, result.err)
		require.False(t, result.wrongBatch)
		totalRows += result.rows
	}
	require.Equal(t, rowsPerWorker*len(args), totalRows)
}

func TestNestedFixedBucketShufflesMakeProgressUnderBackpressure(t *testing.T) {
	const (
		bucketNum = int32(2)
		rows      = objectio.BlockMaxRows * 4
	)
	mp := mpool.MustNewZero()
	procs := []*process.Process{
		testutil.NewProcessWithMPool(t, "", mp),
		testutil.NewProcessWithMPool(t, "", mp),
	}
	innerPool := NewShufflePool(bucketNum, bucketNum, false)
	outerPool := NewShufflePool(bucketNum, bucketNum, false)
	inners := make([]*Shuffle, bucketNum)
	outers := make([]*Shuffle, bucketNum)

	valueForBucket := func(target uint64) int64 {
		for value := int64(0); ; value++ {
			if plan2.SimpleInt64HashToRange(uint64(value), uint64(bucketNum)) == target {
				return value
			}
		}
	}
	innerKey := valueForBucket(0)
	outerKey := valueForBucket(1)

	for i := range outers {
		var inputs []*batch.Batch
		if i == 0 {
			input := batch.NewWithSize(2)
			innerValues := make([]int64, rows)
			outerValues := make([]int64, rows)
			for row := range rows {
				innerValues[row] = innerKey
				outerValues[row] = outerKey
			}
			input.Vecs[0] = testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, innerValues)
			input.Vecs[1] = testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, outerValues)
			input.SetRowCount(rows)
			inputs = []*batch.Batch{input}
		}

		inner := NewArgument()
		inner.BucketNum = bucketNum
		inner.CurrentShuffleIdx = int32(i)
		inner.ShuffleColIdx = 0
		inner.ShuffleType = int32(plan.ShuffleType_Hash)
		inner.SetShufflePool(innerPool)
		inner.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
		inners[i] = inner

		outer := NewArgument()
		outer.BucketNum = bucketNum
		outer.CurrentShuffleIdx = int32(i)
		outer.ShuffleColIdx = 1
		outer.ShuffleType = int32(plan.ShuffleType_Hash)
		outer.SetShufflePool(outerPool)
		outer.AppendChild(inner)
		outers[i] = outer
		require.NoError(t, vm.Prepare(outer, procs[i]))
	}

	type workerResult struct {
		rows int
		err  error
	}
	results := make(chan workerResult, len(outers))
	for i := range outers {
		go func(arg *Shuffle, proc *process.Process) {
			result := workerResult{}
			for {
				callResult, err := vm.Exec(arg, proc)
				if err != nil {
					result.err = err
					break
				}
				if callResult.Batch == nil || callResult.Status == vm.ExecStop {
					break
				}
				if !callResult.Batch.IsEmpty() {
					result.rows += callResult.Batch.RowCount()
				}
			}
			results <- result
		}(outers[i], procs[i])
	}

	totalRows := 0
	for range outers {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			totalRows += result.rows
		case <-time.After(5 * time.Second):
			for _, proc := range procs {
				proc.Cancel(context.DeadlineExceeded)
			}
			t.Fatal("nested fixed-bucket shuffles deadlocked")
		}
	}
	require.Equal(t, rows, totalRows)

	for i := range outers {
		require.NoError(t, vm.HandleAllOp(outers[i], func(_ vm.Operator, op vm.Operator) error {
			op.Reset(procs[i], false, nil)
			return nil
		}))
	}
	for i := range outers {
		require.NoError(t, vm.HandleAllOp(outers[i], func(_ vm.Operator, op vm.Operator) error {
			op.Free(procs[i], false, nil)
			return nil
		}))
		inners[i].Release()
		outers[i].Release()
		procs[i].Free()
	}
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShuffleDirectHandoffPreservesBatchOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	input := batch.NewWithSize(1)
	value := int64(0)
	for plan2.SimpleInt64HashToRange(uint64(value), 2) != 0 {
		value++
	}
	var err error
	input.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), value, 4, mp)
	require.NoError(t, err)
	input.SetRowCount(4)
	source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := NewArgument()
	arg.BucketNum = 2
	arg.CurrentShuffleIdx = 0
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.SetShufflePool(NewShufflePool(2, 1, false))
	arg.AppendChild(source)
	require.NoError(t, vm.Prepare(arg, proc))

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, input, result.Batch)
	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)

	inputSize := int64(input.Size())
	arg.Reset(proc, false, nil)
	require.Equal(t, int64(4), arg.OpAnalyzer.GetOpStats().InputRows)
	require.Equal(t, inputSize, arg.OpAnalyzer.GetOpStats().InputSize)
	source.Reset(proc, false, nil)
	arg.Free(proc, false, nil)
	source.Free(proc, false, nil)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShuffleEarlyConsumerCloseDoesNotBlockOtherBuckets(t *testing.T) {
	const (
		bucketNum = int32(2)
		rows      = objectio.BlockMaxRows * 4
	)
	mp := mpool.MustNewZero()
	procs := []*process.Process{
		testutil.NewProcessWithMPool(t, "", mp),
		testutil.NewProcessWithMPool(t, "", mp),
	}
	pool := NewShufflePool(bucketNum, bucketNum, false)
	args := make([]*Shuffle, bucketNum)
	sources := make([]*colexec.MockOperator, bucketNum)
	value := int64(0)
	for plan2.SimpleInt64HashToRange(uint64(value), uint64(bucketNum)) != 1 {
		value++
	}
	for i := range args {
		values := make([]int64, rows)
		for row := range rows {
			values[row] = value
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, values)
		input.SetRowCount(rows)
		sources[i] = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg := NewArgument()
		arg.BucketNum = bucketNum
		arg.CurrentShuffleIdx = int32(i)
		arg.ShuffleColIdx = 0
		arg.ShuffleType = int32(plan.ShuffleType_Hash)
		arg.SetShufflePool(pool)
		arg.AppendChild(sources[i])
		args[i] = arg
		require.NoError(t, vm.Prepare(arg, procs[i]))
	}

	args[0].startLocalProducer(procs[0])
	resetDone := make(chan struct{})
	go func() {
		args[0].Reset(procs[0], false, nil)
		close(resetDone)
	}()

	received := 0
	for {
		result, err := vm.Exec(args[1], procs[1])
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		if !result.Batch.IsEmpty() {
			received += result.Batch.RowCount()
		}
	}
	require.Equal(t, rows*2, received)
	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("closing one bucket left its producer blocked")
	}

	args[1].Reset(procs[1], false, nil)
	for i := range args {
		sources[i].Reset(procs[i], false, nil)
		args[i].Free(procs[i], false, nil)
		sources[i].Free(procs[i], false, nil)
		args[i].Release()
		procs[i].Free()
	}
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShuffleStopsProducersWhenEveryConsumerCloses(t *testing.T) {
	mp := mpool.MustNewZero()
	procs := []*process.Process{
		testutil.NewProcessWithMPool(t, "", mp),
		testutil.NewProcessWithMPool(t, "", mp),
	}
	pool := NewShufflePool(2, 2, false)
	args := make([]*Shuffle, 2)
	children := make([]*handoffThenBlockingShuffleChild, 2)
	for i := range args {
		value := int64(0)
		for plan2.SimpleInt64HashToRange(uint64(value), 2) != uint64(i) {
			value++
		}
		input := batch.NewWithSize(1)
		var err error
		input.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), value, 1, mp)
		require.NoError(t, err)
		input.SetRowCount(1)
		child := &handoffThenBlockingShuffleChild{
			MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}),
			bat:          input,
			blocked:      make(chan struct{}),
		}
		arg := NewArgument()
		arg.BucketNum = 2
		arg.CurrentShuffleIdx = int32(i)
		arg.ShuffleColIdx = 0
		arg.ShuffleType = int32(plan.ShuffleType_Hash)
		arg.SetShufflePool(pool)
		arg.AppendChild(child)
		children[i] = child
		args[i] = arg
		require.NoError(t, vm.Prepare(arg, procs[i]))
		result, err := vm.Exec(arg, procs[i])
		require.NoError(t, err)
		require.Same(t, input, result.Batch)
	}

	firstReset := make(chan struct{})
	go func() {
		args[0].Reset(procs[0], false, nil)
		close(firstReset)
	}()
	select {
	case <-children[0].blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not continue after its consumer closed")
	}
	select {
	case <-firstReset:
		t.Fatal("one closed consumer incorrectly stopped a producer needed by its sibling")
	default:
	}

	secondReset := make(chan struct{})
	go func() {
		args[1].Reset(procs[1], false, nil)
		close(secondReset)
	}()
	for _, done := range []chan struct{}{firstReset, secondReset} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("all closed consumers did not stop every producer")
		}
	}

	for i := range args {
		children[i].Reset(procs[i], false, nil)
		args[i].Free(procs[i], false, nil)
		children[i].Free(procs[i], false, nil)
		args[i].Release()
		procs[i].Free()
	}
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShuffleProducerErrorWakesEveryConsumer(t *testing.T) {
	sentinel := errors.New("producer failed")
	mp := mpool.MustNewZero()
	procs := []*process.Process{
		testutil.NewProcessWithMPool(t, "", mp),
		testutil.NewProcessWithMPool(t, "", mp),
	}
	pool := NewShufflePool(2, 2, false)
	args := make([]*Shuffle, 2)
	for i := range args {
		arg := NewArgument()
		arg.BucketNum = 2
		arg.CurrentShuffleIdx = int32(i)
		arg.ShuffleColIdx = 0
		arg.ShuffleType = int32(plan.ShuffleType_Hash)
		arg.SetShufflePool(pool)
		if i == 0 {
			arg.AppendChild(&failingShuffleChild{MockOperator: colexec.NewMockOperator(), err: sentinel})
		} else {
			arg.AppendChild(colexec.NewMockOperator())
		}
		args[i] = arg
		require.NoError(t, vm.Prepare(arg, procs[i]))
	}

	results := make(chan error, len(args))
	for i := range args {
		go func(arg *Shuffle, proc *process.Process) {
			_, err := vm.Exec(arg, proc)
			results <- err
		}(args[i], procs[i])
	}
	for range args {
		select {
		case err := <-results:
			require.ErrorIs(t, err, sentinel)
		case <-time.After(5 * time.Second):
			t.Fatal("producer error did not wake every consumer")
		}
	}

	for i := range args {
		args[i].Reset(procs[i], true, sentinel)
		args[i].GetChildren(0).Reset(procs[i], true, sentinel)
		args[i].Free(procs[i], true, sentinel)
		args[i].GetChildren(0).Free(procs[i], true, sentinel)
		args[i].Release()
		procs[i].Free()
	}
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestFixedBucketShuffleFailedResetJoinsBlockedProducer(t *testing.T) {
	sentinel := errors.New("query failed")
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	proc.BuildPipelineContext(proc.Ctx)
	started := make(chan struct{})
	child := &blockingShuffleChild{MockOperator: colexec.NewMockOperator(), started: started}
	arg := NewArgument()
	arg.BucketNum = 1
	arg.CurrentShuffleIdx = 0
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(child)
	require.NoError(t, vm.Prepare(arg, proc))

	callDone := make(chan error, 1)
	go func() {
		_, err := vm.Exec(arg, proc)
		callDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local producer did not start")
	}
	proc.Cancel(sentinel)
	select {
	case err := <-callDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline cancellation did not wake the local consumer")
	}

	resetDone := make(chan struct{})
	go func() {
		arg.Reset(proc, true, sentinel)
		close(resetDone)
	}()
	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("failed reset did not cancel and join the local producer")
	}

	child.Reset(proc, true, sentinel)
	arg.Free(proc, true, sentinel)
	child.Free(proc, true, sentinel)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineCleanupJoinsFixedBucketProducerBeforeResettingChild(t *testing.T) {
	sentinel := errors.New("query failed")
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	proc.BuildPipelineContext(proc.Ctx)
	started := make(chan struct{})
	exited := make(chan struct{})
	child := &blockingShuffleChild{
		MockOperator: colexec.NewMockOperator(),
		started:      started,
		exited:       exited,
	}
	arg := NewArgument()
	arg.BucketNum = 1
	arg.CurrentShuffleIdx = 0
	arg.ShuffleColIdx = 0
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(child)
	require.NoError(t, vm.Prepare(arg, proc))

	callDone := make(chan error, 1)
	go func() {
		_, err := vm.Exec(arg, proc)
		callDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local producer did not start")
	}
	proc.Cancel(sentinel)
	select {
	case <-callDone:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline call did not observe cancellation")
	}

	p := pipeline.New(0, nil, arg)
	p.Cleanup(proc, true, true, sentinel)
	require.False(t, child.resetBeforeExit)

	arg.Free(proc, true, sentinel)
	child.Free(proc, true, sentinel)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func getInputBats(tc shuffleTestCase, hasnull bool) []*batch.Batch {
	return []*batch.Batch{
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		batch.EmptyBatch,
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64, hasNull bool) *batch.Batch {
	if hasNull {
		return testutil.NewBatchWithNulls(ts, true, int(rows), proc.Mp())
	}
	return testutil.NewBatch(ts, true, int(rows), proc.Mp())
}

func newBatchSorted(ts []types.Type, proc *process.Process, rows int64, hasNull bool) *batch.Batch {
	if hasNull {
		return testutil.NewBatchWithNulls(ts, false, int(rows), proc.Mp())
	}
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Shuffle, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
