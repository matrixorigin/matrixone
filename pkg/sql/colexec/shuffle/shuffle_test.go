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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	input.Vecs[0] = testutil.NewInt64Vector(4, types.T_int64.ToType(), mp, false, nil, []int64{value, value, value, value})
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
