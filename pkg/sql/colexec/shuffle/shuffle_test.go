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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

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
	require.Equal(t, count, 16*Rows+1)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.arg.Reset(tc.proc, false, nil)
}

func TestShuffle(t *testing.T) {

	for _, tc := range makeTestCases(t) {
		runShuffleCase(t, tc, true)
		// second run
		runShuffleCase(t, tc, false)
		tc.proc.Free()
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestFree(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)

	}
}

func TestReset(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Reset(tc.proc, false, nil)
	}
}

func TestPrint(t *testing.T) {
	sp := NewShufflePool(4, 1)
	sp.Print()
}

// TestEvalAndShuffle covers the ShuffleExpr branch (evalAndShuffle):
// it sets ShuffleExpr to a column reference so the expression executor
// returns the same vector as the input column, exercising
// hashShuffleVecWith[Null|outNull] and rangeShuffleVec for every type
// covered by makeTestCases.
func TestEvalAndShuffle(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		tc.arg.ShuffleExpr = &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			Typ:  plan.Type{Id: int32(tc.types[0].Oid)},
		}
		runShuffleCase(t, tc, true)
		runShuffleCase(t, tc, false)
		tc.proc.Free()
	}
}

// TestEvalAndShuffleConst exercises the IsConst / IsConstNull branches
// of evalAndShuffle and the helpers shuffleConstVecByHash and
// rangeShuffleConstVec for every supported type.
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

	hashCases := []constCase{
		{"hash_int64", plan.ShuffleType_Hash, mkInt64(42), types.T_int64.ToType(), 8, 0, 0, nil, nil},
		{"hash_int32", plan.ShuffleType_Hash, mkInt32(42), types.T_int32.ToType(), 8, 0, 0, nil, nil},
		{"hash_int16", plan.ShuffleType_Hash, mkInt16(42), types.T_int16.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint64", plan.ShuffleType_Hash, mkU64(42), types.T_uint64.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint32", plan.ShuffleType_Hash, mkU32(42), types.T_uint32.ToType(), 8, 0, 0, nil, nil},
		{"hash_uint16", plan.ShuffleType_Hash, mkU16(42), types.T_uint16.ToType(), 8, 0, 0, nil, nil},
		{"hash_varchar", plan.ShuffleType_Hash, mkVarchar("hello"), types.T_varchar.ToType(), 8, 0, 0, nil, nil},
		{"hash_const_null", plan.ShuffleType_Hash, nullExpr, types.T_int64.ToType(), 8, 0, 0, nil, nil},
	}
	rangeCases := []constCase{
		// minmax variants
		{"range_int64_minmax", plan.ShuffleType_Range, mkInt64(50), types.T_int64.ToType(), 8, 1, 1000000, nil, nil},
		{"range_int32_minmax", plan.ShuffleType_Range, mkInt32(50), types.T_int32.ToType(), 8, 1, 1000000, nil, nil},
		{"range_int16_minmax", plan.ShuffleType_Range, mkInt16(50), types.T_int16.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint64_minmax", plan.ShuffleType_Range, mkU64(50), types.T_uint64.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint32_minmax", plan.ShuffleType_Range, mkU32(50), types.T_uint32.ToType(), 8, 1, 1000000, nil, nil},
		{"range_uint16_minmax", plan.ShuffleType_Range, mkU16(50), types.T_uint16.ToType(), 8, 1, 1000000, nil, nil},
		{"range_varchar_minmax", plan.ShuffleType_Range, mkVarchar("abc"), types.T_varchar.ToType(), 8, 0, 1 << 60, nil, nil},
		// slice variants
		{"range_int64_slice", plan.ShuffleType_Range, mkInt64(50), types.T_int64.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_int32_slice", plan.ShuffleType_Range, mkInt32(50), types.T_int32.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_int16_slice", plan.ShuffleType_Range, mkInt16(50), types.T_int16.ToType(), 5, 0, 0, []int64{1, 100, 10000, 100000}, nil},
		{"range_uint64_slice", plan.ShuffleType_Range, mkU64(50), types.T_uint64.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_uint32_slice", plan.ShuffleType_Range, mkU32(50), types.T_uint32.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_uint16_slice", plan.ShuffleType_Range, mkU16(50), types.T_uint16.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		{"range_varchar_slice", plan.ShuffleType_Range, mkVarchar("abc"), types.T_varchar.ToType(), 5, 0, 0, nil, []uint64{1, 100, 10000, 100000}},
		// const null
		{"range_const_null", plan.ShuffleType_Range, nullExpr, types.T_int64.ToType(), 8, 1, 1000000, nil, nil},
	}

	all := append(hashCases, rangeCases...)
	for _, c := range all {
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
			defer arg.Free(proc, false, nil)

			bat := testutil.NewBatch([]types.Type{c.colType}, false, 4, proc.Mp())
			defer bat.Clean(proc.Mp())
			out, err := arg.evalAndShuffle(bat, proc)
			require.NoError(t, err)
			require.NotNil(t, out)
			require.True(t, out.ShuffleIDX >= 0 && out.ShuffleIDX < c.bucketNum)
		})
	}
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
		newLastBatch(),
		batch.EmptyBatch,
	}
}

func newLastBatch() *batch.Batch {
	bat := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 1, mpool.MustNewZero())
	bat.SetLast()
	return bat
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64, hasNull bool) *batch.Batch {
	if hasNull {
		return testutil.NewBatchWithNulls(ts, true, int(rows), proc.Mp())
	}
	return testutil.NewBatch(ts, true, int(rows), proc.Mp())
}

func resetChildren(arg *Shuffle, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
