// Copyright 2022 Matrix Origin
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

package mergesort

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

func TestSort1(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	pool := mocks.GetTestVectorPool()

	// sort not null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, nil)
		vec2 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			vec2.Append(vec.Get(i), vec.IsNull(i))
		}
		vecs := []containers.Vector{vec, vec2}
		_, _ = SortBlockColumns(vecs, 0, pool)
		require.True(t, vecs[0].Equals(vecs[1]), vecType)
		vec.Close()
		vec2.Close()
	}

	// sort null
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10, false, nil)
		vec.Update(rand.Intn(10), nil, true)
		vec.Update(rand.Intn(10), nil, true)
		vec2 := containers.MakeVector(vecType, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			vec2.Append(vec.Get(i), vec.IsNull(i))
		}
		vecs := []containers.Vector{vec, vec2}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		require.True(t, vecs[0].Equals(vecs[1]), vecType)
		vec.Close()
		vec2.Close()
	}
}
func TestSort2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 10000, false, nil)
		t0 := time.Now()
		vecs := []containers.Vector{vec}
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
		t.Logf("%-20v takes %v", vecType.String(), time.Since(t0))
		vec.Close()
	}
}

func TestSortBlockColumns(t *testing.T) {
	vec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	vec.Append([]byte("b"), false)
	vec.Append([]byte("c"), false)
	vec.Append([]byte("a"), false)
	vecSlice := []containers.Vector{vec}

	columns, err := SortBlockColumns(vecSlice, 0, mocks.GetTestVectorPool())
	require.NoError(t, err)
	require.Equal(t, []int64{2, 0, 1}, columns)
	require.Equal(t, []string{"a", "b", "c"}, vector.MustStrCol(vec.GetDownstreamVector()))

	vecWithNull := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	vecWithNull.Append(int32(1), false)
	vecWithNull.Append(int32(2), true)
	vecWithNull.Append(int32(3), false)

	vecSlice = []containers.Vector{vecWithNull}
	columns, err = SortBlockColumns(vecSlice, 0, mocks.GetTestVectorPool())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 0, 2}, columns)
	require.Equal(t, []int32{0, 1, 3}, vector.MustFixedCol[int32](vecWithNull.GetDownstreamVector()))
}

func BenchmarkSortBlockColumns(b *testing.B) {
	vecTypes := types.MockColTypes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var vecs []containers.Vector
		for _, vecType := range vecTypes {
			vec := containers.MockVector(vecType, 10000, false, nil)
			vecs = []containers.Vector{vec}
		}
		b.StartTimer()
		_, _ = SortBlockColumns(vecs, 0, mocks.GetTestVectorPool())
	}
}

func TestAObjMerge(t *testing.T) {
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	vecType := types.T_int32
	batCnt := 4
	vecCnt := 2
	rowCnt := 100
	batches := make([]*containers.Batch, batCnt)

	for i := 0; i < batCnt; i++ {
		batches[i] = containers.NewBatch()
		for j := 0; j < vecCnt; j++ {
			vec := containers.NewVector(vecType.ToType())
			for k := 0; k < rowCnt; k++ {
				vec.Append(int32(i*rowCnt+k), false)
			}
			batches[i].AddVector(strconv.Itoa(j), vec)
		}
	}

	ret, releaseF, mapping := testAObjMerger(vecType.ToType(), testPool)(batches, 0, uint32(rowCnt), batCnt).Merge(context.Background())
	for i := 0; i < batCnt; i++ {
		for j := 0; j < vecCnt; j++ {
			t.Log(vector.MustFixedCol[int32](ret[i].Vecs[j]))
			require.True(t, slices.IsSorted(vector.MustFixedCol[int32](ret[i].Vecs[j])))
		}
	}
	t.Log(mapping)
	releaseF()
}

func TestAObjMergeContainsNull(t *testing.T) {
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	vecType := types.T_int32
	batCnt := 4
	vecCnt := 2
	rowCnt := 100
	batches := make([]*containers.Batch, batCnt)

	for i := 0; i < batCnt; i++ {
		batches[i] = containers.NewBatch()
		for j := 0; j < vecCnt; j++ {
			vec := containers.NewVector(vecType.ToType())
			for k := 0; k < rowCnt; k++ {
				if k < 10 {
					vec.Append(0, true)
				} else {
					vec.Append(int32(i*rowCnt+k), false)
				}
			}
			batches[i].AddVector(strconv.Itoa(j), vec)
		}
	}

	ret, releaseF, mapping := testAObjMerger(vecType.ToType(), testPool)(batches, 0, uint32(rowCnt), batCnt).Merge(context.Background())
	for i := 0; i < batCnt; i++ {
		for j := 0; j < vecCnt; j++ {
			t.Log(vector.MustFixedCol[int32](ret[i].Vecs[j]))
			require.True(t, slices.IsSorted(vector.MustFixedCol[int32](ret[i].Vecs[j])))
		}
	}
	t.Log(mapping)
	releaseF()
}

func TestAObjMergeAllTypes(t *testing.T) {
	vecTypes := types.MockColTypes()
	testPool := &testPool{pool: mocks.GetTestVectorPool()}
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		t0 := time.Now()
		batches := make([]*containers.Batch, 2)
		batches[0] = containers.NewBatch()
		batches[0].AddVector("", vec)
		batches[1] = containers.NewBatch()
		batches[1].AddVector("", vec2)
		_, releaseF, _ := testAObjMerger(vecType, testPool)(batches, 0, 50000, 2).Merge(context.Background())
		t.Logf("%-20v takes %v", vecType, time.Since(t0))
		releaseF()
	}
}

func testAObjMerger(t types.Type, vpool DisposableVecPool) func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
	if t.IsVarlen() {
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[string], sortKeyPos, vector.MustStrCol, rowPerBlk, resultBlkCnt)
		}
	}
	switch t.Oid {
	case types.T_bool:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, BoolLess, sortKeyPos, vector.MustFixedCol[bool], rowPerBlk, resultBlkCnt)
		}
	case types.T_bit:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], rowPerBlk, resultBlkCnt)
		}
	case types.T_int8:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[int8], sortKeyPos, vector.MustFixedCol[int8], rowPerBlk, resultBlkCnt)
		}
	case types.T_int16:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[int16], sortKeyPos, vector.MustFixedCol[int16], rowPerBlk, resultBlkCnt)
		}
	case types.T_int32:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[int32], sortKeyPos, vector.MustFixedCol[int32], rowPerBlk, resultBlkCnt)
		}
	case types.T_int64:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[int64], sortKeyPos, vector.MustFixedCol[int64], rowPerBlk, resultBlkCnt)
		}
	case types.T_float32:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[float32], sortKeyPos, vector.MustFixedCol[float32], rowPerBlk, resultBlkCnt)
		}
	case types.T_float64:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[float64], sortKeyPos, vector.MustFixedCol[float64], rowPerBlk, resultBlkCnt)
		}
	case types.T_uint8:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[uint8], sortKeyPos, vector.MustFixedCol[uint8], rowPerBlk, resultBlkCnt)
		}
	case types.T_uint16:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[uint16], sortKeyPos, vector.MustFixedCol[uint16], rowPerBlk, resultBlkCnt)
		}
	case types.T_uint32:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[uint32], sortKeyPos, vector.MustFixedCol[uint32], rowPerBlk, resultBlkCnt)
		}
	case types.T_uint64:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], rowPerBlk, resultBlkCnt)
		}
	case types.T_date:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[types.Date], sortKeyPos, vector.MustFixedCol[types.Date], rowPerBlk, resultBlkCnt)
		}
	case types.T_timestamp:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[types.Timestamp], sortKeyPos, vector.MustFixedCol[types.Timestamp], rowPerBlk, resultBlkCnt)
		}
	case types.T_datetime:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[types.Datetime], sortKeyPos, vector.MustFixedCol[types.Datetime], rowPerBlk, resultBlkCnt)
		}
	case types.T_time:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[types.Time], sortKeyPos, vector.MustFixedCol[types.Time], rowPerBlk, resultBlkCnt)
		}
	case types.T_enum:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, NumericLess[types.Enum], sortKeyPos, vector.MustFixedCol[types.Enum], rowPerBlk, resultBlkCnt)
		}
	case types.T_decimal64:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, LtTypeLess[types.Decimal64], sortKeyPos, vector.MustFixedCol[types.Decimal64], rowPerBlk, resultBlkCnt)
		}
	case types.T_decimal128:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, LtTypeLess[types.Decimal128], sortKeyPos, vector.MustFixedCol[types.Decimal128], rowPerBlk, resultBlkCnt)
		}
	case types.T_uuid:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, LtTypeLess[types.Uuid], sortKeyPos, vector.MustFixedCol[types.Uuid], rowPerBlk, resultBlkCnt)
		}
	case types.T_TS:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, TsLess, sortKeyPos, vector.MustFixedCol[types.TS], rowPerBlk, resultBlkCnt)
		}
	case types.T_Rowid:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, RowidLess, sortKeyPos, vector.MustFixedCol[types.Rowid], rowPerBlk, resultBlkCnt)
		}
	case types.T_Blockid:
		return func(batches []*containers.Batch, sortKeyPos int, rowPerBlk uint32, resultBlkCnt int) AObjMerger {
			return NewAObjMerger(vpool, batches, BlockidLess, sortKeyPos, vector.MustFixedCol[types.Blockid], rowPerBlk, resultBlkCnt)
		}
	default:
		panic(fmt.Sprintf("unsupported type %s", t.String()))
	}
}

func TestReshapeBatches1(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 4, false, nil)
		vec.Update(rand.Intn(4), nil, true)
		vec2 := containers.MockVector(vecType, 6, false, nil)
		vec2.Update(rand.Intn(6), nil, true)
		t.Log(vec)
		t.Log(vec2)
		vecs := []containers.Vector{vec, vec2}

		inputBatches := make([]*batch.Batch, 2)
		for i := range inputBatches {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vecs[i].GetDownstreamVector()
			inputBatches[i] = bat
		}
		retBatches, releaseF := ReshapeBatches(inputBatches, []uint32{4, 6}, []uint32{5, 5}, pool)
		t.Log(retBatches)
		for i := range retBatches {
			require.Equal(t, 5, retBatches[i].RowCount())
		}
		releaseF()
	}
}

func TestReshapeBatches3(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	vecTypes := types.MockColTypes()
	for _, vecType := range vecTypes {
		vec := containers.MockVector(vecType, 50000, false, nil)
		vec2 := containers.MockVector(vecType, 50000, false, nil)
		vecs := []containers.Vector{vec, vec2}
		inputBatches := make([]*batch.Batch, 2)
		for i := range inputBatches {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vecs[i].GetDownstreamVector()
			inputBatches[i] = bat
		}
		t0 := time.Now()
		_, releaseF := ReshapeBatches(inputBatches, []uint32{50000, 50000}, []uint32{50000, 50000}, pool)
		t.Logf("%v takes %v", vecType, time.Since(t0))
		releaseF()
	}
}
