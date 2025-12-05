// Copyright 2024 Matrix Origin
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

package hashmap_util

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func TestBuildHashMap(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	err := hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc)
	require.NoError(t, err)

	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err = hb.Batches.CopyIntoBatches(inputBatch, proc)
	hb.InputBatchRowCount = inputBatch.RowCount()
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)

	err = hb.BuildHashmap(false, true, true, proc)
	require.NoError(t, err)
	require.Less(t, int64(0), hb.GetSize())
	require.Less(t, uint64(0), hb.GetGroupCount())
	hb.Reset(proc, true)
	hb.Free(proc)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashMapAllocAndFree(t *testing.T) {
	mp := mpool.MustNewZero()

	var hb HashmapBuilder
	var err error
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(10000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(1000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
}

// TestResetWithNilPointers tests that Reset() handles nil pointers gracefully
// This is a regression test for the panic fix where Reset() would crash when
// vecs or UniqueJoinKeys contained nil pointers.
func TestResetWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Test case 1: vecs with nil pointers and needDupVec = true
	hb.needDupVec = true
	hb.vecs = make([][]*vector.Vector, 2)
	hb.vecs[0] = make([]*vector.Vector, 2)
	hb.vecs[1] = make([]*vector.Vector, 2)
	// Set some vectors to nil to simulate partial initialization
	hb.vecs[0][0] = nil
	hb.vecs[0][1] = nil
	hb.vecs[1][0] = nil
	hb.vecs[1][1] = nil

	// Test case 2: vecs with nil slice
	hb.vecs = append(hb.vecs, nil)

	// Test case 3: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Reset should not panic
	hb.Reset(proc, true)
	require.Nil(t, hb.vecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestResetWithMixedNilAndValidPointers tests Reset() with a mix of nil and valid vectors
func TestResetWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil)

	// Test case: vecs with mix of nil and valid vectors
	hb.needDupVec = true
	hb.vecs = make([][]*vector.Vector, 2)
	hb.vecs[0] = []*vector.Vector{vec1, nil}
	hb.vecs[1] = []*vector.Vector{nil, vec2}

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{vec1, nil, vec2}

	// Reset should free valid vectors and not panic on nil
	hb.Reset(proc, true)
	require.Nil(t, hb.vecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithNilPointers tests that Free() handles nil pointers gracefully
func TestFreeWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Test case: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Free should not panic
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithMixedNilAndValidPointers tests Free() with a mix of nil and valid vectors
func TestFreeWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil)

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{vec1, nil, vec2}

	// Free should free valid vectors and not panic on nil
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
