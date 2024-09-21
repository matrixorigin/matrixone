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
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	err := hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, proc)
	require.NoError(t, err)

	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err = hb.Batches.CopyIntoBatches(inputBatch, proc)
	hb.InputBatchRowCount = inputBatch.RowCount()
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)

	rf := &plan.RuntimeFilterSpec{
		Tag:         0,
		MatchPrefix: false,
		UpperLimit:  0,
		Expr:        nil,
	}
	err = hb.BuildHashmap(false, true, rf, proc)
	require.NoError(t, err)
	require.Less(t, int64(0), hb.GetSize())
	require.Less(t, uint64(0), hb.GetGroupCount())
	hb.FreeWithError(proc)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashMapAllocAndFree(t *testing.T) {
	var hb HashmapBuilder
	var err error
	hb.IntHashMap, err = hashmap.NewIntHashMap(false)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(10000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(1000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
}
