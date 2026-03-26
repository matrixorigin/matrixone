// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestHllSketchMarshalAndUnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()
	sketch, err := makeHllSketch(mp)
	require.NoError(t, err)

	hlls := sketch.(*hllSketch)
	hlls.Insert(types.EncodeInt64(ptr(int64(1))))
	hlls.Insert(types.EncodeInt64(ptr(int64(2))))

	data, err := hlls.MarshalBinary()
	require.NoError(t, err)

	restoredMU, err := makeHllSketch(mp)
	require.NoError(t, err)
	restored := restoredMU.(*hllSketch)
	require.NoError(t, restored.UnmarshalBinary(data))
	require.Equal(t, hlls.Estimate(), restored.Estimate())

	readerRestored := &hllSketch{}
	require.NoError(t, readerRestored.UnmarshalFromReader(bytes.NewReader(data)))
	require.Equal(t, hlls.Estimate(), readerRestored.Estimate())
}

func TestApproxCountExecFillMergeFlush(t *testing.T) {
	mp := mpool.MustNewZero()

	left := makeApproxCount(mp, 1, types.T_int64.ToType()).(*approxCountExec)
	right := makeApproxCount(mp, 1, types.T_int64.ToType()).(*approxCountExec)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	values := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(values, []int64{1, 2, 2, 3}, nil, mp))
	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 1, GroupNotMatched}, []*vector.Vector{values}))

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 2, mp)
	require.NoError(t, err)
	require.NoError(t, left.BulkFill(1, []*vector.Vector{constVec}))

	nullVec := vector.NewConstNull(types.T_int64.ToType(), 1, mp)
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{nullVec}))

	rightValues := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(rightValues, []int64{3, 4}, nil, mp))
	require.NoError(t, right.BatchFill(0, []uint64{1, 2}, []*vector.Vector{rightValues}))

	require.NoError(t, left.SetExtraInformation(nil, 0))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))
	require.NoError(t, left.Merge(right, 0, 0))
	require.Greater(t, left.Size(), int64(0))

	vecs, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(3), vector.GetFixedAtNoTypeCheck[uint64](vecs[0], 0))
	require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](vecs[0], 1))

	values.Free(mp)
	constVec.Free(mp)
	nullVec.Free(mp)
	rightValues.Free(mp)
	vecs[0].Free(mp)
	left.Free()
	right.Free()
}
