// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashmap

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestIteratorValidationAndNonMatchingHelpers(t *testing.T) {
	mp := mpool.MustNewZero()
	plain := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(plain, []int64{1, 2}, nil, mp))
	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)

	require.ErrorIs(t, validateIteratorVectors(nil, 0, 1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validateIteratorVectors([]*vector.Vector{nil}, 0, 1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validateIteratorVectors([]*vector.Vector{plain}, -1, 1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validateIteratorVectors([]*vector.Vector{plain}, 0, UnitLimit+1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validateIteratorVectors([]*vector.Vector{plain}, 1, 2), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, validateIteratorVectors([]*vector.Vector{constant}, 12, 4))

	plain.GetGrouping().Add(1)
	require.True(t, hasGroupingInRange([]*vector.Vector{plain}, 0, 2))
	require.True(t, rowHasGrouping([]*vector.Vector{plain}, 1))
	plain.GetNulls().Add(0)
	require.True(t, rowHasNull([]*vector.Vector{plain}, 0))
	require.False(t, rowHasNull([]*vector.Vector{plain}, 1))

	mask := []bool{true, true, true}
	require.Empty(t, prepareNonMatchingMask(mask, 2, false))
	mask = prepareNonMatchingMask(mask, 2, true)
	require.Equal(t, []bool{false, false}, mask)
	mask = prepareNonMatchingMask(nil, 3, true)
	require.Equal(t, []bool{false, false, false}, mask)

	float32Vec := vector.NewVec(types.T_float32.ToType())
	require.NoError(t, vector.AppendFixed(float32Vec, float32(math.NaN()), false, mp))
	float64Vec := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(float64Vec, math.NaN(), false, mp))
	arrayVecs := make([]*vector.Vector, 0, 4)
	for _, tc := range []struct {
		typ  types.Type
		data []byte
	}{
		{types.T_array_float32.ToType(), types.ArrayToBytes([]float32{1, float32(math.NaN())})},
		{types.T_array_float64.ToType(), types.ArrayToBytes([]float64{1, math.NaN()})},
		{types.T_array_bf16.ToType(), types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, float32(math.NaN())}))},
		{types.T_array_float16.ToType(), types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, float32(math.NaN())}))},
	} {
		vec := vector.NewVec(tc.typ)
		require.NoError(t, vector.AppendBytes(vec, tc.data, false, mp))
		arrayVecs = append(arrayVecs, vec)
	}
	for _, vec := range append([]*vector.Vector{float32Vec, float64Vec}, arrayVecs...) {
		zValues := []int64{1}
		nonMatching := []bool{false}
		require.True(t, markNonMatchingNaNs(
			[]*vector.Vector{vec}, 0, 1, zValues, nonMatching))
		require.Equal(t, []int64{0}, zValues)
		values := []uint64{99}
		finishNonMatchingKeys(
			[]*vector.Vector{vec}, 0, values, zValues, nonMatching)
		require.Equal(t, []uint64{0}, values)
		require.Equal(t, []int64{1}, zValues)
	}
	require.False(t, markNonMatchingNaNs(
		[]*vector.Vector{plain}, 0, 1, []int64{1}, []bool{false}))
	require.False(t, markNonMatchingNaNs(
		[]*vector.Vector{float64Vec}, 0, 1, []int64{1}, nil))

	for _, vec := range arrayVecs {
		vec.Free(mp)
	}
	float32Vec.Free(mp)
	float64Vec.Free(mp)
	constant.Free(mp)
	plain.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestPreviewMissingHelpersAndRowPublication(t *testing.T) {
	stringStates := [][3]uint64{{1, 2, 3}, {1, 2, 3}, {4, 5, 6}}
	stringValues := make([]uint64, len(stringStates))
	stringInserted := make([]uint8, len(stringStates))
	newGroups, err := previewMissingStringStates(
		10, []int64{1, 1, 1}, stringStates, stringValues, stringInserted, true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), newGroups)
	require.Equal(t, []uint64{11, 11, 12}, stringValues)
	require.Equal(t, []uint8{1, 0, 1}, stringInserted)

	stringValues = []uint64{0, 10, 0}
	newGroups, err = previewMissingStringStates(
		10, []int64{0, 1, 1}, stringStates, stringValues, stringInserted, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), newGroups)
	require.Equal(t, []uint64{0, 10, 11}, stringValues)
	require.ErrorIs(t,
		func() error {
			_, err := previewMissingStringStates(10, nil, stringStates, nil, nil, true)
			return err
		}(),
		mpool.ErrAllocationAccountInvalid)
	stringValues = []uint64{11, 0, 0}
	_, err = previewMissingStringStates(
		10, []int64{1, 1, 1}, stringStates, stringValues, stringInserted, true)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	hashes := []uint64{7, 7, 9}
	intValues := make([]uint64, len(hashes))
	intInserted := make([]uint8, len(hashes))
	newGroups, err = previewMissingIntHashes(
		20, []int64{1, 1, 1}, hashes, intValues, intInserted, true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), newGroups)
	require.Equal(t, []uint64{21, 21, 22}, intValues)
	require.Equal(t, []uint8{1, 0, 1}, intInserted)

	intValues = []uint64{0, 20, 0}
	newGroups, err = previewMissingIntHashes(
		20, []int64{0, 1, 1}, hashes, intValues, intInserted, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), newGroups)
	require.Equal(t, []uint64{0, 20, 21}, intValues)
	_, err = previewMissingIntHashes(20, nil, hashes, nil, nil, true)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	intValues = []uint64{21, 0, 0}
	_, err = previewMissingIntHashes(
		20, []int64{1, 1, 1}, hashes, intValues, intInserted, true)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	rows := uint64(2)
	updateHashTableRows(&rows, true, []uint64{3, 3, 4}, nil)
	require.Equal(t, uint64(4), rows)
	rows = 2
	updateHashTableRows(&rows, false,
		[]uint64{3, 4, 4}, []int64{1, 0, 1})
	require.Equal(t, uint64(4), rows)
}

func TestTransactionalIteratorWrapperAndEmptyPlanPaths(t *testing.T) {
	mp := mpool.MustNewZero()

	intInput := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(intInput, []int64{7, 8}, nil, mp))
	intMap, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	intIterator := intMap.NewTransactionalIterator()
	require.NoError(t, intIterator.Preflight(0, 2, []*vector.Vector{intInput}))
	_, _, err = intIterator.Find(0, 2, []*vector.Vector{intInput})
	require.NoError(t, err)
	newKey, err := intIterator.DetectDup([]*vector.Vector{intInput}, 0)
	require.NoError(t, err)
	require.True(t, newKey)
	newKey, err = intIterator.DetectDup([]*vector.Vector{intInput}, 0)
	require.NoError(t, err)
	require.False(t, newKey)
	var intPlan InsertPlan
	require.NoError(t, intIterator.PreviewInsert(
		0, 0, []*vector.Vector{intInput}, intMap.GroupCount(), &intPlan))
	values, zValues, err := intIterator.CommitPreview(&intPlan)
	require.NoError(t, err)
	require.Empty(t, values)
	require.Empty(t, zValues)
	require.ErrorIs(t, (*transactionalIntIterator)(nil).Preflight(0, 0, nil),
		mpool.ErrAllocationAccountInvalid)
	intMap.Free()
	intInput.Free(mp)

	strInput := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytesList(
		strInput, [][]byte{[]byte("seven"), []byte("eight")}, nil, mp))
	strMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	strIterator := strMap.NewTransactionalIterator()
	require.NoError(t, strIterator.Preflight(0, 2, []*vector.Vector{strInput}))
	_, _, err = strIterator.Find(0, 2, []*vector.Vector{strInput})
	require.NoError(t, err)
	newKey, err = strIterator.DetectDup([]*vector.Vector{strInput}, 0)
	require.NoError(t, err)
	require.True(t, newKey)
	newKey, err = strIterator.DetectDup([]*vector.Vector{strInput}, 0)
	require.NoError(t, err)
	require.False(t, newKey)
	var strPlan InsertPlan
	require.NoError(t, strIterator.PreviewInsert(
		0, 0, []*vector.Vector{strInput}, strMap.GroupCount(), &strPlan))
	values, zValues, err = strIterator.CommitPreview(&strPlan)
	require.NoError(t, err)
	require.Empty(t, values)
	require.Empty(t, zValues)
	require.ErrorIs(t, (*transactionalStrIterator)(nil).Preflight(0, 0, nil),
		mpool.ErrAllocationAccountInvalid)
	strMap.Free()
	strInput.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestInsertPlanAccessorsRejectInvalidScratchShape(t *testing.T) {
	var nilPlan *InsertPlan
	require.Nil(t, nilPlan.Values())
	require.Nil(t, nilPlan.Inserted())
	require.Zero(t, nilPlan.NewGroups())
	nilPlan.reset()

	plan := &InsertPlan{count: UnitLimit + 1, newGroups: 9, ready: true, complete: true}
	require.Nil(t, plan.Values())
	require.Nil(t, plan.Inserted())
	require.Equal(t, uint64(9), plan.NewGroups())
	plan.reset()
	require.Zero(t, plan.count)
	require.Zero(t, plan.NewGroups())
	require.False(t, plan.ready)
	require.False(t, plan.complete)
}
