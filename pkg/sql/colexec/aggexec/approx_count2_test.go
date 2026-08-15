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
	"encoding/binary"
	"testing"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAccountedHllLazilyActivatesNonNullGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := makeApproxCount(mp, AggIdOfApproxCount,
		types.T_int64.ToType()).(*approxCountExec)
	owner := any(exec).(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(4))
	baseline := account.Snapshot().Used

	nulls := vector.NewConstNull(types.T_int64.ToType(), 4, mp)
	groups := []uint64{1, 2, 3, 4}
	require.NoError(t,
		exec.PreflightBatchFill(0, groups, []*vector.Vector{nulls}))
	require.Equal(t, baseline, account.Snapshot().Used)
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{nulls}))
	for _, state := range exec.state {
		for _, mob := range state.mobs {
			require.Nil(t, mob)
		}
	}

	values := vector.NewVec(types.T_int64.ToType())
	require.NoError(t,
		vector.AppendFixedList(values, []int64{7}, nil, mp))
	require.NoError(t,
		exec.PreflightBatchFill(0, []uint64{2}, []*vector.Vector{values}))
	require.Equal(t, baseline+uint64(hllRegisterCnt), account.Snapshot().Used)
	require.NoError(t,
		exec.BatchFill(0, []uint64{2}, []*vector.Vector{values}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(0),
		vector.GetFixedAtNoTypeCheck[uint64](results[0], 0))
	require.Equal(t, uint64(1),
		vector.GetFixedAtNoTypeCheck[uint64](results[0], 1))
	for _, result := range results {
		result.Free(mp)
	}
	values.Free(mp)
	nulls.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedHllPreflightBroadcastsScalarConstPastPhysicalRows(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := makeApproxCount(mp, AggIdOfApproxCount,
		types.T_int64.ToType()).(*approxCountExec)
	owner := any(exec).(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1))

	input, err := vector.NewConstFixed(
		types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)
	groups := []uint64{1, 1}
	require.NoError(t, exec.PreflightBatchFill(
		3, groups, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(
		3, groups, []*vector.Vector{input}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(1),
		vector.GetFixedAtNoTypeCheck[uint64](results[0], 0))
	results[0].Free(mp)
	input.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestHllSketchMarshalAndUnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()
	sketch, err := makeHllSketch(mp, nil)
	require.NoError(t, err)

	hlls := sketch.(*hllSketch)
	hlls.Insert(types.EncodeInt64(ptr(int64(1))))
	hlls.Insert(types.EncodeInt64(ptr(int64(2))))

	data, err := hlls.MarshalBinary()
	require.NoError(t, err)

	restoredMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	restored := restoredMU.(*hllSketch)
	require.NoError(t, restored.UnmarshalBinary(data))
	require.Equal(t, hlls.Estimate(), restored.Estimate())

	readerRestoredMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	readerRestored := readerRestoredMU.(*hllSketch)
	require.NoError(t, readerRestored.UnmarshalFromReader(bytes.NewReader(data)))
	require.Equal(t, hlls.Estimate(), readerRestored.Estimate())

	hlls.Free()
	restored.Free()
	readerRestored.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllSketchLegacyWireAndEstimateCompatibility(t *testing.T) {
	mp := mpool.MustNewZero()
	legacy := hll.NewNoSparse()
	currentMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	current := currentMU.(*hllSketch)
	defer func() {
		current.Free()
		require.Zero(t, mp.CurrNB())
	}()

	for value := int64(0); value < 10_000; value++ {
		encoded := types.EncodeInt64(&value)
		legacy.Insert(encoded)
		current.Insert(encoded)
	}
	legacyBytes, err := legacy.MarshalBinary()
	require.NoError(t, err)
	currentBytes, err := current.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, legacyBytes, currentBytes)
	require.Equal(t, legacy.Estimate(), current.Estimate())

	legacyDecoder := hll.NewNoSparse()
	require.NoError(t, legacyDecoder.UnmarshalBinary(currentBytes))
	require.Equal(t, current.Estimate(), legacyDecoder.Estimate())

	currentDecoderMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	currentDecoder := currentDecoderMU.(*hllSketch)
	defer currentDecoder.Free()
	require.NoError(t, currentDecoder.UnmarshalBinary(legacyBytes))
	require.Equal(t, legacy.Estimate(), currentDecoder.Estimate())
}

func TestHllSketchMergesLegacySparseWire(t *testing.T) {
	mp := mpool.MustNewZero()
	legacy := hll.New()
	for value := int64(0); value < 100; value++ {
		legacy.Insert(types.EncodeInt64(&value))
	}
	legacyBytes, err := legacy.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, byte(1), legacyBytes[3])

	currentMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	current := currentMU.(*hllSketch)
	require.NoError(t, current.mergeBytes(legacyBytes))
	require.Equal(t, legacy.Estimate(), current.Estimate())
	current.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllSketchMalformedSparseMergeIsAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	destinationMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	destination := destinationMU.(*hllSketch)
	defer func() {
		destination.Free()
		require.Zero(t, mp.CurrNB())
	}()
	for value := int64(0); value < 32; value++ {
		destination.Insert(types.EncodeInt64(&value))
	}
	want := bytes.Clone(destination.regs)

	legacy := hll.New()
	for value := int64(100); value < 180; value++ {
		legacy.Insert(types.EncodeInt64(&value))
	}
	encoded, err := legacy.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, byte(1), encoded[3])

	// Keep a valid temporary-set prefix and a fully decodable list, but make
	// the published list cardinality inconsistent with the payload.
	temporaryCount := binary.BigEndian.Uint32(encoded[4:8])
	metadata := 8 + int(temporaryCount)*4
	require.LessOrEqual(t, metadata+12, len(encoded))
	malformed := bytes.Clone(encoded)
	count := binary.BigEndian.Uint32(malformed[metadata : metadata+4])
	binary.BigEndian.PutUint32(malformed[metadata:metadata+4], count+1)

	require.Error(t, destination.mergeBytes(malformed))
	require.Equal(t, want, destination.regs)
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

func TestHllAddExecFillMergeFlush(t *testing.T) {
	mp := mpool.MustNewZero()

	left := makeHllAdd(mp, 1, types.T_int64.ToType()).(*hllAddExec)
	right := makeHllAdd(mp, 1, types.T_int64.ToType()).(*hllAddExec)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	values := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(values, []int64{1, 2, 2, 3}, nil, mp))
	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 1, GroupNotMatched}, []*vector.Vector{values}))

	rightValues := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(rightValues, []int64{3, 4}, nil, mp))
	require.NoError(t, right.BatchFill(0, []uint64{1, 2}, []*vector.Vector{rightValues}))

	require.NoError(t, left.SetExtraInformation(nil, 0))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))
	require.NoError(t, left.Merge(right, 0, 0))
	require.Greater(t, left.Size(), int64(0))

	vecs, err := left.Flush()
	require.NoError(t, err)
	require.False(t, vecs[0].IsNull(0))
	require.False(t, vecs[0].IsNull(1))

	group1MU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	group1 := group1MU.(*hllSketch)
	require.NoError(t, group1.UnmarshalBinary(vecs[0].GetBytesAt(0)))
	require.Equal(t, uint64(3), group1.Estimate())

	group2MU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	group2 := group2MU.(*hllSketch)
	require.NoError(t, group2.UnmarshalBinary(vecs[0].GetBytesAt(1)))
	require.Equal(t, uint64(1), group2.Estimate())
	group1.Free()
	group2.Free()

	values.Free(mp)
	rightValues.Free(mp)
	vecs[0].Free(mp)
	left.Free()
	right.Free()
}

func TestHllMergeExecFillMergeFlush(t *testing.T) {
	mp := mpool.MustNewZero()

	buildSketch := func(values ...int64) []byte {
		sketch, err := makeHllSketch(mp, nil)
		require.NoError(t, err)
		hlls := sketch.(*hllSketch)
		for _, value := range values {
			hlls.Insert(types.EncodeInt64(&value))
		}
		data, err := hlls.MarshalBinary()
		require.NoError(t, err)
		hlls.Free()
		return data
	}

	left := makeHllMerge(mp, 1, types.T_varbinary.ToType()).(*hllMergeExec)
	right := makeHllMerge(mp, 1, types.T_varbinary.ToType()).(*hllMergeExec)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	values := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(values, buildSketch(1, 2), false, mp))
	require.NoError(t, vector.AppendBytes(values, buildSketch(2, 3), false, mp))
	require.NoError(t, vector.AppendBytes(values, nil, true, mp))
	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2}, []*vector.Vector{values}))

	rightValues := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(rightValues, buildSketch(3, 4), false, mp))
	require.NoError(t, vector.AppendBytes(rightValues, buildSketch(5), false, mp))
	require.NoError(t, right.BatchFill(0, []uint64{1, 2}, []*vector.Vector{rightValues}))

	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))
	require.NoError(t, left.Merge(right, 0, 0))
	require.Greater(t, left.Size(), int64(0))

	vecs, err := left.Flush()
	require.NoError(t, err)

	group1MU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	group1 := group1MU.(*hllSketch)
	require.NoError(t, group1.UnmarshalBinary(vecs[0].GetBytesAt(0)))
	require.Equal(t, uint64(4), group1.Estimate())

	group2MU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	group2 := group2MU.(*hllSketch)
	require.NoError(t, group2.UnmarshalBinary(vecs[0].GetBytesAt(1)))
	require.Equal(t, uint64(1), group2.Estimate())
	group1.Free()
	group2.Free()

	invalid := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(invalid, []byte("bad"), false, mp))
	require.Error(t, left.BatchFill(0, []uint64{1}, []*vector.Vector{invalid}))

	values.Free(mp)
	rightValues.Free(mp)
	invalid.Free(mp)
	vecs[0].Free(mp)
	left.Free()
	right.Free()
}
