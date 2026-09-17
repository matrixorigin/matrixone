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

package aggexec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountDistinctArgumentDrainCommitAndRestore(t *testing.T) {
	mp := mpool.MustNewZero()
	input := testutil.NewInt64Vector(
		5,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{1, 1, 2, 3, 3},
	)
	defer input.Free(mp)
	baseline := mp.CurrNB()

	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.T_int64.ToType()},
	)
	restored, ok := AggFuncExec(exec).(ExactCountDistinctSpillState)
	require.True(t, ok)
	require.NoError(t, restored.GroupGrow(2))
	require.NoError(t, restored.BatchFill(
		0,
		[]uint64{1, 1, 1, 2, 2},
		[]*vector.Vector{input},
	))

	drain, err := restored.BeginArgumentDrain(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), drain.KeyCount())
	require.Positive(t, drain.RetainedBytes())
	payloads := make(map[int][][]byte)
	require.NoError(t, drain.ForEach(func(group int, payload []byte) error {
		payloads[group] = append(payloads[group], bytes.Clone(payload))
		return nil
	}))
	require.Len(t, payloads[0], 2)
	require.Len(t, payloads[1], 1)
	require.NoError(t, drain.Commit())

	zero, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0},
		vector.MustFixedColNoTypeCheck[int64](zero[0]))
	zero[0].Free(mp)
	require.NoError(t, restored.AddDistinctCountContribution(1, 5, nil))

	for group, values := range payloads {
		for _, payload := range values {
			require.NoError(t, restored.InsertDistinctArgument(group, payload))
			require.NoError(t, restored.InsertDistinctArgument(group, payload))
		}
	}
	result, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{2, 6},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	restored.Free()
	require.Equal(t, baseline, mp.CurrNB())
}

func TestCountDistinctArgumentDrainUsesCanonicalMembershipKey(t *testing.T) {
	mp := mpool.MustNewZero()
	input := testutil.NewFloat32Vector(
		2,
		types.New(types.T_float32, 10, 2),
		mp,
		false,
		nil,
		[]float32{1.2300001, 1.23},
	)
	defer input.Free(mp)

	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.New(types.T_float32, 10, 2)},
	)
	spill := exec.(ExactCountDistinctSpillState)
	require.NoError(t, spill.GroupGrow(1))
	require.NoError(t, spill.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{input}))

	drain, err := spill.BeginArgumentDrain(nil)
	require.NoError(t, err)
	var payloads [][]byte
	require.NoError(t, drain.ForEach(func(_ int, payload []byte) error {
		payloads = append(payloads, bytes.Clone(payload))
		return nil
	}))
	require.Len(t, payloads, 1)
	require.NoError(t, drain.Commit())
	for _, payload := range payloads {
		require.NoError(t, spill.InsertDistinctArgument(0, payload))
	}
	result, err := spill.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	exec.Free()
}

func TestCountDistinctArgumentDrainKeepsRepresentative(t *testing.T) {
	mp := mpool.MustNewZero()
	json, err := types.ParseStringToByteJson("1")
	require.NoError(t, err)
	raw, err := types.EncodeJson(json)
	require.NoError(t, err)
	values := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(values, raw, false, mp))

	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
	).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{values}))

	drain, err := exec.BeginArgumentDrain(nil)
	require.NoError(t, err)
	var membership, representative []byte
	require.NoError(t, drain.ForEachWithRepresentative(
		func(_ int, payload, value []byte) error {
			membership = bytes.Clone(payload)
			representative = bytes.Clone(value)
			return nil
		}))
	require.NotEqual(t, raw, membership)
	require.Equal(t, raw, representative)
	drain.Abort()

	exec.Free()
	values.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctLegacyKeepsNonFloatRepresentative(t *testing.T) {
	mp := mpool.MustNewZero()
	json, err := types.ParseStringToByteJson("1")
	require.NoError(t, err)
	raw, err := types.EncodeJson(json)
	require.NoError(t, err)
	values := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(values, raw, false, mp))

	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
	).(*countColumnExec)
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, true))
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{values}))
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{values}))

	drain, err := exec.BeginArgumentDrain(nil)
	require.NoError(t, err)
	var membership, representative []byte
	require.NoError(t, drain.ForEachWithRepresentative(
		func(_ int, payload, value []byte) error {
			membership = bytes.Clone(payload)
			representative = bytes.Clone(value)
			return nil
		}))
	require.NotEqual(t, raw, membership)
	require.Equal(t, raw, representative)
	drain.Abort()

	exec.Free()
	values.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctSpillRestoresAcrossFloatPolicies(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	defer finishTestAggregateAllocation(t, registry, account)

	tupleTypes := []types.Type{
		types.T_float64.ToType(),
		types.New(types.T_char, 2, 0),
	}
	makeExec := func(legacy bool) *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true, tupleTypes,
		).(*countColumnExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, legacy))
		return exec
	}
	makeTuple := func(bits []uint64) ([]*vector.Vector, func()) {
		floats := make([]float64, len(bits))
		for i, value := range bits {
			floats[i] = math.Float64frombits(value)
		}
		floatVec := testutil.NewFloat64Vector(
			len(floats), tupleTypes[0], mp, false, nil, floats)
		charVec := buildVarlenVec(t, mp, tupleTypes[1],
			[]string{"a", "a"}[:len(bits)])
		return []*vector.Vector{floatVec, charVec}, func() {
			floatVec.Free(mp)
			charVec.Free(mp)
		}
	}

	t.Run("legacy-to-modern-normalizes-and-deduplicates", func(t *testing.T) {
		source := makeExec(true)
		vectors, freeVectors := makeTuple([]uint64{
			0x7ff8000000000001,
			0x7ff8000000000002,
		})
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BatchFill(
			0, []uint64{1, 1}, vectors))
		var spill bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(
			0, []int32{0}, &spill))
		source.Free()
		freeVectors()

		target := makeExec(false)
		require.NoError(t, target.UnmarshalSpillFromReader(
			bytes.NewReader(spill.Bytes()), mp))
		one, freeOne := makeTuple([]uint64{0x7ff8000000000001})
		require.NoError(t, target.BatchFill(0, []uint64{1}, one))
		result, err := target.Flush()
		require.NoError(t, err)
		require.Equal(t, []int64{1},
			vector.MustFixedColNoTypeCheck[int64](result[0]))
		result[0].Free(mp)
		freeOne()
		target.Free()
	})

	t.Run("modern-to-legacy-rejects-before-publication", func(t *testing.T) {
		source := makeExec(false)
		vectors, freeVectors := makeTuple([]uint64{
			// Start with the canonical NaN spelling. A raw-key comparison alone
			// cannot distinguish this modern representative from a legacy one;
			// the spill policy header must carry the producer contract explicitly.
			0x7ff8000000000000,
			0x7ff8000000000001,
		})
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BatchFill(0, []uint64{1, 1}, vectors))
		var spill bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(
			0, []int32{0}, &spill))
		freeVectors()
		usedBefore := account.Snapshot().Used

		target := makeExec(true)
		err := target.UnmarshalSpillFromReader(
			bytes.NewReader(spill.Bytes()), mp)
		require.ErrorContains(t, err, "canonical FLOAT DISTINCT spill")
		require.Equal(t, usedBefore, account.Snapshot().Used)
		target.Free()
		source.Free()
	})

	t.Run("modern-fixed-to-legacy-rejects-before-publication", func(t *testing.T) {
		typ := types.T_float64.ToType()
		source := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{typ},
		).(*countColumnExec)
		require.NoError(t, source.SetAllocationAccount(allocation))
		require.NoError(t, ConfigureLegacyDistinctFloatKeys(source, false))
		require.NoError(t, source.GroupGrow(distinctFixedIndexMinGroups))
		values := testutil.NewFloat64Vector(
			1, typ, mp, false, nil,
			[]float64{math.Float64frombits(0x7ff8000000000000)})
		require.NoError(t, source.PreflightBatchFill(
			0, []uint64{1}, []*vector.Vector{values}))
		require.NoError(t, source.BatchFill(
			0, []uint64{1}, []*vector.Vector{values}))
		require.True(t, source.state[0].distinctFixedDeferred)
		var spill bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(
			0, []int32{0}, &spill))
		values.Free(mp)

		usedBefore := account.Snapshot().Used
		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{typ},
		).(*countColumnExec)
		require.NoError(t, target.SetAllocationAccount(allocation))
		require.NoError(t, ConfigureLegacyDistinctFloatKeys(target, true))
		err := target.UnmarshalSpillFromReader(
			bytes.NewReader(spill.Bytes()), mp)
		require.ErrorContains(t, err, "canonical FLOAT DISTINCT spill")
		require.Equal(t, usedBefore, account.Snapshot().Used)
		target.Free()
		source.Free()
	})
}

func TestCountDistinctStateRestoresFloatEquivalencePeers(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  types.Type
		data [][]byte
	}{
		{
			name: "float32",
			typ:  types.T_float32.ToType(),
			data: [][]byte{
				types.EncodeFixed(math.Float32frombits(0x7fc00000)),
				types.EncodeFixed(math.Float32frombits(0xffc00001)),
				types.EncodeFixed(float32(math.Copysign(0, -1))),
				types.EncodeFixed(float32(0)),
			},
		},
		{
			name: "float64",
			typ:  types.T_float64.ToType(),
			data: [][]byte{
				types.EncodeFixed(math.Float64frombits(0x7ff8000000000000)),
				types.EncodeFixed(math.Float64frombits(0xfff8000000000001)),
				types.EncodeFixed(math.Copysign(0, -1)),
				types.EncodeFixed(float64(0)),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true, []types.Type{tc.typ},
			).(*countColumnExec)
			require.NoError(t, exec.GroupGrow(1))

			// This is the pre-canonical fixed-width DISTINCT state: every raw
			// spelling is present on the wire, even though the receiver must
			// rebuild SQL-equivalence membership.
			var wire bytes.Buffer
			require.NoError(t, types.WriteInt32(&wire, 1))
			require.NoError(t, types.WriteUint32(&wire, uint32(len(tc.data))))
			for _, value := range tc.data {
				_, err := wire.Write(value)
				require.NoError(t, err)
			}
			_, err := exec.state[0].readState(mp, &wire, &exec.aggInfo)
			require.NoError(t, err)

			result, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, []int64{2},
				vector.MustFixedColNoTypeCheck[int64](result[0]))
			result[0].Free(mp)
			exec.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCountDistinctStateRestoresLegacyRawOpaqueKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.New(types.T_char, 2, 0)},
	).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(1))

	// This is the pre-canonical opaque DISTINCT state: the two raw CHAR
	// spellings compare equal after PAD SPACE normalization.
	var wire bytes.Buffer
	require.NoError(t, types.WriteInt32(&wire, 1))
	require.NoError(t, types.WriteUint32(&wire, 2))
	first := []byte("a ")
	second := []byte("a")
	require.NoError(t, types.WriteInt32(&wire, int32(len(first))))
	_, err := wire.Write(first)
	require.NoError(t, err)
	require.NoError(t, types.WriteInt32(&wire, int32(len(second))))
	_, err = wire.Write(second)
	require.NoError(t, err)

	_, err = exec.state[0].readState(mp, &wire, &exec.aggInfo)
	require.NoError(t, err)
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	exec.Free()
}

func TestCountDistinctStateRestoresLegacyRawTupleKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	tupleTypes := []types.Type{
		types.New(types.T_char, 2, 0),
		types.New(types.T_char, 2, 0),
	}
	exec := newCountColumnExec(mp, AggIdOfCountColumn, true, tupleTypes).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(1))
	encodeTuple := func(left, right string) []byte {
		payload := make([]byte, 8+len(left)+len(right))
		binary.BigEndian.PutUint32(payload, uint32(len(left)))
		copy(payload[4:], left)
		offset := 4 + len(left)
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(right)))
		copy(payload[offset+4:], right)
		return payload
	}
	var wire bytes.Buffer
	require.NoError(t, types.WriteInt32(&wire, 1))
	require.NoError(t, types.WriteUint32(&wire, 2))
	for _, payload := range [][]byte{
		encodeTuple("a ", "b"),
		encodeTuple("a", "b"),
	} {
		require.NoError(t, types.WriteInt32(&wire, int32(len(payload))))
		_, err := wire.Write(payload)
		require.NoError(t, err)
	}

	_, err := exec.state[0].readState(mp, &wire, &exec.aggInfo)
	require.NoError(t, err)
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	exec.Free()
}

func TestCountDistinctLegacyPayloadCannotMimicCanonicalWireFrame(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.New(types.T_varchar, 16, 0)},
	).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(1))
	var wire bytes.Buffer
	require.NoError(t, types.WriteInt32(&wire, 1))
	require.NoError(t, types.WriteUint32(&wire, 2))
	for _, value := range [][]byte{[]byte("MOCDK1a"), []byte("a")} {
		require.NoError(t, types.WriteInt32(&wire, int32(len(value))))
		_, err := wire.Write(value)
		require.NoError(t, err)
	}
	_, err := exec.state[0].readState(mp, &wire, &exec.aggInfo)
	require.NoError(t, err)
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{2}, vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	exec.Free()
}

func TestCountDistinctArgumentDrainAbortKeepsResidentOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	input := testutil.NewInt64Vector(
		3,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{7, 7, 8},
	)
	defer input.Free(mp)
	baseline := mp.CurrNB()

	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.T_int64.ToType()},
	)
	spill, ok := exec.(ExactCountDistinctSpillState)
	require.True(t, ok)
	require.NoError(t, spill.GroupGrow(1))
	require.NoError(t, spill.BatchFill(
		0,
		[]uint64{1, 1, 1},
		[]*vector.Vector{input},
	))

	drain, err := spill.BeginArgumentDrain(nil)
	require.NoError(t, err)
	wantErr := errors.New("injected drain failure")
	err = drain.ForEach(func(int, []byte) error { return wantErr })
	require.ErrorIs(t, err, wantErr)
	drain.Abort()

	result, err := spill.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{2},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	spill.Free()
	require.Equal(t, baseline, mp.CurrNB())
}

func TestCountDistinctDrainCommitFailureKeepsResidentOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(20 << 10)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		},
	)
	require.NoError(t, err)
	exec, err := MakeSingleGroupAgg(
		mp,
		AggIdOfCountColumn,
		true,
		allocation,
		nil,
		types.T_int64.ToType(),
	)
	require.NoError(t, err)
	SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
	require.NoError(t, exec.GroupGrow(1))
	input := testutil.NewInt64Vector(
		1, types.T_int64.ToType(), mp, false, nil, []int64{9})
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(
		0, []uint64{1}, []*vector.Vector{input}))
	spill := exec.(ExactCountDistinctSpillState)
	drain, err := spill.BeginArgumentDrain(allocation)
	require.NoError(t, err)
	err = drain.Commit()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	drain.Abort()

	result, err := spill.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	input.Free(mp)
	spill.Free()
	require.NoError(t, spill.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctSpillStateRejectsInvalidTransitions(t *testing.T) {
	mp := mpool.MustNewZero()
	plain := newCountColumnExec(
		mp, AggIdOfCountColumn, false, []types.Type{types.T_int64.ToType()}).(ExactCountDistinctSpillState)
	require.False(t, plain.SupportsExactCountDistinctSpill())
	_, err := plain.HasDistinctArguments()
	require.Error(t, err)
	_, _, err = plain.DistinctArgumentStats()
	require.Error(t, err)
	_, err = plain.BeginArgumentDrain(nil)
	require.Error(t, err)
	require.Error(t, plain.RehomeDistinctArgumentState(nil))
	require.Error(t, plain.InsertDistinctArgument(0, nil))
	require.Error(t, plain.AddDistinctCountContribution(0, 1, nil))
	plain.Free()

	var nilDrain *countDistinctArgumentDrain
	require.Zero(t, nilDrain.KeyCount())
	require.Zero(t, nilDrain.RetainedBytes())
	require.Error(t, nilDrain.ForEach(func(int, []byte) error { return nil }))
	require.Error(t, nilDrain.Commit())
	nilDrain.Abort()

	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()}).(ExactCountDistinctSpillState)
	require.NoError(t, exec.GroupGrow(1))
	has, err := exec.HasDistinctArguments()
	require.NoError(t, err)
	require.False(t, has)
	keys, retained, err := exec.DistinctArgumentStats()
	require.NoError(t, err)
	require.Zero(t, keys)
	require.Positive(t, retained)
	require.Error(t, exec.InsertDistinctArgument(-1, nil))
	require.Error(t, exec.InsertDistinctArgument(1, nil))
	require.Error(t, exec.AddDistinctCountContribution(-1, 1, nil))
	require.Error(t, exec.AddDistinctCountContribution(0, math.MaxUint64, nil))
	require.NoError(t, exec.AddDistinctCountContribution(0, math.MaxInt64, nil))
	require.ErrorContains(t, exec.AddDistinctCountContribution(0, 1, nil), "overflow")
	// Fixed-width exact-distinct states accept the canonical payload emitted by
	// their drain.  An arbitrary byte string would violate the type contract;
	// malformed/retyped payloads are rejected before publication.
	value := int64(7)
	require.NoError(t, exec.InsertDistinctArgument(0, types.EncodeInt64(&value)))
	require.ErrorContains(t, exec.RehomeDistinctArgumentState(nil), "non-empty")

	drain, err := exec.BeginArgumentDrain(nil)
	require.NoError(t, err)
	drain.Abort()
	require.Error(t, drain.ForEach(func(int, []byte) error { return nil }))
	require.Error(t, drain.Commit())
	drain.Abort()
	exec.Free()
	require.Zero(t, mp.CurrNB())
}
