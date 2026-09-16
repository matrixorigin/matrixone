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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountDistinctLegacyFloatKeepsFixedIndexAndAllPayloads(t *testing.T) {
	tests := []struct {
		name   string
		typ    types.Type
		width  int
		values []uint64
	}{
		{
			name:  "float32",
			typ:   types.T_float32.ToType(),
			width: 4,
			values: []uint64{
				0x7fc00001, 0x7fc00002, 0xffc00001,
				0x80000000, 0x00000000, 0x3f800000,
			},
		},
		{
			name:  "float64",
			typ:   types.T_float64.ToType(),
			width: 8,
			values: []uint64{
				0x7ff8000000000001, 0x7ff8000000000002,
				0xfff8000000000001, 0x8000000000000000,
				0x0000000000000000, 0x3ff0000000000000,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()) }()

			var vec *vector.Vector
			if tc.typ.Oid == types.T_float32 {
				values := make([]float32, len(tc.values))
				for i, bits := range tc.values {
					values[i] = math.Float32frombits(uint32(bits))
				}
				vec = testutil.NewFloat32Vector(
					len(values), tc.typ, mp, false, nil, values)
			} else {
				values := make([]float64, len(tc.values))
				for i, bits := range tc.values {
					values[i] = math.Float64frombits(bits)
				}
				vec = testutil.NewFloat64Vector(
					len(values), tc.typ, mp, false, nil, values)
			}

			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true, []types.Type{tc.typ},
			).(*countColumnExec)
			require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, true))
			require.NoError(t, exec.GroupGrow(distinctFixedIndexMinGroups))
			// Legacy mode still uses the fixed index. Its key policy, rather than a
			// raw representative sidecar, preserves every old-protocol NaN bit
			// pattern without sacrificing the hot-path performance.
			require.True(t, exec.state[0].distinctFixedDeferred)
			groups := make([]uint64, len(tc.values))
			for i := range groups {
				groups[i] = 1
			}
			require.NoError(t, exec.PreflightBatchFill(0, groups, []*vector.Vector{vec}))
			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, int64(5),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			result[0].Free(mp)

			var encoded bytes.Buffer
			SetCanonicalDistinctKeyWire(exec, false)
			require.NoError(t, exec.SaveIntermediateResultOfChunk(0, &encoded))
			wire := encoded.Bytes()
			require.GreaterOrEqual(t, len(wire), 20+len(tc.values)*tc.width)
			seen := make(map[uint64]bool, len(tc.values))
			for i := range tc.values {
				var raw [8]byte
				copy(raw[:tc.width], wire[20+i*tc.width:20+(i+1)*tc.width])
				seen[binary.LittleEndian.Uint64(raw[:])] = true
			}
			for _, bits := range tc.values {
				if bits == 0x80000000 || bits == 0x8000000000000000 {
					bits = 0
				}
				require.True(t, seen[bits], "legacy wire lost key %#x", bits)
			}

			vec.Free(mp)
			exec.Free()
		})
	}
}

func TestCountDistinctModernFloatKeepsFixedIndexFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	vec := testutil.NewFloat64Vector(
		1, types.T_float64.ToType(), mp, false, nil,
		[]float64{math.Float64frombits(0x7ff8000000000001)})
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_float64.ToType()},
	).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(distinctFixedIndexMinGroups))
	require.True(t, exec.state[0].distinctFixedDeferred)
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{vec}))
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{vec}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	vec.Free(mp)
	exec.Free()
}

func TestCountDistinctLegacyFloatDoesNotDisableIntegerFixedIndex(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_int64.ToType()},
	).(*countColumnExec)
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, true))
	require.NoError(t, exec.GroupGrow(distinctFixedIndexMinGroups))
	require.True(t, exec.state[0].distinctFixedDeferred)
	exec.Free()
}

func TestCountDistinctLegacyFloatPartialAndSpillRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	typ := types.T_float64.ToType()
	bits := uint64(0x7ff8000000000001)
	newValues := func() *vector.Vector {
		return testutil.NewFloat64Vector(
			1, typ, mp, false, nil,
			[]float64{math.Float64frombits(bits)})
	}
	newExec := func(legacy bool, groups int) *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{typ},
		).(*countColumnExec)
		require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, legacy))
		require.NoError(t, exec.GroupGrow(groups))
		return exec
	}

	legacySource := newExec(true, distinctFixedIndexMinGroups)
	values := newValues()
	require.NoError(t, legacySource.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{values}))
	require.NoError(t, legacySource.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
	var partial bytes.Buffer
	require.NoError(t, legacySource.SaveIntermediateResultOfChunk(0, &partial))
	partialBytes := partial.Bytes()
	require.Equal(t, bits,
		binary.LittleEndian.Uint64(partialBytes[20:28]))

	modernTarget := newExec(false, distinctFixedIndexMinGroups)
	require.NoError(t, modernTarget.UnmarshalFromReader(
		bytes.NewReader(partialBytes), mp))
	require.True(t, modernTarget.state[0].distinctFixedDeferred)
	legacyPeer := newExec(true, 1)
	peerValues := newValues()
	require.NoError(t, legacyPeer.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{peerValues}))
	require.NoError(t, legacyPeer.BatchFill(0, []uint64{1}, []*vector.Vector{peerValues}))
	require.NoError(t, modernTarget.PreflightBatchMerge(
		legacyPeer, 0, []uint64{1}))
	require.NoError(t, modernTarget.BatchMerge(legacyPeer, 0, []uint64{1}))
	result, err := modernTarget.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	for _, vec := range result {
		vec.Free(mp)
	}

	var spill bytes.Buffer
	require.NoError(t, legacySource.SaveSpillIntermediateRows(
		0, []int32{0}, &spill))
	restored := newExec(false, 1)
	require.NoError(t, restored.UnmarshalSpillFromReader(
		bytes.NewReader(spill.Bytes()), mp))
	var restoredWire bytes.Buffer
	require.NoError(t, restored.SaveIntermediateResultOfChunk(0, &restoredWire))
	restoredBytes := restoredWire.Bytes()
	require.Equal(t, bits,
		binary.LittleEndian.Uint64(restoredBytes[20:28]))

	peerValues.Free(mp)
	values.Free(mp)
	restored.Free()
	legacyPeer.Free()
	modernTarget.Free()
	legacySource.Free()
}

func TestCountDistinctFloatPolicyCrossRepresentationMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	typ := types.T_float64.ToType()
	values := func() *vector.Vector {
		return testutil.NewFloat64Vector(
			2, typ, mp, false, nil, []float64{
				math.Float64frombits(0x7ff8000000000001),
				math.Float64frombits(0x7ff8000000000002),
			})
	}
	newExec := func(legacy bool, groups int) *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{typ},
		).(*countColumnExec)
		require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, legacy))
		require.NoError(t, exec.GroupGrow(groups))
		return exec
	}
	fill := func(exec *countColumnExec) {
		vec := values()
		require.NoError(t, exec.PreflightBatchFill(
			0, []uint64{1, 1}, []*vector.Vector{vec}))
		require.NoError(t, exec.BatchFill(
			0, []uint64{1, 1}, []*vector.Vector{vec}))
		vec.Free(mp)
	}
	count := func(exec *countColumnExec) int64 {
		result, err := exec.Flush()
		require.NoError(t, err)
		got := vector.GetFixedAtNoTypeCheck[int64](result[0], 0)
		result[0].Free(mp)
		return got
	}

	t.Run("legacy-to-modern-fixed", func(t *testing.T) {
		source := newExec(true, distinctFixedIndexMinGroups)
		target := newExec(false, distinctFixedIndexMinGroups)
		fill(source)
		require.NoError(t, target.PreflightBatchMerge(source, 0, []uint64{1}))
		require.NoError(t, target.BatchMerge(source, 0, []uint64{1}))
		require.Equal(t, int64(1), count(target))
		source.Free()
		target.Free()
	})

	t.Run("legacy-to-modern-skiplist", func(t *testing.T) {
		source := newExec(true, distinctFixedIndexMinGroups)
		target := newExec(false, 1)
		fill(source)
		require.NoError(t, target.PreflightBatchMerge(source, 0, []uint64{1}))
		require.NoError(t, target.BatchMerge(source, 0, []uint64{1}))
		require.Equal(t, int64(1), count(target))
		source.Free()
		target.Free()
	})

	t.Run("modern-to-legacy-rejected-before-mutation", func(t *testing.T) {
		source := newExec(false, distinctFixedIndexMinGroups)
		target := newExec(true, distinctFixedIndexMinGroups)
		require.False(t, source.legacyDistinctFloatKeys)
		require.True(t, target.legacyDistinctFloatKeys)
		require.False(t, source.state[0].legacyDistinctFloatKeys)
		require.True(t, target.state[0].legacyDistinctFloatKeys)
		fill(source)
		require.NoError(t, target.PreflightBatchMerge(source, 0, []uint64{1}))
		require.Error(t, target.BatchMerge(source, 0, []uint64{1}))
		require.Equal(t, int64(0), count(target))
		source.Free()
		target.Free()
	})
}

func TestDistinctFloatCompatibilityKeyCodecs(t *testing.T) {
	float32Type := types.T_float32.ToType()
	float64Type := types.T_float64.ToType()
	raw32 := make([]byte, 4)
	raw64 := make([]byte, 8)
	binary.LittleEndian.PutUint32(raw32, 0x7fc00001)
	binary.LittleEndian.PutUint64(raw64, 0xfff8000000000001)

	require.Equal(t, len(raw32), distinctPayloadKeySize(float32Type, raw32, true))
	require.Equal(t, len(raw64), distinctPayloadKeySize(float64Type, raw64, true))
	legacy32 := appendDistinctPayloadKey(nil, float32Type, raw32, true)
	legacy64 := appendDistinctPayloadKey(nil, float64Type, raw64, true)
	require.Equal(t, raw32, legacy32)
	require.Equal(t, raw64, legacy64)

	modern32 := appendDistinctPayloadKey(nil, float32Type, raw32, false)
	modern64 := appendDistinctPayloadKey(nil, float64Type, raw64, false)
	require.Equal(t,
		distinctFloat32KeyBits(math.Float32frombits(0x7fc00001), 0, false),
		binary.LittleEndian.Uint32(modern32))
	require.Equal(t,
		distinctFloat64KeyBits(math.Float64frombits(0xfff8000000000001), false),
		binary.LittleEndian.Uint64(modern64))

	integerType := types.T_int64.ToType()
	integerRaw := make([]byte, 8)
	binary.LittleEndian.PutUint64(integerRaw, 42)
	require.Equal(t, len(integerRaw), distinctPayloadKeySize(integerType, integerRaw, true))
	require.Len(t, appendDistinctPayloadKey(nil, integerType, integerRaw, true), len(integerRaw))
}

func TestDistinctFloatMergeNormalizesRepresentations(t *testing.T) {
	float32Info := &aggInfo{
		isDistinct: true,
		argTypes:   []types.Type{types.T_float32.ToType()},
	}
	float64Info := &aggInfo{
		isDistinct: true,
		argTypes:   []types.Type{types.T_float64.ToType()},
	}

	legacy32 := uint64(0x7fc00001)
	got32, err := normalizeDistinctFixedValue(
		float32Info, legacy32, true, false)
	require.NoError(t, err)
	require.Equal(t, uint64(distinctFloat32KeyBits(
		math.Float32frombits(uint32(legacy32)), 0, false)), got32)

	legacy64 := uint64(0xfff8000000000001)
	got64, err := normalizeDistinctFixedValue(
		float64Info, legacy64, true, false)
	require.NoError(t, err)
	require.Equal(t, distinctFloat64KeyBits(
		math.Float64frombits(legacy64), false), got64)

	integerInfo := &aggInfo{
		isDistinct: true,
		argTypes:   []types.Type{types.T_int64.ToType()},
	}
	gotInteger, err := normalizeDistinctFixedValue(integerInfo, 42, true, false)
	require.NoError(t, err)
	require.Equal(t, uint64(42), gotInteger)
	_, err = normalizeDistinctFixedValue(float64Info, legacy64, false, true)
	require.Error(t, err)

	payload32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload32, uint32(legacy32))
	require.NoError(t, normalizeDistinctPayloadInPlace(
		float32Info.argTypes[0], payload32, false))
	require.Equal(t, uint32(got32), binary.LittleEndian.Uint32(payload32))

	payload64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload64, legacy64)
	require.NoError(t, normalizeDistinctPayloadInPlace(
		float64Info.argTypes[0], payload64, false))
	require.Equal(t, got64, binary.LittleEndian.Uint64(payload64))
	require.Error(t, normalizeDistinctPayloadInPlace(
		float64Info.argTypes[0], payload64, true))
	require.ErrorIs(t, normalizeDistinctPayloadInPlace(
		float32Info.argTypes[0], make([]byte, 3), false),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, normalizeDistinctPayloadInPlace(
		float64Info.argTypes[0], make([]byte, 4), false),
		mpool.ErrAllocationAccountInvariant)

	key := make([]byte, kAggArgPrefixSz+len(payload64))
	binary.BigEndian.PutUint16(key, 0)
	binary.LittleEndian.PutUint64(key[kAggArgPrefixSz:], legacy64)
	require.NoError(t, normalizeDistinctKeyInPlace(
		float64Info, key, true, false))
	require.Equal(t, got64,
		binary.LittleEndian.Uint64(key[kAggArgPrefixSz:]))
	require.ErrorIs(t, normalizeDistinctKeyInPlace(
		float64Info, []byte{0}, true, false),
		mpool.ErrAllocationAccountInvariant)

	tupleInfo := &aggInfo{
		isDistinct: true,
		argTypes: []types.Type{
			types.T_float64.ToType(),
			types.New(types.T_char, 2, 0),
		},
	}
	tuplePayload := make([]byte, 0, 4+8+4+1)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload64)))
	tuplePayload = append(tuplePayload, size[:]...)
	tuplePayload = append(tuplePayload, payload64...)
	binary.BigEndian.PutUint32(size[:], 1)
	tuplePayload = append(tuplePayload, size[:]...)
	tuplePayload = append(tuplePayload, 'a')
	tupleKey := append([]byte{0, 0}, tuplePayload...)
	require.NoError(t, normalizeDistinctKeyInPlace(
		tupleInfo, tupleKey, true, false))
	require.Equal(t, got64,
		binary.LittleEndian.Uint64(tupleKey[kAggArgPrefixSz+4:]))
	require.Equal(t, []byte{'a'},
		tupleKey[kAggArgPrefixSz+4+len(payload64)+4:])

	malformed := append([]byte{0, 0}, tuplePayload...)
	malformed = append(malformed, 0)
	require.ErrorIs(t, normalizeDistinctKeyInPlace(
		tupleInfo, malformed, true, false),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, normalizeDistinctKeyInPlace(
		tupleInfo, []byte{0, 0, 0}, true, false),
		mpool.ErrAllocationAccountInvariant)
}

func TestConfigureLegacyDistinctFloatKeysFreezesAfterStateAdmission(t *testing.T) {
	var nilExec *aggExec
	require.ErrorIs(t, nilExec.setLegacyDistinctFloatKeys(true),
		mpool.ErrAllocationAccountInvalid)

	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_float64.ToType()},
	).(*countColumnExec)
	require.True(t, RequiresModernDistinctFloatKeyWire(exec))
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, true))
	require.True(t, exec.legacyDistinctFloatKeys)
	require.False(t, RequiresModernDistinctFloatKeyWire(exec))
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(exec, true))
	require.NoError(t, exec.GroupGrow(1))
	require.ErrorContains(t,
		ConfigureLegacyDistinctFloatKeys(exec, false),
		"after state admission")
	exec.Free()
}

func TestCountDistinctLegacyTupleAppendNormalizesFloatKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	tupleTypes := []types.Type{
		types.T_float64.ToType(),
		types.New(types.T_char, 2, 0),
	}
	source := newCountColumnExec(
		mp, AggIdOfCountColumn, true, tupleTypes,
	).(*countColumnExec)
	target := newCountColumnExec(
		mp, AggIdOfCountColumn, true, tupleTypes,
	).(*countColumnExec)
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(source, true))
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, target.preAllocateGroupsWithNulls(1, true))

	floats := testutil.NewFloat64Vector(
		2, tupleTypes[0], mp, false, nil, []float64{
			math.Float64frombits(0x7ff8000000000001),
			math.Float64frombits(0xfff8000000000001),
		})
	chars := vector.NewVec(tupleTypes[1])
	require.NoError(t, vector.AppendBytes(chars, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(chars, []byte("a"), false, mp))
	require.NoError(t, source.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{floats, chars}))
	require.Equal(t, uint32(2), source.state[0].argCnt[0])

	offset, err := target.state[0].appendFromStateArg(
		mp, 0, &source.state[0], &target.aggInfo)
	require.NoError(t, err)
	require.Equal(t, int32(1), offset)
	require.Equal(t, uint32(1), target.state[0].argCnt[0])

	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(1),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	floats.Free(mp)
	chars.Free(mp)
	source.Free()
	target.Free()
}

func TestCountDistinctModernToLegacyPreflightRejectsWithoutReservation(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newReviewAggregateAllocation(t, 2<<20)
	source := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_float64.ToType()},
	).(*countColumnExec)
	target := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_float64.ToType()},
	).(*countColumnExec)
	require.NoError(t, source.SetAllocationAccount(allocation))
	require.NoError(t, target.SetAllocationAccount(allocation))
	require.NoError(t, ConfigureLegacyDistinctFloatKeys(target, true))
	require.NoError(t, source.GroupGrow(distinctFixedIndexMinGroups))
	require.NoError(t, target.GroupGrow(distinctFixedIndexMinGroups))

	values := testutil.NewFloat64Vector(
		2, types.T_float64.ToType(), mp, false, nil, []float64{
			math.Float64frombits(0x7ff8000000000001),
			math.Float64frombits(0x7ff8000000000002),
		})
	require.NoError(t, source.PreflightBatchFill(
		0, []uint64{1, 1}, []*vector.Vector{values}))
	require.NoError(t, source.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{values}))
	beforeUsed := account.Snapshot().Used
	beforeCount := target.state[0].argCnt[0]
	err := target.PreflightBatchMerge(source, 0, []uint64{1})
	require.ErrorContains(t, err, "canonical FLOAT DISTINCT state")
	require.Equal(t, beforeUsed, account.Snapshot().Used)
	require.Equal(t, beforeCount, target.state[0].argCnt[0])

	values.Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctBulkFillChunksBeyondUnitLimit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		disableFixed bool
	}{
		{name: "fixed-over-two-units", rows: hashmap.UnitLimit*2 + 1},
		{name: "legacy-over-one-unit", rows: hashmap.UnitLimit + 1, disableFixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := make([]int64, tc.rows)
			for i := range values {
				values[i] = int64(i)
			}
			vec := testutil.NewInt64Vector(
				tc.rows, types.T_int64.ToType(), mp, false, nil, values)
			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true,
				[]types.Type{types.T_int64.ToType()},
			).(*countColumnExec)
			require.NoError(t, exec.GroupGrow(1024))
			if tc.disableFixed {
				require.True(t, exec.disableEmptyDistinctFixedStates())
			}

			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, int64(tc.rows),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			for _, v := range result {
				v.Free(mp)
			}
			exec.Free()
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCountDistinctBatchFillChunksAndBoundaryDuplicates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		disableFixed bool
	}{
		{name: "fixed-over-two-units", rows: hashmap.UnitLimit*2 + 1},
		{name: "legacy-over-one-unit", rows: hashmap.UnitLimit + 1, disableFixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := make([]int64, tc.rows)
			for i := range values {
				values[i] = int64(i)
			}
			// Keep one duplicate across a unit boundary.  The direct BatchFill
			// wrapper must preserve the resident membership set while it splits
			// the input into bounded admission units.
			values[len(values)-1] = values[0]
			vec := testutil.NewInt64Vector(
				tc.rows, types.T_int64.ToType(), mp, false, nil, values)
			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true,
				[]types.Type{types.T_int64.ToType()},
			).(*countColumnExec)
			require.NoError(t, exec.GroupGrow(1024))
			if tc.disableFixed {
				require.True(t, exec.disableEmptyDistinctFixedStates())
			}
			groups := make([]uint64, tc.rows)
			for i := range groups {
				groups[i] = 1
			}

			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, int64(tc.rows-1),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			result[0].Free(mp)
			exec.Free()
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestDistinctFixedBatchProbeIsBounded(t *testing.T) {
	var batch distinctFixedBatch
	batch.reset()
	for i := 0; i < len(batch.groups); i++ {
		duplicate, err := batch.seenOrInsert(1, uint64(i))
		require.NoError(t, err)
		require.False(t, duplicate)
	}
	_, err := batch.seenOrInsert(1, uint64(len(batch.groups)))
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
}

func TestCountDistinctFixedAdmissionRetainsNonPublishingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_int64.ToType()},
	).(*countColumnExec)
	require.NoError(t, exec.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1024))

	values := []int64{7, 7, 0, 8, 8, 9}
	nulls := []bool{false, false, true, false, false, false}
	groups := []uint64{1, 1, 1, 1, 1, GroupNotMatched}
	vec := testutil.NewInt64Vector(
		len(values), types.T_int64.ToType(), mp, false, nulls, values)
	require.NoError(t, exec.PreflightBatchFill(
		0, groups, []*vector.Vector{vec}))
	require.Equal(t, groups,
		exec.distinctFixedAdmission.groups[:len(groups)])
	require.Equal(t,
		[]bool{true, false, false, true, false, false},
		exec.distinctFixedAdmission.publish[:len(groups)])
	require.True(t, exec.distinctFixedAdmission.matches(0, groups, vec))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(2),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	vec.Free(mp)
	exec.Free()
	require.NoError(t, exec.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctFixedMergeFromLegacySource(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newReviewAggregateAllocation(t, 2<<20)
	makeExec := func() *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1024))
		return exec
	}
	source := makeExec()
	target := makeExec()

	// Force the source into the compatibility skiplist representation while
	// retaining the target's fixed index.  This models a small-account/legacy
	// spill source being merged into a current execution state.
	require.True(t, source.disableEmptyDistinctFixedStates())
	sourceValues := make([]int64, hashmap.UnitLimit-6)
	for i := range sourceValues {
		sourceValues[i] = int64(i)
	}
	targetValues := []int64{0}
	fill := func(exec *countColumnExec, values []int64) {
		vec := testutil.NewInt64Vector(
			len(values), types.T_int64.ToType(), mp, false, nil, values)
		groups := make([]uint64, len(values))
		for i := range groups {
			groups[i] = 1
		}
		require.NoError(t, exec.PreflightBatchFill(
			0, groups, []*vector.Vector{vec}))
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))
		vec.Free(mp)
	}
	fill(source, sourceValues)
	fill(target, targetValues)
	require.False(t, source.state[0].distinctFixedDeferred)
	require.True(t, target.state[0].distinctFixedDeferred)
	require.Equal(t, 256, len(target.state[0].distinctIndex.slotKeys))

	mergeGroups := []uint64{1}
	require.NoError(t, target.PreflightBatchMerge(
		source, 0, mergeGroups))
	// The source has 250 candidates and the target already has one.  Fixed
	// admission must grow the target index before publication (70% load bound),
	// rather than reserving the compatibility skiplist arena.
	require.Equal(t, 512, len(target.state[0].distinctIndex.slotKeys))
	require.NoError(t, target.BatchMerge(source, 0, mergeGroups))

	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(250),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func newReviewAggregateAllocation(
	t *testing.T,
	limit uint64,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount, *AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		AllocationAccountSites{
			VectorData:     1,
			VectorArea:     2,
			VectorNulls:    3,
			VectorGrouping: 4,
			ArgumentCount:  5,
			ArgumentArena:  6,
		},
	)
	require.NoError(t, err)
	return registry, account, allocation
}

func TestCountDistinctLegacyMergeFromFixedSource(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newReviewAggregateAllocation(t, 2<<20)
	makeExec := func() *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1024))
		return exec
	}
	source := makeExec()
	target := makeExec()
	require.True(t, target.disableEmptyDistinctFixedStates())

	fill := func(exec *countColumnExec, values []int64, groups []uint64) {
		require.Len(t, groups, len(values))
		vec := testutil.NewInt64Vector(
			len(values), types.T_int64.ToType(), mp, false, nil, values)
		for offset := 0; offset < len(groups); offset += hashmap.UnitLimit {
			end := min(offset+hashmap.UnitLimit, len(groups))
			require.NoError(t, exec.PreflightBatchFill(
				offset, groups[offset:end], []*vector.Vector{vec}))
			require.NoError(t, exec.BatchFill(
				offset, groups[offset:end], []*vector.Vector{vec}))
		}
		vec.Free(mp)
	}
	makeKey := func(value int64) []byte {
		key := make([]byte, kAggArgPrefixSz+8)
		binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], 0)
		binary.LittleEndian.PutUint64(key[kAggArgPrefixSz:], uint64(value))
		return key
	}
	keyNeed := func(value int64) uint64 {
		plan := arenaskl.MakeAddPlan(makeKey(value))
		consumed, trailing, ok := plan.ArenaFootprint(
			kAggArgPrefixSz+8, 0)
		require.True(t, ok)
		return consumed + trailing
	}
	var targetValues []int64
	var missingValues [2]int64
	for value := int64(1); value < 10000; value++ {
		arena := target.state[0].argSkl.Arena()
		used := uint64(arena.Size())
		capacity := uint64(arena.Capacity())
		one := keyNeed(value)
		two := one + keyNeed(value+1)
		three := two + keyNeed(value+2)
		if used+two <= capacity && used+three > capacity {
			missingValues = [2]int64{value, value + 1}
			break
		}
		targetValues = append(targetValues, value)
		fill(target, []int64{value}, []uint64{1})
	}
	require.Greater(t, len(targetValues), 2)
	require.NotEqual(t, [2]int64{}, missingValues)
	missing := []int64{missingValues[0], missingValues[1]}
	sourceValues := make([]int64, 0, (len(targetValues)+len(missing))*2)
	sourceGroups := make([]uint64, 0, cap(sourceValues))
	for _, value := range targetValues {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 1)
	}
	for _, value := range missing {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 1)
	}
	for _, value := range targetValues {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 2)
	}
	for _, value := range missing {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 2)
	}
	fill(source, sourceValues, sourceGroups)
	require.True(t, source.state[0].distinctFixedDeferred)
	require.False(t, target.state[0].distinctFixedDeferred)

	mergeGroups := []uint64{1, 1}
	beforeCapacity := target.state[0].argSkl.Arena().Capacity()
	beforeSize := target.state[0].argSkl.Arena().Size()
	require.NoError(t, target.PreflightBatchMerge(
		source, 0, mergeGroups))
	// The fixed source is iterated in reverse insertion order and both source
	// groups map to the same legacy target group.  Only the two missing values
	// are new; the exact preflight must deduplicate the reversed overlap and
	// reserve exactly those two nodes without growing the already-boundary arena.
	require.Equal(t, beforeCapacity, target.state[0].argSkl.Arena().Capacity())
	require.Equal(t, beforeSize, target.state[0].argSkl.Arena().Size())
	require.NoError(t, target.BatchMerge(source, 0, mergeGroups))
	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(len(targetValues)+len(missing)),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
