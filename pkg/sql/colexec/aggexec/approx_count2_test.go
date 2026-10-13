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
	"io"
	"math"
	"slices"
	"testing"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type hllStableEmptyWriter struct {
	err   error
	short bool
	calls int
}

func (w *hllStableEmptyWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.err != nil {
		return 0, w.err
	}
	if w.short && w.calls == 2 {
		return len(data) - 1, nil
	}
	return len(data), nil
}

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

func TestAccountedHllCanonicalScratchIsReservedBeforeFill(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := makeApproxCount(mp, AggIdOfApproxCount,
		types.T_json.ToType()).(*approxCountExec)
	owner := any(exec).(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1))
	baseline := account.Snapshot().Used

	input := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(
		input, mustHLLJSON(t, "[1,{\"n\":2.0}]"), false, mp))
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{input}))
	require.NotNil(t, exec.state[0].argScratch)
	require.Equal(t, baseline+uint64(hllRegisterCnt)+
		uint64(cap(exec.state[0].argScratch)), account.Snapshot().Used)
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{input}))

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
	require.NoError(t, restored.UnmarshalBinary(canonicalEmptyHLL[:]))
	require.False(t, restored.hasValue)

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
	require.Equal(t, hllLegacyVersion, legacyBytes[0])
	require.Equal(t, hllVersion, currentBytes[0])
	require.Equal(t, legacyBytes[1:], currentBytes[1:])
	require.Equal(t, legacy.Estimate(), current.Estimate())

	legacyDecoder := hll.NewNoSparse()
	require.NoError(t, legacyDecoder.UnmarshalBinary(currentBytes))
	require.Equal(t, current.Estimate(), legacyDecoder.Estimate())

	currentDecoderMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	currentDecoder := currentDecoderMU.(*hllSketch)
	defer currentDecoder.Free()
	require.NoError(t, currentDecoder.UnmarshalBinary(legacyBytes))
	require.Equal(t, hllLegacyVersion, currentDecoder.wireVersion)
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
	legacySparseCurrent := bytes.Clone(legacyBytes)
	legacySparseCurrent[0] = hllVersion
	require.ErrorContains(t, current.mergeBytes(legacySparseCurrent), "invalid HLL sparse hash version")

	currentNonEmptyMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	currentNonEmpty := currentNonEmptyMU.(*hllSketch)
	currentNonEmpty.Insert(types.EncodeInt64(ptr(int64(101))))
	require.ErrorContains(t, currentNonEmpty.mergeBytes(legacyBytes), "incompatible HLL hash versions")
	currentNonEmpty.Free()
	current.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllMergeEmptyStatesAreVersionNeutral(t *testing.T) {
	mp := mpool.MustNewZero()
	legacySource := hll.NewNoSparse()
	legacyValue := types.EncodeInt64(ptr(int64(1)))
	legacySource.Insert(legacyValue)
	legacyBytes, err := legacySource.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, hllLegacyVersion, legacyBytes[0])

	currentMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	currentSource := currentMU.(*hllSketch)
	currentSource.Insert(legacyValue)
	currentBytes, err := currentSource.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, hllVersion, currentBytes[0])
	currentSource.Free()

	sparse, err := hll.New().MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, byte(1), sparse[3])
	require.Len(t, sparse, 20)
	sparseLegacy := bytes.Clone(sparse)
	sparseLegacy[0] = hllLegacyVersion
	sparseCurrent := bytes.Clone(sparse)
	sparseCurrent[0] = hllVersion

	emptyDenseLegacy := bytes.Clone(canonicalEmptyHLL[:])
	emptyDenseLegacy[0] = hllLegacyVersion
	emptyDenseCurrent := bytes.Clone(canonicalEmptyHLL[:])
	emptyDenseCurrent[0] = hllVersion

	for _, tc := range []struct {
		name        string
		destination []byte
		empties     [][]byte
		version     byte
	}{
		{
			name:        "legacy destination accepts all empty versions",
			destination: legacyBytes,
			empties:     [][]byte{emptyDenseCurrent, emptyDenseLegacy, sparseCurrent, sparseLegacy},
			version:     hllLegacyVersion,
		},
		{
			name:        "current destination accepts all empty versions",
			destination: currentBytes,
			empties:     [][]byte{emptyDenseLegacy, emptyDenseCurrent, sparseLegacy, sparseCurrent},
			version:     hllVersion,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			destinationMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			destination := destinationMU.(*hllSketch)
			defer destination.Free()
			require.NoError(t, destination.UnmarshalBinary(tc.destination))
			want := bytes.Clone(destination.regs)
			for _, empty := range tc.empties {
				require.NoError(t, destination.mergeBytes(empty))
			}
			require.Equal(t, want, destination.regs)
			require.Equal(t, tc.version, destination.effectiveWireVersion())
		})
	}

	empties := [][]byte{emptyDenseLegacy, emptyDenseCurrent, sparseLegacy, sparseCurrent}
	for _, empty := range empties {
		for _, nonEmpty := range [][]byte{legacyBytes, currentBytes} {
			t.Run("empty then non-empty", func(t *testing.T) {
				destinationMU, err := makeHllSketch(mp, nil)
				require.NoError(t, err)
				destination := destinationMU.(*hllSketch)
				defer destination.Free()
				require.NoError(t, destination.mergeBytes(empty))
				require.False(t, destination.hasRegisters())

				require.NoError(t, destination.mergeBytes(nonEmpty))
				expectedMU, err := makeHllSketch(mp, nil)
				require.NoError(t, err)
				expected := expectedMU.(*hllSketch)
				require.NoError(t, expected.UnmarshalBinary(nonEmpty))
				require.Equal(t, expected.regs, destination.regs)
				require.Equal(t, nonEmpty[0], destination.effectiveWireVersion())
				expected.Free()
			})
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestStableEmptyHLLStateUsesRequestedVersion(t *testing.T) {
	for _, version := range []byte{hllLegacyVersion, hllVersion} {
		var encoded bytes.Buffer
		require.NoError(t, stableEmptyHLLState(version)(&encoded))
		data := encoded.Bytes()
		require.Len(t, data, 4+hllEncodedSize)
		require.Equal(t, int32(hllEncodedSize), int32(binary.LittleEndian.Uint32(data[:4])))
		require.Equal(t, version, data[4])

		mp := mpool.MustNewZero()
		mu, err := makeHllSketch(mp, nil)
		require.NoError(t, err)
		require.NoError(t, mu.(*hllSketch).UnmarshalBinary(data[4:]))
		mu.(*hllSketch).Free()
		require.Zero(t, mp.CurrNB())
	}
}

func TestStableEmptyHLLStatePropagatesWriterErrors(t *testing.T) {
	require.ErrorIs(t,
		stableEmptyHLLState(hllVersion)(&hllStableEmptyWriter{err: io.ErrClosedPipe}),
		io.ErrClosedPipe)
	require.ErrorIs(t,
		stableEmptyHLLState(hllVersion)(&hllStableEmptyWriter{short: true}),
		io.ErrShortWrite)
}

func TestConfigureHLLFloatZeroStateOnlyChangesApproxCount(t *testing.T) {
	mp := mpool.MustNewZero()
	values := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(
		values, math.Copysign(0, -1), false, mp))

	approx := makeApproxCount(mp, AggIdOfApproxCountDistinct,
		types.T_float64.ToType()).(*approxCountExec)
	ConfigureHLLFloatZeroState(approx)
	require.NoError(t, approx.GroupGrow(1))
	require.NoError(t, approx.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
	require.Equal(t, hllFloatZeroVersion,
		approx.state[0].mobs[0].(*hllSketch).effectiveWireVersion())
	approx.Free()

	for _, makePersisted := range []func() AggFuncExec{
		func() AggFuncExec { return makeHllAdd(mp, AggIdOfHllAdd, types.T_float64.ToType()) },
		func() AggFuncExec {
			return makeHllMerge(mp, AggIdOfHllMerge, types.T_varbinary.ToType())
		},
	} {
		exec := makePersisted()
		ConfigureHLLFloatZeroState(exec)
		require.NoError(t, exec.GroupGrow(1))
		results, err := exec.Flush()
		require.NoError(t, err)
		expectedVersion := hllLegacyVersion
		if _, ok := exec.(*hllAddExec); ok {
			expectedVersion = hllVersion
		}
		require.Equal(t, expectedVersion, results[0].GetBytesAt(0)[0])
		results[0].Free(mp)
		exec.Free()
	}

	values.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestHllWireVersionValidation(t *testing.T) {
	require.Equal(t, hllVersion, (&hllSketch{}).effectiveWireVersion())
	sketch := &hllSketch{regs: make([]byte, hllRegisterCnt), wireVersion: hllVersion}
	require.Equal(t, hllVersion, sketch.effectiveWireVersion())
	require.ErrorContains(t, sketch.useWireVersion(1), "invalid HLL hash version")
}

func TestHllV4UsesCanonicalTypedValues(t *testing.T) {
	tests := []struct {
		name  string
		typ   types.Type
		left  []byte
		right []byte
	}{
		{
			name: "char-pad-space",
			typ:  types.New(types.T_char, 4, 0),
			left: []byte("a"), right: []byte("a "),
		},
		{
			name:  "json-numeric-encoding",
			typ:   types.T_json.ToType(),
			left:  mustHLLJSON(t, "[1,{\"n\":2.0}]"),
			right: mustHLLJSON(t, "[1.0,{\"n\":2}]"),
		},
		{
			name:  "vector-signed-zero",
			typ:   types.T_array_float32.ToType(),
			left:  types.ArrayToBytes([]float32{1, 0, 3}),
			right: types.ArrayToBytes([]float32{1, float32(math.Copysign(0, -1)), 3}),
		},
		{
			name:  "float32-nan-payload",
			typ:   types.T_float32.ToType(),
			left:  types.EncodeFixed(math.Float32frombits(0x7fc00000)),
			right: types.EncodeFixed(math.Float32frombits(0xffc00001)),
		},
		{
			name:  "float64-nan-payload",
			typ:   types.T_float64.ToType(),
			left:  types.EncodeFixed(math.Float64frombits(0x7ff8000000000000)),
			right: types.EncodeFixed(math.Float64frombits(0xfff8000000000001)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			leftMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			rightMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			left := leftMU.(*hllSketch)
			right := rightMU.(*hllSketch)
			insertHLLValue(left, tc.typ, tc.left)
			insertHLLValue(right, tc.typ, tc.right)
			require.Equal(t, left.regs, right.regs)
			left.Free()
			right.Free()
			require.Zero(t, mp.CurrNB())
		})
	}

	mp := mpool.MustNewZero()
	v3MU, err := makeHllSketchWithVersion(mp, nil, hllFloatZeroVersion)
	require.NoError(t, err)
	v4MU, err := makeHllSketchWithVersion(mp, nil, hllVersion)
	require.NoError(t, err)
	v3 := v3MU.(*hllSketch)
	v4 := v4MU.(*hllSketch)
	v3.Insert([]byte("a"))
	v4.Insert([]byte("a"))
	require.ErrorContains(t, v4.Merge(v3), "incompatible HLL hash versions")
	v3.Free()
	v4.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllAddUsesCanonicalTypedStateForCharAndJSON(t *testing.T) {
	tests := []struct {
		name  string
		typ   types.Type
		left  []byte
		right []byte
	}{
		{
			name:  "char-pad-space",
			typ:   types.New(types.T_char, 4, 0),
			left:  []byte("a"),
			right: []byte("a "),
		},
		{
			name:  "json-numeric-encoding",
			typ:   types.T_json.ToType(),
			left:  mustHLLJSON(t, "[1,{\"n\":2.0}]"),
			right: mustHLLJSON(t, "[1.0,{\"n\":2}]"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			run := func(valuesBytes ...[]byte) []byte {
				exec := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				require.False(t, exec.legacyWireState)
				require.NoError(t, exec.GroupGrow(1))
				values := vector.NewVec(tc.typ)
				for _, value := range valuesBytes {
					require.NoError(t, vector.AppendBytes(values, value, false, mp))
				}
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
				result, err := exec.Flush()
				require.NoError(t, err)
				encoded := bytes.Clone(result[0].GetBytesAt(0))
				require.Equal(t, hllVersion, encoded[0])
				result[0].Free(mp)
				values.Free(mp)
				exec.Free()
				return encoded
			}

			leftOnly := run(tc.left)
			rightOnly := run(tc.right)
			both := run(tc.left, tc.right)
			require.Equal(t, leftOnly, rightOnly)
			require.Equal(t, leftOnly, both)
			require.Zero(t, mp.CurrNB())
		})
	}

	// VARCHAR is the control: trailing spaces are significant in its SQL
	// equality domain and must not be folded by HLL_ADD_AGG.
	mp := mpool.MustNewZero()
	typ := types.New(types.T_varchar, 4, 0)
	run := func(value []byte) []byte {
		exec := makeHllAdd(mp, 1, typ).(*hllAddExec)
		require.True(t, exec.legacyWireState)
		require.NoError(t, exec.GroupGrow(1))
		values := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(values, value, false, mp))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
		result, err := exec.Flush()
		require.NoError(t, err)
		encoded := bytes.Clone(result[0].GetBytesAt(0))
		result[0].Free(mp)
		values.Free(mp)
		exec.Free()
		return encoded
	}
	require.NotEqual(t, run([]byte("a")), run([]byte("a ")))
	require.Zero(t, mp.CurrNB())
}

func TestHllAddRestoredLegacyStateKeepsRawHashDomain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.Type
		left  []byte
		right []byte
	}{
		{
			name:  "char",
			typ:   types.New(types.T_char, 4, 0),
			left:  []byte("a"),
			right: []byte("a "),
		},
		{
			name:  "json",
			typ:   types.T_json.ToType(),
			left:  mustHLLJSON(t, "1"),
			right: mustHLLJSON(t, "1.0"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			ConfigureHLLLegacyState(source)
			require.NoError(t, source.GroupGrow(1))
			left := vector.NewVec(tc.typ)
			require.NoError(t, vector.AppendBytes(left, tc.left, false, mp))
			require.NoError(t, source.BulkFill(0, []*vector.Vector{left}))
			var intermediate bytes.Buffer
			require.NoError(t, source.SaveIntermediateResult(
				1, [][]uint8{{1}}, &intermediate))

			restored := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			require.NoError(t, restored.UnmarshalFromReader(
				bytes.NewReader(intermediate.Bytes()), mp))
			right := vector.NewVec(tc.typ)
			require.NoError(t, vector.AppendBytes(right, tc.right, false, mp))
			require.NoError(t, restored.BulkFill(0, []*vector.Vector{right}))
			result, err := restored.Flush()
			require.NoError(t, err)
			require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])

			expected := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			ConfigureHLLLegacyState(expected)
			require.NoError(t, expected.GroupGrow(1))
			both := vector.NewVec(tc.typ)
			require.NoError(t, vector.AppendBytes(both, tc.left, false, mp))
			require.NoError(t, vector.AppendBytes(both, tc.right, false, mp))
			require.NoError(t, expected.BulkFill(0, []*vector.Vector{both}))
			expectedResult, err := expected.Flush()
			require.NoError(t, err)
			require.Equal(t, expectedResult[0].GetBytesAt(0),
				result[0].GetBytesAt(0))

			expectedResult[0].Free(mp)
			both.Free(mp)
			expected.Free()
			result[0].Free(mp)
			right.Free(mp)
			restored.Free()
			left.Free(mp)
			source.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func mustHLLJSON(t *testing.T, text string) []byte {
	t.Helper()
	value, err := types.ParseStringToByteJson(text)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(value)
	require.NoError(t, err)
	return encoded
}

func TestHllLegacyWireStateCoversEmptyAndFloatingPointWidths(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  types.Type
	}{
		{name: "float32", typ: types.T_float32.ToType()},
		{name: "float64", typ: types.T_float64.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			exec := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			ConfigureHLLLegacyState(exec)
			require.NoError(t, exec.GroupGrow(1))

			empty, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, hllLegacyVersion, empty[0].GetBytesAt(0)[0])
			empty[0].Free(mp)

			values := vector.NewVec(tc.typ)
			if tc.typ.Oid == types.T_float32 {
				require.NoError(t, vector.AppendFixedList(values,
					[]float32{0, float32(math.Copysign(0, -1))}, nil, mp))
			} else {
				require.NoError(t, vector.AppendFixedList(values,
					[]float64{0, math.Copysign(0, -1)}, nil, mp))
			}
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])
			expected := hll.NewNoSparse()
			for row := 0; row < values.Length(); row++ {
				expected.Insert(values.GetRawBytesAt(row))
			}
			expectedBytes, err := expected.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, expectedBytes[hllHeaderSize:],
				result[0].GetBytesAt(0)[hllHeaderSize:])
			legacyDecoder := hll.NewNoSparse()
			require.NoError(t, legacyDecoder.UnmarshalBinary(result[0].GetBytesAt(0)))
			require.Equal(t, uint64(2), legacyDecoder.Estimate())

			values.Free(mp)
			result[0].Free(mp)
			exec.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestHllAddVectorCanonicalizesWithVersionedWireState(t *testing.T) {
	tests := []struct {
		name  string
		typ   types.Type
		left  []byte
		right []byte
	}{
		{
			name:  "float32",
			typ:   types.T_array_float32.ToType(),
			left:  types.ArrayToBytes([]float32{1, 0, 3}),
			right: types.ArrayToBytes([]float32{1, float32(math.Copysign(0, -1)), 3}),
		},
		{
			name:  "float64",
			typ:   types.T_array_float64.ToType(),
			left:  types.ArrayToBytes([]float64{1, 0, 3}),
			right: types.ArrayToBytes([]float64{1, math.Copysign(0, -1), 3}),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			exec := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			require.False(t, exec.legacyWireState)
			require.NoError(t, exec.GroupGrow(1))
			runOne := func(value []byte) []byte {
				one := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				require.NoError(t, one.GroupGrow(1))
				input := vector.NewVec(tc.typ)
				require.NoError(t, vector.AppendBytes(input, value, false, mp))
				require.NoError(t, one.BulkFill(0, []*vector.Vector{input}))
				oneResult, err := one.Flush()
				require.NoError(t, err)
				data := bytes.Clone(oneResult[0].GetBytesAt(0))
				oneResult[0].Free(mp)
				input.Free(mp)
				one.Free()
				return data
			}
			leftOnly := runOne(tc.left)
			rightOnly := runOne(tc.right)
			require.Equal(t, leftOnly, rightOnly)

			partialLeft := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			partialRight := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			require.NoError(t, partialLeft.GroupGrow(1))
			require.NoError(t, partialRight.GroupGrow(1))
			leftValues := vector.NewVec(tc.typ)
			rightValues := vector.NewVec(tc.typ)
			require.NoError(t, vector.AppendBytes(leftValues, tc.left, false, mp))
			require.NoError(t, vector.AppendBytes(rightValues, tc.right, false, mp))
			require.NoError(t, partialLeft.BulkFill(0, []*vector.Vector{leftValues}))
			require.NoError(t, partialRight.BulkFill(0, []*vector.Vector{rightValues}))
			require.NoError(t, partialLeft.BatchMerge(
				partialRight, 0, []uint64{1}))
			partialResult, err := partialLeft.Flush()
			require.NoError(t, err)
			require.Equal(t, leftOnly, partialResult[0].GetBytesAt(0))
			partialResult[0].Free(mp)
			leftValues.Free(mp)
			rightValues.Free(mp)
			partialLeft.Free()
			partialRight.Free()

			values := vector.NewVec(tc.typ)
			require.NoError(t, vector.AppendBytes(values, tc.left, false, mp))
			require.NoError(t, vector.AppendBytes(values, tc.right, false, mp))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
			result, err := exec.Flush()
			require.NoError(t, err)
			encoded := result[0].GetBytesAt(0)
			require.Equal(t, hllVersion, encoded[0])

			require.Equal(t, leftOnly, encoded)

			// Canonicalization is scratch-only; the input vector must retain the
			// original representative bytes for downstream consumers.
			require.Equal(t, tc.left, values.GetBytesAt(0))
			require.Equal(t, tc.right, values.GetBytesAt(1))

			result[0].Free(mp)
			values.Free(mp)
			exec.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestHllAddVectorRestoresVersionedHashDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_array_float32.ToType()
	legacy := makeHllAdd(mp, 1, typ).(*hllAddExec)
	ConfigureHLLLegacyState(legacy)
	require.NoError(t, legacy.GroupGrow(1))
	oldValue := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(oldValue,
		types.ArrayToBytes([]float32{1, float32(math.Copysign(0, -1)), 3}), false, mp))
	require.NoError(t, legacy.BulkFill(0, []*vector.Vector{oldValue}))
	var intermediate bytes.Buffer
	require.NoError(t, legacy.SaveIntermediateResult(
		1, [][]uint8{{1}}, &intermediate))
	newValue := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(newValue,
		types.ArrayToBytes([]float32{1, 0, 3}), false, mp))
	// The legacy producer remains the reference for an old v2 state: appending
	// +0 hashes it as a distinct raw value from the existing -0.
	require.NoError(t, legacy.BulkFill(0, []*vector.Vector{newValue}))
	legacyResult, err := legacy.Flush()
	require.NoError(t, err)
	expected := bytes.Clone(legacyResult[0].GetBytesAt(0))
	require.Equal(t, hllLegacyVersion, expected[0])
	legacyResult[0].Free(mp)

	restored := makeHllAdd(mp, 1, typ).(*hllAddExec)
	require.NoError(t, restored.UnmarshalFromReader(
		bytes.NewReader(intermediate.Bytes()), mp))
	// Unmarshalling preserves the v2 marker. The upgraded executor therefore
	// keeps raw hashing for this old state instead of silently switching it to
	// the new v4 vector domain.
	require.NoError(t, restored.BulkFill(0, []*vector.Vector{newValue}))
	result, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])
	require.Equal(t, expected, result[0].GetBytesAt(0))

	result[0].Free(mp)
	newValue.Free(mp)
	oldValue.Free(mp)
	restored.Free()
	legacy.Free()
	require.Zero(t, mp.CurrNB())
}

func TestAccountedHllAddVectorReservesCanonicalScratch(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := makeHllAdd(mp, 1, types.T_array_float32.ToType()).(*hllAddExec)
	owner := any(exec).(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1))

	values := vector.NewVec(types.T_array_float32.ToType())
	require.NoError(t, vector.AppendBytes(values,
		types.ArrayToBytes([]float32{1, float32(math.Copysign(0, -1)), 3}), false, mp))
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{values}))
	require.NotNil(t, exec.state[0].argScratch)
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{values}))

	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, hllVersion, result[0].GetBytesAt(0)[0])
	result[0].Free(mp)
	values.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestHllLegacyWireStateUsesPreflightAndRoundTripsIntermediate(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  types.Type
	}{
		{name: "float32", typ: types.T_float32.ToType()},
		{name: "float64", typ: types.T_float64.ToType()},
	} {
		for _, accounted := range []bool{false, true} {
			mode := "lazy"
			if accounted {
				mode = "accounted-preflight"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				mp := mpool.MustNewZero()
				var registry *mpool.AllocationAccountRegistry
				var account *mpool.AllocationAccount
				var allocation *AllocationAccount
				if accounted {
					registry, account, allocation = newTestAggregateAllocation(t)
				}
				source := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				ConfigureHLLLegacyState(source)
				sourceOwner := any(source).(AllocationAccountOwner)
				if accounted {
					require.NoError(t, sourceOwner.SetAllocationAccount(allocation))
				}
				require.NoError(t, source.GroupGrow(1))

				values := vector.NewVec(tc.typ)
				if tc.typ.Oid == types.T_float32 {
					require.NoError(t, vector.AppendFixed(values,
						float32(math.Copysign(0, -1)), false, mp))
				} else {
					require.NoError(t, vector.AppendFixed(values,
						math.Copysign(0, -1), false, mp))
				}
				if accounted {
					require.NoError(t, source.PreflightBatchFill(
						0, []uint64{1}, []*vector.Vector{values}))
				}
				require.NoError(t, source.BulkFill(0, []*vector.Vector{values}))

				var intermediate bytes.Buffer
				require.NoError(t, source.SaveIntermediateResult(
					1, [][]uint8{{1}}, &intermediate))
				restored := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				ConfigureHLLLegacyState(restored)
				require.NoError(t, restored.UnmarshalFromReader(
					bytes.NewReader(intermediate.Bytes()), mp))
				result, err := restored.Flush()
				require.NoError(t, err)
				require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])
				expected := hll.NewNoSparse()
				expected.Insert(values.GetRawBytesAt(0))
				expectedBytes, err := expected.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, expectedBytes[hllHeaderSize:],
					result[0].GetBytesAt(0)[hllHeaderSize:])
				legacyDecoder := hll.NewNoSparse()
				require.NoError(t, legacyDecoder.UnmarshalBinary(
					result[0].GetBytesAt(0)))

				emptySource := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				ConfigureHLLLegacyState(emptySource)
				emptyOwner := any(emptySource).(AllocationAccountOwner)
				if accounted {
					require.NoError(t, emptyOwner.SetAllocationAccount(allocation))
				}
				require.NoError(t, emptySource.GroupGrow(1))
				var emptyIntermediate bytes.Buffer
				require.NoError(t, emptySource.SaveIntermediateResult(
					1, [][]uint8{{1}}, &emptyIntermediate))
				emptyRestored := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
				ConfigureHLLLegacyState(emptyRestored)
				require.NoError(t, emptyRestored.UnmarshalFromReader(
					bytes.NewReader(emptyIntermediate.Bytes()), mp))
				emptyResult, err := emptyRestored.Flush()
				require.NoError(t, err)
				require.Equal(t, hllLegacyVersion, emptyResult[0].GetBytesAt(0)[0])
				expectedEmpty := bytes.Clone(canonicalEmptyHLL[:])
				expectedEmpty[0] = hllLegacyVersion
				require.Equal(t, expectedEmpty, emptyResult[0].GetBytesAt(0))

				emptyResult[0].Free(mp)
				emptyRestored.Free()
				emptySource.Free()
				result[0].Free(mp)
				restored.Free()
				values.Free(mp)
				source.Free()
				if accounted {
					require.NoError(t, emptyOwner.ClearAllocationAccount(allocation))
					require.NoError(t, sourceOwner.ClearAllocationAccount(allocation))
					finishTestAggregateAllocation(t, registry, account)
				}
				require.Zero(t, mp.CurrNB())
			})
		}
	}
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

	emptyDestinationMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	emptyDestination := emptyDestinationMU.(*hllSketch)
	emptyWant := bytes.Clone(emptyDestination.regs)
	require.Error(t, emptyDestination.mergeBytes(malformed))
	require.Equal(t, emptyWant, emptyDestination.regs)
	require.False(t, emptyDestination.hasValue)
	require.Equal(t, hllVersion, emptyDestination.effectiveWireVersion())
	emptyDestination.Free()
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

func TestHllFloatSignedZeroUsesOneSQLValue(t *testing.T) {
	mp := mpool.MustNewZero()
	values := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(values,
		[]float64{0, math.Copysign(0, -1)}, nil, mp))

	approx := makeApproxCount(mp, 1, types.T_float64.ToType()).(*approxCountExec)
	require.NoError(t, approx.GroupGrow(1))
	require.NoError(t, approx.BatchFill(0, []uint64{1, 1}, []*vector.Vector{values}))
	approxResult, err := approx.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](approxResult[0], 0))

	values.Free(mp)
	approxResult[0].Free(mp)
	approx.Free()

	float32Values := vector.NewVec(types.T_float32.ToType())
	require.NoError(t, vector.AppendFixedList(float32Values,
		[]float32{0, float32(math.Copysign(0, -1))}, nil, mp))
	float32Approx := makeApproxCount(mp, 1, types.T_float32.ToType()).(*approxCountExec)
	require.NoError(t, float32Approx.GroupGrow(1))
	require.NoError(t, float32Approx.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{float32Values}))
	float32Result, err := float32Approx.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](float32Result[0], 0))
	float32Values.Free(mp)
	float32Result[0].Free(mp)
	float32Approx.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllAddFloatSignedZeroUsesCanonicalState(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  types.Type
		data any
	}{
		{name: "float32", typ: types.T_float32.ToType(), data: []float32{0, float32(math.Copysign(0, -1))}},
		{name: "float64", typ: types.T_float64.ToType(), data: []float64{0, math.Copysign(0, -1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			exec := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			require.False(t, exec.legacyWireState)
			require.NoError(t, exec.GroupGrow(1))
			values := vector.NewVec(tc.typ)
			switch data := tc.data.(type) {
			case []float32:
				require.NoError(t, vector.AppendFixedList(values, data, nil, mp))
			case []float64:
				require.NoError(t, vector.AppendFixedList(values, data, nil, mp))
			}
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
			result, err := exec.Flush()
			require.NoError(t, err)
			encoded := result[0].GetBytesAt(0)
			require.Equal(t, hllVersion, encoded[0])
			restoredMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			restored := restoredMU.(*hllSketch)
			require.NoError(t, restored.UnmarshalBinary(encoded))
			require.Equal(t, uint64(1), restored.Estimate())
			values.Free(mp)
			result[0].Free(mp)
			restored.Free()
			exec.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestHllAddRestoredLegacyFloatStateKeepsRawHashDomain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.Type
		left  any
		right any
	}{
		{name: "float32", typ: types.T_float32.ToType(), left: float32(0), right: float32(math.Copysign(0, -1))},
		{name: "float64", typ: types.T_float64.ToType(), left: float64(0), right: math.Copysign(0, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			ConfigureHLLLegacyState(source)
			require.NoError(t, source.GroupGrow(1))
			left := vector.NewVec(tc.typ)
			switch value := tc.left.(type) {
			case float32:
				require.NoError(t, vector.AppendFixed(left, value, false, mp))
			case float64:
				require.NoError(t, vector.AppendFixed(left, value, false, mp))
			}
			require.NoError(t, source.BulkFill(0, []*vector.Vector{left}))
			var intermediate bytes.Buffer
			require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &intermediate))

			restored := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(intermediate.Bytes()), mp))
			right := vector.NewVec(tc.typ)
			switch value := tc.right.(type) {
			case float32:
				require.NoError(t, vector.AppendFixed(right, value, false, mp))
			case float64:
				require.NoError(t, vector.AppendFixed(right, value, false, mp))
			}
			require.NoError(t, restored.BulkFill(0, []*vector.Vector{right}))
			result, err := restored.Flush()
			require.NoError(t, err)
			require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])

			expected := makeHllAdd(mp, 1, tc.typ).(*hllAddExec)
			ConfigureHLLLegacyState(expected)
			require.NoError(t, expected.GroupGrow(1))
			both := vector.NewVec(tc.typ)
			switch leftValue := tc.left.(type) {
			case float32:
				rightValue := tc.right.(float32)
				require.NoError(t, vector.AppendFixed(both, leftValue, false, mp))
				require.NoError(t, vector.AppendFixed(both, rightValue, false, mp))
			case float64:
				rightValue := tc.right.(float64)
				require.NoError(t, vector.AppendFixed(both, leftValue, false, mp))
				require.NoError(t, vector.AppendFixed(both, rightValue, false, mp))
			}
			require.NoError(t, expected.BulkFill(0, []*vector.Vector{both}))
			expectedResult, err := expected.Flush()
			require.NoError(t, err)
			require.Equal(t, expectedResult[0].GetBytesAt(0), result[0].GetBytesAt(0))

			expectedResult[0].Free(mp)
			both.Free(mp)
			expected.Free()
			result[0].Free(mp)
			right.Free(mp)
			restored.Free()
			left.Free(mp)
			source.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestHllMergeAcceptsNewCanonicalFloatState(t *testing.T) {
	mp := mpool.MustNewZero()
	left := makeHllAdd(mp, 1, types.T_float64.ToType()).(*hllAddExec)
	require.NoError(t, left.GroupGrow(1))
	leftValues := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(leftValues, float64(0), false, mp))
	require.NoError(t, left.BulkFill(0, []*vector.Vector{leftValues}))
	leftEncoded, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, hllVersion, leftEncoded[0].GetBytesAt(0)[0])

	right := makeHllAdd(mp, 1, types.T_float64.ToType()).(*hllAddExec)
	require.NoError(t, right.GroupGrow(1))
	rightValues := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(
		rightValues, math.Copysign(0, -1), false, mp))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{rightValues}))
	rightEncoded, err := right.Flush()
	require.NoError(t, err)
	require.Equal(t, hllVersion, rightEncoded[0].GetBytesAt(0)[0])

	merge := makeHllMerge(mp, 1, types.T_varbinary.ToType()).(*hllMergeExec)
	require.NoError(t, merge.GroupGrow(1))
	input := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(input, leftEncoded[0].GetBytesAt(0), false, mp))
	require.NoError(t, vector.AppendBytes(input, rightEncoded[0].GetBytesAt(0), false, mp))
	require.NoError(t, merge.BulkFill(0, []*vector.Vector{input}))
	merged, err := merge.Flush()
	require.NoError(t, err)
	require.Equal(t, hllVersion, merged[0].GetBytesAt(0)[0])
	mergedSketchMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	mergedSketch := mergedSketchMU.(*hllSketch)
	require.NoError(t, mergedSketch.UnmarshalBinary(merged[0].GetBytesAt(0)))
	require.Equal(t, uint64(1), mergedSketch.Estimate())

	leftValues.Free(mp)
	leftEncoded[0].Free(mp)
	left.Free()
	rightValues.Free(mp)
	rightEncoded[0].Free(mp)
	right.Free()
	input.Free(mp)
	merged[0].Free(mp)
	mergedSketch.Free()
	merge.Free()
	require.Zero(t, mp.CurrNB())
}

func TestHllFloatSignedZeroHashVersionsCannotBeMerged(t *testing.T) {
	tests := []struct {
		name       string
		typ        types.Type
		legacyZero []byte
		newZero    []byte
	}{
		{
			name:       "float32",
			typ:        types.T_float32.ToType(),
			legacyZero: types.EncodeFixed(float32(math.Copysign(0, -1))),
			newZero:    types.EncodeFixed(float32(0)),
		},
		{
			name:       "float64",
			typ:        types.T_float64.ToType(),
			legacyZero: types.EncodeFixed(math.Copysign(0, -1)),
			newZero:    types.EncodeFixed(float64(0)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			legacy := hll.NewNoSparse()
			legacy.Insert(tc.legacyZero)
			legacyBytes, err := legacy.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, hllLegacyVersion, legacyBytes[0])

			currentMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			current := currentMU.(*hllSketch)
			defer func() {
				current.Free()
				require.Zero(t, mp.CurrNB())
			}()
			insertHLLValue(current, tc.typ, tc.newZero)
			currentBytes, err := current.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, hllVersion, currentBytes[0])
			require.NotEqual(t, legacyBytes[hllHeaderSize:], currentBytes[hllHeaderSize:])
			require.Equal(t, uint64(1), current.Estimate())

			legacyRestoredMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			legacyRestored := legacyRestoredMU.(*hllSketch)
			defer legacyRestored.Free()
			require.NoError(t, legacyRestored.UnmarshalBinary(legacyBytes))
			require.Equal(t, uint64(1), legacyRestored.Estimate())

			currentRestoredMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			currentRestored := currentRestoredMU.(*hllSketch)
			defer currentRestored.Free()
			require.NoError(t, currentRestored.UnmarshalBinary(currentBytes))
			require.Equal(t, uint64(1), currentRestored.Estimate())

			legacyRegs := bytes.Clone(legacyRestored.regs)
			require.ErrorContains(t, legacyRestored.Merge(currentRestored), "incompatible HLL hash versions")
			require.Equal(t, legacyRegs, legacyRestored.regs)
			currentRegs := bytes.Clone(currentRestored.regs)
			require.ErrorContains(t, currentRestored.Merge(legacyRestored), "incompatible HLL hash versions")
			require.Equal(t, currentRegs, currentRestored.regs)

			legacyDestinationMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			legacyDestination := legacyDestinationMU.(*hllSketch)
			defer legacyDestination.Free()
			require.NoError(t, legacyDestination.mergeBytes(legacyBytes))
			require.ErrorContains(t, legacyDestination.mergeBytes(currentBytes), "incompatible HLL hash versions")

			currentDestinationMU, err := makeHllSketch(mp, nil)
			require.NoError(t, err)
			currentDestination := currentDestinationMU.(*hllSketch)
			defer currentDestination.Free()
			require.NoError(t, currentDestination.mergeBytes(currentBytes))
			require.ErrorContains(t, currentDestination.mergeBytes(legacyBytes), "incompatible HLL hash versions")
		})
	}
}

func TestHllMergePreservesLegacyWireVersion(t *testing.T) {
	mp := mpool.MustNewZero()
	legacy := hll.NewNoSparse()
	value := types.EncodeFixed(math.Copysign(0, -1))
	legacy.Insert(value)
	legacyBytes, err := legacy.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, hllLegacyVersion, legacyBytes[0])

	exec := makeHllMerge(mp, 1, types.T_varbinary.ToType()).(*hllMergeExec)
	values := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(values, legacyBytes, false, mp))
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])

	restoredMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	restored := restoredMU.(*hllSketch)
	require.NoError(t, restored.UnmarshalBinary(result[0].GetBytesAt(0)))
	require.Equal(t, uint64(1), restored.Estimate())

	values.Free(mp)
	result[0].Free(mp)
	restored.Free()
	exec.Free()
	require.Zero(t, mp.CurrNB())
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
	require.Equal(t, hllLegacyVersion, vecs[0].GetBytesAt(0)[0])
	require.Equal(t, hllLegacyVersion, vecs[0].GetBytesAt(1)[0])

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

func TestHllAddPersistedLegacyStateSurvivesUpgradeAppendAndMerge(t *testing.T) {
	mp := mpool.MustNewZero()

	buildLegacyState := func(values ...int64) []byte {
		// hll.NewNoSparse is the independent pre-upgrade producer. Keeping this
		// state outside makeHllAdd prevents the regression from proving only that
		// two current producers agree with each other.
		legacy := hll.NewNoSparse()
		for _, value := range values {
			legacy.Insert(types.EncodeInt64(&value))
		}
		data, err := legacy.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, hllLegacyVersion, data[0])
		return data
	}

	buildAddState := func(values ...int64) []byte {
		exec := makeHllAdd(mp, 1, types.T_int64.ToType()).(*hllAddExec)
		require.NoError(t, exec.GroupGrow(1))
		input := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input, values, nil, mp))
		groups := slices.Repeat([]uint64{1}, len(values))
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
		result, err := exec.Flush()
		require.NoError(t, err)
		data := bytes.Clone(result[0].GetBytesAt(0))
		require.Equal(t, hllLegacyVersion, data[0])
		result[0].Free(mp)
		input.Free(mp)
		exec.Free()
		return data
	}

	baseState := buildLegacyState(1, 2)
	appendedState := buildAddState(3)

	merge := makeHllMerge(mp, 1, types.T_varbinary.ToType()).(*hllMergeExec)
	require.NoError(t, merge.GroupGrow(1))
	states := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(states, baseState, false, mp))
	require.NoError(t, vector.AppendBytes(states, appendedState, false, mp))
	require.NoError(t, merge.BatchFill(0, []uint64{1, 1}, []*vector.Vector{states}))
	result, err := merge.Flush()
	require.NoError(t, err)
	require.Equal(t, hllLegacyVersion, result[0].GetBytesAt(0)[0])

	restoredMU, err := makeHllSketch(mp, nil)
	require.NoError(t, err)
	restored := restoredMU.(*hllSketch)
	require.NoError(t, restored.UnmarshalBinary(result[0].GetBytesAt(0)))
	require.Equal(t, uint64(3), restored.Estimate())

	result[0].Free(mp)
	states.Free(mp)
	restored.Free()
	merge.Free()
	require.Zero(t, mp.CurrNB())
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
