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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCanonicalDistinctKeyWireCanUseLegacyPeerFormat(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	agg, err := MakeAgg(mp, AggIdOfCountColumn, true, types.T_varchar.ToType())
	require.NoError(t, err)
	defer agg.Free()
	require.True(t, RequiresCanonicalDistinctKeyWire(agg))

	SetCanonicalDistinctKeyWire(agg, false)
	require.False(t, RequiresCanonicalDistinctKeyWire(agg))
	SetCanonicalDistinctKeyWire(agg, true)
	require.True(t, RequiresCanonicalDistinctKeyWire(agg))
}

func TestLegacyDistinctWireKeepsRawRepresentative(t *testing.T) {
	encodeLegacyState := func(t *testing.T, mp *mpool.MPool, exec *countColumnExec, payload []byte) {
		t.Helper()
		var state bytes.Buffer
		require.NoError(t, types.WriteInt32(&state, 1))
		require.NoError(t, types.WriteUint32(&state, 1))
		require.NoError(t, types.WriteInt32(&state, int32(len(payload))))
		_, err := state.Write(payload)
		require.NoError(t, err)
		_, err = exec.state[0].readState(mp, &state, &exec.aggInfo)
		require.NoError(t, err)
	}

	encodeTuple := func(left, right []byte) []byte {
		payload := make([]byte, 8+len(left)+len(right))
		binary.BigEndian.PutUint32(payload, uint32(len(left)))
		copy(payload[4:], left)
		offset := 4 + len(left)
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(right)))
		copy(payload[offset+4:], right)
		return payload
	}

	t.Run("json", func(t *testing.T) {
		mp := mpool.MustNewZero()
		json, err := types.ParseStringToByteJson("1")
		require.NoError(t, err)
		raw, err := types.EncodeJson(json)
		require.NoError(t, err)
		values := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(values, raw, false, mp))

		source := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
		SetCanonicalDistinctKeyWire(source, false)
		var encoded bytes.Buffer
		require.NoError(t, source.SaveIntermediateResultOfChunk(0, &encoded))

		legacy := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		require.NoError(t, legacy.GroupGrow(1))
		SetCanonicalDistinctKeyWire(legacy, false)
		encodeLegacyState(t, mp, legacy, raw)

		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		SetCanonicalDistinctKeyWire(target, false)
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
		require.NoError(t, target.BatchMerge(legacy, 0, []uint64{1}))
		result, err := target.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))

		result[0].Free(mp)
		target.Free()
		legacy.Free()
		source.Free()
		values.Free(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("multi-column", func(t *testing.T) {
		mp := mpool.MustNewZero()
		leftType := types.New(types.T_char, 2, 0)
		rightType := types.New(types.T_char, 2, 0)
		left := vector.NewVec(leftType)
		right := vector.NewVec(rightType)
		require.NoError(t, vector.AppendBytes(left, []byte("a "), false, mp))
		require.NoError(t, vector.AppendBytes(right, []byte("b"), false, mp))
		raw := encodeTuple([]byte("a "), []byte("b"))

		source := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{leftType, rightType},
		).(*countColumnExec)
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BatchFill(0, []uint64{1}, []*vector.Vector{left, right}))
		SetCanonicalDistinctKeyWire(source, false)
		var encoded bytes.Buffer
		require.NoError(t, source.SaveIntermediateResultOfChunk(0, &encoded))

		legacy := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{leftType, rightType},
		).(*countColumnExec)
		require.NoError(t, legacy.GroupGrow(1))
		SetCanonicalDistinctKeyWire(legacy, false)
		encodeLegacyState(t, mp, legacy, raw)

		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{leftType, rightType},
		).(*countColumnExec)
		SetCanonicalDistinctKeyWire(target, false)
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
		require.NoError(t, target.BatchMerge(legacy, 0, []uint64{1}))
		result, err := target.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))

		result[0].Free(mp)
		target.Free()
		legacy.Free()
		source.Free()
		left.Free(mp)
		right.Free(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("spill-round-trip", func(t *testing.T) {
		mp := mpool.MustNewZero()
		json, err := types.ParseStringToByteJson("1")
		require.NoError(t, err)
		raw, err := types.EncodeJson(json)
		require.NoError(t, err)
		values := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(values, raw, false, mp))

		source := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
		SetCanonicalDistinctKeyWire(source, false)
		var spill bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(0, []int32{0}, &spill))

		restored := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		SetCanonicalDistinctKeyWire(restored, false)
		require.NoError(t, restored.UnmarshalSpillFromReader(
			bytes.NewReader(spill.Bytes()), mp))
		var downgraded bytes.Buffer
		require.NoError(t, restored.SaveIntermediateResultOfChunk(0, &downgraded))

		legacy := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		require.NoError(t, legacy.GroupGrow(1))
		SetCanonicalDistinctKeyWire(legacy, false)
		encodeLegacyState(t, mp, legacy, raw)

		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_json.ToType()},
		).(*countColumnExec)
		SetCanonicalDistinctKeyWire(target, false)
		require.NoError(t, target.UnmarshalFromReader(
			bytes.NewReader(downgraded.Bytes()), mp))
		require.NoError(t, target.BatchMerge(legacy, 0, []uint64{1}))
		result, err := target.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))

		result[0].Free(mp)
		target.Free()
		legacy.Free()
		restored.Free()
		source.Free()
		values.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
}

func TestLegacyDistinctRestoreCanonicalizesBeforeAppendAndMerge(t *testing.T) {
	mustJSON := func(text string) []byte {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		raw, err := types.EncodeJson(value)
		require.NoError(t, err)
		return raw
	}
	encodeTuple := func(values ...[]byte) []byte {
		size := 4 * len(values)
		for _, value := range values {
			size += len(value)
		}
		payload := make([]byte, size)
		offset := 0
		for _, value := range values {
			binary.BigEndian.PutUint32(payload[offset:], uint32(len(value)))
			offset += 4
			copy(payload[offset:], value)
			offset += len(value)
		}
		return payload
	}
	encodeLegacyState := func(t *testing.T, exec *countColumnExec, payload []byte) {
		t.Helper()
		var state bytes.Buffer
		require.NoError(t, types.WriteInt32(&state, 1))
		require.NoError(t, types.WriteUint32(&state, 1))
		require.NoError(t, types.WriteInt32(&state, int32(len(payload))))
		_, err := state.Write(payload)
		require.NoError(t, err)
		_, err = exec.state[0].readState(exec.mp, &state, &exec.aggInfo)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		argTypes   []types.Type
		legacy     []byte
		newVectors func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name:     "char-pad-space",
			argTypes: []types.Type{types.New(types.T_char, 2, 0)},
			legacy:   []byte("a "),
			newVectors: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := vector.NewVec(types.New(types.T_char, 2, 0))
				require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, mp))
				return []*vector.Vector{vec}
			},
		},
		{
			name:     "json-numeric-encoding",
			argTypes: []types.Type{types.T_json.ToType()},
			legacy:   mustJSON("1"),
			newVectors: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				value, err := types.ParseStringToByteJson("1.0")
				require.NoError(t, err)
				raw, err := types.EncodeJson(value)
				require.NoError(t, err)
				vec := vector.NewVec(types.T_json.ToType())
				require.NoError(t, vector.AppendBytes(vec, raw, false, mp))
				return []*vector.Vector{vec}
			},
		},
		{
			name: "tuple-float64-nan",
			argTypes: []types.Type{
				types.T_float64.ToType(),
				types.New(types.T_char, 2, 0),
			},
			legacy: encodeTuple(
				types.EncodeFixed(math.Float64frombits(0x7ff8000000000001)),
				[]byte("a"),
			),
			newVectors: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				floatVec := vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixed(
					floatVec,
					math.Float64frombits(0xfff8000000000001),
					false,
					mp,
				))
				charVec := vector.NewVec(types.New(types.T_char, 2, 0))
				require.NoError(t, vector.AppendBytes(charVec, []byte("a"), false, mp))
				return []*vector.Vector{floatVec, charVec}
			},
		},
	}

	for _, tc := range tests {
		for _, action := range []string{"append", "merge"} {
			t.Run(tc.name+"/"+action, func(t *testing.T) {
				mp := mpool.MustNewZero()
				target := newCountColumnExec(
					mp, AggIdOfCountColumn, true, tc.argTypes,
				).(*countColumnExec)
				require.NoError(t, target.GroupGrow(1))
				SetCanonicalDistinctKeyWire(target, false)
				encodeLegacyState(t, target, tc.legacy)

				values := tc.newVectors(t, mp)
				groups := []uint64{1}
				if action == "append" {
					require.NoError(t, target.BatchFill(0, groups, values))
				} else {
					source := newCountColumnExec(
						mp, AggIdOfCountColumn, true, tc.argTypes,
					).(*countColumnExec)
					require.NoError(t, source.GroupGrow(1))
					require.NoError(t, source.BatchFill(0, groups, values))
					require.NoError(t, target.BatchMerge(source, 0, groups))
					source.Free()
				}
				for _, value := range values {
					value.Free(mp)
				}

				result, err := target.Flush()
				require.NoError(t, err)
				require.Equal(t, int64(1),
					vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
				result[0].Free(mp)
				target.Free()
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestCanonicalDistinctSpillRestorePreservesMembershipKey(t *testing.T) {
	mustJSON := func(t *testing.T, mp *mpool.MPool) *vector.Vector {
		t.Helper()
		value, err := types.ParseStringToByteJson("1")
		require.NoError(t, err)
		raw, err := types.EncodeJson(value)
		require.NoError(t, err)
		vec := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(vec, raw, false, mp))
		return vec
	}

	tests := []struct {
		name      string
		argTypes  []types.Type
		newValues func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name:     "json",
			argTypes: []types.Type{types.T_json.ToType()},
			newValues: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{mustJSON(t, mp)}
			},
		},
		{
			name: "float-json-tuple",
			argTypes: []types.Type{
				types.T_float64.ToType(),
				types.T_json.ToType(),
			},
			newValues: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				floatVec := vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixed(
					floatVec,
					math.Float64frombits(0x7ff8000000000001),
					false,
					mp,
				))
				return []*vector.Vector{floatVec, mustJSON(t, mp)}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := tc.newValues(t, mp)
			duplicateValues := tc.newValues(t, mp)
			source := newCountColumnExec(
				mp, AggIdOfCountColumn, true, tc.argTypes,
			).(*countColumnExec)
			decoded := newCountColumnExec(
				mp, AggIdOfCountColumn, true, tc.argTypes,
			).(*countColumnExec)
			restored := newCountColumnExec(
				mp, AggIdOfCountColumn, true, tc.argTypes,
			).(*countColumnExec)
			defer func() {
				source.Free()
				decoded.Free()
				restored.Free()
				for _, value := range values {
					value.Free(mp)
				}
				for _, value := range duplicateValues {
					value.Free(mp)
				}
				require.Zero(t, mp.CurrNB())
			}()

			require.NoError(t, source.GroupGrow(1))
			require.NoError(t, source.BatchFill(
				0, []uint64{1}, values))
			SetCanonicalDistinctKeyWire(source, true)
			var canonical bytes.Buffer
			require.NoError(t, source.SaveIntermediateResultOfChunk(0, &canonical))

			// Cross a canonical partial-result boundary before creating the
			// private spill record. This is the sequence used by distributed
			// aggregate recovery, not just an in-memory spill of the producer.
			SetCanonicalDistinctKeyWire(decoded, true)
			require.NoError(t, decoded.UnmarshalFromReader(
				bytes.NewReader(canonical.Bytes()), mp))
			var spill bytes.Buffer
			require.NoError(t, decoded.SaveSpillIntermediateRows(
				0, []int32{0}, &spill))

			SetCanonicalDistinctKeyWire(restored, true)
			require.NoError(t, restored.UnmarshalSpillFromReader(
				bytes.NewReader(spill.Bytes()), mp))
			require.NoError(t, restored.BatchFill(
				0, []uint64{1}, duplicateValues))
			result, err := restored.Flush()
			require.NoError(t, err)
			require.Equal(t, int64(1),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			result[0].Free(mp)
		})
	}
}

func TestGroupConcatIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}),
		emptyNull: true,
	}

	left := vector.NewVec(types.T_varchar.ToType())
	right := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendBytes(left, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(left, []byte("b"), false, mp))
	require.NoError(t, vector.AppendFixedList(right, []int64{1, 2}, nil, mp))
	defer left.Free(mp)
	defer right.Free(mp)

	exec := newGroupConcatExec(mp, info, ",")
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{left, right}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newGroupConcatExec(mp, info, ",")
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	results, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "a1,b2", string(results[0].GetBytesAt(0)))
	results[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestGroupConcatLegacyIntermediateWireRemainsReadable(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	values := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(values, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte("b"), false, mp))
	defer values.Free(mp)

	writer := newGroupConcatExec(mp, info, "|")
	require.NoError(t, writer.GroupGrow(1))
	require.NoError(t, writer.BatchFill(
		0, []uint64{1, 1}, []*vector.Vector{values}))
	SetGroupConcatSourceRowWire(writer, false)
	SetGroupConcatSourceRowProvenanceWire(writer, false)
	var encoded bytes.Buffer
	require.NoError(t, writer.SaveIntermediateResult(1, [][]uint8{{1}}, &encoded))
	require.NotContains(t, encoded.Bytes(), groupConcatSourcePayloadMagic)

	reader := newGroupConcatExec(mp, info, "|")
	SetGroupConcatSourceRowWire(reader, false)
	require.NoError(t, reader.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	require.False(t, GroupConcatSourceRowsTrusted(reader))
	results, err := reader.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "a|b", string(results[0].GetBytesAt(0)))
	results[0].Free(mp)
	writer.Free()
	reader.Free()
}

func TestAccountedDistinctGroupConcatLegacyIntermediateUsesSingleFallbackOrdinal(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	newExec := func() *groupConcatExec {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("|", 4), 0))
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
		require.NoError(t, exec.GroupGrow(1))
		return exec
	}
	source := newExec()
	target := newExec()
	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"aa", "bbb"})
	groups := []uint64{1, 1}
	defer func() {
		values.Free(mp)
		source.Free()
		target.Free()
		require.NoError(t, source.ClearAllocationAccount(allocation))
		require.NoError(t, target.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	}()

	require.NoError(t, source.PreflightBatchFill(0, groups, []*vector.Vector{values}))
	require.NoError(t, source.BatchFill(0, groups, []*vector.Vector{values}))
	SetGroupConcatSourceRowWire(source, false)
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &encoded))
	require.NotContains(t, encoded.Bytes(), groupConcatSourcePayloadMagic)
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	results, err := target.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "aa|b", string(results[0].GetBytesAt(0)))
	results[0].Free(mp)

	sink := &groupConcatWarningSink{}
	ReportGroupConcatWarnings(target, sink)
	require.Equal(t, uint64(1), sink.total)
	require.Equal(t, []string{"Row 2 was cut by GROUP_CONCAT()"}, sink.messages)
}

func TestOrderedGroupConcatIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	config := testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|")

	values := vector.NewVec(types.T_varchar.ToType())
	orderKeys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendBytes(values, []byte("b"), false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte("a"), false, mp))
	require.NoError(t, vector.AppendFixedList(orderKeys, []int64{2, 3, 1}, nil, mp))
	defer values.Free(mp)
	defer orderKeys.Free(mp)

	exec := newGroupConcatExec(mp, info, ",")
	require.NoError(t, exec.SetExtraInformation(config, 0))
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1},
		[]*vector.Vector{values, orderKeys},
	))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newGroupConcatExec(mp, info, ",")
	require.NoError(t, restored.SetExtraInformation(config, 0))
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	results, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "a|b", string(results[0].GetBytesAt(0)))
	results[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestGroupConcatUntrustedEmptyPartialForcesFallbackOrdinal(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}

	// The remote producer consumed input rows but had no non-NULL GROUP_CONCAT
	// payload. The explicit v66 trailer must preserve that untrusted namespace
	// instead of relying on a value entry that does not exist.
	remote := newGroupConcatExec(mp, info, "|")
	require.NoError(t, remote.GroupGrow(1))
	SetGroupConcatSourceRowWire(remote, false)
	SetGroupConcatSourceRowProvenanceWire(remote, true)
	SetGroupConcatSourceRowsTrusted(remote, false)
	var encoded bytes.Buffer
	require.NoError(t, remote.SaveIntermediateResultOfChunk(0, &encoded))

	target := newGroupConcatExec(mp, info, "|")
	require.NoError(t, target.SetExtraInformation(EncodeGroupConcatConfig("|", 4), 0))
	require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
	require.False(t, GroupConcatSourceRowsTrusted(target))
	SetGroupConcatMultiGroupContext(target, true)

	values := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(values, []byte("aa"), false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte("bbb"), false, mp))
	defer values.Free(mp)
	trusted := newGroupConcatExec(mp, info, "|")
	require.NoError(t, trusted.SetExtraInformation(EncodeGroupConcatConfig("|", 4), 0))
	require.NoError(t, trusted.GroupGrow(1))
	// Deliberately use a disjoint producer namespace. If the receiver ever
	// treats the mixed state as trusted, the truncation would be reported as
	// row 102 instead of the deterministic fallback ordinal 2.
	SetGroupConcatInputRowBase(trusted, 100)
	require.NoError(t, trusted.BatchFill(0, []uint64{1, 1}, []*vector.Vector{values}))
	require.NoError(t, target.BatchMerge(trusted, 0, []uint64{1}))
	require.False(t, GroupConcatSourceRowsTrusted(target))

	// The downgrade must also be monotonic when the trusted producer arrives
	// first. This is the ordering used by a coordinator that receives local
	// state before a second CN's empty/NULL-only state.
	reverse := newGroupConcatExec(mp, info, "|")
	require.NoError(t, reverse.SetExtraInformation(EncodeGroupConcatConfig("|", 4), 0))
	require.NoError(t, reverse.GroupGrow(1))
	SetGroupConcatMultiGroupContext(reverse, true)
	require.NoError(t, reverse.BatchMerge(trusted, 0, []uint64{1}))
	require.True(t, GroupConcatSourceRowsTrusted(reverse))
	require.NoError(t, reverse.BatchMerge(remote, 0, []uint64{1}))
	require.False(t, GroupConcatSourceRowsTrusted(reverse))
	reverseResult, err := reverse.Flush()
	require.NoError(t, err)
	require.Equal(t, "aa|b", string(reverseResult[0].GetBytesAt(0)))
	reverseResult[0].Free(mp)
	reverseSink := &groupConcatWarningSink{}
	ReportGroupConcatWarnings(reverse, reverseSink)
	require.Equal(t, []string{"Row 2 was cut by GROUP_CONCAT()"}, reverseSink.messages)
	reverse.Free()

	var reencoded bytes.Buffer
	require.NoError(t, target.SaveIntermediateResultOfChunk(0, &reencoded))
	downstream := newGroupConcatExec(mp, info, "|")
	require.NoError(t, downstream.UnmarshalFromReader(
		bytes.NewReader(reencoded.Bytes()), mp))
	require.False(t, GroupConcatSourceRowsTrusted(downstream))
	downstream.Free()

	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, "aa|b", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	sink := &groupConcatWarningSink{}
	ReportGroupConcatWarnings(target, sink)
	require.Equal(t, uint64(1), sink.total)
	require.Equal(t, []string{"Row 2 was cut by GROUP_CONCAT()"}, sink.messages)

	remote.Free()
	trusted.Free()
	target.Free()
}

func TestAccountedDistinctGroupConcatIntermediateKeepsSourceRows(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	info := multiAggInfo{
		aggID:     AggIdOfGroupConcat,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	newExec := func() *groupConcatExec {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("|", 4), 0))
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
		require.NoError(t, exec.GroupGrow(2))
		return exec
	}
	source := newExec()
	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{
		"aa", "aa", "b", "", "c", "x", "", "yy", "z",
	})
	values.SetNull(3)
	values.SetNull(6)
	groups := []uint64{1, 1, 1, 1, 1, 2, 2, 2, 2}
	require.NoError(t, source.PreflightBatchFill(0, groups, []*vector.Vector{values}))
	require.NoError(t, source.BatchFill(0, groups, []*vector.Vector{values}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))

	target := newExec()
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	results, err := target.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "aa|b", string(results[0].GetBytesAt(0)))
	require.Equal(t, "x|yy", string(results[0].GetBytesAt(1)))
	sink := &groupConcatWarningSink{}
	ReportGroupConcatWarnings(target, sink)
	require.Equal(t, uint64(2), sink.total)
	require.ElementsMatch(t, []string{
		"Row 3 was cut by GROUP_CONCAT()",
		"Row 8 was cut by GROUP_CONCAT()",
	}, sink.messages)

	results[0].Free(mp)
	values.Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestJsonObjectAggIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	info := multiAggInfo{
		aggID:     AggIdOfJsonObjectAgg,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}

	keyVec := vector.NewVec(types.T_varchar.ToType())
	valVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendBytes(keyVec, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(keyVec, []byte("b"), false, mp))
	require.NoError(t, vector.AppendFixedList(valVec, []int64{1, 2}, nil, mp))
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	exec := newJsonObjectAggExec(mp, info)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{keyVec, valVec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newJsonObjectAggExec(mp, info)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	results, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)

	text, err := types.DecodeJson(results[0].GetBytesAt(0)).MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1,"b":2}`, string(text))
	results[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestMedianIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []int64{1, 3, 2, 4}, nil, mp))
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	results, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 2.5, vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
	results[0].Free(mp)
	exec.Free()
	restored.Free()
}
