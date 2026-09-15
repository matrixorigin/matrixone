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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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

func TestJSONOpaqueAggregateIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()

	protocolVersion := bytejson.MySQLOpaqueProtocolVersion
	arrayInfo := multiAggInfo{
		aggID:     AggIdOfJsonArrayAgg,
		argTypes:  []types.Type{types.T_varbinary.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	values := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(values, []byte{0, 0xff, 'A'}, false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte{}, false, mp))

	array := newJsonArrayAggExec(mp, arrayInfo)
	ConfigureJSONAggregateOpaqueProtocol(array, protocolVersion)
	require.NoError(t, array.GroupGrow(1))
	require.NoError(t, array.BatchFill(0, []uint64{1, 1}, []*vector.Vector{values}))
	var arrayWire bytes.Buffer
	require.NoError(t, array.SaveIntermediateResult(1, [][]uint8{{1}}, &arrayWire))

	arrayRestored := newJsonArrayAggExec(mp, arrayInfo)
	ConfigureJSONAggregateOpaqueProtocol(arrayRestored, protocolVersion)
	require.NoError(t, arrayRestored.UnmarshalFromReader(
		bytes.NewReader(arrayWire.Bytes()), mp))
	arrayResult, err := arrayRestored.Flush()
	require.NoError(t, err)
	require.Len(t, arrayResult, 1)
	arrayJSON, err := types.DecodeJson(arrayResult[0].GetBytesAt(0)).MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `["base64:type15:AP9B","base64:type15:"]`, string(arrayJSON))

	key := vector.NewVec(types.T_varchar.ToType())
	bitValues := vector.NewVec(types.New(types.T_bit, 8, 0))
	require.NoError(t, vector.AppendBytes(key, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(key, []byte("b"), false, mp))
	require.NoError(t, vector.AppendFixedList(bitValues, []uint64{0xaa, 7}, nil, mp))
	objectInfo := multiAggInfo{
		aggID:     AggIdOfJsonObjectAgg,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.New(types.T_bit, 8, 0)},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	object := newJsonObjectAggExec(mp, objectInfo)
	ConfigureJSONAggregateOpaqueProtocol(object, protocolVersion)
	require.NoError(t, object.GroupGrow(1))
	require.NoError(t, object.BatchFill(0, []uint64{1, 1}, []*vector.Vector{key, bitValues}))
	var objectWire bytes.Buffer
	require.NoError(t, object.SaveIntermediateResult(1, [][]uint8{{1}}, &objectWire))

	objectRestored := newJsonObjectAggExec(mp, objectInfo)
	ConfigureJSONAggregateOpaqueProtocol(objectRestored, protocolVersion)
	require.NoError(t, objectRestored.UnmarshalFromReader(
		bytes.NewReader(objectWire.Bytes()), mp))
	objectResult, err := objectRestored.Flush()
	require.NoError(t, err)
	require.Len(t, objectResult, 1)
	objectJSON, err := types.DecodeJson(objectResult[0].GetBytesAt(0)).MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"base64:type16:qg==","b":"base64:type16:Bw=="}`, string(objectJSON))

	arrayResult[0].Free(mp)
	objectResult[0].Free(mp)
	values.Free(mp)
	key.Free(mp)
	bitValues.Free(mp)
	array.Free()
	arrayRestored.Free()
	object.Free()
	objectRestored.Free()
}

func TestJSONOpaqueArrayDistinctUsesSourceValueEquality(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()

	protocolVersion := bytejson.MySQLOpaqueProtocolVersion
	tests := []struct {
		name     string
		typ      types.Type
		append   func(*vector.Vector)
		wantJSON string
	}{
		{
			name: "varbinary", typ: types.T_varbinary.ToType(),
			append: func(v *vector.Vector) {
				require.NoError(t, vector.AppendBytes(v, []byte{0, 0xff}, false, mp))
				require.NoError(t, vector.AppendBytes(v, []byte{0, 0xff}, false, mp))
				require.NoError(t, vector.AppendBytes(v, []byte{0}, false, mp))
			},
			wantJSON: `["base64:type15:AP8=","base64:type15:AA=="]`,
		},
		{
			name: "bit", typ: types.New(types.T_bit, 8, 0),
			append: func(v *vector.Vector) {
				require.NoError(t, vector.AppendFixed(v, uint64(0xaa), false, mp))
				require.NoError(t, vector.AppendFixed(v, uint64(0xaa), false, mp))
				require.NoError(t, vector.AppendFixed(v, uint64(7), false, mp))
			},
			wantJSON: `["base64:type16:qg==","base64:type16:Bw=="]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			values := vector.NewVec(tc.typ)
			tc.append(values)
			exec := newJsonArrayAggExec(mp, multiAggInfo{
				aggID: AggIdOfJsonArrayAgg, distinct: true,
				argTypes: []types.Type{tc.typ}, retType: types.T_json.ToType(),
				emptyNull: true,
			})
			ConfigureJSONAggregateOpaqueProtocol(exec, protocolVersion)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0,
				[]uint64{1, 1, 1}, []*vector.Vector{values}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			got, err := types.DecodeJson(result[0].GetBytesAt(0)).MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tc.wantJSON, string(got))
			result[0].Free(mp)
			exec.Free()
			values.Free(mp)
		})
	}
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
