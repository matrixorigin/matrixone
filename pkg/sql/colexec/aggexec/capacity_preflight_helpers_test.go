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
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCapacityPreflightMetadataHelperBoundaries(t *testing.T) {
	var events [hashmap.UnitLimit]prepareParamKindEvent
	eventCount := 0
	require.ErrorIs(t, addPrepareParamKindEvent(
		&events, &eventCount, -1, 0, 0, vector.PrepareParamInteger),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, -1, 0, vector.PrepareParamInteger),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, 0, -1, vector.PrepareParamInteger),
		mpool.ErrAllocationAccountInvalid)
	require.NoError(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, 0, 0, vector.PrepareParamInteger))
	eventCount = len(events)
	require.ErrorIs(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, 0, 0, vector.PrepareParamInteger),
		mpool.ErrAllocationAccountInvalid)

	var areaNeeds [hashmap.UnitLimit]vectorAreaChunkCapacity
	areaCount := 0
	require.NoError(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 0, -1, []byte("ignored")))
	require.NoError(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 0, 0, []byte("inline")))
	long := make([]byte, types.VarlenaInlineSize+1)
	require.NoError(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 0, 0, long))
	require.NoError(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 0, 0, long))
	require.Equal(t, 1, areaCount)
	areaNeeds[0].bytes[0] = math.MaxInt
	require.ErrorIs(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 0, 0, long), mpool.ErrAllocationAllocatorLimit)
	areaCount = len(areaNeeds)
	require.ErrorIs(t, addVectorAreaCapacity(
		&areaNeeds, &areaCount, 1, 0, long), mpool.ErrAllocationAccountInvalid)

	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	winner, err := winnerForGroup(&winners, &winnerCount, 7)
	require.NoError(t, err)
	require.Equal(t, -1, winner.winner)
	again, err := winnerForGroup(&winners, &winnerCount, 7)
	require.NoError(t, err)
	require.Same(t, winner, again)
	winnerCount = len(winners)
	_, err = winnerForGroup(&winners, &winnerCount, 9)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	require.True(t, typesEqual(nil, nil))
	require.False(t, typesEqual([]types.Type{types.T_int64.ToType()}, nil))
	require.False(t, typesEqual(
		[]types.Type{types.T_int64.ToType()},
		[]types.Type{types.T_uint64.ToType()}))

	var bitmapTargets [hashmap.UnitLimit]bitmapPreflightTarget
	bitmapCount := 0
	require.ErrorIs(t, addBitmapPreflightTarget(
		&bitmapTargets, &bitmapCount, 0, 0, -1), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, addBitmapPreflightTarget(
		&bitmapTargets, &bitmapCount, 0, 0, 2))
	require.NoError(t, addBitmapPreflightTarget(
		&bitmapTargets, &bitmapCount, 0, 0, 3))
	require.Equal(t, 5, bitmapTargets[0].required)
	bitmapTargets[0].required = math.MaxInt
	require.ErrorIs(t, addBitmapPreflightTarget(
		&bitmapTargets, &bitmapCount, 0, 0, 1), mpool.ErrAllocationAllocatorLimit)
	bitmapCount = len(bitmapTargets)
	require.ErrorIs(t, addBitmapPreflightTarget(
		&bitmapTargets, &bitmapCount, 1, 0, 1), mpool.ErrAllocationAccountInvalid)
}

func TestCapacityPreflightHelperApplicationAndJSONPaths(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	makeExec := func(id int64, params ...types.Type) GroupAggFuncExec {
		exec, err := MakeGroupAgg(mp, id, false, allocation, nil, params...)
		require.NoError(t, err)
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
		return exec
	}

	anyAggregate := makeExec(AggIdOfAny, types.T_varchar.ToType())
	require.NoError(t, anyAggregate.GroupGrow(1))
	base := anyAggregate.(aggregateBaseCarrier).aggregateBase()
	var events [hashmap.UnitLimit]prepareParamKindEvent
	eventCount := 0
	require.NoError(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, 0, 0, vector.PrepareParamInteger))
	require.NoError(t, addPrepareParamKindEvent(
		&events, &eventCount, 0, 0, 0, vector.PrepareParamFloat))
	require.NoError(t, base.applyPrepareParamKindEvents(&events, eventCount))
	events[0].chunk = 9
	require.ErrorIs(t, base.applyPrepareParamKindEvents(&events, 1),
		mpool.ErrAllocationAccountInvariant)
	events[0] = prepareParamKindEvent{chunk: 0, column: 9, row: 0}
	require.ErrorIs(t, base.applyPrepareParamKindEvents(&events, 1),
		mpool.ErrAllocationAccountInvariant)

	var areaNeeds [hashmap.UnitLimit]vectorAreaChunkCapacity
	areaNeeds[0] = vectorAreaChunkCapacity{chunk: 0}
	areaNeeds[0].bytes[0] = types.VarlenaInlineSize + 1
	require.NoError(t, base.applyVectorAreaCapacity(&areaNeeds, 1))
	areaNeeds[0].chunk = 9
	require.ErrorIs(t, base.applyVectorAreaCapacity(&areaNeeds, 1),
		mpool.ErrAllocationAccountInvariant)
	areaNeeds[0].chunk = 0
	areaNeeds[0].bytes[1] = 1
	require.ErrorIs(t, base.applyVectorAreaCapacity(&areaNeeds, 1),
		mpool.ErrAllocationAccountInvariant)

	bitmapExec := makeExec(AggIdOfBitmapConstruct, types.T_uint64.ToType()).(*bmpConstructExec)
	require.NoError(t, bitmapExec.GroupGrow(1))
	_, _, _, value, err := bitmapExec.bitmapTarget(1)
	require.NoError(t, err)
	require.Nil(t, value)
	var bitmapTargets [hashmap.UnitLimit]bitmapPreflightTarget
	bitmapTargets[0] = bitmapPreflightTarget{chunk: 0, row: 0, required: 4}
	require.NoError(t, bitmapExec.applyBitmapPreflight(&bitmapTargets, 1))
	_, _, _, value, err = bitmapExec.bitmapTarget(1)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.GreaterOrEqual(t, cap(value.values), 4)
	bitmapTargets[0].chunk = 9
	require.ErrorIs(t, bitmapExec.applyBitmapPreflight(&bitmapTargets, 1),
		mpool.ErrAllocationAccountInvariant)
	bitmapTargets[0] = bitmapPreflightTarget{chunk: 0, row: AggBatchSize, required: 1}
	require.ErrorIs(t, bitmapExec.applyBitmapPreflight(&bitmapTargets, 1),
		mpool.ErrAllocationAccountInvariant)

	jsonExec := makeExec(AggIdOfJsonArrayAgg, types.T_json.ToType())
	require.NoError(t, jsonExec.GroupGrow(1))
	jsonBase := jsonExec.(aggregateBaseCarrier).aggregateBase()
	jsonValue, err := bytejson.CreateByteJSONWithCheck(map[string]any{"k": "v"})
	require.NoError(t, err)
	encoded, err := jsonValue.Marshal()
	require.NoError(t, err)
	jsonVec := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(jsonVec, encoded, false, mp))
	size, err := accountedJSONValueSize(jsonVec, 0)
	require.NoError(t, err)
	require.Positive(t, size)
	_, err = accountedJSONValueSize(nil, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	var argNeeds [hashmap.UnitLimit]argumentChunkCapacity
	var progress [hashmap.UnitLimit]argumentTargetProgress
	argCount, progressCount := 0, 0
	require.NoError(t, addJSONArgumentCapacity(
		jsonBase, &argNeeds, &argCount, &progress, &progressCount,
		1, 3, func(dst []byte) ([]byte, error) { return append(dst, 1, 2, 3), nil }))
	require.ErrorIs(t, addJSONArgumentCapacity(
		jsonBase, &argNeeds, &argCount, &progress, &progressCount,
		1, -1, func(dst []byte) ([]byte, error) { return dst, nil }),
		mpool.ErrAllocationAllocatorLimit)
	require.ErrorIs(t, addJSONArgumentCapacity(
		jsonBase, &argNeeds, &argCount, &progress, &progressCount,
		1, 2, func(dst []byte) ([]byte, error) { return append(dst, 1), nil }),
		mpool.ErrAllocationAccountInvariant)
	injected := errors.New("json builder failed")
	require.ErrorIs(t, addJSONArgumentCapacity(
		jsonBase, &argNeeds, &argCount, &progress, &progressCount,
		1, 1, func([]byte) ([]byte, error) { return nil, injected }), injected)

	jsonVec.Free(mp)
	for _, exec := range []GroupAggFuncExec{anyAggregate, bitmapExec, jsonExec} {
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
	}
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
