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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestJSONAggregatesLegacyInvalidUTF8(t *testing.T) {
	for _, accounted := range []bool{false, true} {
		for _, object := range []bool{false, true} {
			t.Run(fmt.Sprintf("accounted=%t/object=%t", accounted, object), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
				typ := types.T_varchar.ToType()
				values := buildVarlenVec(t, mp, typ, []string{string([]byte{'v', 0xff, 'x'})})
				defer values.Free(mp)
				vectors := []*vector.Vector{values}
				params := []types.Type{typ}
				id := AggIdOfJsonArrayAgg
				if object {
					keys := buildVarlenVec(t, mp, typ, []string{string([]byte{'k', 0xff})})
					defer keys.Free(mp)
					vectors = []*vector.Vector{keys, values}
					params = []types.Type{typ, typ}
					id = AggIdOfJsonObjectAgg
				}
				exec, err := MakeAgg(mp, id, false, params...)
				require.NoError(t, err)
				var cleanup func()
				defer func() {
					exec.Free()
					if cleanup != nil {
						cleanup()
					}
				}()
				if accounted {
					registry, account, allocation := newTestAggregateAllocation(t)
					owner := exec.(AllocationAccountOwner)
					require.NoError(t, owner.SetAllocationAccount(allocation))
					cleanup = func() {
						require.NoError(t, owner.ClearAllocationAccount(allocation))
						finishTestAggregateAllocation(t, registry, account)
					}
				}
				require.NoError(t, exec.GroupGrow(1))
				if accounted {
					require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, []uint64{1}, vectors))
				}
				require.NoError(t, exec.BatchFill(0, []uint64{1}, vectors))
				results, err := exec.Flush()
				if results != nil {
					defer func() {
						for _, result := range results {
							result.Free(mp)
						}
					}()
				}
				require.NoError(t, err)
				visible, err := types.DecodeJson(results[0].GetBytesAt(0)).MarshalJSON()
				require.NoError(t, err)
				if object {
					require.JSONEq(t, `{"k�":"v�x"}`, string(visible))
				} else {
					require.JSONEq(t, `["v�x"]`, string(visible))
				}
			})
		}
	}
}

func TestJSONAggregateLegacyUTF8ScratchAdmissionFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
	})
	require.NoError(t, err)
	entries := []jsonAggregateEntry{{key: []byte{0xff}, value: []byte{bytejson.TpCodeString, 1, 0xfe}}}
	_, err = normalizeJSONAggregateEntries(entries, true, true, allocation, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, []byte{0xff}, entries[0].key)
	require.Equal(t, []byte{bytejson.TpCodeString, 1, 0xfe}, entries[0].value)
	finishTestAggregateAllocation(t, registry, account)
}

func TestJSONObjectAggregateLegacyBinaryKey(t *testing.T) {
	for _, accounted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accounted=%t", accounted), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			keyType, valueType := types.T_varbinary.ToType(), types.T_varchar.ToType()
			keys := buildVarlenVec(t, mp, keyType, []string{string([]byte{0xff})})
			defer keys.Free(mp)
			values := buildVarlenVec(t, mp, valueType, []string{"value"})
			defer values.Free(mp)
			exec, err := MakeAgg(mp, AggIdOfJsonObjectAgg, false, keyType, valueType)
			require.NoError(t, err)
			var cleanup func()
			defer func() {
				exec.Free()
				if cleanup != nil {
					cleanup()
				}
			}()
			if accounted {
				registry, account, allocation := newTestAggregateAllocation(t)
				owner := exec.(AllocationAccountOwner)
				require.NoError(t, owner.SetAllocationAccount(allocation))
				cleanup = func() {
					require.NoError(t, owner.ClearAllocationAccount(allocation))
					finishTestAggregateAllocation(t, registry, account)
				}
			}
			groups := []uint64{1}
			input := []*vector.Vector{keys, values}
			require.NoError(t, exec.GroupGrow(1))
			if accounted {
				require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, input))
			}
			require.NoError(t, exec.BatchFill(0, groups, input))
			results, err := exec.Flush()
			if results != nil {
				defer func() {
					for _, result := range results {
						result.Free(mp)
					}
				}()
			}
			require.NoError(t, err)
			visible, err := types.DecodeJson(results[0].GetBytesAt(0)).MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, `{"�":"value"}`, string(visible))
		})
	}
}

func TestJSONAggregatesLegacyUTF8DistinctAndKeys(t *testing.T) {
	for _, accounted := range []bool{false, true} {
		for _, object := range []bool{false, true} {
			t.Run(fmt.Sprintf("accounted=%t/object=%t", accounted, object), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
				typ := types.T_varchar.ToType()
				values := []string{string([]byte{0xff}), string([]byte{0xfe}), string([]byte{0xff}), "�", string([]byte{'a', 0xff, 0xfe, 'b'})}
				if object {
					values = []string{"first", "second", "third", "quote\ntext", "last"}
				}
				valueVec := buildVarlenVec(t, mp, typ, values)
				defer valueVec.Free(mp)
				vectors := []*vector.Vector{valueVec}
				params := []types.Type{typ}
				id := AggIdOfJsonArrayAgg
				if object {
					keys := buildVarlenVec(t, mp, typ, []string{string([]byte{0xff}), string([]byte{0xfe}), "�", "valid", string([]byte{'a', 0xff, 0xfe, 'b'})})
					defer keys.Free(mp)
					vectors = []*vector.Vector{keys, valueVec}
					params = []types.Type{typ, typ}
					id = AggIdOfJsonObjectAgg
				}
				exec, err := MakeAgg(mp, id, !object, params...)
				require.NoError(t, err)
				var cleanup func()
				defer func() {
					exec.Free()
					if cleanup != nil {
						cleanup()
					}
				}()
				if accounted {
					registry, account, allocation := newTestAggregateAllocation(t)
					owner := exec.(AllocationAccountOwner)
					require.NoError(t, owner.SetAllocationAccount(allocation))
					cleanup = func() {
						require.NoError(t, owner.ClearAllocationAccount(allocation))
						finishTestAggregateAllocation(t, registry, account)
					}
				}
				groups := []uint64{1, 1, 1, 1, 1}
				require.NoError(t, exec.GroupGrow(1))
				if accounted {
					require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, vectors))
				}
				require.NoError(t, exec.BatchFill(0, groups, vectors))
				results, err := exec.Flush()
				if results != nil {
					defer func() {
						for _, result := range results {
							result.Free(mp)
						}
					}()
				}
				require.NoError(t, err)
				visible, err := types.DecodeJson(results[0].GetBytesAt(0)).MarshalJSON()
				require.NoError(t, err)
				if object {
					require.JSONEq(t, `{"�":"third","valid":"quote\ntext","a��b":"last"}`, string(visible))
				} else {
					require.JSONEq(t, `["�","�","�","a��b"]`, string(visible))
				}
			})
		}
	}
}
