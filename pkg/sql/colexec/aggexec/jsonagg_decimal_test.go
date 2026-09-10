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
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type jsonAggregateDecimalCase struct {
	name     string
	typ      types.Type
	values   func(*testing.T, *mpool.MPool) *vector.Vector
	expected []string
}

func TestJSONAggregateDecimalEncodingPreservesExactText(t *testing.T) {
	cases := []jsonAggregateDecimalCase{
		{
			name: "decimal64-fractional-adjacent",
			typ:  types.New(types.T_decimal64, 18, 17),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal64Vector(t, mp, 18, 17, []string{
					"0.12345678901234567", "0.12345678901234568",
				})
			},
			expected: []string{"0.12345678901234567", "0.12345678901234568"},
		},
		{
			name: "decimal128-integer-adjacent",
			typ:  types.New(types.T_decimal128, 38, 0),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal128Vector(t, mp, 38, 0, []string{
					"99999999999999999999999999999999999998",
					"99999999999999999999999999999999999999",
				})
			},
			expected: []string{
				"99999999999999999999999999999999999998",
				"99999999999999999999999999999999999999",
			},
		},
		{
			name: "decimal128-fractional-adjacent",
			typ:  types.New(types.T_decimal128, 38, 35),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal128Vector(t, mp, 38, 35, []string{
					"0.12345678901234567890123456789012345",
					"0.12345678901234567890123456789012346",
				})
			},
			expected: []string{
				"0.12345678901234567890123456789012345",
				"0.12345678901234567890123456789012346",
			},
		},
		{
			name: "decimal256-precision-39",
			typ:  types.New(types.T_decimal256, 39, 0),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal256Vector(t, mp, 39, 0, []string{
					"123456789012345678901234567890123456789",
					"-123456789012345678901234567890123456789",
				})
			},
			expected: []string{
				"123456789012345678901234567890123456789",
				"-123456789012345678901234567890123456789",
			},
		},
		{
			name: "decimal256-precision-40-scaled",
			typ:  types.New(types.T_decimal256, 40, 10),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal256Vector(t, mp, 40, 10, []string{
					"123456789012345678901234567890.1234567890",
					"-0.0000000001",
					"0.0000000000",
				})
			},
			expected: []string{
				"123456789012345678901234567890.1234567890",
				"-0.0000000001",
				"0.0000000000",
			},
		},
		{
			name: "decimal256-precision-76-scaled",
			typ:  types.New(types.T_decimal256, 76, 38),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal256Vector(t, mp, 76, 38, []string{
					"12345678901234567890123456789012345678.12345678901234567890123456789012345678",
					"-12345678901234567890123456789012345678.12345678901234567890123456789012345678",
				})
			},
			expected: []string{
				"12345678901234567890123456789012345678.12345678901234567890123456789012345678",
				"-12345678901234567890123456789012345678.12345678901234567890123456789012345678",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := tc.values(t, mp)
			defer func() {
				vec.Free(mp)
				require.Zero(t, mp.CurrNB())
			}()

			require.Len(t, tc.expected, vec.Length())
			for row, want := range tc.expected {
				size, err := jsonAggregateValueSize(vec, uint64(row))
				require.NoError(t, err)
				encoded, err := appendJSONAggregateValue(make([]byte, 0, size), vec, uint64(row))
				require.NoError(t, err)
				require.Equal(t, size, len(encoded), "size and append must agree")
				assertExactJSONDecimal(t, types.DecodeJson(encoded), want)

				legacy, err := buildValueByteJson(vec, uint64(row))
				require.NoError(t, err)
				assertExactJSONDecimal(t, legacy, want)
			}
		})
	}
}

func buildJSONDecimal64Vector(
	t *testing.T, mp *mpool.MPool, width, scale int32, values []string,
) *vector.Vector {
	t.Helper()
	typ := types.New(types.T_decimal64, width, scale)
	decoded := make([]types.Decimal64, len(values))
	for i, value := range values {
		var err error
		decoded[i], err = types.ParseDecimal64(value, width, scale)
		require.NoError(t, err)
	}
	return buildFixedVec(t, mp, typ, decoded)
}

func buildJSONDecimal128Vector(
	t *testing.T, mp *mpool.MPool, width, scale int32, values []string,
) *vector.Vector {
	t.Helper()
	typ := types.New(types.T_decimal128, width, scale)
	decoded := make([]types.Decimal128, len(values))
	for i, value := range values {
		var err error
		decoded[i], err = types.ParseDecimal128(value, width, scale)
		require.NoError(t, err)
	}
	return buildFixedVec(t, mp, typ, decoded)
}

func buildJSONDecimal256Vector(
	t *testing.T, mp *mpool.MPool, width, scale int32, values []string,
) *vector.Vector {
	t.Helper()
	typ := types.New(types.T_decimal256, width, scale)
	decoded := make([]types.Decimal256, len(values))
	for i, value := range values {
		var err error
		decoded[i], err = types.ParseDecimal256(value, width, scale)
		require.NoError(t, err)
	}
	return buildFixedVec(t, mp, typ, decoded)
}

func assertExactJSONDecimal(t *testing.T, value bytejson.ByteJson, want string) {
	t.Helper()
	require.Equal(t, bytejson.TpCodeDecimal, value.Type)
	require.Equal(t, want, string(value.GetString()))

	visible, err := value.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, want, string(visible))

	decoder := json.NewDecoder(bytes.NewReader(visible))
	decoder.UseNumber()
	var decoded json.Number
	require.NoError(t, decoder.Decode(&decoded))
	require.Equal(t, json.Number(want), decoded)
}

func TestJSONAggregatesDecimalExactForBothStoragePaths(t *testing.T) {
	cases := []jsonAggregateDecimalCase{
		{
			name: "decimal64",
			typ:  types.New(types.T_decimal64, 18, 17),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal64Vector(t, mp, 18, 17, []string{
					"0.12345678901234567", "0.12345678901234568",
				})
			},
			expected: []string{"0.12345678901234567", "0.12345678901234568"},
		},
		{
			name: "decimal128",
			typ:  types.New(types.T_decimal128, 38, 0),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal128Vector(t, mp, 38, 0, []string{
					"99999999999999999999999999999999999998",
					"99999999999999999999999999999999999999",
				})
			},
			expected: []string{
				"99999999999999999999999999999999999998",
				"99999999999999999999999999999999999999",
			},
		},
		{
			name: "decimal256",
			typ:  types.New(types.T_decimal256, 40, 10),
			values: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildJSONDecimal256Vector(t, mp, 40, 10, []string{
					"123456789012345678901234567890.1234567890",
					"-0.0000000001",
				})
			},
			expected: []string{
				"123456789012345678901234567890.1234567890",
				"-0.0000000001",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, accounted := range []bool{false, true} {
				t.Run(map[bool]string{false: "legacy", true: "accounted"}[accounted], func(t *testing.T) {
					mp := mpool.MustNewZero()
					values := tc.values(t, mp)
					keys := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "b"})
					array, object := runJSONAggregateValues(t, mp, tc.typ, values, keys, accounted)
					defer func() {
						values.Free(mp)
						keys.Free(mp)
						require.Zero(t, mp.CurrNB())
					}()

					wantArray := make([]any, len(tc.expected))
					wantObject := make(map[string]any, len(tc.expected))
					for i, expected := range tc.expected {
						wantArray[i] = json.Number(expected)
						wantObject[string(rune('a'+i))] = json.Number(expected)
					}
					assertJSONWithNumbers(t, array, wantArray)
					assertJSONWithNumbers(t, object, wantObject)
				})
			}
		})
	}
}

func TestJSONAggregateDecimalGroupedMergeAndSpill(t *testing.T) {
	typ := types.New(types.T_decimal256, 40, 10)
	valuesText := []string{
		"123456789012345678901234567890.1234567890",
		"123456789012345678901234567891.1234567890",
		"-0.0000000001",
		"0.0000000000",
	}
	wantArray := [][]any{
		{json.Number(valuesText[0]), json.Number(valuesText[1])},
		{json.Number(valuesText[2]), json.Number(valuesText[3])},
		{nil},
		nil,
	}
	wantObject := []map[string]any{
		{"a": json.Number(valuesText[0]), "b": json.Number(valuesText[1])},
		{"c": json.Number(valuesText[2]), "d": json.Number(valuesText[3])},
		{"e": nil},
		nil,
	}

	for _, aggregate := range []struct {
		name string
		id   int64
	}{
		{name: "array", id: AggIdOfJsonArrayAgg},
		{name: "object", id: AggIdOfJsonObjectAgg},
	} {
		for _, accounted := range []bool{false, true} {
			t.Run(aggregate.name+map[bool]string{false: "-legacy", true: "-accounted"}[accounted], func(t *testing.T) {
				mp := mpool.MustNewZero()
				values := buildJSONDecimal256Vector(t, mp, 40, 10, valuesText)
				require.NoError(t, vector.AppendFixed(values, types.Decimal256{}, true, mp))
				keys := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "b", "c", "d", "e"})

				left, leftOwner, allocation, registry := makeJSONAggregateForTest(t, mp, aggregate.id, typ, accounted)
				right, rightOwner, _, _ := makeJSONAggregateForTest(t, mp, aggregate.id, typ, accounted, allocation)
				require.NoError(t, left.GroupGrow(4))
				require.NoError(t, right.GroupGrow(4))
				leftVectors, rightVectors := []*vector.Vector{values}, []*vector.Vector{values}
				leftGroups := []uint64{1, 1, 2}
				rightGroups := []uint64{2, 3}
				if aggregate.id == AggIdOfJsonObjectAgg {
					leftVectors = []*vector.Vector{keys, values}
					rightVectors = leftVectors
				}
				fillJSONAggregateForTest(t, left, 0, leftGroups, leftVectors, accounted)
				fillJSONAggregateForTest(t, right, 3, rightGroups, rightVectors, accounted)
				if accounted {
					require.NoError(t, left.(BatchCapacityPreflight).PreflightBatchMerge(
						right, 1, []uint64{2, 3}))
				}
				switch aggregate.id {
				case AggIdOfJsonArrayAgg:
					require.NoError(t, left.BatchMerge(right, 1, []uint64{2, 3}))
				case AggIdOfJsonObjectAgg:
					require.NoError(t, left.BatchMerge(right, 1, []uint64{2, 3}))
				}

				var spill bytes.Buffer
				require.NoError(t, left.(SpillStateCodec).SaveSpillIntermediateRows(
					0, []int32{0, 1, 2, 3}, &spill))
				restored, restoredOwner, _, _ := makeJSONAggregateForTest(
					t, mp, aggregate.id, typ, accounted, allocation)
				require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
					bytes.NewReader(spill.Bytes()), mp))
				results, err := restored.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				for group := range wantArray {
					if results[0].IsNull(uint64(group)) {
						if aggregate.id == AggIdOfJsonArrayAgg {
							require.Nil(t, wantArray[group])
						} else {
							require.Nil(t, wantObject[group])
						}
						continue
					}
					value := types.DecodeJson(results[0].GetBytesAt(group))
					if aggregate.id == AggIdOfJsonArrayAgg {
						if wantArray[group] == nil {
							require.True(t, value.IsNull())
							continue
						}
						assertJSONWithNumbers(t, value, wantArray[group])
					} else {
						assertJSONWithNumbers(t, value, wantObject[group])
					}
				}
				results[0].Free(mp)
				values.Free(mp)
				keys.Free(mp)
				left.Free()
				right.Free()
				restored.Free()
				if accounted {
					require.NoError(t, leftOwner.ClearAllocationAccount(allocation))
					require.NoError(t, rightOwner.ClearAllocationAccount(allocation))
					require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
					finishTestAggregateAllocation(t, registry, allocation.account)
				}
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestJSONAggregateDecimalDistinctDuplicate(t *testing.T) {
	for _, accounted := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "accounted"}[accounted], func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := buildJSONDecimal64Vector(t, mp, 18, 17, []string{
				"0.12345678901234567", "0.12345678901234567",
			})
			defer values.Free(mp)
			allocation := (*AllocationAccount)(nil)
			var registry *mpool.AllocationAccountRegistry
			if accounted {
				registry, _, allocation = newTestAggregateAllocation(t)
			}
			exec, owner, account := makeJSONDistinctArrayForTest(t, mp, values.GetType(), allocation)
			groups := []uint64{1, 1}
			if accounted {
				require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, []*vector.Vector{values}))
			}
			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{values}))
			result, err := exec.Flush()
			require.NoError(t, err)
			assertJSONWithNumbers(t, types.DecodeJson(result[0].GetBytesAt(0)), []any{json.Number("0.12345678901234567")})
			result[0].Free(mp)
			exec.Free()
			if accounted {
				require.NoError(t, owner.ClearAllocationAccount(allocation))
				finishTestAggregateAllocation(t, registry, account.account)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestJSONAggregateDecimalDistinctAdjacentMergeAndSpill(t *testing.T) {
	cases := []struct {
		name   string
		typ    types.Type
		values []string
	}{
		{
			name: "decimal128-38-digit",
			typ:  types.New(types.T_decimal128, 38, 0),
			values: []string{
				"99999999999999999999999999999999999998",
				"99999999999999999999999999999999999999",
				"99999999999999999999999999999999999998",
			},
		},
		{
			name: "decimal256-38-digit",
			typ:  types.New(types.T_decimal256, 38, 0),
			values: []string{
				"99999999999999999999999999999999999998",
				"99999999999999999999999999999999999999",
				"99999999999999999999999999999999999998",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, accounted := range []bool{false, true} {
				path := map[bool]string{false: "legacy", true: "accounted"}[accounted]
				t.Run(path, func(t *testing.T) {
					mp := mpool.MustNewZero()
					values := buildJSONDecimalVectorForType(t, mp, tc.typ, tc.values)
					var (
						registry   *mpool.AllocationAccountRegistry
						allocation *AllocationAccount
					)
					if accounted {
						registry, _, allocation = newTestAggregateAllocation(t)
					}

					left, leftOwner := makeJSONDistinctAggregateForTest(
						t, mp, AggIdOfJsonArrayAgg, tc.typ, allocation)
					right, rightOwner := makeJSONDistinctAggregateForTest(
						t, mp, AggIdOfJsonArrayAgg, tc.typ, allocation)
					require.NoError(t, left.GroupGrow(1))
					require.NoError(t, right.GroupGrow(1))

					if accounted {
						require.NoError(t, left.(BatchCapacityPreflight).PreflightBatchFill(
							0, []uint64{1}, []*vector.Vector{values}))
						require.NoError(t, right.(BatchCapacityPreflight).PreflightBatchFill(
							1, []uint64{1, 1}, []*vector.Vector{values}))
						require.NoError(t, left.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
						require.NoError(t, right.BatchFill(1, []uint64{1, 1}, []*vector.Vector{values}))
						require.NoError(t, left.(BatchCapacityPreflight).PreflightBatchMerge(
							right, 0, []uint64{1}))
						require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))
					} else {
						// Legacy DISTINCT owns a separate hash set and intentionally
						// rejects merging two populated DISTINCT executors. Exercise
						// the cross-batch path and use spill/restore for this storage
						// mode; the accounted path above covers BatchMerge.
						require.NoError(t, left.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
						require.NoError(t, left.BatchFill(1, []uint64{1, 1}, []*vector.Vector{values}))
					}

					var spill bytes.Buffer
					require.NoError(t, left.(SpillStateCodec).SaveSpillIntermediateRows(
						0, []int32{0}, &spill))
					restored, restoredOwner := makeJSONDistinctAggregateForTest(
						t, mp, AggIdOfJsonArrayAgg, tc.typ, allocation)
					require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
						bytes.NewReader(spill.Bytes()), mp))
					results, err := restored.Flush()
					require.NoError(t, err)
					require.False(t, results[0].IsNull(0))
					want := []any{json.Number(tc.values[0]), json.Number(tc.values[1])}
					assertJSONWithNumbers(t, types.DecodeJson(results[0].GetBytesAt(0)), want)

					results[0].Free(mp)
					left.Free()
					right.Free()
					restored.Free()
					values.Free(mp)
					if accounted {
						require.NoError(t, leftOwner.ClearAllocationAccount(allocation))
						require.NoError(t, rightOwner.ClearAllocationAccount(allocation))
						require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
						finishTestAggregateAllocation(t, registry, allocation.account)
					}
					require.Zero(t, mp.CurrNB())
				})
			}
		})
	}
}

func TestJSONAggregateDecimalNullAndEmptySQLBits(t *testing.T) {
	typ := types.New(types.T_decimal256, 76, 0)
	for _, aggregate := range []struct {
		name string
		id   int64
	}{
		{name: "array", id: AggIdOfJsonArrayAgg},
		{name: "object", id: AggIdOfJsonObjectAgg},
	} {
		for _, accounted := range []bool{false, true} {
			path := map[bool]string{false: "legacy", true: "accounted"}[accounted]
			t.Run(aggregate.name+"-"+path, func(t *testing.T) {
				mp := mpool.MustNewZero()
				nulls := vector.NewConstNull(typ, 1, mp)
				keys := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"e"})
				var (
					registry   *mpool.AllocationAccountRegistry
					allocation *AllocationAccount
				)
				if accounted {
					registry, _, allocation = newTestAggregateAllocation(t)
				}
				exec, owner, _, _ := makeJSONAggregateForTest(
					t, mp, aggregate.id, typ, accounted, allocation)
				require.NoError(t, exec.GroupGrow(2))
				groups := []uint64{2}
				vectors := []*vector.Vector{nulls}
				if aggregate.id == AggIdOfJsonObjectAgg {
					vectors = []*vector.Vector{keys, nulls}
				}
				if accounted {
					require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, vectors))
				}
				require.NoError(t, exec.BatchFill(0, groups, vectors))
				results, err := exec.Flush()
				require.NoError(t, err)
				require.True(t, results[0].IsNull(0), "empty group must be SQL NULL")
				require.False(t, results[0].IsNull(1), "all-NULL input must be a JSON value")
				value := types.DecodeJson(results[0].GetBytesAt(1))
				if aggregate.id == AggIdOfJsonArrayAgg {
					assertJSONWithNumbers(t, value, []any{nil})
				} else {
					assertJSONWithNumbers(t, value, map[string]any{"e": nil})
				}
				results[0].Free(mp)
				exec.Free()
				nulls.Free(mp)
				keys.Free(mp)
				if accounted {
					require.NoError(t, owner.ClearAllocationAccount(allocation))
					finishTestAggregateAllocation(t, registry, allocation.account)
				}
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestJSONAggregateDecimalConstMaxAndNull(t *testing.T) {
	typ := types.New(types.T_decimal256, 76, 0)
	maxText := strings.Repeat("9", 76)
	maxValue, err := types.ParseDecimal256(maxText, 76, 0)
	require.NoError(t, err)
	require.Len(t, maxText, 76)

	for _, inputCase := range []struct {
		name      string
		makeInput func(*testing.T, *mpool.MPool) *vector.Vector
		want      func() []any
		wantObj   func() map[string]any
	}{
		{
			name: "const-decimal256-max",
			makeInput: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				value, err := vector.NewConstFixed(typ, maxValue, 3, mp)
				require.NoError(t, err)
				return value
			},
			want: func() []any {
				return []any{json.Number(maxText), json.Number(maxText), json.Number(maxText)}
			},
			wantObj: func() map[string]any {
				return map[string]any{
					"a": json.Number(maxText), "b": json.Number(maxText), "c": json.Number(maxText),
				}
			},
		},
		{
			name: "const-null",
			makeInput: func(_ *testing.T, mp *mpool.MPool) *vector.Vector {
				return vector.NewConstNull(typ, 3, mp)
			},
			want: func() []any {
				return []any{nil, nil, nil}
			},
			wantObj: func() map[string]any {
				return map[string]any{"a": nil, "b": nil, "c": nil}
			},
		},
	} {
		t.Run(inputCase.name, func(t *testing.T) {
			for _, aggregate := range []struct {
				name string
				id   int64
			}{
				{name: "array", id: AggIdOfJsonArrayAgg},
				{name: "object", id: AggIdOfJsonObjectAgg},
			} {
				for _, accounted := range []bool{false, true} {
					path := map[bool]string{false: "legacy", true: "accounted"}[accounted]
					t.Run(aggregate.name+"-"+path, func(t *testing.T) {
						mp := mpool.MustNewZero()
						values := inputCase.makeInput(t, mp)
						keys := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "b", "c"})
						var (
							registry   *mpool.AllocationAccountRegistry
							allocation *AllocationAccount
						)
						if accounted {
							registry, _, allocation = newTestAggregateAllocation(t)
						}
						exec, owner, _, _ := makeJSONAggregateForTest(
							t, mp, aggregate.id, typ, accounted, allocation)
						require.NoError(t, exec.GroupGrow(1))
						groups := []uint64{1, 1, 1}
						vectors := []*vector.Vector{values}
						if aggregate.id == AggIdOfJsonObjectAgg {
							vectors = []*vector.Vector{keys, values}
						}
						if accounted {
							require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, vectors))
						}
						require.NoError(t, exec.BatchFill(0, groups, vectors))
						results, err := exec.Flush()
						require.NoError(t, err)
						require.False(t, results[0].IsNull(0))
						value := types.DecodeJson(results[0].GetBytesAt(0))
						if aggregate.id == AggIdOfJsonArrayAgg {
							assertJSONWithNumbers(t, value, inputCase.want())
						} else {
							assertJSONWithNumbers(t, value, inputCase.wantObj())
						}
						results[0].Free(mp)
						exec.Free()
						values.Free(mp)
						keys.Free(mp)
						if accounted {
							require.NoError(t, owner.ClearAllocationAccount(allocation))
							finishTestAggregateAllocation(t, registry, allocation.account)
						}
						require.Zero(t, mp.CurrNB())
					})
				}
			}
		})
	}
}

func TestAccountedJSONDecimalPreflightExactBudget(t *testing.T) {
	typ := types.New(types.T_decimal256, 76, 0)
	maxText := strings.Repeat("9", 76)

	run := func(limit uint64) (uint64, uint32, error) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4,
			ArgumentCount: 5, ArgumentArena: 6,
		})
		require.NoError(t, err)
		exec, err := MakeAgg(mp, AggIdOfJsonArrayAgg, false, typ)
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		input := buildJSONDecimal256Vector(t, mp, 76, 0, []string{maxText})
		err = exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, []uint64{1}, []*vector.Vector{input})
		published := exec.(*jsonArrayAggExec).state[0].argCnt[0]
		peak := account.Snapshot().Peak
		input.Free(mp)
		exec.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
		return peak, published, err
	}

	peak, published, err := run(128 << 20)
	require.NoError(t, err)
	require.Zero(t, published)
	require.Positive(t, peak)
	_, published, err = run(peak)
	require.NoError(t, err)
	require.Zero(t, published)
	_, published, err = run(peak - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, published)
}

func buildJSONDecimalVectorForType(
	t *testing.T, mp *mpool.MPool, typ types.Type, values []string,
) *vector.Vector {
	t.Helper()
	switch typ.Oid {
	case types.T_decimal64:
		return buildJSONDecimal64Vector(t, mp, typ.Width, typ.Scale, values)
	case types.T_decimal128:
		return buildJSONDecimal128Vector(t, mp, typ.Width, typ.Scale, values)
	case types.T_decimal256:
		return buildJSONDecimal256Vector(t, mp, typ.Width, typ.Scale, values)
	default:
		t.Fatalf("unsupported decimal test type: %v", typ)
		return nil
	}
}

func makeJSONDistinctAggregateForTest(
	t *testing.T,
	mp *mpool.MPool,
	id int64,
	typ types.Type,
	allocation *AllocationAccount,
) (AggFuncExec, AllocationAccountOwner) {
	t.Helper()
	params := []types.Type{typ}
	if id == AggIdOfJsonObjectAgg {
		params = []types.Type{types.T_varchar.ToType(), typ}
	}
	exec, err := MakeAgg(mp, id, true, params...)
	require.NoError(t, err)
	var owner AllocationAccountOwner
	if allocation != nil {
		owner = exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	return exec, owner
}

func runJSONAggregateValues(
	t *testing.T,
	mp *mpool.MPool,
	typ types.Type,
	values, keys *vector.Vector,
	accounted bool,
) (bytejson.ByteJson, bytejson.ByteJson) {
	t.Helper()
	array, arrayOwner, arrayRegistry, arrayAccount := makeJSONAggregateForPath(t, mp, AggIdOfJsonArrayAgg, typ, accounted)
	object, objectOwner, objectRegistry, objectAccount := makeJSONAggregateForPath(t, mp, AggIdOfJsonObjectAgg, typ, accounted)
	groups := make([]uint64, values.Length())
	for i := range groups {
		groups[i] = 1
	}
	if accounted {
		require.NoError(t, array.(BatchCapacityPreflight).PreflightBatchFill(0, groups, []*vector.Vector{values}))
		require.NoError(t, object.(BatchCapacityPreflight).PreflightBatchFill(0, groups, []*vector.Vector{keys, values}))
	}
	require.NoError(t, array.BatchFill(0, groups, []*vector.Vector{values}))
	require.NoError(t, object.BatchFill(0, groups, []*vector.Vector{keys, values}))
	arrayResult, err := array.Flush()
	require.NoError(t, err)
	objectResult, err := object.Flush()
	require.NoError(t, err)
	arrayValue := types.DecodeJson(append([]byte(nil), arrayResult[0].GetBytesAt(0)...))
	objectValue := types.DecodeJson(append([]byte(nil), objectResult[0].GetBytesAt(0)...))
	arrayResult[0].Free(mp)
	objectResult[0].Free(mp)
	array.Free()
	object.Free()
	if accounted {
		require.NoError(t, arrayOwner.ClearAllocationAccount(arrayAccount))
		require.NoError(t, objectOwner.ClearAllocationAccount(objectAccount))
		finishTestAggregateAllocation(t, arrayRegistry, arrayAccount.account)
		finishTestAggregateAllocation(t, objectRegistry, objectAccount.account)
	}
	return arrayValue, objectValue
}

func makeJSONAggregateForPath(
	t *testing.T,
	mp *mpool.MPool,
	id int64,
	typ types.Type,
	accounted bool,
) (AggFuncExec, AllocationAccountOwner, *mpool.AllocationAccountRegistry, *AllocationAccount) {
	t.Helper()
	var allocation *AllocationAccount
	var registry *mpool.AllocationAccountRegistry
	if accounted {
		registry, _, allocation = newTestAggregateAllocation(t)
	}
	exec, err := MakeAgg(mp, id, false, func() []types.Type {
		if id == AggIdOfJsonObjectAgg {
			return []types.Type{types.T_varchar.ToType(), typ}
		}
		return []types.Type{typ}
	}()...)
	require.NoError(t, err)
	var owner AllocationAccountOwner
	if accounted {
		owner = exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	require.NoError(t, exec.GroupGrow(1))
	return exec, owner, registry, allocation
}

func makeJSONAggregateForTest(
	t *testing.T,
	mp *mpool.MPool,
	id int64,
	typ types.Type,
	accounted bool,
	shared ...*AllocationAccount,
) (AggFuncExec, AllocationAccountOwner, *AllocationAccount, *mpool.AllocationAccountRegistry) {
	t.Helper()
	var allocation *AllocationAccount
	var registry *mpool.AllocationAccountRegistry
	if accounted {
		if len(shared) > 0 {
			allocation = shared[0]
		} else {
			registry, _, allocation = newTestAggregateAllocation(t)
		}
	}
	exec, err := MakeAgg(mp, id, false, func() []types.Type {
		if id == AggIdOfJsonObjectAgg {
			return []types.Type{types.T_varchar.ToType(), typ}
		}
		return []types.Type{typ}
	}()...)
	require.NoError(t, err)
	var owner AllocationAccountOwner
	if accounted {
		owner = exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	return exec, owner, allocation, registry
}

func fillJSONAggregateForTest(
	t *testing.T,
	exec AggFuncExec,
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
	accounted bool,
) {
	t.Helper()
	if accounted {
		require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(offset, groups, vectors))
	}
	require.NoError(t, exec.BatchFill(offset, groups, vectors))
}

func makeJSONDistinctArrayForTest(
	t *testing.T,
	mp *mpool.MPool,
	typ *types.Type,
	allocation *AllocationAccount,
) (AggFuncExec, AllocationAccountOwner, *AllocationAccount) {
	t.Helper()
	exec, err := MakeAgg(mp, AggIdOfJsonArrayAgg, true, *typ)
	require.NoError(t, err)
	var owner AllocationAccountOwner
	if allocation != nil {
		owner = exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	require.NoError(t, exec.GroupGrow(1))
	return exec, owner, allocation
}

func assertJSONWithNumbers(t *testing.T, value bytejson.ByteJson, want any) {
	t.Helper()
	visible, err := value.MarshalJSON()
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(visible))
	decoder.UseNumber()
	var got any
	require.NoError(t, decoder.Decode(&got))
	require.Equal(t, want, got)
}
