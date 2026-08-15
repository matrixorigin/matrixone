// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAccountedJSONAggregatesLifecycleAndSpill(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	jsonValue := func(value any) []byte {
		t.Helper()
		bj, err := bytejson.CreateByteJSONWithCheck(value)
		require.NoError(t, err)
		data, err := bj.Marshal()
		require.NoError(t, err)
		return data
	}
	values := vector.NewVec(types.T_json.ToType())
	for _, value := range [][]byte{
		jsonValue(int64(1)),
		jsonValue("two"),
		jsonValue(int64(1)),
		jsonValue(nil),
		jsonValue(nil),
	} {
		require.NoError(t, vector.AppendBytes(values, value, false, mp))
	}
	keys := buildVarlenVec(t, mp, types.T_varchar.ToType(),
		[]string{"b", "a", "a", "c", "c"})
	defer values.Free(mp)
	defer keys.Free(mp)

	for _, tc := range []struct {
		name     string
		id       int64
		distinct bool
		params   []types.Type
		vectors  []*vector.Vector
		want     string
	}{
		{
			name: "array-distinct", id: AggIdOfJsonArrayAgg, distinct: true,
			params:  []types.Type{types.T_json.ToType()},
			vectors: []*vector.Vector{values},
			want:    `[1,"two",null,null]`,
		},
		{
			name: "object-last-wins", id: AggIdOfJsonObjectAgg,
			params:  []types.Type{types.T_varchar.ToType(), types.T_json.ToType()},
			vectors: []*vector.Vector{keys, values},
			want:    `{"a":1,"b":1,"c":null}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := MakeAgg(mp, tc.id, tc.distinct, tc.params...)
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			require.NoError(t, exec.GroupGrow(1))
			groups := slices.Repeat([]uint64{1}, values.Length())
			preflight := exec.(BatchCapacityPreflight)
			require.NoError(t, preflight.PreflightBatchFill(0, groups, tc.vectors))
			peak := account.Snapshot().Peak
			require.NoError(t, exec.BatchFill(0, groups, tc.vectors))
			require.Equal(t, peak, account.Snapshot().Peak,
				"mutation must reuse preflighted retained capacity")

			var spill bytes.Buffer
			require.NoError(t, exec.(SpillStateCodec).SaveSpillIntermediateResult(
				1, 0, []uint8{1}, &spill))
			restored, err := MakeAgg(mp, tc.id, tc.distinct, tc.params...)
			require.NoError(t, err)
			restoredOwner := restored.(AllocationAccountOwner)
			require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
			require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
				bytes.NewReader(spill.Bytes()), mp))
			results, err := restored.Flush()
			require.NoError(t, err)
			visible, err := types.DecodeJson(results[0].GetBytesAt(0)).MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(visible))
			results[0].Free(mp)
			exec.Free()
			restored.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
		})
	}

	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedJSONPreflightOneByteShortDoesNotPublish(t *testing.T) {
	value, err := bytejson.CreateByteJSONWithCheck(map[string]any{
		"payload": strings.Repeat("x", 4096),
	})
	require.NoError(t, err)
	raw, err := value.Marshal()
	require.NoError(t, err)

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
		exec, err := MakeAgg(mp, AggIdOfJsonArrayAgg, false, types.T_json.ToType())
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		input := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(input, raw, false, mp))
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
	_, published, err = run(peak - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, published)
}

func TestAccountedJSONAggregatePreservesLegacyValueSemantics(t *testing.T) {
	mp := mpool.MustNewZero()

	tests := []struct {
		name string
		vec  *vector.Vector
	}{
		{
			name: "decimal64-integer",
			vec:  buildFixedVec(t, mp, types.New(types.T_decimal64, 18, 0), []types.Decimal64{123}),
		},
		{
			name: "decimal64-fraction",
			vec:  buildFixedVec(t, mp, types.New(types.T_decimal64, 18, 2), []types.Decimal64{123}),
		},
		{
			name: "decimal128",
			vec: buildFixedVec(t, mp, types.New(types.T_decimal128, 38, 0),
				[]types.Decimal128{{B0_63: 456}}),
		},
		{
			name: "date",
			vec:  buildFixedVec(t, mp, types.T_date.ToType(), []types.Date{types.Date(1)}),
		},
		{
			name: "time",
			vec:  buildFixedVec(t, mp, types.T_time.ToType(), []types.Time{types.Time(1)}),
		},
		{
			name: "datetime",
			vec: buildFixedVec(t, mp, types.T_datetime.ToType(),
				[]types.Datetime{types.Datetime(1)}),
		},
		{
			name: "timestamp",
			vec: buildFixedVec(t, mp, types.T_timestamp.ToType(),
				[]types.Timestamp{types.Timestamp(1)}),
		},
		{
			name: "array-float32",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_float32.ToType(), []float32{1.5, -2.5}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
		{
			name: "array-float64",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_float64.ToType(), []float64{1.5, -2.5}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
		{
			name: "array-bf16",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_bf16.ToType(), []types.BF16{
						types.BF16FromFloat32(1.5), types.BF16FromFloat32(-2.5)}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
		{
			name: "array-float16",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_float16.ToType(), []types.Float16{
						types.Float16FromFloat32(1.5), types.Float16FromFloat32(-2.5)}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
		{
			name: "array-int8",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_int8.ToType(), []int8{-128, 127}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
		{
			name: "array-uint8",
			vec: func() *vector.Vector {
				vec, err := vector.NewConstArray(
					types.T_array_uint8.ToType(), []uint8{0, 255}, 1, mp)
				require.NoError(t, err)
				return vec
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer test.vec.Free(mp)
			legacy := runJSONArrayAggregate(t, mp, test.vec, nil)

			registry, account, allocation := newTestAggregateAllocation(t)
			accounted := runJSONArrayAggregate(t, mp, test.vec, allocation)
			finishTestAggregateAllocation(t, registry, account)

			require.Equal(t, legacy.Type, accounted.Type)
			require.Equal(t, legacy.Data, accounted.Data)
			legacyElement := legacy.GetArrayElem(0)
			accountedElement := accounted.GetArrayElem(0)
			require.Equal(t, legacyElement.TYPE(), accountedElement.TYPE())
			require.Equal(t, legacyElement.Data, accountedElement.Data)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func runJSONArrayAggregate(
	t *testing.T,
	mp *mpool.MPool,
	input *vector.Vector,
	allocation *AllocationAccount,
) bytejson.ByteJson {
	t.Helper()
	exec, err := MakeAgg(mp, AggIdOfJsonArrayAgg, false, *input.GetType())
	require.NoError(t, err)
	var owner AllocationAccountOwner
	if allocation != nil {
		owner = exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	require.NoError(t, exec.GroupGrow(1))
	groups := []uint64{1}
	if allocation != nil {
		require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, groups, []*vector.Vector{input}))
	}
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
	results, err := exec.Flush()
	require.NoError(t, err)
	data := append([]byte(nil), results[0].GetBytesAt(0)...)
	result := types.DecodeJson(data)
	results[0].Free(mp)
	exec.Free()
	if allocation != nil {
		require.NoError(t, owner.ClearAllocationAccount(allocation))
	}
	return result
}

func buildFixedVec[T types.FixedSizeTExceptStrType](t *testing.T, mp *mpool.MPool, typ types.Type, vals []T) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList[T](v, vals, nil, mp))
	return v
}

func buildVarlenVec(t *testing.T, mp *mpool.MPool, typ types.Type, vals []string) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	for _, s := range vals {
		require.NoError(t, vector.AppendBytes(v, []byte(s), false, mp))
	}
	return v
}
func fromValueListToVector(
	mp *mpool.MPool,
	typ types.Type, values any, isNull []bool) *vector.Vector {
	var err error

	v := vector.NewVec(typ)

	if typ.IsVarlen() {
		sts := values.([]string)

		if len(isNull) > 0 {
			for i, value := range sts {
				if err = vector.AppendBytes(v, []byte(value), isNull[i], mp); err != nil {
					break
				}
			}
		} else {
			for _, value := range sts {
				if err = vector.AppendBytes(v, []byte(value), false, mp); err != nil {
					break
				}
			}
		}

	} else {
		switch typ.Oid {
		case types.T_int64:
			err = vector.AppendFixedList[int64](v, values.([]int64), isNull, mp)

		case types.T_bool:
			err = vector.AppendFixedList[bool](v, values.([]bool), isNull, mp)

		case types.T_decimal128:
			err = vector.AppendFixedList[types.Decimal128](v, values.([]types.Decimal128), isNull, mp)

		default:
			panic(fmt.Sprintf("test util do not support the type %s now", typ))
		}
	}

	if err != nil {
		panic(err)
	}
	return v
}

func TestJsonArrayAggBinaryUnsupported(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     32,
		distinct:  false,
		argTypes:  []types.Type{types.T_binary.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	vec := fromValueListToVector(mg, types.T_binary.ToType(), []string{"abc"}, nil)
	err := exec.Fill(0, 0, []*vector.Vector{vec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "binary data not supported")

	vec.Free(mg)
	exec.Free()
}

func TestJsonObjectAggKeyMustBeString(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     33,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	keyVec := fromValueListToVector(mg, types.T_int64.ToType(), []int64{1}, nil)
	valVec := fromValueListToVector(mg, types.T_int64.ToType(), []int64{2}, nil)
	err := exec.Fill(0, 0, []*vector.Vector{keyVec, valVec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "key must be a string")

	keyVec.Free(mg)
	valVec.Free(mg)
	exec.Free()
}

func TestJsonObjectAggPreAllocate(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     34,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.PreAllocateGroups(2))
	require.NoError(t, exec.GroupGrow(1))
	require.Equal(t, 1, exec.GetNumGroups())

	keyVec := fromValueListToVector(mg, types.T_varchar.ToType(), []string{"k"}, nil)
	valVec := fromValueListToVector(mg, types.T_varchar.ToType(), []string{"v"}, nil)
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{keyVec, valVec}))

	keyVec.Free(mg)
	valVec.Free(mg)
	exec.Free()
}

func TestBuildValueByteJsonCoversTypes(t *testing.T) {
	mg := mpool.MustNewZero()

	cases := []struct {
		name    string
		vec     *vector.Vector
		row     uint64
		wantVal any
		wantErr string
	}{
		{"any-null", vector.NewConstNull(types.T_any.ToType(), 1, mg), 0, nil, ""},
		{"bool", buildFixedVec(t, mg, types.T_bool.ToType(), []bool{true}), 0, true, ""},
		{"int32", buildFixedVec(t, mg, types.T_int32.ToType(), []int32{3}), 0, float64(3), ""},
		{"uint64", buildFixedVec(t, mg, types.T_uint64.ToType(), []uint64{7}), 0, float64(7), ""},
		{"float64", buildFixedVec(t, mg, types.T_float64.ToType(), []float64{1.25}), 0, 1.25, ""},
		{"decimal64", buildFixedVec(t, mg, types.T_decimal64.ToType(), []types.Decimal64{123}), 0, float64(123), ""},
		{"decimal128", buildFixedVec(t, mg, types.T_decimal128.ToType(), []types.Decimal128{{B0_63: 456}}), 0, float64(456), ""},
		{"date", buildFixedVec(t, mg, types.T_date.ToType(), []types.Date{types.Date(1)}), 0, "0001-01-02", ""},
		{"time", buildFixedVec(t, mg, types.T_time.ToType(), []types.Time{types.Time(1)}), 0, "00:00:00", ""},
		{"datetime", buildFixedVec(t, mg, types.T_datetime.ToType(), []types.Datetime{types.Datetime(1)}), 0, "0001-01-01 00:00:00", ""},
		{"timestamp", buildFixedVec(t, mg, types.T_timestamp.ToType(), []types.Timestamp{types.Timestamp(1)}), 0, "0001-01-01 00:00:00.000001 UTC", ""},
		{"string", buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"hi"}), 0, "hi", ""},
		{"array-f32", func() *vector.Vector {
			v := vector.NewVec(types.T_array_float32.ToType())
			data := types.ArrayToBytes([]float32{1.5, 2.5})
			require.NoError(t, vector.AppendBytes(v, data, false, mg))
			return v
		}(), 0, []any{1.5, 2.5}, ""},
		{"array-f64", func() *vector.Vector {
			v := vector.NewVec(types.T_array_float64.ToType())
			data := types.ArrayToBytes([]float64{3.5, 4.5})
			require.NoError(t, vector.AppendBytes(v, data, false, mg))
			return v
		}(), 0, []any{3.5, 4.5}, ""},
		{"uuid", func() *vector.Vector {
			v := vector.NewVec(types.T_uuid.ToType())
			id, err := types.ParseUuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixedList[types.Uuid](v, []types.Uuid{id}, nil, mg))
			return v
		}(), 0, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", ""},
		{"json", func() *vector.Vector {
			v := vector.NewVec(types.T_json.ToType())
			bj, err := bytejson.CreateByteJSONWithCheck(map[string]any{"a": float64(1)})
			require.NoError(t, err)
			raw, err := bj.Marshal()
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(v, raw, false, mg))
			return v
		}(), 0, map[string]any{"a": float64(1)}, ""},
		// Narrow vector element types. json_arrayagg on such a column fell to
		// the default arm and failed with "unsupported type for json aggregate"
		// while the vecf32 column beside it worked. All four widen to JSON
		// numbers, exactly as the vecf32 arm does — the point of the fixtures
		// below is that a WRONG element type is visible in the value, not just
		// in the rounding: -128/127 for int8 and 0/255 for uint8 alias the same
		// two bytes, so a shared arm would swap them.
		{"array_bf16", func() *vector.Vector {
			v := vector.NewVec(types.T_array_bf16.ToType())
			require.NoError(t, vector.AppendArrayList[types.BF16](v, [][]types.BF16{
				{types.BF16FromFloat32(1.5), types.BF16FromFloat32(-2.5)}}, nil, mg))
			return v
		}(), 0, []any{1.5, -2.5}, ""},
		{"array_float16", func() *vector.Vector {
			v := vector.NewVec(types.T_array_float16.ToType())
			require.NoError(t, vector.AppendArrayList[types.Float16](v, [][]types.Float16{
				{types.Float16FromFloat32(1.5), types.Float16FromFloat32(-2.5)}}, nil, mg))
			return v
		}(), 0, []any{1.5, -2.5}, ""},
		{"array_int8", func() *vector.Vector {
			v := vector.NewVec(types.T_array_int8.ToType())
			require.NoError(t, vector.AppendArrayList[int8](v, [][]int8{{-128, 127}}, nil, mg))
			return v
		}(), 0, []any{float64(-128), float64(127)}, ""},
		{"array_uint8", func() *vector.Vector {
			v := vector.NewVec(types.T_array_uint8.ToType())
			require.NoError(t, vector.AppendArrayList[uint8](v, [][]uint8{{0, 255}}, nil, mg))
			return v
		}(), 0, []any{float64(0), float64(255)}, ""},
		{"binary-error", buildVarlenVec(t, mg, types.T_binary.ToType(), []string{"a"}), 0, "", "binary data not supported"},
		{"unsupported", buildFixedVec(t, mg, types.T_decimal256.ToType(), []types.Decimal256{{}}), 0, "", "unsupported type"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.vec.Free(mg)
			res, err := buildValueByteJson(tt.vec, tt.row)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			j, err := res.MarshalJSON()
			require.NoError(t, err)
			var got any
			require.NoError(t, json.Unmarshal(j, &got))
			require.Equal(t, tt.wantVal, got)
		})
	}
}

func TestJsonAggHelpers(t *testing.T) {
	exec := newJsonArrayAggExec(mpool.MustNewZero(), multiAggInfo{
		aggID:     0,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	})
	require.NoError(t, exec.GroupGrow(1))
	require.Equal(t, 1, exec.GetNumGroups())
	exec.Free()
}

func TestJsonArrayAggPreAllocateAndSize(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     40,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec.PreAllocateGroups(2))
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	require.NotNil(t, exec.GetOptResult())
	require.GreaterOrEqual(t, exec.Size(), int64(0))
	exec.Free()
}

func TestJsonObjectAggBulkFillAndSize(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     41,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	require.NotNil(t, exec.GetOptResult())

	keys := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"k1", "k2"})
	vals := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"v1", "v2"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{keys, vals}))
	require.Greater(t, exec.Size(), int64(0))

	keys.Free(mg)
	vals.Free(mg)
	exec.Free()
}

func TestJsonArrayAggBatchMergeSkip(t *testing.T) {
	mg := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     45,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec1 := newJsonArrayAggExec(mg, info)
	exec2 := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec1.GroupGrow(2))
	require.NoError(t, exec2.GroupGrow(2))

	val := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"x"})
	require.NoError(t, exec2.Fill(0, 0, []*vector.Vector{val}))

	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{GroupNotMatched, 1}))

	val.Free(mg)
	exec1.Free()
	exec2.Free()
}

func TestJsonAggDistinctAndMergeErrorPaths(t *testing.T) {
	mg := mpool.MustNewZero()

	arrayInfo := multiAggInfo{
		aggID:     46,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	arrayExec := newJsonArrayAggExec(mg, arrayInfo)
	require.NoError(t, arrayExec.GroupGrow(1))
	require.True(t, arrayExec.IsDistinct())
	values := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(values, []byte("x"), false, mg))
	require.NoError(t, vector.AppendBytes(values, []byte("x"), false, mg))
	require.NoError(t, vector.AppendBytes(values, nil, true, mg))
	require.NoError(t, arrayExec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{values}))
	require.NoError(t, arrayExec.BulkFill(0, []*vector.Vector{values}))
	vecs, err := arrayExec.Flush()
	require.NoError(t, err)
	j, err := types.DecodeJson(vecs[0].GetBytesAt(0)).MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `["x",null,null]`, string(j))

	objectInfo := multiAggInfo{
		aggID:     47,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	left := newJsonObjectAggExec(mg, objectInfo)
	right := newJsonObjectAggExec(mg, objectInfo)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.True(t, left.IsDistinct())
	keys := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"k"})
	vals := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"v"})
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{keys, vals}))
	require.NoError(t, right.Fill(0, 0, []*vector.Vector{keys, vals}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{keys, vals}))
	require.Error(t, left.Merge(right, 0, 0))
	err = left.BatchMerge(right, 0, []uint64{1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "distinct agg should be run in only one node")

	values.Free(mg)
	vecs[0].Free(mg)
	keys.Free(mg)
	vals.Free(mg)
	arrayExec.Free()
	left.Free()
	right.Free()
}

func TestJsonAggNonDistinctWrapperPaths(t *testing.T) {
	mg := mpool.MustNewZero()

	arrayInfo := multiAggInfo{
		aggID:     48,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	leftArray := newJsonArrayAggExec(mg, arrayInfo)
	rightArray := newJsonArrayAggExec(mg, arrayInfo)
	require.NoError(t, leftArray.PreAllocateGroups(1))
	require.NoError(t, leftArray.GroupGrow(1))
	require.NoError(t, rightArray.GroupGrow(1))
	ints := buildFixedVec(t, mg, types.T_int64.ToType(), []int64{1, 2})
	require.NoError(t, leftArray.Fill(0, 0, []*vector.Vector{ints}))
	require.NoError(t, rightArray.Fill(0, 1, []*vector.Vector{ints}))
	require.NoError(t, leftArray.Merge(rightArray, 0, 0))

	objectInfo := multiAggInfo{
		aggID:     49,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	leftObject := newJsonObjectAggExec(mg, objectInfo)
	rightObject := newJsonObjectAggExec(mg, objectInfo)
	require.NoError(t, leftObject.PreAllocateGroups(1))
	require.NoError(t, leftObject.GroupGrow(1))
	require.NoError(t, rightObject.GroupGrow(1))
	keys := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"a", "b"})
	vals := buildVarlenVec(t, mg, types.T_varchar.ToType(), []string{"x", "y"})
	require.NoError(t, leftObject.Fill(0, 0, []*vector.Vector{keys, vals}))
	require.NoError(t, rightObject.Fill(0, 1, []*vector.Vector{keys, vals}))
	require.NoError(t, leftObject.Merge(rightObject, 0, 0))

	ints.Free(mg)
	keys.Free(mg)
	vals.Free(mg)
	leftArray.Free()
	rightArray.Free()
	leftObject.Free()
	rightObject.Free()
}
