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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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

func TestJsonArrayAggMarshalUnmarshal(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     10,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}

	exec := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	nullVec := vector.NewConstNull(types.T_varchar.ToType(), 1, mg.Mp()) // nolint:staticcheck
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{nullVec}))       // cover const null handling

	vec := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"x", "y"}, nil)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	data, err := exec.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	execCopy := newJsonArrayAggExec(mg, info)
	require.NoError(t, execCopy.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))

	results, err := execCopy.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	bj := types.DecodeJson(results[0].GetBytesAt(0))
	jsonBytes, err := bj.MarshalJSON()
	require.NoError(t, err)
	var got []any
	require.NoError(t, json.Unmarshal(jsonBytes, &got))
	require.Equal(t, []any{nil, "x", "y"}, got)

	nullVec.Free(mg.Mp())
	vec.Free(mg.Mp())
	exec.Free()
	execCopy.Free()
}

func TestJsonArrayAggDistinctAndMerge(t *testing.T) {
	mg := hackAggMemoryManager()
	infoDistinct := multiAggInfo{
		aggID:     11,
		distinct:  true,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}

	exec := newJsonArrayAggExec(mg, infoDistinct)
	require.NoError(t, exec.GroupGrow(2))

	vec := fromValueListToVector(mg.Mp(), types.T_int64.ToType(), []int64{1, 1, 2}, nil)
	require.NoError(t, exec.BatchFill(0, []uint64{GroupNotMatched, 1, 2}, []*vector.Vector{vec}))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
	require.Greater(t, exec.Size(), int64(0))

	// Cover merge path on distinct exec when no data has been added to the peer.
	emptyPeer := newJsonArrayAggExec(mg, infoDistinct)
	require.NoError(t, emptyPeer.GroupGrow(1))
	require.Error(t, exec.Merge(emptyPeer, 0, 0))

	vec.Free(mg.Mp())
	exec.Free()
	emptyPeer.Free()

	infoNonDistinct := multiAggInfo{
		aggID:     12,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}

	exec1 := newJsonArrayAggExec(mg, infoNonDistinct)
	exec2 := newJsonArrayAggExec(mg, infoNonDistinct)
	require.NoError(t, exec1.GroupGrow(2))
	require.NoError(t, exec2.GroupGrow(2))

	vecA := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"a", "b"}, nil)
	vecB := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"c"}, nil)

	require.NoError(t, exec1.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vecA}))
	require.NoError(t, exec2.Fill(0, 0, []*vector.Vector{vecB}))
	require.NoError(t, exec2.Fill(1, 0, []*vector.Vector{vecB}))

	require.NoError(t, exec1.Merge(exec2, 0, 0))
	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{1, 2}))

	data, err := exec1.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	execCopy := newJsonArrayAggExec(mg, infoNonDistinct)
	require.NoError(t, execCopy.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))

	rs, err := execCopy.Flush()
	require.NoError(t, err)
	require.Len(t, rs, 1)
	require.Equal(t, 2, rs[0].Length())

	vecA.Free(mg.Mp())
	vecB.Free(mg.Mp())
	exec1.Free()
	exec2.Free()
	execCopy.Free()
}

func TestJsonObjectAggFlow(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     20,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}

	exec1 := newJsonObjectAggExec(mg, info)
	exec2 := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec1.GroupGrow(2))
	require.NoError(t, exec2.GroupGrow(2))

	keyVec := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"k1", "k2"}, nil)
	valVec := fromValueListToVector(
		mg.Mp(),
		types.T_int64.ToType(),
		[]int64{1, 2},
		fromIdxListToNullList(0, 1, []int{1}),
	)
	require.NoError(t, exec1.BatchFill(0, []uint64{1, 2}, []*vector.Vector{keyVec, valVec}))

	keyVec2 := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"k2"}, nil)
	valVec2 := fromValueListToVector(mg.Mp(), types.T_int64.ToType(), []int64{3}, nil)
	require.NoError(t, exec2.Fill(0, 0, []*vector.Vector{keyVec2, valVec2}))
	require.NoError(t, exec2.Fill(1, 0, []*vector.Vector{keyVec2, valVec2}))

	require.NoError(t, exec1.Merge(exec2, 0, 0))
	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{1, 2}))

	data, err := exec1.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	execCopy := newJsonObjectAggExec(mg, info)
	require.NoError(t, execCopy.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))

	rs, err := execCopy.Flush()
	require.NoError(t, err)
	require.Len(t, rs, 1)

	payload := rs[0].GetBytesAt(0)
	bj := types.DecodeJson(payload)
	raw, err := bj.MarshalJSON()
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(raw, &obj))
	require.Contains(t, obj, "k1")
	require.Contains(t, obj, "k2")

	keyVec.Free(mg.Mp())
	valVec.Free(mg.Mp())
	keyVec2.Free(mg.Mp())
	valVec2.Free(mg.Mp())
	exec1.Free()
	exec2.Free()
	execCopy.Free()
}

func TestJsonArrayAggDistinctRoundTrip(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     30,
		distinct:  true,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	intVec := fromValueListToVector(mg.Mp(), types.T_int64.ToType(), []int64{1, 2, 2}, nil)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{intVec}))

	data, err := exec.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	execCopy := newJsonArrayAggExec(mg, info)
	require.NoError(t, execCopy.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))

	rs, err := execCopy.Flush()
	require.NoError(t, err)
	require.Len(t, rs, 1)

	bj := types.DecodeJson(rs[0].GetBytesAt(0))
	raw, err := bj.MarshalJSON()
	require.NoError(t, err)

	var got []any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, []any{float64(1), float64(2)}, got)

	intVec.Free(mg.Mp())
	exec.Free()
	execCopy.Free()
}

func TestJsonArrayAggDistinctUnmarshalMissingData(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     31,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonArrayAggExec(mg, info)
	result, empties, err := exec.ret.marshalToBytes()
	require.NoError(t, err)

	err = exec.unmarshal(nil, result, empties, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "distinct data missing")
	exec.Free()
}

func TestJsonArrayAggBinaryUnsupported(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     32,
		distinct:  false,
		argTypes:  []types.Type{types.T_binary.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	vec := fromValueListToVector(mg.Mp(), types.T_binary.ToType(), []string{"abc"}, nil)
	err := exec.Fill(0, 0, []*vector.Vector{vec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "binary data not supported")

	vec.Free(mg.Mp())
	exec.Free()
}

func TestJsonObjectAggKeyMustBeString(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     33,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	keyVec := fromValueListToVector(mg.Mp(), types.T_int64.ToType(), []int64{1}, nil)
	valVec := fromValueListToVector(mg.Mp(), types.T_int64.ToType(), []int64{2}, nil)
	err := exec.Fill(0, 0, []*vector.Vector{keyVec, valVec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "key must be a string")

	keyVec.Free(mg.Mp())
	valVec.Free(mg.Mp())
	exec.Free()
}

func TestJsonObjectAggPreAllocate(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     34,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.PreAllocateGroups(2))
	require.Len(t, exec.groups, 2)
	require.NoError(t, exec.GroupGrow(1))

	keyVec := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"k"}, nil)
	valVec := fromValueListToVector(mg.Mp(), types.T_varchar.ToType(), []string{"v"}, nil)
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{keyVec, valVec}))

	data, err := exec.marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	keyVec.Free(mg.Mp())
	valVec.Free(mg.Mp())
	exec.Free()
}

func TestBuildValueByteJsonCoversTypes(t *testing.T) {
	mg := hackAggMemoryManager()

	cases := []struct {
		name    string
		vec     *vector.Vector
		row     uint64
		wantVal any
		wantErr string
	}{
		{"any-null", vector.NewConstNull(types.T_any.ToType(), 1, mg.Mp()), 0, nil, ""},
		{"bool", buildFixedVec(t, mg.Mp(), types.T_bool.ToType(), []bool{true}), 0, true, ""},
		{"int32", buildFixedVec(t, mg.Mp(), types.T_int32.ToType(), []int32{3}), 0, float64(3), ""},
		{"uint64", buildFixedVec(t, mg.Mp(), types.T_uint64.ToType(), []uint64{7}), 0, float64(7), ""},
		{"float64", buildFixedVec(t, mg.Mp(), types.T_float64.ToType(), []float64{1.25}), 0, 1.25, ""},
		{"decimal64", buildFixedVec(t, mg.Mp(), types.T_decimal64.ToType(), []types.Decimal64{123}), 0, float64(123), ""},
		{"decimal128", buildFixedVec(t, mg.Mp(), types.T_decimal128.ToType(), []types.Decimal128{{B0_63: 456}}), 0, float64(456), ""},
		{"date", buildFixedVec(t, mg.Mp(), types.T_date.ToType(), []types.Date{types.Date(1)}), 0, "0001-01-02", ""},
		{"time", buildFixedVec(t, mg.Mp(), types.T_time.ToType(), []types.Time{types.Time(1)}), 0, "00:00:00", ""},
		{"datetime", buildFixedVec(t, mg.Mp(), types.T_datetime.ToType(), []types.Datetime{types.Datetime(1)}), 0, "0001-01-01 00:00:00", ""},
		{"timestamp", buildFixedVec(t, mg.Mp(), types.T_timestamp.ToType(), []types.Timestamp{types.Timestamp(1)}), 0, "0001-01-01 00:00:00.000001 UTC", ""},
		{"string", buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"hi"}), 0, "hi", ""},
		{"array-f32", func() *vector.Vector {
			v := vector.NewVec(types.T_array_float32.ToType())
			data := types.ArrayToBytes([]float32{1.5, 2.5})
			require.NoError(t, vector.AppendBytes(v, data, false, mg.Mp()))
			return v
		}(), 0, []any{1.5, 2.5}, ""},
		{"array-f64", func() *vector.Vector {
			v := vector.NewVec(types.T_array_float64.ToType())
			data := types.ArrayToBytes([]float64{3.5, 4.5})
			require.NoError(t, vector.AppendBytes(v, data, false, mg.Mp()))
			return v
		}(), 0, []any{3.5, 4.5}, ""},
		{"uuid", func() *vector.Vector {
			v := vector.NewVec(types.T_uuid.ToType())
			id, err := types.ParseUuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixedList[types.Uuid](v, []types.Uuid{id}, nil, mg.Mp()))
			return v
		}(), 0, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", ""},
		{"json", func() *vector.Vector {
			v := vector.NewVec(types.T_json.ToType())
			bj, err := bytejson.CreateByteJSONWithCheck(map[string]any{"a": float64(1)})
			require.NoError(t, err)
			raw, err := bj.Marshal()
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(v, raw, false, mg.Mp()))
			return v
		}(), 0, map[string]any{"a": float64(1)}, ""},
		{"binary-error", buildVarlenVec(t, mg.Mp(), types.T_binary.ToType(), []string{"a"}), 0, "", "binary data not supported"},
		{"unsupported", buildFixedVec(t, mg.Mp(), types.T_decimal256.ToType(), []types.Decimal256{{}}), 0, "", "unsupported type"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.vec.Free(mg.Mp())
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

func TestJsonAggRegistersAndHelpers(t *testing.T) {
	RegisterJsonArrayAgg(101)
	RegisterJsonObjectAgg(202)
	require.Equal(t, int64(101), AggIdOfJsonArrayAgg)
	require.Equal(t, int64(202), AggIdOfJsonObjectAgg)

	exec := newJsonArrayAggExec(hackAggMemoryManager(), multiAggInfo{
		aggID:     0,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	})
	exec.ensureGroup(0)
	exec.ensureGroup(0) // cover else branch
	exec.Free()
}

func TestJsonArrayAggPreAllocateAndSize(t *testing.T) {
	mg := hackAggMemoryManager()
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
	mg := hackAggMemoryManager()
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

	keys := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"k1", "k2"})
	vals := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"v1", "v2"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{keys, vals}))
	require.Greater(t, exec.Size(), int64(0))

	keys.Free(mg.Mp())
	vals.Free(mg.Mp())
	exec.Free()
}

func TestJsonObjectAggDistinctPaths(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     42,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec.GroupGrow(1))

	keyVec := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"k"})
	valVec := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"v"})
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{keyVec, valVec}))

	data, err := exec.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	execCopy := newJsonObjectAggExec(mg, info)
	require.NoError(t, execCopy.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))

	require.Error(t, exec.Merge(execCopy, 0, 0))

	keyVec.Free(mg.Mp())
	valVec.Free(mg.Mp())
	exec.Free()
	execCopy.Free()
}

func TestJsonArrayAggUnmarshalWithEmptyGroups(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     43,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec1 := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec1.GroupGrow(2))
	vec := buildFixedVec(t, mg.Mp(), types.T_int64.ToType(), []int64{1})
	require.NoError(t, exec1.Fill(0, 0, []*vector.Vector{vec}))
	data, err := exec1.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	exec2 := newJsonArrayAggExec(mg, info)
	require.NoError(t, exec2.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))
	exec2.Free()
	vec.Free(mg.Mp())
	exec1.Free()
}

func TestJsonObjectAggUnmarshalWithEmptyGroups(t *testing.T) {
	mg := hackAggMemoryManager()
	info := multiAggInfo{
		aggID:     44,
		distinct:  false,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	exec1 := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec1.GroupGrow(2))
	keyVec := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"k"})
	valVec := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"v"})
	require.NoError(t, exec1.Fill(0, 0, []*vector.Vector{keyVec, valVec}))
	data, err := exec1.marshal()
	require.NoError(t, err)

	encoded := &EncodedAgg{}
	require.NoError(t, encoded.Unmarshal(data))

	exec2 := newJsonObjectAggExec(mg, info)
	require.NoError(t, exec2.unmarshal(nil, encoded.Result, encoded.Empties, encoded.Groups))
	exec2.Free()

	keyVec.Free(mg.Mp())
	valVec.Free(mg.Mp())
	exec1.Free()
}

func TestJsonArrayAggBatchMergeSkip(t *testing.T) {
	mg := hackAggMemoryManager()
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

	val := buildVarlenVec(t, mg.Mp(), types.T_varchar.ToType(), []string{"x"})
	require.NoError(t, exec2.Fill(0, 0, []*vector.Vector{val}))

	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{GroupNotMatched, 1}))

	val.Free(mg.Mp())
	exec1.Free()
	exec2.Free()
}
