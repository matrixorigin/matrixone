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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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
