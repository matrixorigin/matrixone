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
