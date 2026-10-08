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

package batch

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPipelineGroupingRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{7, 0, 0}, []bool{false, true, true}, mp))
	input.Vecs[0].GetGrouping().Add(2)
	input.Attrs = []string{"key"}
	input.ExtraBuf = []byte{2, 0, 3, 1}
	input.SetRowCount(3)
	input.Vecs[0].SetPrepareParamKind(vector.PrepareParamInteger)
	data, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	output := NewOffHeapEmpty()
	t.Cleanup(func() { output.Clean(mp) })
	require.NoError(t, output.UnmarshalBinaryForPipeline(data, mp))
	require.Equal(t, input.Attrs, output.Attrs)
	require.Equal(t, input.ExtraBuf, output.ExtraBuf)
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](output.Vecs[0], 0))
	require.True(t, output.Vecs[0].IsNull(1))
	require.False(t, output.Vecs[0].GetGrouping().Contains(1))
	require.True(t, output.Vecs[0].IsNull(2))
	require.True(t, output.Vecs[0].GetGrouping().Contains(2), "remote transport must distinguish ROLLUP NULL from SQL NULL")
	require.Equal(t, vector.PrepareParamInteger, output.Vecs[0].GetPrepareParamKindAt(0))

	input.Vecs[0].GetGrouping().Reset()
	plain, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	legacy, err := input.MarshalBinaryWithPrepareParamKinds(&bytes.Buffer{}, true)
	require.NoError(t, err)
	require.Equal(t, legacy, plain, "ordinary batches must retain their legacy wire bytes")
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	require.False(t, output.HasGrouping(), "decoder reuse must clear previous grouping")
}

func TestPipelineGroupingConstAndMalformed(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewRollupConst(types.T_int32.ToType(), 3, mp)
	input.SetRowCount(3)
	data, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	t.Run("constant logical rows", func(t *testing.T) {
		output := NewOffHeapEmpty()
		t.Cleanup(func() { output.Clean(mp) })
		require.NoError(t, output.UnmarshalBinaryForPipeline(data, mp))
		for row := uint64(0); row < 3; row++ {
			require.True(t, output.Vecs[0].GetGrouping().Contains(row))
		}
	})
	end := len(data) - pipelineGroupingFooterSize
	start := end - int(binary.LittleEndian.Uint64(data[end+8:]))
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"oversized section", func(b []byte) []byte { binary.LittleEndian.PutUint64(b[end+8:], ^uint64(0)); return b }},
		{"row mismatch", func(b []byte) []byte { binary.LittleEndian.PutUint64(b[start:], 2); return b }},
		{"vector mismatch", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[start+8:], 2); return b }},
		{"oversized bitmap", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[start+12:], 1<<30); return b }},
		{"unknown version", func(b []byte) []byte { b[end+7]++; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := NewOffHeapEmpty()
			t.Cleanup(func() { output.Clean(mp) })
			require.Error(t, output.UnmarshalBinaryForPipeline(tc.mutate(bytes.Clone(data)), mp))
		})
	}
	input.Vecs[0].GetGrouping().Add(3)
	bad, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	output := NewOffHeapEmpty()
	t.Cleanup(func() { output.Clean(mp) })
	require.ErrorContains(t, output.UnmarshalBinaryForPipeline(bad, mp), "exceeds vector length")
}
