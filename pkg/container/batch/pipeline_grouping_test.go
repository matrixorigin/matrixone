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

func TestPipelineEnvelopeBoundariesAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	for _, value := range []string{"one", "two", "three"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), false, mp))
	}
	input.SetRowCount(3)
	input.ExtraBuf = []byte(pipelineGroupingMagic)
	plain, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	prefix, err := stableBatchPayloadLength(plain)
	require.NoError(t, err)
	require.Equal(t, len(plain), prefix)
	// Magic at the footer candidate position is part of ExtraBuf, not a footer.
	end := len(plain) - pipelineGroupingFooterSize
	require.Equal(t, pipelineGroupingMagic, string(plain[end:end+8]))
	output := NewOffHeapEmpty()
	t.Cleanup(func() { output.Clean(mp) })
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	require.Equal(t, input.ExtraBuf, output.ExtraBuf)

	input.Vecs[0].GetGrouping().Add(2)
	groupOnly, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	prefix, err = stableBatchPayloadLength(groupOnly)
	require.NoError(t, err)
	end = len(groupOnly) - pipelineGroupingFooterSize
	require.Equal(t, prefix, end-int(binary.LittleEndian.Uint64(groupOnly[end+8:])))
	require.NoError(t, output.UnmarshalBinaryForPipeline(groupOnly, mp))
	require.True(t, output.Vecs[0].GetGrouping().Contains(2))

	input.Vecs[0].SetPrepareParamKind(vector.PrepareParamFloat)
	require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceSQLPrepare))
	for _, tc := range []struct {
		name          string
		includeSource bool
		version       byte
	}{
		{"v1", false, prepareParamKindBatchVersionV1},
		{"v2", true, prepareParamKindBatchVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := input.MarshalBinaryForPipeline(&bytes.Buffer{}, true, tc.includeSource)
			require.NoError(t, err)
			prefix, err := stableBatchPayloadLength(encoded)
			require.NoError(t, err)
			require.Equal(t, tc.version, encoded[prefix+3])
			require.NoError(t, output.UnmarshalBinaryForPipeline(encoded, mp))
			require.True(t, output.Vecs[0].GetGrouping().Contains(2))
			require.Equal(t, vector.PrepareParamFloat, output.Vecs[0].GetPrepareParamKindAt(0))
			if tc.includeSource {
				require.Equal(t, types.StringSourceSQLPrepare, output.Vecs[0].GetStringSourceAt(0))
			}
			end := len(encoded) - pipelineGroupingFooterSize
			size := binary.LittleEndian.Uint64(encoded[end+8:])
			crossesStable := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(crossesStable[end+8:], uint64(end-prefix)+1)
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(crossesStable, mp), "invalid pipeline grouping payload size")
			cutsTrailer := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(cutsTrailer[end+8:], size+1)
			require.Error(t, output.UnmarshalBinaryForPipeline(cutsTrailer, mp))
			badBoth := bytes.Clone(encoded)
			badBoth[prefix] = 'X'
			binary.LittleEndian.PutUint64(badBoth[end-int(size):], 4)
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(badBoth, mp), "invalid prepared parameter batch trailer")
			badFrameAndFooter := bytes.Clone(encoded)
			binary.NativeEndian.PutUint64(badFrameAndFooter[:8], ^uint64(0))
			binary.LittleEndian.PutUint64(badFrameAndFooter[end+8:], ^uint64(0))
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(badFrameAndFooter, mp), "invalid batch row count")
		})
	}
	require.Error(t, output.UnmarshalBinaryForPipeline(plain[:len(plain)-1], mp))
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	require.False(t, output.HasGrouping())
	require.Equal(t, vector.PrepareParamNone, output.Vecs[0].GetPrepareParamKindAt(0))
}
