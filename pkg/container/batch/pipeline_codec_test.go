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

// legacyPipelineGroupingTrailer builds the retired ROLLUP grouping section an
// older writer appended after the stable batch payload:
// [int64 rows][int32 nVecs][per vector: int32 bitmap size] followed by the
// 16-byte footer "MOGROUP\x01" + little-endian uint64 section size (the
// section excludes the footer). Every bitmap is empty here.
func legacyPipelineGroupingTrailer(rows int64, nVecs int32) []byte {
	section := binary.LittleEndian.AppendUint64(nil, uint64(rows))
	section = binary.LittleEndian.AppendUint32(section, uint32(nVecs))
	for range nVecs {
		section = binary.LittleEndian.AppendUint32(section, 0)
	}
	out := append(section, pipelineGroupingMagic...)
	return binary.LittleEndian.AppendUint64(out, uint64(len(section)))
}

func TestPipelineIgnoresLegacyGroupingTrailer(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{7, 0, 0}, []bool{false, true, true}, mp))
	input.extraBuf = []byte{2, 0, 3, 1}
	input.SetRowCount(3)
	plain, err := input.MarshalBinaryWithBuffer(&bytes.Buffer{}, true)
	require.NoError(t, err)

	output := NewWithSize(0)
	t.Cleanup(func() { output.Clean(mp) })
	check := func() {
		require.Equal(t, 3, output.RowCount())
		require.Equal(t, input.extraBuf, output.extraBuf)
		require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](output.Vecs[0], 0))
		require.False(t, output.Vecs[0].IsNull(0))
		require.True(t, output.Vecs[0].IsNull(1))
		require.True(t, output.Vecs[0].IsNull(2))
	}
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	check()

	legacy := append(bytes.Clone(plain), legacyPipelineGroupingTrailer(3, 1)...)
	require.NoError(t, output.UnmarshalBinaryForPipeline(legacy, mp))
	check()
}

func TestPipelineRejectsMalformedLegacyGroupingTrailerSize(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{1, 2, 3}, nil, mp))
	input.SetRowCount(3)
	plain, err := input.MarshalBinaryWithBuffer(&bytes.Buffer{}, true)
	require.NoError(t, err)
	data := append(bytes.Clone(plain), legacyPipelineGroupingTrailer(3, 1)...)
	end := len(data) - pipelineGroupingFooterSize
	for _, tc := range []struct {
		name string
		size uint64
	}{
		{"crosses stable payload", uint64(end-len(plain)) + 1},
		{"oversized", ^uint64(0)},
		{"too small", 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(data)
			binary.LittleEndian.PutUint64(bad[end+8:], tc.size)
			output := NewWithSize(0)
			t.Cleanup(func() { output.Clean(mp) })
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(bad, mp), "invalid pipeline grouping payload size")
		})
	}
	t.Run("truncated", func(t *testing.T) {
		output := NewWithSize(0)
		t.Cleanup(func() { output.Clean(mp) })
		require.Error(t, output.UnmarshalBinaryForPipeline(data[:len(data)-1], mp))
	})
}

func TestPipelineEnvelopeBoundariesAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	for _, value := range []string{"one", "two", "three"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), false, mp))
	}
	input.SetRowCount(3)
	input.extraBuf = []byte(pipelineGroupingMagic)
	plain, err := input.MarshalBinaryWithBuffer(&bytes.Buffer{}, true)
	require.NoError(t, err)
	prefix, err := stableBatchPayloadLength(plain)
	require.NoError(t, err)
	require.Equal(t, len(plain), prefix)
	// Magic at the footer candidate position is part of ExtraBuf, not a footer.
	end := len(plain) - pipelineGroupingFooterSize
	require.Equal(t, pipelineGroupingMagic, string(plain[end:end+8]))
	output := NewWithSize(0)
	t.Cleanup(func() { output.Clean(mp) })
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	require.Equal(t, input.extraBuf, output.extraBuf)

	groupOnly := append(bytes.Clone(plain), legacyPipelineGroupingTrailer(3, 1)...)
	end = len(groupOnly) - pipelineGroupingFooterSize
	require.Equal(t, prefix, end-int(binary.LittleEndian.Uint64(groupOnly[end+8:])))
	require.NoError(t, output.UnmarshalBinaryForPipeline(groupOnly, mp))
	requireStringValues(t, output.Vecs[0], "one", "two", "three")

	// An older writer inserted a version-1 transport trailer between the
	// stable payload and the grouping section.
	current := append(legacyV1TransportWire(t, groupOnly[:prefix],
		[]byte{batchMetadataModeNone | batchMetadataBinaryFlag}), groupOnly[prefix:]...)
	for _, tc := range []struct {
		name    string
		encoded []byte
		version byte
	}{
		{"v1", current, batchMetadataVersionV1},
		// An older writer's version-2 trailer carries a string-source
		// section that is validated and dropped.
		{"v2", legacyV2TransportWire(t, current,
			legacyUniformStringSource(types.StringSourceSQLPrepare)), batchMetadataVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := tc.encoded
			prefix, err := stableBatchPayloadLength(encoded)
			require.NoError(t, err)
			require.Equal(t, tc.version, encoded[prefix+3])
			require.NoError(t, output.UnmarshalBinaryForPipeline(encoded, mp))
			requireStringValues(t, output.Vecs[0], "one", "two", "three")
			end := len(encoded) - pipelineGroupingFooterSize
			size := binary.LittleEndian.Uint64(encoded[end+8:])
			crossesStable := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(crossesStable[end+8:], uint64(end-prefix)+1)
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(crossesStable, mp), "invalid pipeline grouping payload size")
			cutsTrailer := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(cutsTrailer[end+8:], size+1)
			require.Error(t, output.UnmarshalBinaryForPipeline(cutsTrailer, mp))
			badTrailer := bytes.Clone(encoded)
			badTrailer[prefix] = 'X'
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(badTrailer, mp), "invalid prepared parameter batch trailer")
			badFrameAndFooter := bytes.Clone(encoded)
			binary.NativeEndian.PutUint64(badFrameAndFooter[:8], ^uint64(0))
			binary.LittleEndian.PutUint64(badFrameAndFooter[end+8:], ^uint64(0))
			require.ErrorContains(t, output.UnmarshalBinaryForPipeline(badFrameAndFooter, mp), "invalid batch row count")
		})
	}
	require.Error(t, output.UnmarshalBinaryForPipeline(plain[:len(plain)-1], mp))
	require.NoError(t, output.UnmarshalBinaryForPipeline(plain, mp))
	requireStringValues(t, output.Vecs[0], "one", "two", "three")
}
