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

package objectio

import (
	"bytes"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestCompressionBuffersOverlapBoundaries(t *testing.T) {
	b := make([]byte, 32)
	other := make([]byte, 32)
	cases := []struct {
		name        string
		left, right []byte
		want        bool
	}{
		{"nil left", nil, b, false},
		{"nil right", b, nil, false},
		{"empty left", b[4:4], b, false},
		{"empty right", b, b[4:4], false},
		{"same range", b[4:12], b[4:12], true},
		{"same start left shorter", b[4:8], b[4:12], true},
		{"same start right shorter", b[4:12], b[4:8], true},
		{"left contains right", b[4:16], b[7:12], true},
		{"right contains left", b[7:12], b[4:16], true},
		{"left partial", b[4:10], b[9:15], true},
		{"right partial", b[9:15], b[4:10], true},
		{"left adjacent", b[4:10], b[10:15], false},
		{"right adjacent", b[10:15], b[4:10], false},
		{"left separated", b[4:9], b[10:15], false},
		{"right separated", b[10:15], b[4:9], false},
		{"different buffers", b, other, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, compressionBuffersOverlap(tc.left, tc.right))
		})
	}
}

func TestWriteWithCompressOverlapPositions(t *testing.T) {
	input := bytes.Repeat([]byte("matrixone overlap"), 16)
	const used = 512
	bound := lz4.CompressBlockBound(len(input))
	cases := []struct {
		name        string
		start       int
		wantScratch bool
	}{
		{"same start", used, true},
		{"input partially before output", used - len(input)/2, true},
		{"input partially after output", used + bound - len(input)/2, true},
		{"input inside output", used + 1, true},
		{"adjacent before output", used - len(input), false},
		{"adjacent after output", used + bound, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arena := NewArena(used + bound + len(input))
			t.Cleanup(arena.FreeBuffers)
			arena.usedOffset = used
			arena.totalRequested = used
			source := arena.data[tc.start : tc.start+len(input)]
			copy(source, input)
			writer := &objectWriterV1{arena: arena}
			out, ext, err := writer.WriteWithCompress(7, source)
			require.NoError(t, err)
			checkCompressedExtent(t, ext, 7, input, out)
			if tc.wantScratch {
				require.NotNil(t, arena.compressBuf)
			} else {
				require.Nil(t, arena.compressBuf)
			}
		})
	}
}
