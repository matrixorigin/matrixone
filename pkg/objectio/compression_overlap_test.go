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

func TestWriteWithCompressBorrowedInputPositions(t *testing.T) {
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
		{"adjacent before output", used - len(input), true},
		{"adjacent after output", used + bound, true},
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

func TestWriteWithCompressBorrowedSeparateInputFallsBack(t *testing.T) {
	input := bytes.Repeat([]byte("borrowed"), 256)
	arena := NewArena(lz4.CompressBlockBound(len(input)))
	t.Cleanup(arena.FreeBuffers)
	writer := &objectWriterV1{arena: arena}
	out, extent, err := writer.WriteWithCompress(3, input)
	require.NoError(t, err)
	checkCompressedExtent(t, extent, 3, input, out)
	require.NotNil(t, arena.compressBuf, "arbitrary callers retain the scratch path")
}
