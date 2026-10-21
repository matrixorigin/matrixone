// Copyright 2025 Matrix Origin
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

package crt

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func zstdFrame(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// matrixone#30652: a truncated zstd suffix must surface as an error instead
// of being treated as a clean final chunk (silent prefix commit).
func TestZstdTruncationGuard_Truncated(t *testing.T) {
	payload := append(zstdFrame(t, "1,row-1\n2,row-2\n"), zstdFrame(t, "3,row-3\n4,row-4\n")[:10]...)
	zr, err := zstd.NewReader(bytes.NewReader(payload), zstd.WithDecoderConcurrency(1))
	require.NoError(t, err)
	defer zr.Close()
	data, err := io.ReadAll(zstdTruncationGuard{d: zr})
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, string(data), "2,row-2")
	assert.NotContains(t, string(data), "3,row-3")
}

// a clean multi-frame zstd stream is untouched.
func TestZstdTruncationGuard_Clean(t *testing.T) {
	payload := append(zstdFrame(t, "1,row-1\n"), zstdFrame(t, "2,row-2\n")...)
	zr, err := zstd.NewReader(bytes.NewReader(payload), zstd.WithDecoderConcurrency(1))
	require.NoError(t, err)
	defer zr.Close()
	data, err := io.ReadAll(zstdTruncationGuard{d: zr})
	require.NoError(t, err)
	assert.Equal(t, "1,row-1\n2,row-2\n", string(data))
}
