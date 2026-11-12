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

package zipstream

import (
	"archive/zip"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewReader(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range [][2]string{{"d/", ""}, {".hidden", "skip"}, {"a.txt", "first"}, {"b.txt", "second"}} {
		w, err := zw.Create(f[0])
		require.NoError(t, err)
		_, err = w.Write([]byte(f[1]))
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())

	r, closeDecoder, err := NewReader(t.Context(), bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "first", string(data))
	if closeDecoder != nil {
		require.NoError(t, closeDecoder())
	}

	_, _, err = NewReader(t.Context(), bytes.NewReader([]byte("plain text")))
	require.ErrorContains(t, err, "not a zip archive")

	// A stored entry written with a data descriptor before the file to load cannot be skipped.
	buf.Reset()
	zw = zip.NewWriter(&buf)
	w, err := zw.CreateHeader(&zip.FileHeader{Name: ".hidden", Method: zip.Store})
	require.NoError(t, err)
	_, err = w.Write([]byte("skip"))
	require.NoError(t, err)
	w, err = zw.Create("a.txt")
	require.NoError(t, err)
	_, err = w.Write([]byte("first"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	_, _, err = NewReader(t.Context(), bytes.NewReader(buf.Bytes()))
	require.ErrorContains(t, err, "cannot be skipped in a stream")
}
