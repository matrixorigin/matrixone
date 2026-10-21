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

package spillio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWritePreextendedRoundTrip(t *testing.T) {
	name := filepath.Join(t.TempDir(), "spill")
	var expected []byte
	for _, size := range []int{1, 4095, 65537, 17} {
		func() {
			f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			payload := bytes.Repeat([]byte{byte(size)}, size)
			n, err := WritePreextended(f, payload)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			expected = append(expected, payload...)
		}()
	}
	got, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	// A failed truncate must leave the existing record intact and report no write.
	f, err := os.Open(name)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	n, err := WritePreextended(f, []byte("unadmitted-tail"))
	require.Error(t, err)
	require.Zero(t, n)
	got, err = os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	require.NoError(t, f.Close())
	n, err = WritePreextended(f, []byte("closed"))
	require.Error(t, err)
	require.Zero(t, n)
}
