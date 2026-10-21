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
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWritePreextendedXFSAllocation(t *testing.T) {
	root := t.TempDir()
	var fs syscall.Statfs_t
	require.NoError(t, syscall.Statfs(root, &fs))
	if fs.Type != 0x58465342 {
		t.Skip("requires TMPDIR on XFS")
	}
	name := filepath.Join(root, "spill")
	payload := bytes.Repeat([]byte("x"), 64<<10)
	const writes = 17 // Cross the first 1 MiB speculative allocation boundary.
	for range writes {
		func() {
			f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			n, err := WritePreextended(f, payload)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
		}()
	}
	st, err := os.Stat(name)
	require.NoError(t, err)
	require.Equal(t, int64(writes*len(payload)), st.Size())
	allocated := st.Sys().(*syscall.Stat_t).Blocks * 512
	require.LessOrEqual(t, allocated, st.Size()+64<<10, "speculative EOF allocation must not dwarf the admitted payload")
	got, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(payload, writes), got)
}
