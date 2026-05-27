// Copyright 2024 Matrix Origin
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

package datalink

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A live (un-pinned) datalink reads its bytes from the external file via the
// GetForETL path, and StatSize reports the live file size.
func TestNewReadCloserLive(t *testing.T) {
	proc := testutil.NewProc(t)
	dir := t.TempDir()
	fp := filepath.Join(dir, "f.txt")
	require.NoError(t, os.WriteFile(fp, []byte("live-content"), 0o600))

	dl, err := NewDatalink("file://"+fp, proc)
	require.NoError(t, err)
	require.Empty(t, dl.ContentHash)

	got, err := dl.GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, []byte("live-content"), got)

	sz, err := dl.StatSize(proc)
	require.NoError(t, err)
	require.Equal(t, int64(len("live-content")), sz)
}

// NewDatalink populates ContentHash and MoPath for a pinned URL, and leaves
// ContentHash empty for a live one.
func TestNewDatalinkContentHash(t *testing.T) {
	hash := strings.Repeat("a", 64)
	dl, err := NewDatalink("file:///x.txt?contenthash="+hash, nil)
	require.NoError(t, err)
	require.Equal(t, hash, dl.ContentHash)
	require.Equal(t, CASKey(hash), dl.MoPath)

	live, err := NewDatalink("file:///x.txt", nil)
	require.NoError(t, err)
	require.Empty(t, live.ContentHash)
}

// ContentHash always addresses the same CAS object as MoPath, regardless of the
// case used in the contenthash query key or value.
func TestNewDatalinkContentHashConsistentWithMoPath(t *testing.T) {
	hash := strings.Repeat("a", 64)
	for _, raw := range []string{
		"file:///x.txt?contenthash=" + hash,
		"file:///x.txt?ContentHash=" + strings.ToUpper(hash),
	} {
		dl, err := NewDatalink(raw, nil)
		require.NoError(t, err, raw)
		require.Equal(t, hash, dl.ContentHash, raw)
		require.Equal(t, CASKey(dl.ContentHash), dl.MoPath, raw)
	}
}

// A pinned datalink reads its bytes from the CAS, decoupled from the original
// path. NewProc(t) backs SHARED with LocalFS (no ETLFileService), matching
// standalone, so this exercises the direct-Read path rather than GetForETL.
func TestNewReadCloserPinned(t *testing.T) {
	proc := testutil.NewProc(t)

	casFS, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)
	content := []byte("frozen-bytes")
	hash, err := CASPut(proc.Ctx, casFS, content)
	require.NoError(t, err)

	// a bogus original path with a valid contenthash is still served from the CAS
	dl, err := NewDatalink("file:///bogus/path.txt?contenthash="+hash, proc)
	require.NoError(t, err)

	got, err := dl.GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, content, got)

	sz, err := dl.StatSize(proc)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), sz)
}

// Reading a pinned datalink whose CAS object is missing errors out: it never
// falls back to the live (bogus) path.
func TestNewReadCloserPinnedMissing(t *testing.T) {
	proc := testutil.NewProc(t)

	dl, err := NewDatalink("file:///bogus/path.txt?contenthash="+strings.Repeat("b", 64), proc)
	require.NoError(t, err)

	_, err = dl.GetBytes(proc)
	require.Error(t, err)

	_, err = dl.StatSize(proc)
	require.Error(t, err)
}
