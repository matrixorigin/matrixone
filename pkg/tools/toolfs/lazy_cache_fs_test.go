// Copyright 2021 Matrix Origin
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

package toolfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

const objectIOMagic = 0xFFFFFFFF

func TestLazyCacheFSReadsMagicPrefixedRemoteObjectsWithoutLocalChecksum(t *testing.T) {
	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	payload := make([]byte, 4, 32)
	binary.LittleEndian.PutUint32(payload, objectIOMagic)
	payload = append(payload, []byte("checkpoint-object-bytes")...)

	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}))

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	vec := &fileservice.IOVector{
		FilePath: "SHARED:ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, payload, vec.Entries[0].Data)

	var buf bytes.Buffer
	cacheVec := &fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset:        4,
			Size:          10,
			WriterForRead: &buf,
		}},
	}
	require.NoError(t, fs.ReadCache(ctx, cacheVec))
	require.Equal(t, payload[4:14], buf.Bytes())
}

func TestLazyCacheFSEvictsOldEntriesWhenOverLimit(t *testing.T) {
	t.Setenv("MO_TOOL_REMOTE_CACHE_SIZE", "32")

	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	first := bytes.Repeat([]byte("a"), 24)
	second := bytes.Repeat([]byte("b"), 24)
	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/first",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(first)),
			Data:   first,
		}},
	}))
	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/second",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(second)),
			Data:   second,
		}},
	}))

	fs, root, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	readAll := func(path string) []byte {
		vec := &fileservice.IOVector{
			FilePath: path,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   -1,
			}},
		}
		require.NoError(t, fs.Read(ctx, vec))
		return vec.Entries[0].Data
	}

	require.Equal(t, first, readAll("ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/first"))

	require.Equal(t, second, readAll("ckp/second"))
	require.NoFileExists(t, filepath.Join(root, "ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/second"))

	require.Equal(t, first, readAll("ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/first"))
	require.NoFileExists(t, filepath.Join(root, "ckp/second"))
}

func TestParseLazyCacheSize(t *testing.T) {
	tests := []struct {
		value string
		want  int64
		ok    bool
	}{
		{value: "42", want: 42, ok: true},
		{value: "2k", want: 2 << 10, ok: true},
		{value: "3MB", want: 3 << 20, ok: true},
		{value: "4GiB", want: 4 << 30, ok: true},
		{value: "", ok: false},
		{value: "bad", ok: false},
	}

	for _, tt := range tests {
		got, ok := parseLazyCacheSize(tt.value)
		require.Equal(t, tt.ok, ok)
		if tt.ok {
			require.Equal(t, tt.want, got)
		}
	}
}

func TestLazyCacheMaxBytesFromEnvDefault(t *testing.T) {
	old := os.Getenv("MO_TOOL_REMOTE_CACHE_SIZE")
	t.Cleanup(func() {
		if old == "" {
			_ = os.Unsetenv("MO_TOOL_REMOTE_CACHE_SIZE")
		} else {
			_ = os.Setenv("MO_TOOL_REMOTE_CACHE_SIZE", old)
		}
	})
	_ = os.Unsetenv("MO_TOOL_REMOTE_CACHE_SIZE")
	require.Equal(t, defaultLazyCacheMaxBytes, lazyCacheMaxBytesFromEnv())
}
