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

package indexfile

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// cachedEntries counts the entry directories on disk under the cache directory.
func cachedEntries(t *testing.T, cache *fileservice.IndexFileCache) int {
	t.Helper()
	n := 0
	require.NoError(t, filepath.WalkDir(cache.Path(), func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		if strings.HasSuffix(d.Name(), ".mofsdir") {
			n++
			return fs.SkipDir
		}
		return nil
	}))
	return n
}

// An open File holds its cache entry until Close, or, after Keep, until the caller releases
// it; a Delete removes the entry at once and the open File stays readable.
func TestOpenHoldsCacheEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(2*ChunkSize+7, 31)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 40, rows: func() []chunkRow { return fileRows(ref, data, 5) }}
	cache := newCache(t, t.TempDir())
	env := newEnv(t, ctrl, tbl, cache)
	ctx := context.Background()
	name := entryName(40, &ref)

	f, err := Open(ctx, env, ref)
	require.NoError(t, err)
	release, err := cache.Hold(name)
	require.NoError(t, err)
	require.NoError(t, cache.Delete(ctx, name))
	require.Equal(t, 0, cachedEntries(t, cache), "a held entry is deleted at once")
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	release()

	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, 1, cachedEntries(t, cache), "a deleted entry is rebuilt and cached again")
	kept := f.Keep()
	require.NoError(t, f.Close())
	require.Equal(t, 1, cachedEntries(t, cache))
	kept()
	require.Equal(t, 1, cachedEntries(t, cache), "releasing the last hold keeps an entry that was not removed")

	env.Cache = nil
	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	f.Keep()()
	require.NoError(t, f.Close())
}
