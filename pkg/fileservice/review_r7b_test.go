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

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestBatchDeleteInvalidatesOtherIndexObjects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "both_index_entries_writable"},
		{name: "first_index_shard_read_only", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			fs, _, data := newIndexObjectFS(t, true)
			const first, second = "first-object", "object"
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: first, Policy: SkipAllCache,
				Entries: []IOEntry{{Size: int64(len(data)), Data: data}}}))
			cacheIndexObject(t, fs, first, data)
			cacheIndexObject(t, fs, second, data)
			firstDir, firstCached := fs.indexObjectCached(first)
			secondDir, secondCached := fs.indexObjectCached(second)
			require.True(t, firstCached && secondCached)
			firstShard := filepath.Dir(firstDir)
			require.NotEqual(t, firstShard, filepath.Dir(secondDir))
			if tc.readOnly {
				require.NoError(t, os.Chmod(firstShard, 0o500))
				defer func() { require.NoError(t, os.Chmod(firstShard, 0o755)) }()
			}

			err := fs.Delete(ctx, first, second)
			if tc.readOnly {
				require.ErrorIs(t, err, os.ErrPermission)
			} else {
				require.NoError(t, err)
			}
			_, err = fs.StatFile(ctx, second)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "the second object is deleted from storage: %v", err)
			vec := &IOVector{FilePath: second, Policy: IndexFileObject | ExactColumnsRead,
				Entries: []IOEntry{{Size: 16}}}
			defer vec.Release()
			err = fs.Read(ctx, vec)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound),
				"the second object's index entry should be invalidated independently: read returned %d bytes, error %v", len(vec.Entries[0].Data), err)
		})
	}
}

func TestBatchDeleteRemovesOtherDiskCacheObjects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "both_shards_writable"},
		{name: "first_shard_read_only", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			fs, _, data := newPrefetchRangeFS(t, 4096, 300<<10)
			const first, second = "first-object", "object"
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: first, Policy: SkipDiskCache | SkipMemoryCache,
				Entries: []IOEntry{{Size: int64(len(data)), Data: data}}}))
			read := func(file string, policy Policy, offset int64) error {
				vec := &IOVector{FilePath: file, Policy: policy | SkipMemoryCache,
					Entries: []IOEntry{{Offset: offset, Size: 10}}}
				defer vec.Release()
				err := fs.Read(ctx, vec)
				fs.diskCache.prefetchObjects.reads.Wait()
				return err
			}
			for _, file := range []string{first, second} {
				require.NoError(t, read(file, SkipFullFilePreloads, 100))
				require.NoError(t, read(file, 0, 200<<10))
			}
			firstPath, secondPath := fs.diskCache.pathForFile(first), fs.diskCache.pathForFile(second)
			require.True(t, fs.diskCache.cache.Contains(firstPath) && fs.diskCache.cache.Contains(secondPath))
			require.NotEmpty(t, fs.diskCache.ranges.of(fs.diskCache.rangeObjectKey(second)))
			firstShard := filepath.Dir(firstPath)
			require.NotEqual(t, firstShard, filepath.Dir(secondPath))
			if tc.readOnly {
				require.NoError(t, os.Chmod(firstShard, 0o500))
				defer func() { require.NoError(t, os.Chmod(firstShard, 0o755)) }()
			}

			err := fs.Delete(ctx, first, second)
			if tc.readOnly {
				require.ErrorIs(t, err, os.ErrPermission)
			} else {
				require.NoError(t, err)
			}
			for _, policy := range []Policy{0, SkipFullFilePreloads} {
				err = read(second, policy, 100)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "policy %v: %v", policy, err)
			}
			require.False(t, fs.diskCache.cache.Contains(secondPath), "the second object's whole-object entry is removed")
			require.Empty(t, fs.diskCache.ranges.of(fs.diskCache.rangeObjectKey(second)), "the second object's range files are removed")
		})
	}
}

func TestLocalFSBatchDeleteInvalidatesDeletedFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "both_directories_writable"},
		{name: "second_directory_read_only", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			root := t.TempDir()
			fs, err := NewLocalFS(ctx, "test", root, CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20)}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			const first, second = "a/first", "b/second"
			read := func(file string) ([]byte, error) {
				vec := &IOVector{FilePath: file, Entries: []IOEntry{{Size: 3, ToCacheData: CacheOriginalData}}}
				defer vec.Release()
				err := fs.Read(ctx, vec)
				return append([]byte(nil), vec.Entries[0].Data...), err
			}
			for _, file := range []string{first, second} {
				require.NoError(t, fs.Write(ctx, IOVector{FilePath: file, Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))
				data, err := read(file)
				require.NoError(t, err)
				require.Equal(t, "abc", string(data))
			}
			if tc.readOnly {
				dir := filepath.Join(root, "b")
				require.NoError(t, os.Chmod(dir, 0o500))
				defer func() { require.NoError(t, os.Chmod(dir, 0o755)) }()
			}

			err = fs.Delete(ctx, first, second)
			if tc.readOnly {
				require.ErrorIs(t, err, os.ErrPermission)
			} else {
				require.NoError(t, err)
			}
			data, err := read(first)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "the deleted first file is not read from the cache: read %q, error %v", data, err)
		})
	}
}
