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

package fileservice

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingObjectReader struct {
	err error
}

func TestDiskObjectStorage(t *testing.T) {
	ctx := context.Background()

	testFileService(t, 0, func(name string) FileService {
		dir := t.TempDir()
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:     name,
				Endpoint: "disk",
				Bucket:   dir,
			},
			DisabledCacheConfig,
			//CacheConfig{
			//	MemoryCapacity: ptrTo(toml.ByteSize(1 << 30)),
			//	DiskPath:       ptrTo(diskCacheDir),
			//	DiskCapacity:   ptrTo(toml.ByteSize(1 << 30)),
			//},
			nil,
			false,
			true,
		)
		if err != nil {
			t.Fatal(err)
		}
		return fs
	})
}

func (r failingObjectReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestDiskObjectStorageWriteFailureRemovesTempFile(t *testing.T) {
	root := t.TempDir()
	storage := &diskObjectStorage{path: root}
	readErr := errors.New("read failed")

	err := storage.Write(context.Background(), "nested/object", failingObjectReader{err: readErr}, nil, nil)
	require.ErrorIs(t, err, readErr)

	tempFiles, err := filepath.Glob(filepath.Join(root, "*.mofstemp"))
	require.NoError(t, err)
	require.Empty(t, tempFiles)
	_, err = os.Stat(filepath.Join(root, "nested", "object"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskObjectStorageSizeMismatchRemovesTempFile(t *testing.T) {
	root := t.TempDir()
	storage := &diskObjectStorage{path: root}

	err := storage.Write(context.Background(), "object", strings.NewReader(""), ptrTo[int64](1), nil)
	require.Error(t, err)

	tempFiles, globErr := filepath.Glob(filepath.Join(root, "*.mofstemp"))
	require.NoError(t, globErr)
	require.Empty(t, tempFiles)
}

func TestDiskObjectStorageDeleteReturnsRemovalError(t *testing.T) {
	root := t.TempDir()
	storage := &diskObjectStorage{path: root}
	objectDir := filepath.Join(root, "object")
	require.NoError(t, os.Mkdir(objectDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(objectDir, "child"), []byte("data"), 0644))
	laterObject := filepath.Join(root, "later")
	require.NoError(t, os.WriteFile(laterObject, []byte("data"), 0644))

	err := storage.Delete(context.Background(), "object", "later")
	require.Error(t, err)
	_, err = os.Stat(laterObject)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, storage.Delete(context.Background(), "missing"))
	_, err = os.Stat(filepath.Join(objectDir, "child"))
	require.NoError(t, err)
}
