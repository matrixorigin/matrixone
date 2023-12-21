// Copyright 2022 Matrix Origin
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

	"github.com/stretchr/testify/assert"
)

func TestLocalETLFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			dir := t.TempDir()
			fs, err := NewLocalETLFS(name, dir)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			dir := t.TempDir()
			fs, err := NewLocalETLFS("etl", dir)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("symlink to dir", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()

		aPath := filepath.Join(dir, "a")
		err := os.Mkdir(aPath, 0755)
		assert.Nil(t, err)

		bPath := filepath.Join(dir, "b")
		err = os.Symlink(aPath, bPath)
		assert.Nil(t, err)

		filePathInA := filepath.Join(aPath, "foo")
		err = os.WriteFile(
			filePathInA,
			[]byte("foo"),
			0644,
		)
		assert.Nil(t, err)

		filePathInB := filepath.Join(bPath, "foo")
		fs, readPath, err := GetForETL(ctx, nil, filePathInB)
		assert.Nil(t, err)

		vec := IOVector{
			FilePath: readPath,
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("foo"), vec.Entries[0].Data)

		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, "foo", entries[0].Name)

	})

	t.Run("deref symlink", func(t *testing.T) {
		dir := t.TempDir()

		aPath := filepath.Join(dir, "a")
		err := os.Mkdir(aPath, 0755)
		assert.Nil(t, err)

		bPath := filepath.Join(dir, "b")
		err = os.Symlink(aPath, bPath)
		assert.Nil(t, err)

		filePathInA := filepath.Join(aPath, "foo")
		err = os.WriteFile(
			filePathInA,
			[]byte("foo"),
			0644,
		)
		assert.Nil(t, err)

		fs, err := NewLocalETLFS("foo", dir)
		assert.Nil(t, err)

		ctx := context.Background()
		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.Equal(t, 2, len(entries))
		assert.True(t, entries[0].IsDir)
		assert.True(t, entries[1].IsDir)

	})

}

func TestLocalETLFSEmptyRootPath(t *testing.T) {
	fs, err := NewLocalETLFS(
		"test",
		"",
	)
	assert.Nil(t, err)
	assert.NotNil(t, fs)
}

func TestLocalETLFSWithBadSymlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	err := os.Symlink(
		"file-not-exists-fdsafdsafdsa",
		filepath.Join(dir, "foo"),
	)
	assert.Nil(t, err)
	fs, err := NewLocalETLFS("test", dir)
	assert.Nil(t, err)
	_, err = fs.List(ctx, "")
	assert.Nil(t, err)
}
