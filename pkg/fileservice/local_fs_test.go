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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func(name string) FileService {
			dir := t.TempDir()
			fs, err := NewLocalFS(name, dir, -1, -1, "", nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, -1, -1, "", nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, -1, -1, "", nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, 128*1024, -1, "", nil)
			assert.Nil(t, err)
			return fs
		})
	})

}

func BenchmarkLocalFS(b *testing.B) {
	benchmarkFileService(b, func() FileService {
		dir := b.TempDir()
		fs, err := NewLocalFS("local", dir, -1, -1, "", nil)
		assert.Nil(b, err)
		return fs
	})
}
