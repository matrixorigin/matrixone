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
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func TestLocalFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func(name string) FileService {
			dir := t.TempDir()
			fs, err := NewLocalFS(name, dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			dir := t.TempDir()
			fs, err := NewLocalFS("local", dir, CacheConfig{
				MemoryCapacity: 128 * 1024,
			}, nil)
			assert.Nil(t, err)
			return fs
		})
	})

}

func BenchmarkLocalFS(b *testing.B) {
	benchmarkFileService(b, func() FileService {
		dir := b.TempDir()
		fs, err := NewLocalFS("local", dir, DisabledCacheConfig, nil)
		assert.Nil(b, err)
		return fs
	})
}

func TestLocalFSWithDiskCache(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)
	const (
		n       = 128
		dataLen = 128
	)

	// new fs
	fs, err := NewLocalFS(
		"foo",
		t.TempDir(),
		CacheConfig{
			DiskPath:                  t.TempDir(),
			DiskCapacity:              dataLen * n / 32,
			enableDiskCacheForLocalFS: true,
		},
		nil,
	)
	assert.Nil(t, err)

	// prepare data
	datas := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		data := make([]byte, dataLen)
		_, err := rand.Read(data)
		assert.Nil(t, err)
		datas = append(datas, data)
	}

	// write
	for i := 0; i < n; i++ {
		data := datas[i]
		vec := IOVector{
			FilePath: fmt.Sprintf("%d", i),
			Entries: []IOEntry{
				{
					Data: data,
					Size: int64(len(data)),
				},
			},
		}
		err := fs.Write(ctx, vec)
		assert.Nil(t, err)
	}

	// read
	for i := 0; i < n*10; i++ {
		idx := i % n
		expected := datas[idx]
		length := 8
		for j := 0; j < dataLen/length; j++ {
			offset := j * length
			vec := IOVector{
				FilePath: fmt.Sprintf("%d", idx),
				Entries: []IOEntry{
					{
						Offset: int64(offset),
						Size:   int64(length),
					},
				},
			}
			err := fs.Read(ctx, &vec)
			assert.Nil(t, err)
			assert.Equal(t, expected[offset:offset+length], vec.Entries[0].Data)
		}
	}

}
