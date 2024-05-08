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
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
)

func TestCacheExample(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	fs, err := NewLocalFS(
		ctx,
		"rc",
		dir,
		CacheConfig{
			MemoryCapacity: ptrTo[toml.ByteSize](32 << 20),
		},
		nil,
	)
	assert.Nil(t, err)

	// write
	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Data: []byte("42"),
				Size: 2,
			},
		},
	})
	assert.Nil(t, err)

	// read
	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   2,
				ToCacheData: func(_ io.Reader, data []byte, allocator CacheDataAllocator) (memorycache.CacheData, error) {
					cacheData := allocator.Alloc(len(data))
					copy(cacheData.Bytes(), data)
					return cacheData, nil
				},
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	value := vec.Entries[0].CachedData
	assert.Equal(t, []byte("42"), value.Bytes())

	vec.Release()
	fs.FlushCache()
}
