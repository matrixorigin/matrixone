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
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testCache(
	t *testing.T,
	newFS func() FileService,
) {

	testFileService(t, func() FileService {
		fs, err := NewCacheFS(newFS(), 1024)
		assert.Nil(t, err)
		return fs
	})

	t.Run("cache", func(t *testing.T) {
		fs, err := NewCacheFS(newFS(), 1024)
		assert.Nil(t, err)
		fs.stats = new(lruStats)

		ctx := context.Background()

		buf := new(bytes.Buffer)
		err = gob.NewEncoder(buf).Encode(map[int]int{
			42: 42,
		})
		assert.Nil(t, err)
		data := buf.Bytes()

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: len(data),
					Data: data,
				},
			},
		})
		assert.Nil(t, err)

		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: len(data),
					ToObject: func(r io.Reader) (any, int, error) {
						var m map[int]int
						if err := gob.NewDecoder(r).Decode(&m); err != nil {
							return nil, 0, err
						}
						return m, 1, nil
					},
				},
			},
		}

		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		m, ok := vec.Entries[0].Object.(map[int]int)
		assert.True(t, ok)
		assert.Equal(t, 1, len(m))
		assert.Equal(t, 42, m[42])
		assert.Equal(t, 1, vec.Entries[0].ObjectSize)

		// read again
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		m, ok = vec.Entries[0].Object.(map[int]int)
		assert.True(t, ok)
		assert.Equal(t, 1, len(m))
		assert.Equal(t, 42, m[42])
		assert.Equal(t, 1, vec.Entries[0].ObjectSize)

		assert.Equal(t, fs.stats.Read, int64(2))
		assert.Equal(t, fs.stats.CacheHit, int64(1))

	})

}
