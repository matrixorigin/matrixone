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

	"github.com/stretchr/testify/assert"
)

func TestMemCacheLeak(t *testing.T) {
	ctx := context.Background()

	fs, err := NewMemoryFS("test", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	})
	assert.Nil(t, err)

	m := NewMemCache(WithLRU(4))

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				ToObjectBytes: func(reader io.Reader, data []byte) ([]byte, int64, error) {
					return []byte{42}, 1, nil
				},
			},
		},
	}
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), m.objCache.Size())
	assert.Equal(t, int64(1), vec.Entries[0].ObjectSize)

	// read from cache
	vec = &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				ToObjectBytes: func(reader io.Reader, data []byte) ([]byte, int64, error) {
					return []byte{42}, 1, nil
				},
			},
		},
	}
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), m.objCache.Size())
	assert.Equal(t, int64(1), vec.Entries[0].ObjectSize)

}
