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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/objcache"
	"github.com/stretchr/testify/assert"
)

func TestCacheWithRCExample(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewLocalFS(
		"rc",
		dir,
		32<<20,
		0,
		"",
		nil,
	)
	assert.Nil(t, err)

	ctx := context.Background()

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
				ToObject: func(_ io.Reader, data []byte) (any, int64, error) {
					i, err := strconv.Atoi(string(data))
					assert.Nil(t, err)
					return objcache.NewRCValue(i), 8, nil
				},
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	value := vec.Entries[0].Object.(*objcache.RCValue[int])
	assert.Equal(t, 42, value.Value)

	value.IncRef()       // pin, cache will not evict this item
	defer value.DecRef() // unpin

}

func TestCacheWithReleasableExample(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewLocalFS(
		"rc",
		dir,
		32<<20,
		0,
		"",
		nil,
	)
	assert.Nil(t, err)

	ctx := context.Background()
	pool := NewPool(64, func() []byte {
		return make([]byte, 1024)
	}, nil, nil)

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
				ToObject: func(_ io.Reader, data []byte) (any, int64, error) {
					// allocs from pool
					copied, put := pool.Get()
					copied = copied[:copy(copied, data)]
					return objcache.NewReleasableValue(copied, func() {
						// return to pool when evict
						put()
					}), int64(len(copied)), nil
				},
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	value := vec.Entries[0].Object.(objcache.ReleasableValue[[]byte])
	assert.Equal(t, []byte("42"), value.Value)

}
