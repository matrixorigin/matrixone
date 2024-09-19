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
	"bytes"
	"context"
	"io"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testObjectStorage[T ObjectStorage](
	t *testing.T,
	newStorage func(t *testing.T) T,
) {

	t.Run("basic", func(t *testing.T) {
		storage := newStorage(t)
		ctx := context.Background()

		prefix := time.Now().Format("2006-01-02-15-04-05.000000")
		name := path.Join(prefix, "foo")

		// write
		err := storage.Write(ctx, name, bytes.NewReader([]byte("foo")), 3, nil)
		assert.Nil(t, err)

		// list
		n := 0
		err = storage.List(ctx, prefix+"/", func(isPrefix bool, key string, size int64) (bool, error) {
			n++
			assert.Equal(t, false, isPrefix)
			assert.Equal(t, name, key)
			assert.Equal(t, int64(3), size)
			return true, nil
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, n)

		// stat
		size, err := storage.Stat(ctx, name)
		assert.Nil(t, err)
		assert.Equal(t, int64(3), size)

		// exists
		exists, err := storage.Exists(ctx, name)
		assert.Nil(t, err)
		assert.True(t, exists)
		exists, err = storage.Exists(ctx, "bar")
		assert.Nil(t, err)
		assert.False(t, exists)

		// read
		r, err := storage.Read(ctx, name, nil, nil)
		assert.Nil(t, err)
		content, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, []byte("foo"), content)
		assert.Nil(t, r.Close())

		// delete
		err = storage.Delete(ctx, name)
		assert.Nil(t, err)
	})

}
