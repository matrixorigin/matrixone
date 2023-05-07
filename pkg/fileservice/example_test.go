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

func TestCacheExample(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewLocalFS(
		"rc",
		dir,
		CacheConfig{
			MemoryCapacity: 32 << 20,
		},
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
				ToObjectBytes: func(_ io.Reader, data []byte) ([]byte, int64, error) {
					return data, 8, nil
				},
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	value := vec.Entries[0].ObjectBytes
	assert.Equal(t, []byte("42"), value)

}
