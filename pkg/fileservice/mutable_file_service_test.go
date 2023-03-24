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
	"testing"

	"github.com/stretchr/testify/assert"
)

func testMutableFileService(
	t *testing.T,
	newFS func() MutableFileService,
) {

	ctx := context.Background()
	fs := newFS()
	err := fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 4,
				Data: []byte("abcd"),
			},
		},
	})
	assert.Nil(t, err)

	mutator, err := fs.NewMutator(ctx, "foo")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, mutator.Close())
	}()

	// overwrite
	err = mutator.Mutate(ctx, IOEntry{
		Size: 3,
		Data: []byte("123"),
	})
	assert.Nil(t, err)

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   4,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, []byte("123d"), vec.Entries[0].Data)

	// append
	err = mutator.Mutate(ctx, IOEntry{
		Offset: 4,
		Size:   1,
		Data:   []byte("q"),
	})
	assert.Nil(t, err)

	vec = &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   5,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, []byte("123dq"), vec.Entries[0].Data)

	// append
	err = mutator.Append(ctx, IOEntry{
		Offset: 0,
		Size:   3,
		Data:   []byte("123"),
	})
	assert.Nil(t, err)

	vec = &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, []byte("123dq123"), vec.Entries[0].Data)

	// context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = mutator.Mutate(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	err = mutator.Append(ctx)
	assert.ErrorIs(t, err, context.Canceled)

}
