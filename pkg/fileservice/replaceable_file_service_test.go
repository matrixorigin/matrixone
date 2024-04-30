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

func testReplaceableFileService(
	t *testing.T,
	newFS func() ReplaceableFileService,
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

	err = fs.Replace(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 2,
				Data: []byte("cd"),
			},
		},
	})
	assert.Nil(t, err)

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: -1,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	assert.Equal(t, 2, len(vec.Entries[0].Data))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = fs.Replace(ctx, IOVector{})
	assert.ErrorIs(t, err, context.Canceled)

}
