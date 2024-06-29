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

package memorycache

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	cachepb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	ctx := context.Background()

	cache := NewCache(
		1<<30,
		nil,
		nil,
		nil,
		malloc.GetDefault(nil),
	)
	defer cache.Flush()

	for i := 1; i < 1024; i++ {
		key := cachepb.CacheKey{
			Path: fmt.Sprintf("%d", i),
			Sz:   int64(i),
		}

		data := cache.Alloc(i)
		rand.Read(data.Bytes())

		err := cache.Set(ctx, key, data)
		require.NoError(t, err)
		data.Release()

		data2, ok := cache.Get(ctx, key)
		require.True(t, ok)
		require.Equal(t, data.Bytes(), data2.Bytes())
		data2.Release()
	}

}
