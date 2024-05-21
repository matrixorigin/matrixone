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
	"testing"

	cache "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	// test New with postSet, postGet, postEvict
	c := NewCache(
		100,
		func(key cache.CacheKey, value CacheData) {},
		func(key cache.CacheKey, value CacheData) {},
		func(key cache.CacheKey, value CacheData) {},
	)
	// test Alloc and Set
	key := cache.CacheKey{Path: "x", Sz: 1}
	data := c.Alloc(1)
	err := c.Set(context.TODO(), key, data)
	require.NoError(t, err)
	// test Get
	data2, ok := c.Get(context.TODO(), key)
	require.True(t, ok)
	require.Equal(t, data, data2)
}
