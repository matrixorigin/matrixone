// Copyright 2021 - 2023 Matrix Origin
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

package gossip

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/stretchr/testify/assert"
)

func TestDistKeyCache_AddItem(t *testing.T) {
	dc := newDistKeyCache("127.0.0.1:8888")
	assert.NotNil(t, dc)
	ck := cache.CacheKey{
		Path:   "p1",
		Offset: 10,
		Sz:     10,
	}
	dc.AddItem(ck, gossip.Operation_Set)
	assert.Equal(t, 1, len(dc.queueMu.itemQueue))
}

func TestDistKeyCache_Target(t *testing.T) {
	dc := newDistKeyCache("127.0.0.1:8888")
	assert.NotNil(t, dc)
	k1 := cache.CacheKey{
		Path:   "p1",
		Offset: 10,
		Sz:     10,
	}
	k2 := cache.CacheKey{
		Path:   "p2",
		Offset: 20,
		Sz:     20,
	}
	tk := cache.TargetCacheKey{
		TargetKey: map[string]*cache.CacheKeys{
			"127.0.0.1:8889": {
				Keys: []cache.CacheKey{k1},
			},
		},
	}
	dc.updateCache(&tk)
	target := dc.Target(k1)
	assert.Equal(t, "127.0.0.1:8889", target)

	target = dc.Target(k2)
	assert.Equal(t, "", target)
}
