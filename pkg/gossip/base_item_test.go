// Copyright 2021 - 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataCacheKey_AddItem(t *testing.T) {
	dc := newBaseStore[query.CacheKey]("127.0.0.1:8888")
	assert.NotNil(t, dc)
	k := query.CacheKey{
		Path:   "p1",
		Offset: 10,
		Sz:     10,
	}
	dc.AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_CacheKey{
			CacheKey: &k,
		},
	})
	assert.Equal(t, 1, len(dc.queueMu.itemQueue))
}

func TestStatsInfoKey_AddItem(t *testing.T) {
	si := newBaseStore[statsinfo.StatsInfoKey]("127.0.0.1:8888")
	assert.NotNil(t, si)
	k := statsinfo.StatsInfoKey{
		DatabaseID: 1,
		TableID:    2,
	}
	si.AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_StatsInfoKey{
			StatsInfoKey: &k,
		},
	})
	assert.Equal(t, 1, len(si.queueMu.itemQueue))
}

func TestDataCacheKey_Target(t *testing.T) {
	dc := newBaseStore[query.CacheKey]("127.0.0.1:8888")
	assert.NotNil(t, dc)
	k1 := query.CacheKey{
		Path:   "p1",
		Offset: 10,
		Sz:     10,
	}
	k2 := query.CacheKey{
		Path:   "p2",
		Offset: 20,
		Sz:     20,
	}
	dc.update("127.0.0.1:8889", []query.CacheKey{k1})
	target := dc.Target(k1)
	assert.Equal(t, "127.0.0.1:8889", target)

	target = dc.Target(k2)
	assert.Equal(t, "", target)
}

func TestStatsInfoKey_Target(t *testing.T) {
	dc := newBaseStore[statsinfo.StatsInfoKey]("127.0.0.1:8888")
	assert.NotNil(t, dc)
	k1 := statsinfo.StatsInfoKey{
		DatabaseID: 11,
		TableID:    12,
	}
	k2 := statsinfo.StatsInfoKey{
		DatabaseID: 21,
		TableID:    22,
	}
	dc.update("127.0.0.1:8889", []statsinfo.StatsInfoKey{k1})
	target := dc.Target(k1)
	assert.Equal(t, "127.0.0.1:8889", target)

	target = dc.Target(k2)
	assert.Equal(t, "", target)
}
