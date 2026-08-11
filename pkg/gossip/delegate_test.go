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
	"fmt"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestDelegate_NodeMeta(t *testing.T) {
	d := newDelegate(&zap.Logger{}, "127.0.0.1:8889")
	m := d.NodeMeta(100)
	assert.NotNil(t, m)
	assert.Equal(t, "127.0.0.1:8889", string(m))
}

func TestDelegate_DataCache_GetBroadcastsAndNotify(t *testing.T) {
	d := newDelegate(&zap.Logger{}, "127.0.0.1:8889")
	data := d.GetBroadcasts(4, 32*1024)
	assert.Equal(t, 0, len(data))

	for i := 0; i < 10; i++ {
		ck := query.CacheKey{
			Path:   fmt.Sprintf("p%d", i),
			Offset: int64(10 * i),
			Sz:     int64(10 * i),
		}
		d.getDataCacheKey().AddItem(gossip.CommonItem{
			Operation: gossip.Operation_Set,
			Key: &gossip.CommonItem_CacheKey{
				CacheKey: &ck,
			},
		})
		assert.Equal(t, i+1, len(d.dataCacheKey.queueMu.itemQueue))
	}

	data = d.GetBroadcasts(4, 32*1024)
	assert.NotNil(t, data)
	assert.Len(t, data, 10)

	t.Run("self", func(t *testing.T) {
		for _, single := range data {
			d.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d.dataCacheKey.mu.keyTarget))
		for i := 0; i < 15; i++ {
			target := d.getDataCacheKey().Target(query.CacheKey{
				Path:   fmt.Sprintf("p%d", i),
				Offset: int64(10 * i),
				Sz:     int64(10 * i),
			})
			assert.Equal(t, "", target)
		}
	})

	t.Run("other", func(t *testing.T) {
		d1 := newDelegate(&zap.Logger{}, "127.0.0.1:7779")
		for _, single := range data {
			d1.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d1.dataCacheKey.mu.keyTarget))
		for i := 0; i < 15; i++ {
			target := d1.getDataCacheKey().Target(query.CacheKey{
				Path:   fmt.Sprintf("p%d", i),
				Offset: int64(10 * i),
				Sz:     int64(10 * i),
			})
			if i < 10 {
				assert.Equal(t, "127.0.0.1:8889", target)
			} else {
				assert.Equal(t, "", target)
			}
		}
	})
}

func TestDelegate_StatsInfo_GetBroadcastsAndNotify(t *testing.T) {
	d := newDelegate(&zap.Logger{}, "127.0.0.1:8889")
	data := d.GetBroadcasts(4, 32*1024)
	assert.Equal(t, 0, len(data))

	var i uint64
	for i = 0; i < 10; i++ {
		ck := statsinfo.StatsInfoKey{
			DatabaseID: i * 2,
			TableID:    i * 3,
		}
		d.getStatsInfoKey().AddItem(gossip.CommonItem{
			Operation: gossip.Operation_Set,
			Key: &gossip.CommonItem_StatsInfoKey{
				StatsInfoKey: &ck,
			},
		})
		assert.Equal(t, int(i+1), len(d.statsInfoKey.queueMu.itemQueue))
	}

	data = d.GetBroadcasts(4, 32*1024)
	assert.NotNil(t, data)
	assert.Len(t, data, 10)

	t.Run("self", func(t *testing.T) {
		for _, single := range data {
			d.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d.statsInfoKey.mu.keyTarget))
		var i uint64
		for i = 0; i < 15; i++ {
			target := d.getStatsInfoKey().Target(statsinfo.StatsInfoKey{
				DatabaseID: i * 2,
				TableID:    i * 3,
			})
			assert.Equal(t, "", target)
		}
	})

	t.Run("other", func(t *testing.T) {
		d1 := newDelegate(&zap.Logger{}, "127.0.0.1:7779")
		for _, single := range data {
			d1.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d1.statsInfoKey.mu.keyTarget))
		var i uint64
		for i = 0; i < 15; i++ {
			target := d1.getStatsInfoKey().Target(statsinfo.StatsInfoKey{
				DatabaseID: i * 2,
				TableID:    i * 3,
			})
			if i < 10 {
				assert.Equal(t, "127.0.0.1:8889", target)
			} else {
				assert.Equal(t, "", target)
			}
		}
	})
}

func TestDelegateNotifyLeaveRemovesRoutesOwnedByDepartedNode(t *testing.T) {
	const (
		receiverAddr  = "127.0.0.1:7778"
		departingAddr = "127.0.0.1:8888"
		survivingAddr = "127.0.0.1:9998"
	)
	receiver := newDelegate(zap.NewNop(), receiverAddr)
	departing := newDelegate(zap.NewNop(), departingAddr)
	surviving := newDelegate(zap.NewNop(), survivingAddr)

	departingDataKey := query.CacheKey{Path: "departing"}
	survivingDataKey := query.CacheKey{Path: "surviving"}
	departingStatsKey := statsinfo.StatsInfoKey{DatabaseID: 1, TableID: 1}
	overwrittenStatsKey := statsinfo.StatsInfoKey{DatabaseID: 2, TableID: 2}
	survivingStatsKey := statsinfo.StatsInfoKey{DatabaseID: 3, TableID: 3}

	departing.getDataCacheKey().AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_CacheKey{
			CacheKey: &departingDataKey,
		},
	})
	for _, key := range []*statsinfo.StatsInfoKey{&departingStatsKey, &overwrittenStatsKey} {
		departing.getStatsInfoKey().AddItem(gossip.CommonItem{
			Operation: gossip.Operation_Set,
			Key: &gossip.CommonItem_StatsInfoKey{
				StatsInfoKey: key,
			},
		})
	}
	for _, data := range departing.GetBroadcasts(4, 32*1024) {
		receiver.NotifyMsg(data)
	}

	surviving.getDataCacheKey().AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_CacheKey{
			CacheKey: &survivingDataKey,
		},
	})
	for _, key := range []*statsinfo.StatsInfoKey{&overwrittenStatsKey, &survivingStatsKey} {
		surviving.getStatsInfoKey().AddItem(gossip.CommonItem{
			Operation: gossip.Operation_Set,
			Key: &gossip.CommonItem_StatsInfoKey{
				StatsInfoKey: key,
			},
		})
	}
	for _, data := range surviving.GetBroadcasts(4, 32*1024) {
		receiver.NotifyMsg(data)
	}

	assert.Equal(t, departingAddr, receiver.getDataCacheKey().Target(departingDataKey))
	assert.Equal(t, survivingAddr, receiver.getDataCacheKey().Target(survivingDataKey))
	assert.Equal(t, departingAddr, receiver.getStatsInfoKey().Target(departingStatsKey))
	assert.Equal(t, survivingAddr, receiver.getStatsInfoKey().Target(overwrittenStatsKey))
	assert.Equal(t, survivingAddr, receiver.getStatsInfoKey().Target(survivingStatsKey))

	receiver.NotifyLeave(&memberlist.Node{Meta: []byte(departingAddr)})

	assert.Empty(t, receiver.getDataCacheKey().Target(departingDataKey))
	assert.Equal(t, survivingAddr, receiver.getDataCacheKey().Target(survivingDataKey))
	assert.Empty(t, receiver.getStatsInfoKey().Target(departingStatsKey))
	assert.Equal(t, survivingAddr, receiver.getStatsInfoKey().Target(overwrittenStatsKey))
	assert.Equal(t, survivingAddr, receiver.getStatsInfoKey().Target(survivingStatsKey))
}

func TestDelegatePushPullStateDisabled(t *testing.T) {
	const senderAddr = "127.0.0.1:8888"
	sender := newDelegate(zap.NewNop(), senderAddr)
	receiver := newDelegate(zap.NewNop(), "127.0.0.1:8889")
	dataKey := query.CacheKey{Path: "data", Offset: 10, Sz: 20}
	statsKey := statsinfo.StatsInfoKey{DatabaseID: 30, TableID: 40}

	sender.getDataCacheKey().AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_CacheKey{
			CacheKey: &dataKey,
		},
	})
	sender.getStatsInfoKey().AddItem(gossip.CommonItem{
		Operation: gossip.Operation_Set,
		Key: &gossip.CommonItem_StatsInfoKey{
			StatsInfoKey: &statsKey,
		},
	})
	for _, data := range sender.GetBroadcasts(4, 32*1024) {
		receiver.NotifyMsg(data)
	}

	assertRoutesUnchanged := func(t *testing.T) {
		t.Helper()
		receiver.dataCacheKey.mu.Lock()
		assert.Equal(t, map[query.CacheKey]string{dataKey: senderAddr}, receiver.dataCacheKey.mu.keyTarget)
		receiver.dataCacheKey.mu.Unlock()
		receiver.statsInfoKey.mu.Lock()
		assert.Equal(t, map[statsinfo.StatsInfoKey]string{statsKey: senderAddr}, receiver.statsInfoKey.mu.keyTarget)
		receiver.statsInfoKey.mu.Unlock()
	}
	assertRoutesUnchanged(t)
	assert.Nil(t, receiver.LocalState(false))
	assert.Nil(t, receiver.LocalState(true))

	testCases := []struct {
		name string
		buf  []byte
	}{
		{name: "empty", buf: nil},
		{name: "one-byte", buf: []byte{0x01}},
		{name: "two-bytes", buf: []byte{0x01, 0x02}},
		{name: "three-bytes", buf: []byte{0x01, 0x02, 0x03}},
		{name: "four-bytes", buf: []byte{0x00, 0x00, 0x00, 0x00}},
		{name: "random-bytes", buf: []byte{0x7a, 0x31, 0xf4, 0x09, 0x88, 0x42, 0xce}},
		{name: "maximum-size-header", buf: []byte{0xff, 0xff, 0xff, 0xff}},
	}
	for _, join := range []bool{false, true} {
		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("join=%t/%s", join, testCase.name), func(t *testing.T) {
				assert.NotPanics(t, func() {
					receiver.MergeRemoteState(testCase.buf, join)
				})
				assertRoutesUnchanged(t)
			})
		}
	}
}
