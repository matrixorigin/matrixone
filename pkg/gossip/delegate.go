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

	"github.com/hashicorp/memberlist"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"go.uber.org/zap"
)

type Module interface {
	// Data returns all data that need to send to other nodes.
	// The second return value is the left size of the limit size.
	Data(limit int) ([]gossip.GossipData, int)
}

type delegate struct {
	logger *zap.Logger
	//cacheServiceAddr is the service address of the remote cache server.
	cacheServiceAddr string

	// dataCacheKey keeps the key cache of local set/delete
	// items and remote key info.
	// dataCacheKey *DataCacheKey
	dataCacheKey *BaseStore[query.CacheKey]

	// statsInfoKey keeps the keys of stats info.
	statsInfoKey *BaseStore[statsinfo.StatsInfoKey]
}

func newDelegate(logger *zap.Logger, addr string) *delegate {
	d := &delegate{
		logger:           logger,
		cacheServiceAddr: addr,
		dataCacheKey:     newBaseStore[query.CacheKey](addr),
		statsInfoKey:     newBaseStore[statsinfo.StatsInfoKey](addr),
	}
	return d
}

func (d *delegate) getDataCacheKey() client.KeyRouter[query.CacheKey] {
	return d.dataCacheKey
}

func (d *delegate) getStatsInfoKey() client.KeyRouter[statsinfo.StatsInfoKey] {
	return d.statsInfoKey
}

// NodeMeta implements the memberlist.Delegate interface.
func (d *delegate) NodeMeta(limit int) []byte {
	meta := []byte(d.cacheServiceAddr)
	if len(meta) > limit {
		panic(fmt.Sprintf("meta size %d is larger then limit %d", len(meta), limit))
	}
	return meta
}

// NotifyMsg implements the memberlist.Delegate interface.
func (d *delegate) NotifyMsg(data []byte) {
	if len(data) == 0 {
		return
	}
	var item gossip.GossipData
	if err := item.Unmarshal(data); err != nil {
		d.logger.Error("failed to unmarshal cache item from buf", zap.Error(err))
		return
	}
	switch t := item.Data.(type) {
	case *gossip.GossipData_CacheKeyItem:
		d.dataCacheKey.mu.Lock()
		defer d.dataCacheKey.mu.Unlock()
		m := d.dataCacheKey.mu.keyTarget

		if t.CacheKeyItem.Operation == gossip.Operation_Set {
			m[t.CacheKeyItem.CacheKey] = t.CacheKeyItem.TargetAddress
		} else if t.CacheKeyItem.Operation == gossip.Operation_Delete {
			delete(m, t.CacheKeyItem.CacheKey)
		}

	case *gossip.GossipData_StatsInfoKeyItem:
		d.statsInfoKey.mu.Lock()
		defer d.statsInfoKey.mu.Unlock()
		m := d.statsInfoKey.mu.keyTarget

		if t.StatsInfoKeyItem.Operation == gossip.Operation_Set {
			m[t.StatsInfoKeyItem.StatsInfoKey] = t.StatsInfoKeyItem.TargetAddress
		} else if t.StatsInfoKeyItem.Operation == gossip.Operation_Delete {
			delete(m, t.StatsInfoKeyItem.StatsInfoKey)
		}
	}
}

func (d *delegate) broadcastData(limit int) []gossip.GossipData {
	left := limit
	items, left := d.dataCacheKey.Data(left)
	data := make([]gossip.GossipData, 0, len(items))
	for _, item := range items {
		data = append(data, gossip.GossipData{
			Data: &gossip.GossipData_CacheKeyItem{
				CacheKeyItem: &gossip.CacheKeyItem{
					Operation:     item.Operation,
					TargetAddress: item.TargetAddress,
					CacheKey:      *item.Key.(*gossip.CommonItem_CacheKey).CacheKey,
				},
			},
		})
	}
	if left == 0 {
		return data
	}

	items, _ = d.statsInfoKey.Data(left)
	for _, item := range items {
		data = append(data, gossip.GossipData{
			Data: &gossip.GossipData_StatsInfoKeyItem{
				StatsInfoKeyItem: &gossip.StatsInfoKeyItem{
					Operation:     item.Operation,
					TargetAddress: item.TargetAddress,
					StatsInfoKey:  *item.Key.(*gossip.CommonItem_StatsInfoKey).StatsInfoKey,
				},
			},
		})
	}

	return data
}

// GetBroadcasts implements the memberlist.Delegate interface.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	// For the case that node leave from cluster.
	if d == nil {
		return nil
	}
	items := d.broadcastData(limit - overhead)
	data := make([][]byte, 0, len(items))
	for _, item := range items {
		bytes, err := item.Marshal()
		if err != nil {
			d.logger.Error("failed to marshal item", zap.Error(err))
			return nil
		}
		data = append(data, bytes)
	}
	return data
}

// LocalState implements the memberlist.Delegate interface.
func (d *delegate) LocalState(bool) []byte {
	// Routes are propagated incrementally by GetBroadcasts and NotifyMsg.
	// Do not expose route snapshots through memberlist push/pull state.
	return nil
}

// MergeRemoteState implements the memberlist.Delegate interface.
// Route snapshots are not part of MatrixOne's push/pull protocol, so ignore
// remote user state without parsing or allocating from untrusted contents.
func (d *delegate) MergeRemoteState([]byte, bool) {}

// NotifyJoin implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyJoin(*memberlist.Node) {}

// NotifyLeave implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyLeave(node *memberlist.Node) {
	cacheServiceAddr := string(node.Meta)
	if len(cacheServiceAddr) == 0 {
		return
	}
	d.dataCacheKey.removeByTarget(cacheServiceAddr)
	d.statsInfoKey.removeByTarget(cacheServiceAddr)
}

// NotifyUpdate implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyUpdate(*memberlist.Node) {}
