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
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/memberlist"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

var (
	binaryEnc = binary.BigEndian
)

type delegate struct {
	logger *zap.Logger
	// distKeyCache keeps the key cache of local set/delete
	// items and remote key info.
	distKeyCache *DistKeyCache
	//cacheServiceAddr is the service address of the remote cache server.
	cacheServiceAddr string
}

func newDelegate(logger *zap.Logger, addr string) *delegate {
	return &delegate{
		logger:           logger,
		cacheServiceAddr: addr,
		distKeyCache:     newDistKeyCache(addr),
	}
}

func (d *delegate) getDistKeyCache() fileservice.KeyRouter {
	return d.distKeyCache
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
	var item gossip.CacheKeyItem
	if err := item.Unmarshal(data); err != nil {
		d.logger.Error("failed to unmarshal cache item from buf", zap.Error(err))
		return
	}
	d.distKeyCache.cacheMu.Lock()
	defer d.distKeyCache.cacheMu.Unlock()
	if item.Operation == gossip.Operation_Set {
		d.distKeyCache.cacheMu.keyTarget[item.CacheKey] = item.TargetAddress
	} else if item.Operation == gossip.Operation_Delete {
		delete(d.distKeyCache.cacheMu.keyTarget, item.CacheKey)
	}
}

// GetBroadcasts implements the memberlist.Delegate interface.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	// For the case that node leave from cluster.
	if d == nil {
		return nil
	}
	var items []gossip.CacheKeyItem
	d.distKeyCache.queueMu.Lock()
	count := len(d.distKeyCache.queueMu.itemQueue)
	if count == 0 {
		d.distKeyCache.queueMu.Unlock()
		return nil
	}
	sz := d.distKeyCache.queueMu.itemQueue[0].Size()
	limitCount := (limit - overhead) / sz
	if count < limitCount {
		items = d.distKeyCache.queueMu.itemQueue
		d.distKeyCache.queueMu.itemQueue = make([]gossip.CacheKeyItem, 0, defaultCacheQueueSize)
		d.distKeyCache.queueMu.Unlock()
	} else {
		items = d.distKeyCache.queueMu.itemQueue[:limitCount]
		d.distKeyCache.queueMu.itemQueue = d.distKeyCache.queueMu.itemQueue[limitCount:]
		d.distKeyCache.queueMu.Unlock()
	}

	data := make([][]byte, 0, len(items))
	for _, item := range items {
		bytes, err := item.Marshal()
		if err != nil {
			d.logger.Error("failed to marshal cache item", zap.Error(err))
			return nil
		}
		data = append(data, bytes)
	}
	return data
}

// LocalState implements the memberlist.Delegate interface.
func (d *delegate) LocalState(join bool) []byte {
	// If this a join action, there is no user data on this node.
	if join {
		return nil
	}
	// By converting structure, the space could be less.
	tk := cache.TargetCacheKey{
		TargetKey: make(map[string]*cache.CacheKeys),
	}
	d.distKeyCache.cacheMu.Lock()
	for key, target := range d.distKeyCache.cacheMu.keyTarget {
		if _, ok := tk.TargetKey[target]; !ok {
			tk.TargetKey[target] = &cache.CacheKeys{}
		}
		tk.TargetKey[target].Keys = append(tk.TargetKey[target].Keys, key)
	}
	d.distKeyCache.cacheMu.Unlock()

	data, err := tk.Marshal()
	if err != nil {
		d.logger.Error("failed to marshal cache data", zap.Error(err))
		return nil
	}
	dst := make([]byte, lz4.CompressBlockBound(len(data)+4))
	l := lz4.Compressor{}
	n, err := l.CompressBlock(data, dst[4:])
	if err != nil {
		d.logger.Error("failed to compress cache data", zap.Error(err))
		return nil
	}
	binaryEnc.PutUint32(dst, uint32(len(data)))
	return dst[:n+4]
}

// MergeRemoteState implements the memberlist.Delegate interface.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	// We do NOT merge remote user data if this is a pull/push action.
	// Means that, we only accept user data from a remote node when this
	// is a new node and is trying to join to the gossip cluster.
	if !join {
		return
	}
	sz := binaryEnc.Uint32(buf)
	dst := make([]byte, sz)
	n, err := lz4.UncompressBlock(buf[4:], dst)
	if err != nil {
		d.logger.Error("failed to uncompress cache data from buf", zap.Error(err))
		return
	}
	dst = dst[:n]
	tk := cache.TargetCacheKey{}
	if err := tk.Unmarshal(dst); err != nil {
		d.logger.Error("failed to unmarshal cache data", zap.Error(err))
		return
	}
	d.distKeyCache.updateCache(&tk)
}

// NotifyJoin implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyJoin(*memberlist.Node) {}

// NotifyLeave implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyLeave(node *memberlist.Node) {
	cacheServiceAddr := string(node.Meta)
	if len(cacheServiceAddr) == 0 {
		return
	}
	d.distKeyCache.cacheMu.Lock()
	defer d.distKeyCache.cacheMu.Unlock()
	for key, target := range d.distKeyCache.cacheMu.keyTarget {
		if cacheServiceAddr == target {
			delete(d.distKeyCache.cacheMu.keyTarget, key)
		}
	}
}

// NotifyUpdate implements the memberlist.EventDelegate interface.
func (d *delegate) NotifyUpdate(*memberlist.Node) {}
