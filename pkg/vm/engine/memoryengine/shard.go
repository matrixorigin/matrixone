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

package memoryengine

import (
	"context"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type Shard = metadata.DNShard

type shardsFunc = func() ([]Shard, error)

type getDefsFunc = func(context.Context) ([]engine.TableDef, error)

func NewDefaultShardPolicy(heap *mheap.Mheap) ShardPolicy {
	return FallbackShard{
		NewHashShard(heap),
		new(NoShard),
	}
}

type ShardPolicy interface {
	Vector(
		ctx context.Context,
		tableID ID,
		getDefs getDefsFunc,
		colName string,
		vec *vector.Vector,
		nodes []logservicepb.DNStore,
	) (
		sharded []*ShardedVector,
		err error,
	)

	Batch(
		ctx context.Context,
		tableID ID,
		getDefs getDefsFunc,
		batch *batch.Batch,
		nodes []logservicepb.DNStore,
	) (
		sharded []*ShardedBatch,
		err error,
	)
}

type ShardedVector struct {
	Shard  Shard
	Vector *vector.Vector
}

type ShardedBatch struct {
	Shard Shard
	Batch *batch.Batch
}

func (e *Engine) allShards() (shards []Shard, err error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}
	for _, store := range clusterDetails.DNStores {
		for _, shard := range store.Shards {
			shards = append(shards, Shard{
				DNShardRecord: metadata.DNShardRecord{
					ShardID: shard.ShardID,
				},
				ReplicaID: shard.ReplicaID,
				Address:   store.ServiceAddress,
			})
		}
	}
	return
}

func (e *Engine) anyShard() (shards []Shard, err error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}
	for _, store := range clusterDetails.DNStores {
		for _, shard := range store.Shards {
			shards = append(shards, Shard{
				DNShardRecord: metadata.DNShardRecord{
					ShardID: shard.ShardID,
				},
				ReplicaID: shard.ReplicaID,
				Address:   store.ServiceAddress,
			})
			return
		}
	}
	return
}

func thisShard(shard Shard) func() ([]Shard, error) {
	shards := []Shard{shard}
	return func() ([]Shard, error) {
		return shards, nil
	}
}

func theseShards(shards []Shard) func() ([]Shard, error) {
	return func() ([]Shard, error) {
		return shards, nil
	}
}

// NoShard doesn't do sharding at all
type NoShard struct {
	setOnce sync.Once
	shard   Shard
}

func (s *NoShard) setShard(stores []logservicepb.DNStore) {
	s.setOnce.Do(func() {
		type ShardInfo struct {
			Store logservicepb.DNStore
			Shard logservicepb.DNShardInfo
		}
		infos := make([]ShardInfo, 0, len(stores))
		for _, store := range stores {
			for _, info := range store.Shards {
				infos = append(infos, ShardInfo{
					Store: store,
					Shard: info,
				})
			}
		}
		if len(infos) == 0 {
			panic("no shard")
		}
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Shard.ShardID < infos[j].Shard.ShardID
		})
		info := infos[0]
		s.shard = Shard{
			DNShardRecord: metadata.DNShardRecord{
				ShardID: info.Shard.ShardID,
			},
			ReplicaID: info.Shard.ReplicaID,
			Address:   info.Store.ServiceAddress,
		}
	})
}

var _ ShardPolicy = new(NoShard)

func (s *NoShard) Vector(
	ctx context.Context,
	tableID ID,
	getDefs getDefsFunc,
	colName string,
	vec *vector.Vector,
	nodes []logservicepb.DNStore,
) (
	sharded []*ShardedVector,
	err error,
) {
	s.setShard(nodes)
	sharded = append(sharded, &ShardedVector{
		Shard:  s.shard,
		Vector: vec,
	})
	return
}

func (s *NoShard) Batch(
	ctx context.Context,
	tableID ID,
	getDefs getDefsFunc,
	bat *batch.Batch,
	nodes []logservicepb.DNStore,
) (
	sharded []*ShardedBatch,
	err error,
) {
	s.setShard(nodes)
	sharded = append(sharded, &ShardedBatch{
		Shard: s.shard,
		Batch: bat,
	})
	return
}
