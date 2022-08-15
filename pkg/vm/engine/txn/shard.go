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

package txnengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type Shard = metadata.DNShard

type ShardPolicy interface {
	Vector(vec *vector.Vector, nodes []logservicepb.DNNode) ([]*ShardedVector, error)
	Batch(batch *batch.Batch, nodes []logservicepb.DNNode) ([]*ShardedBatch, error)
	Nodes(nodes []logservicepb.DNNode) ([]Shard, error)
}

type ShardedVector struct {
	Shard  Shard
	Vector *vector.Vector
}

type ShardedBatch struct {
	Shard Shard
	Batch *batch.Batch
}

func (e *Engine) allNodesShards() ([]Shard, error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}
	return e.shardPolicy.Nodes(clusterDetails.DNNodes)
}

func (e *Engine) firstNodeShard() ([]Shard, error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}
	return e.shardPolicy.Nodes(clusterDetails.DNNodes[:1])
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
