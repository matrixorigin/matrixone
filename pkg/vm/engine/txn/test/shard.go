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

package testtxnengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

var _ txnengine.ShardPolicy = new(testEnv)

func (t *testEnv) Batch(batch *batch.Batch, nodes []logservicepb.DNNode) (shards []*txnengine.ShardedBatch, err error) {
	shards = append(shards, &txnengine.ShardedBatch{
		Shard: t.nodes[0].shard,
		Batch: batch,
	})
	return
}

func (t *testEnv) Vector(vec *vector.Vector, nodes []logservicepb.DNNode) (shards []*txnengine.ShardedVector, err error) {
	shards = append(shards, &txnengine.ShardedVector{
		Shard:  t.nodes[0].shard,
		Vector: vec,
	})
	return
}

func (t *testEnv) Nodes(nodes []logservicepb.DNNode) (shards []txnengine.Shard, err error) {
	for _, node := range nodes {
		for _, n := range t.nodes {
			if n.info.UUID == node.UUID {
				shards = append(shards, n.shard)
				break
			}
		}
	}
	return
}
