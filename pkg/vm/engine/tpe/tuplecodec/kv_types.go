// Copyright 2021 Matrix Origin
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

package tuplecodec

import (
	"fmt"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

type KVType int

const (
	KV_MEMORY KVType = iota
	KV_CUBE   KVType = iota + 1
)

type KVHandler interface {
	GetKVType() KVType

	// NextID gets the next id of the type
	NextID(typ string) (uint64, error)

	// Set writes the key-value (overwrite)
	Set(key TupleKey, value TupleValue) error

	// SetBatch writes the batch of key-value (overwrite)
	SetBatch(keys []TupleKey, values []TupleValue) error

	// DedupSet writes the key-value. It will fail if the key exists
	DedupSet(key TupleKey, value TupleValue) error

	// DedupSetBatch writes the batch of keys-values. It will fail if there is one key exists
	DedupSetBatch(keys []TupleKey, values []TupleValue) error

	// Delete deletes the key
	Delete(key TupleKey) error

	// DeleteWithPrefix keys with the prefix
	DeleteWithPrefix(prefix TupleKey) error

	// Get gets the value of the key
	Get(key TupleKey) (TupleValue, error)

	// GetBatch gets the values of the keys
	GetBatch(keys []TupleKey) ([]TupleValue, error)

	// GetRange gets the values among the range [startKey,endKey).
	GetRange(startKey TupleKey, endKey TupleKey) ([]TupleValue, error)

	// GetRangeWithLimit gets the values from the startKey with limit
	//return parameters:
	//[][]byte : return keys
	//[][]byte : return values
	//bool: true - the scanner accomplished in all shards.
	//[]byte : the start key for the next scan. If last parameter is false, this parameter is nil.
	GetRangeWithLimit(startKey TupleKey, endKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error)

	GetRangeWithPrefixLimit(startKey TupleKey, endKey TupleKey, prefix TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error)

	// GetWithPrefix gets the values of the prefix with limit.
	// The prefixLen denotes the prefix[:prefixLen] is the real prefix.
	// When we invoke GetWithPrefix several times, the prefix is the real
	// prefix in the first time. But from the second time, the prefix is the
	// last key in previous results of the GetWithPrefix.
	//return parameters:
	//[][]byte : return keys
	//[][]byte : return values
	//bool: true - the scanner accomplished in all shards.
	//[]byte : the start key for the next scan. If last parameter is false, this parameter is nil.
	GetWithPrefix(prefixOrStartkey TupleKey, prefixLen int, prefixEnd []byte, needKeyOnly bool, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error)

	// GetShardsWithRange get the shards that holds the range [startKey,endKey)
	GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error)

	// GetShardsWithPrefix get the shards that holds the keys with prefix
	GetShardsWithPrefix(prefix TupleKey) (interface{}, error)
}

type CubeShards struct {
	Shards []metapb.Shard `json:"shards"`
}

func (cs CubeShards) String() string {
	s := fmt.Sprintf("shardCont %d \n", len(cs.Shards))
	for i, shard := range cs.Shards {
		s += fmt.Sprintf("[shardIndex %d shardId %d startKey %v endKey %v] ;\n",
			i,
			shard.GetID(),
			shard.GetStart(),
			shard.GetEnd())
	}
	return s
}

type ShardNode struct {
	//the address of the store of the leader replica of the shard
	Addr string
	//the id of the store of the leader replica of the shard
	StoreID uint64
	//the bytes of the id
	StoreIDbytes string
	//shards the node will read
	Shards CubeShards
}

type ShardInfo struct {
	startKey   []byte
	endKey     []byte
	shardID    uint64
	statistics metapb.ShardStats
	node       ShardNode
}

func (si ShardInfo) GetStartKey() []byte {
	return si.startKey
}

func (si ShardInfo) GetEndKey() []byte {
	return si.endKey
}

func (si ShardInfo) GetShardID() uint64 {
	return si.shardID
}
func (si ShardInfo) GetStatistics() metapb.ShardStats {
	return si.statistics
}

func (si ShardInfo) GetShardNode() ShardNode {
	return si.node
}

type Shards struct {
	//all nodes that hold the table
	nodes []ShardNode

	//all shards that hold the table
	shardInfos []ShardInfo
}

func (s Shards) GetShardInfos() []ShardInfo {
	return s.shardInfos
}

func (s Shards) GetShardNodes() []ShardNode {
	return s.nodes
}

func (s *Shards) SetShardInfos(infos []ShardInfo) {
	s.shardInfos = infos
}

func (s *Shards) SetShardNodes(nodes []ShardNode) {
	s.nodes = nodes
}
