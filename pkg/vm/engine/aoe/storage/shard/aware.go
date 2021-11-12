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

package shard

type Aware interface {
	ShardCreated(id uint64)
	ShardDeleted(id uint64)
	OnStats(stats interface{})
}

type Node interface {
	GetId() uint64
}

type NodeAware interface {
	Aware
	ShardNodeCreated(shardId, nodeId uint64)
	ShardNodeDeleted(shardId, nodeId uint64)
}
