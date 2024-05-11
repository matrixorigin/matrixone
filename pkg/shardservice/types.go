// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// ShardServer used for balance and allocate shards on their cns.
// ShardServer adheres to the following principles:
//  1. As far as possible, it ensures that the table's shards are evenly
//     distributed across the available CNs.
//  2. When an imbalance is found and the concurrency is readjusted, migrate
//     the least number of shards to the new CN.
//  3. TableShard.BindVersion is incremented when the shard is bound to
//     a new cn.
//
// ShardServer periodically obtains information about the CNs in the cluster
// and performs a re-balance operation if it finds a change in the number of CNs.
//
// The balancer generates a corresponding CMD when it creates a new bind, and 2
// CMDs when it move a shard to another cn (one to delete the bind on the old
// cn, and one to add a new bind on the new cn). These CMDs are returned to the
// corresponding CMD for execution in the heartbeat request of each CN.
type ShardServer interface {
	// Start start the server
	Start()
	// Stop stop the server
	Stop()
}

// ShardService is sharding service. Each CN node holds an instance of the
// ShardService.
type ShardService interface {
	// Create creates table shards metadata in current txn. And create shard
	// binds after txn committed asynchronously. Nothing happened if txn aborted.
	//
	// ShardBalancer will allocate CN after TableShardBind created.
	Create(table uint64, txnOp client.TxnOperator) error
	// Delete deletes table shards metadata in current txn. Table shards need
	// to be deleted if table deleted. Nothing happened if txn aborted.
	Delete(table uint64, txnOp client.TxnOperator) error
	GetShards(table uint64) ([]pb.TableShard, error)
}

// Scheduler is used to schedule shards on cn. Each scheduler is responsible for
// the scheduling of a single accusation, e.g. balance_scheduler is responsible
// for scheduling shard equalization between CNs. allocate_scheduler is responsible
// for the creation of shards for newly created Table.
type scheduler interface {
	// schedule schedules shards on CNs.
	schedule(r *rt, filters ...filter) error
}

type Env interface {
	HasCN(serviceID string) bool
	Available(tenantID uint32, cn string) bool
}

// filter is used to filter out or select certain CNs when selecting CNs for ShardBalance.
type filter interface {
	filter(r *rt, cn []*cn) []*cn
}
