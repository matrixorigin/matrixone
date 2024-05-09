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
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// ShardBalancer used for balance shard and cn bind.
// Balancer adheres to the following principles:
//  1. As far as possible, it ensures that the table's shards are evenly
//     distributed across the available CNs.
//  2. When an imbalance is found and the concurrency is readjusted, migrate
//     the least number of shards to the new CN.
//  3. TableShardBind.BindVersion is incremented when the shard is bound to
//     a new cn.
//
// Balancer periodically obtains information about the CNs in the cluster and
// performs a re-balance operation if it finds a change in the number of CNs.
//
// The balancer generates a corresponding CMD when it creates a new bind, and 2
// CMDs when it updates a Bind (one to delete the bind on the old cn, and one to
// add a new bind on the new cn). These CMDs are returned to the corresponding
// CMD for execution in the heartbeat request of each CN.
type ShardBalancer interface {
	// Add add a table shards to the balancer.
	Add(table uint64) error
	// Delete remove a table shards from the balancer.
	Delete(table uint64) error
	// GetBinds returns all table shard binds of the specified table. It will
	// create table shard binds if not exists.
	GetBinds(table uint64) ([]pb.TableShardBind, error)
	// Balance balances table shards and cn binds manually.
	Balance(table uint64) error
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
	GetBinds(table uint64) ([]pb.TableShardBind, error)
}

type Env interface {
	GetTableShards(table uint64) (pb.TableShards, error)
	GetPartitionIDs(table uint64) ([]uint64, error)
	HasCN(serviceID string) bool
}

// Operator is a description of a command that needs to be sent down to CN for execution.
// The command needs to be executed within a specified time frame. If the timeout is
// exceeded, the command is automatically terminated and the ShardBalancer recalculates
// to use the another CN to execute the command.
type Operator struct {
	CreateAt  time.Time
	TimeoutAt time.Time
	Cmd       pb.Cmd
	done      func(error)
}

// Filter is used to filter out or select certain CNs when selecting CNs for ShardBalance.
type Filter interface {
	// Source used to select source CNs.
	Source(cn []cn) []cn
	// Target used to select target CNs.
	Target(cn []cn) []cn
}

type shards struct {
	metadata  pb.TableShards
	binds     []pb.TableShardBind
	allocated bool
}

type state int

var (
	up   = state(0)
	down = state(1)
)

type cn struct {
	state    state
	last     time.Time
	metadata metadata.CNService
	binds    []pb.TableShardBind
}

func (c *cn) available(tenantID uint32) bool {
	if c.state == down {
		return false
	}
	// TODO: check tenantID
	return true
}
