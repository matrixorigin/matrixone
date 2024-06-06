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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	defaultTimeout = time.Second * 10
)

func GetService() ShardService {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.ShardService)
	if !ok {
		return &service{}
	}
	return v.(ShardService)
}

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
	// Close close the shard server
	Close() error
}

// ShardService is sharding service. Each CN node holds an instance of the
// ShardService.
type ShardService interface {
	// Read read data from shards.
	Read(ctx context.Context, req ReadRequest, opts ReadOptions) error

	// GetShardInfo returns the metadata of the shards corresponding to the table.
	GetShardInfo(table uint64) (uint64, pb.Policy, bool, error)
	// Create creates table shards metadata in current txn. And create shard
	// binds after txn committed asynchronously. Nothing happened if txn aborted.
	//
	// ShardBalancer will allocate CN after TableShardBind created.
	Create(ctx context.Context, table uint64, txnOp client.TxnOperator) error
	// Delete deletes table shards metadata in current txn. Table shards need
	// to be deleted if table deleted. Nothing happened if txn aborted.
	Delete(ctx context.Context, table uint64, txnOp client.TxnOperator) error
	// ReplicaCount returns the number of running replicas on current cn.
	ReplicaCount() int64
	// Close close the service
	Close() error
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
	Available(accountID uint64, cn string) bool
	Draining(cn string) bool

	UpdateState(cn string, state metadata.WorkState)
}

// filter is used to filter out or select certain CNs when selecting CNs for ShardBalance.
type filter interface {
	filter(r *rt, cn []*cn) []*cn
}

type ReadFunc func(
	ctx context.Context,
	shard pb.TableShard,
	engine engine.Engine,
	payload []byte,
	ts timestamp.Timestamp,
) ([]byte, error)

// ShardStorage is used to store metadata for Table Shards, handle read operations for
// shards, and Log tail subscriptions.
type ShardStorage interface {
	// Get returns the latest metadata of the shards corresponding to the table.
	Get(table uint64) (uint64, pb.ShardsMetadata, error)
	// GetChanged returns the table ids of the shards that have been changed.
	GetChanged(tables map[uint64]uint32, applyDeleted func(uint64), applyChanged func(uint64)) error
	// Create creates the metadata for the sharding corresponding to the table with the given
	// transaction.
	Create(ctx context.Context, table uint64, txnOp client.TxnOperator) (bool, error)
	// Create delete the metadata for the sharding corresponding to the table with the given
	// transaction.
	Delete(ctx context.Context, table uint64, txnOp client.TxnOperator) (bool, error)
	// Unsubscribe unsubscribes the log tail of the tables.
	Unsubscribe(tables ...uint64) error
	// WaitLogAppliedAt wait until the log tail corresponding to ts has been fully consumed.
	// Ensure that subsequent reads have full log tail data.
	WaitLogAppliedAt(ctx context.Context, ts timestamp.Timestamp) error
	// Read read data with the given timestamp
	Read(ctx context.Context, shard pb.TableShard, method int, payload []byte, ts timestamp.Timestamp) ([]byte, error)
}

var (
	DefaultOptions = ReadOptions{}
)

type ReadOptions struct {
	hash    uint64
	readAt  timestamp.Timestamp
	shardID uint64
	adjust  func(*pb.TableShard)
}

const (
	ReadData   = 0
	ReadRanges = 1
	ReadStats  = 2
	ReadRows   = 3
)

type ReadRequest struct {
	TableID uint64
	Method  int
	Data    []byte
	Apply   func([]byte)
}
