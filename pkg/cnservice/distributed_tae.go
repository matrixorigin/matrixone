// Copyright 2021 - 2022 Matrix Origin
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

package cnservice

import (
	"context"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *service) initDistributedTAE(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {

	// txn client
	client, err := s.getTxnClient()
	if err != nil {
		return err
	}
	pu.TxnClient = client

	// hakeeper
	hakeeper, err := s.getHAKeeperClient()
	if err != nil {
		return err
	}

	txnOperator, err := pu.TxnClient.New()
	if err != nil {
		return err
	}

	// Should be no fixed or some size?
	mp, err := mpool.NewMPool("distributed_tae", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	proc := process.New(ctx, mp, pu.TxnClient, txnOperator, pu.FileService)

	// engine
	pu.StorageEngine = disttae.New(
		proc,
		ctx,
		client,
		hakeeper,
		memoryengine.GetClusterDetailsFromHAKeeper(
			ctx,
			hakeeper,
		),
	)

	return nil
}

func (s *service) initDistributedTAEDebug(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {

	ck := clock.DefaultClock()
	if ck == nil {
		ck = clock.NewHLCClock(func() int64 {
			return time.Now().Unix()
		}, math.MaxInt)
	}

	// Should be no fixed or some size?
	mp, err := mpool.NewMPool("distributed_tae_debug", 0, mpool.NoFixed)
	if err != nil {
		return err
	}

	shard := logservicepb.DNShardInfo{
		ShardID:   2,
		ReplicaID: 2,
	}
	shards := []logservicepb.DNShardInfo{
		shard,
	}
	dnAddr := "1"
	dnStore := logservicepb.DNStore{
		UUID:           uuid.NewString(),
		ServiceAddress: dnAddr,
		Shards:         shards,
	}

	storage, err := memorystorage.NewMemoryStorage(
		mp,
		memorystorage.SnapshotIsolation,
		ck,
		memoryengine.RandomIDGenerator,
	)
	if err != nil {
		return err
	}

	txnClient := memorystorage.NewStorageTxnClient(
		ck,
		map[string]*memorystorage.Storage{
			dnAddr: storage,
		},
	)
	pu.TxnClient = txnClient

	txnOperator, err := pu.TxnClient.New()
	if err != nil {
		return err
	}

	proc := process.New(ctx, mp, pu.TxnClient, txnOperator, pu.FileService)

	// engine
	pu.StorageEngine = disttae.New(
		proc,
		ctx,
		txnClient,
		memoryengine.RandomIDGenerator,
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
	)

	return nil
}
