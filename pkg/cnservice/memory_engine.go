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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (s *service) initMemoryEngine(
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

	// engine
	mp, err := mpool.NewMPool("cnservice_mem_engine", 0, mpool.Mid)
	if err != nil {
		return err
	}
	pu.StorageEngine = memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(mp),
		pu.GetClusterDetails,
		memoryengine.NewHakeeperIDGenerator(hakeeper),
	)

	return nil
}

func (s *service) initMemoryEngineNonDist(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {
	ck := runtime.ProcessLevelRuntime().Clock()
	mp, err := mpool.NewMPool("cnservice_mem_engine_nondist", 0, mpool.Mid)
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

	engine := memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(mp),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
		memoryengine.RandomIDGenerator,
	)
	pu.StorageEngine = engine

	return nil
}
