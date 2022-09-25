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
	"github.com/matrixorigin/matrixone/pkg/config"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	txnstorage "github.com/matrixorigin/matrixone/pkg/txn/storage/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
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
	guestMMU := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	heap := mheap.New(guestMMU)
	pu.StorageEngine = txnengine.New(
		ctx,
		txnengine.NewDefaultShardPolicy(heap),
		txnengine.GetClusterDetailsFromHAKeeper(
			ctx,
			hakeeper,
		),
	)

	return nil
}

func (s *service) initMemoryEngineNonDist(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {
	ck := clock.DefaultClock()
	if ck == nil {
		ck = clock.NewHLCClock(func() int64 {
			return time.Now().Unix()
		}, math.MaxInt)
	}

	storage, err := txnstorage.NewMemoryStorage(
		testutil.NewMheap(),
		txnstorage.SnapshotIsolation,
		ck,
	)
	if err != nil {
		return err
	}

	txnClient := txnstorage.NewStorageTxnClient(
		ck,
		storage,
	)
	pu.TxnClient = txnClient

	shard := logservicepb.DNShardInfo{
		ShardID:   2,
		ReplicaID: 2,
	}
	shards := []logservicepb.DNShardInfo{
		shard,
	}
	dnStore := logservicepb.DNStore{
		UUID:           uuid.NewString(),
		ServiceAddress: "1",
		Shards:         shards,
	}
	guestMMU := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
	heap := mheap.New(guestMMU)
	engine := txnengine.New(
		ctx,
		txnengine.NewDefaultShardPolicy(heap),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
	)
	pu.StorageEngine = engine

	return nil
}
