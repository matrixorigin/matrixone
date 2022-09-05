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

package txnstorage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/assert"
)

func TestFrontend(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Minute,
	)
	defer cancel()

	frontendParameters := &config.FrontendParameters{
		MoVersion:    "1",
		RootName:     "root",
		RootPassword: "111",
		DumpUser:     "dump",
		DumpPassword: "111",
	}
	frontendParameters.SetDefaultValues()

	engine := txnengine.New(
		ctx,
		new(txnengine.ShardToSingleStatic),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					{
						UUID:           uuid.NewString(),
						ServiceAddress: "1",
						Shards: []logservicepb.DNShardInfo{
							{
								ShardID:   2,
								ReplicaID: 2,
							},
						},
					},
				},
			}, nil
		},
	)

	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		testutil.NewMheap(),
		SnapshotIsolation,
		clock,
	)
	assert.Nil(t, err)
	txnClient := &StorageTxnClient{
		clock:   clock,
		storage: storage,
	}

	pu := &config.ParameterUnit{
		SV:            frontendParameters,
		HostMmu:       host.New(frontendParameters.HostMmuLimitation),
		Mempool:       mempool.New(),
		StorageEngine: engine,
		TxnClient:     txnClient,
		FileService:   testutil.NewFS(),
	}
	ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

	err = frontend.InitSysTenant(ctx)
	assert.Nil(t, err)
}
