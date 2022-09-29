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

package memorystorage

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
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"

	"github.com/prashantv/gostub"
)

func mockRecordStatement(ctx context.Context) (context.Context, *gostub.Stubs) {
	stm := &trace.StatementInfo{}
	ctx = trace.ContextWithStatement(ctx, stm)
	stubs := gostub.Stub(&frontend.RecordStatement, func(context.Context, *frontend.Session, *process.Process, frontend.ComputationWrapper, time.Time) context.Context {
		return ctx
	})
	return ctx, stubs
}

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

	hostMMU := host.New(frontendParameters.HostMmuLimitation)
	guestMMU := guest.New(frontendParameters.GuestMmuLimitation, hostMMU)
	heap := mheap.New(guestMMU)
	memoryPool := mempool.New()

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
	engine := memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(heap),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
	)

	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		heap,
		memtable.SnapshotIsolation,
		clock,
	)
	assert.Nil(t, err)
	txnClient := &StorageTxnClient{
		clock:   clock,
		storage: storage,
	}

	pu := &config.ParameterUnit{
		SV:            frontendParameters,
		HostMmu:       hostMMU,
		Mempool:       memoryPool,
		StorageEngine: engine,
		TxnClient:     txnClient,
		FileService:   testutil.NewFS(),
	}
	ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

	ctx, rsStubs := mockRecordStatement(ctx)
	defer rsStubs.Reset()

	err = frontend.InitSysTenant(ctx)
	assert.Nil(t, err)

	globalVars := new(frontend.GlobalSystemVariables)
	frontend.InitGlobalSystemVariables(globalVars)

	session := frontend.NewSession(
		frontend.NewMysqlClientProtocol(
			0,
			nil, // goetty IOSession
			1024,
			frontendParameters,
		),
		guestMMU,
		memoryPool,
		pu,
		globalVars,
	)
	session.SetRequestContext(ctx)

	_, err = session.AuthenticateUser("root")
	assert.Nil(t, err)

}
