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

package testengine

import (
	"context"
	"math"
	"time"

	"github.com/google/uuid"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func New(
	ctx context.Context,
) (
	eng engine.Engine,
	client client.TxnClient,
	compilerContext plan.CompilerContext,
) {

	ck := clock.DefaultClock()
	if ck == nil {
		ck = clock.NewHLCClock(func() int64 {
			return time.Now().Unix()
		}, math.MaxInt)
	}

	storage, err := memorystorage.NewMemoryStorage(
		testutil.NewMheap(),
		memorystorage.SnapshotIsolation,
		ck,
	)
	if err != nil {
		panic(err)
	}

	client = memorystorage.NewStorageTxnClient(
		ck,
		storage,
	)

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

	e := memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(
			testutil.NewMheap(),
		),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
	)

	txnOp, err := client.New()
	if err != nil {
		panic(err)
	}
	eng = e.Bind(txnOp)

	err = eng.Create(ctx, "test", txnOp)
	if err != nil {
		panic(err)
	}

	db, err := eng.Database(ctx, "test", txnOp)
	if err != nil {
		panic(err)
	}

	memEngine.CreateR(db)
	memEngine.CreateS(db)
	memEngine.CreateT(db)
	memEngine.CreateT1(db)
	memEngine.CreatePart(db)
	memEngine.CreateDate(db)
	memEngine.CreateSupplier(db)
	memEngine.CreateCustomer(db)
	memEngine.CreateLineorder(db)

	if err := txnOp.Commit(ctx); err != nil {
		panic(err)
	}

	txnOp, err = client.New()
	if err != nil {
		panic(err)
	}
	compilerContext = e.NewCompilerContext(ctx, "test", txnOp)
	eng = e.Bind(txnOp)

	return
}
