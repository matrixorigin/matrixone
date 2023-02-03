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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func New(
	ctx context.Context,
) (
	eng engine.Engine,
	client client.TxnClient,
	compilerContext plan.CompilerContext,
) {

	ck := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)

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
		mpool.MustNewZero(),
		ck,
		memoryengine.RandomIDGenerator,
	)
	if err != nil {
		panic(err)
	}

	client = memorystorage.NewStorageTxnClient(
		ck,
		map[string]*memorystorage.Storage{
			dnAddr: storage,
		},
	)

	e := memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(
			mpool.MustNewZero(),
		),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
		memoryengine.RandomIDGenerator,
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

	CreateR(db)
	CreateS(db)
	CreateT(db)
	CreateT1(db)
	CreatePart(db)
	CreateDate(db)
	CreateSupplier(db)
	CreateCustomer(db)
	CreateLineorder(db)

	if err = txnOp.Commit(ctx); err != nil {
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
