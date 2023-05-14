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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ck := runtime.ProcessLevelRuntime().Clock()
	addr := "1"
	services := []metadata.DNService{{
		ServiceID:         uuid.NewString(),
		TxnServiceAddress: "1",
		Shards: []metadata.DNShard{
			{
				DNShardRecord: metadata.DNShardRecord{ShardID: 2},
				ReplicaID:     2,
			},
		},
	}}
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService,
		clusterservice.NewMOCluster(nil, 0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(nil, services)))

	storage, err := memorystorage.NewMemoryStorage(
		mpool.MustNewZeroNoFixed(),
		ck,
		memoryengine.RandomIDGenerator,
	)
	if err != nil {
		panic(err)
	}

	client = memorystorage.NewStorageTxnClient(
		ck,
		map[string]*memorystorage.Storage{
			addr: storage,
		},
	)

	e := memoryengine.New(
		ctx,
		memoryengine.NewDefaultShardPolicy(
			mpool.MustNewZeroNoFixed(),
		),
		memoryengine.RandomIDGenerator,
		clusterservice.GetMOCluster(),
	)

	txnOp, err := client.New(ctx, timestamp.Timestamp{})
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
	CreateCompressFileTable(db)

	if err = txnOp.Commit(ctx); err != nil {
		panic(err)
	}

	txnOp, err = client.New(ctx, timestamp.Timestamp{})
	if err != nil {
		panic(err)
	}
	compilerContext = e.NewCompilerContext(ctx, "test", txnOp)
	eng = e.Bind(txnOp)

	return
}
