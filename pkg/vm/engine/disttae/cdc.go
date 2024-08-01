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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func NewCdcEngine(
	ctx context.Context,
	service string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	etlFs fileservice.FileService,
	// cli client.TxnClient,
) *CdcEngine {
	cdcEng := &CdcEngine{
		service: service,
		mp:      mp,
		fs:      fs,
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
		//cli: cli,
	}

	if err := cdcEng.init(ctx); err != nil {
		panic(err)
	}

	cdcEng.pClient.LogtailRPCClientFactory = DefaultNewRpcStreamToTnLogTailService
	return cdcEng
}

func (cdcEng *CdcEngine) init(ctx context.Context) error {
	cdcEng.Lock()
	defer cdcEng.Unlock()

	cdcEng.catalog = cache.NewCatalog()
	cdcEng.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	return initEngine(ctx, cdcEng.service, cdcEng.catalog, cdcEng.partitions, cdcEng.mp, cdcEng.packerPool)
}

func (cdcEng *CdcEngine) Enqueue(tail *logtail.TableLogtail) {}

func (cdcEng *CdcEngine) GetOrCreateLatestPart(databaseId uint64, tableId uint64) *logtailreplay.Partition {
	cdcEng.Lock()
	defer cdcEng.Unlock()
	partition, ok := cdcEng.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition(cdcEng.service, tableId)
		cdcEng.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (cdcEng *CdcEngine) GetLatestCatalogCache() *cache.CatalogCache {
	return cdcEng.catalog
}

func (cdcEng *CdcEngine) Get(ptr **types.Packer) fileservice.PutBack[*types.Packer] {
	return cdcEng.packerPool.Get(ptr)
}

func (cdcEng *CdcEngine) GetMPool() *mpool.MPool {
	return cdcEng.mp
}

func (cdcEng *CdcEngine) GetFS() fileservice.FileService {
	return cdcEng.fs
}

func (cdcEng *CdcEngine) GetService() string {
	return cdcEng.service
}

func (cdcEng *CdcEngine) getTNServices() []DNStore {
	return getTNServices(cdcEng.service)
}

func (cdcEng *CdcEngine) setPushClientStatus(ready bool) {
	cdcEng.Lock()
	defer cdcEng.Unlock()
	cdcEng.pClient.setStatusUnlock(ready)
}

func (cdcEng *CdcEngine) abortAllRunningTxn() {}

func (cdcEng *CdcEngine) CopyPartitions() map[[2]uint64]*logtailreplay.Partition {
	return copyPartitionsLock(&cdcEng.RWMutex, cdcEng.partitions)
}

func (cdcEng *CdcEngine) cleanMemoryTableWithTable(dbId, tblId uint64) {
	cleanMemoryTableWithTableLock(&cdcEng.RWMutex, cdcEng.partitions, nil, dbId, tblId)
}

func (cdcEng *CdcEngine) PushClient() *PushClient {
	return &cdcEng.pClient
}

func (cdcEng *CdcEngine) Cli() client.TxnClient {
	//return cdcEng.cli
	panic("usp")
}

func (cdcEng *CdcEngine) New(ctx context.Context, op client.TxnOperator) error {
	panic("usp")
}
