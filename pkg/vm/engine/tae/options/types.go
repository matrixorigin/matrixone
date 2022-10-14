// Copyright 2021 Matrix Origin
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

package options

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
)

const (
	// Temp unlimit the txn cache size.
	// In v0.6, we will limit the cache to a reasonable value
	DefaultTxnCacheSize   = mpool.TB
	DefaultIndexCacheSize = 128 * common.M
	DefaultMTCacheSize    = 4 * common.G

	DefaultBlockMaxRows     = uint32(40000)
	DefaultBlocksPerSegment = uint16(40)

	DefaultScannerInterval    = int64(5000)          // millisecond
	DefaultExecutionInterval  = int64(2000)          // millisecond
	DefaultFlushInterval      = int64(3 * 60 * 1000) // millisecond
	DefaultExecutionLevels    = int16(30)
	DefaultCatalogCkpInterval = int64(60000) // millisecond
	DefaultCatalogUnCkpLimit  = int64(10)

	DefaultIOWorkers    = int(8)
	DefaultAsyncWorkers = int(16)

	DefaultLogtailTxnPageSize = 1024

	DefaultLogstoreType = LogstoreBatchStore
)

type LogstoreType string

const (
	LogstoreBatchStore LogstoreType = "batchstore"
	LogstoreLogservice LogstoreType = "logservice"
)

type Options struct {
	CacheCfg      *CacheCfg      `toml:"cache-cfg"`
	StorageCfg    *StorageCfg    `toml:"storage-cfg"`
	CheckpointCfg *CheckpointCfg `toml:"checkpoint-cfg"`
	SchedulerCfg  *SchedulerCfg  `toml:"scheduler-cfg"`
	LogtailCfg    *LogtailCfg
	Catalog       *catalog.Catalog

	Clock     clock.Clock
	Fs        fileservice.FileService
	Lc        logservicedriver.LogServiceClientFactory
	Shard     metadata.DNShard
	LogStoreT LogstoreType
}
