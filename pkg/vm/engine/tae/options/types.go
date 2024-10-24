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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
)

const (
	DefaultIndexCacheSize = 256 * mpool.MB

	DefaultBulkTomestoneTxnThreshold = 10000 // rows
	DefaultLockMergePruneInterval    = time.Minute

	DefaultBlockMaxRows    = objectio.BlockMaxRows
	DefaultBlocksPerObject = uint16(256)

	DefaultScannerInterval              = time.Second * 5
	DefaultCheckpointFlushInterval      = time.Minute
	DefaultCheckpointTransferInterval   = time.Second * 5
	DefaultCheckpointMinCount           = int64(100)
	DefaultCheckpointIncremetalInterval = time.Minute
	DefaultCheckpointGlobalMinCount     = 10
	DefaultGlobalVersionInterval        = time.Hour
	DefaultGCCheckpointInterval         = time.Minute
	DefaultOverallFlushMemControl       = common.Const1GBytes

	DefaultScanGCInterval    = time.Minute * 30
	DefaultGCTTL             = time.Hour
	DefaultGCMergeCount      = 40
	DefaultGCDeleteBatchSize = 1000
	DefaultGCDeleteTimeout   = time.Minute * 10

	DefaultCatalogGCInterval = time.Minute * 3

	DefaultIOWorkers    = int(16)
	DefaultAsyncWorkers = int(16)

	DefaultLogtailTxnPageSize = 256

	DefaultLogstoreType = LogstoreBatchStore
)

type LogstoreType string

const (
	LogstoreBatchStore LogstoreType = "batchstore"
	LogstoreLogservice LogstoreType = "logservice"
)

type Options struct {
	StorageCfg    *StorageCfg    `toml:"storage-cfg"`
	CheckpointCfg *CheckpointCfg `toml:"checkpoint-cfg"`
	SchedulerCfg  *SchedulerCfg  `toml:"scheduler-cfg"`
	GCCfg         *GCCfg         `toml:"gc-cfg"`
	LogtailCfg    *LogtailCfg
	MergeCfg      *MergeConfig
	CatalogCfg    *CatalogCfg

	BulkTomestoneTxnThreshold uint64
	// MaxMessageSize is the size of max message which is sent to log-service.
	MaxMessageSize   uint64
	TransferTableTTL time.Duration
	IncrementalDedup bool
	IsStandalone     bool
	LogStoreT        LogstoreType

	Fs                fileservice.FileService                  `toml:"-"`
	LocalFs           fileservice.FileService                  `toml:"-"`
	Lc                logservicedriver.LogServiceClientFactory `toml:"-"`
	Ctx               context.Context                          `toml:"-"`
	Shard             metadata.TNShard                         `toml:"-"`
	Clock             clock.Clock                              `toml:"-"`
	TaskServiceGetter taskservice.Getter                       `toml:"-"`
	SID               string                                   `toml:"-"`
}
