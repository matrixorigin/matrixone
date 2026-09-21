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

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
)

// GCReadProtector is the narrow snapshot-protection boundary exposed to a TAE
// pre-GC bootstrap hook. The concrete value is gc.SidecarReadProtector; keeping
// this interface here avoids making the low-level options package own a lease
// manager or planner dependency.
type GCReadProtector interface {
	Begin(context.Context) (
		register func(context.Context, []byte, []string, time.Time) error,
		rollback func(context.Context, []byte) error,
		close func(),
		err error,
	)
	Unregister(context.Context, []byte) error
}

// PreGCBootstrapContext identifies the exact TAE shard whose GC protection is
// being bootstrapped. SharedFileService is the same durable object service used
// by the TAE storage. The hook runs after SyncProtectionManager construction
// and before checkpoint replay or DiskCleaner.Start.
type PreGCBootstrapContext struct {
	SharedFileService fileservice.FileService
	Shard             metadata.TNShard
	Protector         GCReadProtector
	// StorageGeneration lazily obtains the identity of the locked, persistent
	// local directory. Only an explicitly opted-in recovery hook may call it,
	// synchronously during bootstrap. It is nil for a replay-only DB. Generic
	// bootstrap hooks do not create generation files or require host identity.
	StorageGeneration func() ([]byte, error)
}

type PreGCBootstrapHook func(context.Context, PreGCBootstrapContext) error

const (
	DefaultBulkTomestoneTxnThreshold = 10000 // rows
	DefaultLockMergePruneInterval    = time.Minute

	DefaultBlockMaxRows    = objectio.BlockMaxRows
	DefaultBlocksPerObject = uint16(256)

	DefaultScannerInterval               = time.Second * 5
	DefaultCheckpointFlushInterval       = time.Minute
	DefaultCheckpointTransferInterval    = time.Second * 5
	DefaultCheckpointMinCount            = int64(100)
	DefaultCheckpointIncrementalInterval = time.Minute * 5
	DefaultCheckpointIncremetalInterval  = DefaultCheckpointIncrementalInterval
	DefaultCheckpointGlobalMinCount      = 10
	DefaultGlobalVersionInterval         = time.Hour
	DefaultGCCheckpointInterval          = time.Minute
	DefaultOverallFlushMemControl        = common.Const1GBytes

	DefaultScanGCInterval    = time.Minute * 30
	DefaultGCTTL             = time.Hour
	DefaultGCInMemoryTTL     = time.Hour
	DefaultGCMergeCount      = 40
	DefaultGCScanCount       = 10
	DefaultGCDeleteBatchSize = 1000
	DefaultGCDeleteTimeout   = time.Minute * 10
	DefaultGCDeleteWorkerNum = 4

	DefaultCatalogGCInterval = time.Minute * 3

	DefaultIOWorkers    = int(16)
	DefaultAsyncWorkers = int(16)

	DefaultLogtailTxnPageSize = 256
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
	TransferTableTTL          time.Duration
	IncrementalDedup          bool
	IsStandalone              bool
	GCTimeCheckerFactory      func(any) func(*types.TS) bool

	Fs                fileservice.FileService                  `toml:"-"`
	LocalFs           fileservice.FileService                  `toml:"-"`
	TmpFs             *fileservice.TmpFileService              `toml:"-"`
	WalClientFactory  logservicedriver.LogServiceClientFactory `toml:"-"`
	Ctx               context.Context                          `toml:"-"`
	Shard             metadata.TNShard                         `toml:"-"`
	Clock             clock.Clock                              `toml:"-"`
	TaskServiceGetter taskservice.Getter                       `toml:"-"`
	SID               string                                   `toml:"-"`
	PreGCBootstrap    PreGCBootstrapHook                       `toml:"-"`
}
