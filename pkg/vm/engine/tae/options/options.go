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
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}

	if o.CacheCfg == nil {
		o.CacheCfg = &CacheCfg{
			IndexCapacity:  DefaultIndexCacheSize,
			InsertCapacity: DefaultMTCacheSize,
			TxnCapacity:    DefaultTxnCacheSize,
		}
	}

	if o.StorageCfg == nil {
		o.StorageCfg = &StorageCfg{
			BlockMaxRows:     DefaultBlockMaxRows,
			SegmentMaxBlocks: DefaultBlocksPerSegment,
		}
	}

	if o.CheckpointCfg == nil {
		o.CheckpointCfg = &CheckpointCfg{
			ScannerInterval:    DefaultScannerInterval,
			ExecutionInterval:  DefaultExecutionInterval,
			FlushInterval:      DefaultFlushInterval,
			ExecutionLevels:    DefaultExecutionLevels,
			CatalogCkpInterval: DefaultCatalogCkpInterval,
			CatalogUnCkpLimit:  DefaultCatalogUnCkpLimit,
		}
	}

	if o.SchedulerCfg == nil {
		o.SchedulerCfg = &SchedulerCfg{
			IOWorkers:    DefaultIOWorkers,
			AsyncWorkers: DefaultAsyncWorkers,
		}
	}

	if o.Clock == nil {
		o.Clock = clock.NewHLCClock(func() int64 {
			return time.Now().UTC().UnixNano()
		}, 0)
	}

	if o.LogtailCfg == nil {
		o.LogtailCfg = &LogtailCfg{
			PageSize: DefaultLogtailTxnPageSize,
		}
	}

	if o.LogStoreT == "" {
		o.LogStoreT = DefaultLogstoreType
	}

	return o
}
