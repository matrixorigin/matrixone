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

func WithTransferTableTTL(ttl time.Duration) func(*Options) {
	return func(opts *Options) {
		opts.TransferTableTTL = ttl
	}
}

func WithCheckpointMinCount(count int64) func(*Options) {
	return func(opts *Options) {
		if opts.CheckpointCfg == nil {
			opts.CheckpointCfg = new(CheckpointCfg)
		}
		opts.CheckpointCfg.MinCount = count
	}
}

func WithFlushInterval(interval time.Duration) func(*Options) {
	return func(opts *Options) {
		if opts.CheckpointCfg == nil {
			opts.CheckpointCfg = new(CheckpointCfg)
		}
		opts.CheckpointCfg.FlushInterval = interval
	}
}

func WithCheckpointScanInterval(interval time.Duration) func(*Options) {
	return func(opts *Options) {
		if opts.CheckpointCfg == nil {
			opts.CheckpointCfg = new(CheckpointCfg)
		}
		opts.CheckpointCfg.ScanInterval = interval
	}
}

func WithCheckpointIncrementaInterval(interval time.Duration) func(*Options) {
	return func(opts *Options) {
		if opts.CheckpointCfg == nil {
			opts.CheckpointCfg = new(CheckpointCfg)
		}
		opts.CheckpointCfg.IncrementalInterval = interval
	}
}

func WithCheckpointGlobalInterval(interval time.Duration) func(*Options) {
	return func(opts *Options) {
		if opts.CheckpointCfg == nil {
			opts.CheckpointCfg = new(CheckpointCfg)
		}
		opts.CheckpointCfg.GlobalInterval = interval
	}
}

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}

	if o.TransferTableTTL == time.Duration(0) {
		o.TransferTableTTL = time.Second * 120
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
		o.CheckpointCfg = new(CheckpointCfg)
	}
	if o.CheckpointCfg.ScanInterval <= 0 {
		o.CheckpointCfg.ScanInterval = DefaultScannerInterval
	}
	if o.CheckpointCfg.FlushInterval <= 0 {
		o.CheckpointCfg.FlushInterval = DefaultCheckpointFlushInterval
	}
	if o.CheckpointCfg.IncrementalInterval <= 0 {
		o.CheckpointCfg.IncrementalInterval = DefaultCheckpointIncremetalInterval
	}
	if o.CheckpointCfg.GlobalInterval <= 0 {
		o.CheckpointCfg.GlobalInterval = DefaultCheckpointGlobalInterval
	}
	if o.CheckpointCfg.MinCount <= 0 {
		o.CheckpointCfg.MinCount = DefaultCheckpointMinCount
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
