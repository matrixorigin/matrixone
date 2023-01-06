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

import "time"

type CacheCfg struct {
	IndexCapacity  uint64 `toml:"index-cache-size"`
	InsertCapacity uint64 `toml:"insert-cache-size"`
	TxnCapacity    uint64 `toml:"txn-cache-size"`
}

type StorageCfg struct {
	BlockMaxRows     uint32 `toml:"block-max-rows"`
	SegmentMaxBlocks uint16 `toml:"segment-max-blocks"`
}

type CheckpointCfg struct {
	FlushInterval             time.Duration `toml:"flush-inerterval"`
	MinCount                  int64         `toml:"checkpoint-min-count"`
	ScanInterval              time.Duration `toml:"scan-interval"`
	IncrementalInterval       time.Duration `toml:"checkpoint-incremental-interval"`
	GlobalMinCount            int64         `toml:"checkpoint-global-interval"`
	ForceUpdateGlobalInterval bool
	GlobalVersionInterval     time.Duration
}

type GCCfg struct {
	GCTTL          time.Duration
	ScanGCInterval time.Duration
}

type SchedulerCfg struct {
	IOWorkers    int `toml:"io-workers"`
	AsyncWorkers int `toml:"async-workers"`
}

type LogtailCfg struct {
	PageSize int32 `toml:"page-size"`
}
