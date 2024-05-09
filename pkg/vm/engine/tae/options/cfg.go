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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

const (
	defaultRpcMaxMessageSize      = 16 * mpool.KB
	defaultRpcEnableChecksum      = true
	defaultLogtailCollectInterval = 50 * time.Millisecond
	defaultResponseSendTimeout    = 10 * time.Second
	defaultRpcStreamPoisonTime    = 5 * time.Second
)

type StorageCfg struct {
	BlockMaxRows    uint32 `toml:"block-max-rows"`
	ObjectMaxBlocks uint16 `toml:"object-max-blocks"`
}

type CheckpointCfg struct {
	FlushInterval             time.Duration `toml:"flush-inerterval"`
	MinCount                  int64         `toml:"checkpoint-min-count"`
	ScanInterval              time.Duration `toml:"scan-interval"`
	IncrementalInterval       time.Duration `toml:"checkpoint-incremental-interval"`
	GlobalMinCount            int64         `toml:"checkpoint-global-interval"`
	OverallFlushMemControl    uint64        `toml:"overall-flush-mem-control"`
	ForceUpdateGlobalInterval bool
	GlobalVersionInterval     time.Duration
	GCCheckpointInterval      time.Duration
	DisableGCCheckpoint       bool
	ReservedWALEntryCount     uint64

	// only for test
	// it is used to control the block rows of the checkpoint
	BlockRows int
	Size      int
}

type GCCfg struct {
	GCTTL          time.Duration `toml:"gc-ttl"`
	ScanGCInterval time.Duration `toml:"scan-gc-interval"`
	DisableGC      bool          `toml:"disable-gc"`
}

type CatalogCfg struct {
	GCInterval time.Duration
	DisableGC  bool
}

type SchedulerCfg struct {
	IOWorkers    int `toml:"io-workers"`
	AsyncWorkers int `toml:"async-workers"`
}

type LogtailCfg struct {
	PageSize int32 `toml:"page-size"`
}

type MergeConfig struct {
	CNMergeMemControlHint uint64
	CNTakeOverAll         bool
	CNTakeOverExceed      uint64
	CNStandaloneTake      bool
}

type LogtailServerCfg struct {
	RpcMaxMessageSize      int64
	RpcEnableChecksum      bool
	RPCStreamPoisonTime    time.Duration
	LogtailCollectInterval time.Duration
	ResponseSendTimeout    time.Duration
}

func NewDefaultLogtailServerCfg() *LogtailServerCfg {
	return &LogtailServerCfg{
		RpcMaxMessageSize:      defaultRpcMaxMessageSize,
		RpcEnableChecksum:      defaultRpcEnableChecksum,
		RPCStreamPoisonTime:    defaultRpcStreamPoisonTime,
		LogtailCollectInterval: defaultLogtailCollectInterval,
		ResponseSendTimeout:    defaultResponseSendTimeout,
	}
}

func (l *LogtailServerCfg) Validate() {
	if l.RpcMaxMessageSize <= 0 {
		l.RpcMaxMessageSize = defaultRpcMaxMessageSize
	}
	if l.RPCStreamPoisonTime <= 0 {
		l.RPCStreamPoisonTime = defaultRpcStreamPoisonTime
	}
	if l.LogtailCollectInterval <= 0 {
		l.LogtailCollectInterval = defaultLogtailCollectInterval
	}
	if l.ResponseSendTimeout <= 0 {
		l.ResponseSendTimeout = defaultResponseSendTimeout
	}
}
