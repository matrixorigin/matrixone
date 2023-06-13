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
	defaultRpcMaxMessageSize        = 16 * mpool.KB
	defaultRpcPayloadCopyBufferSize = 16 * mpool.KB
	defaultRpcEnableChecksum        = true
	defaultLogtailCollectInterval   = 50 * time.Millisecond
	defaultResponseSendTimeout      = 10 * time.Second
	defaultMaxLogtailFetchFailure   = 5
)

type CacheCfg struct {
	IndexCapacity uint64 `toml:"index-cache-size"`
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
	GCCheckpointInterval      time.Duration
	DisableGCCheckpoint       bool
}

type GCCfg struct {
	GCTTL          time.Duration
	ScanGCInterval time.Duration
}

type CatalogCfg struct {
	GCInterval       time.Duration
	MemoryGCInternal time.Duration
	MemoryGCTTL      time.Duration
	DisableGC        bool
}

type SchedulerCfg struct {
	IOWorkers    int `toml:"io-workers"`
	AsyncWorkers int `toml:"async-workers"`
}

type LogtailCfg struct {
	PageSize int32 `toml:"page-size"`
}

type LogtailServerCfg struct {
	RpcMaxMessageSize        int64
	RpcPayloadCopyBufferSize int64
	RpcEnableChecksum        bool
	LogtailCollectInterval   time.Duration
	ResponseSendTimeout      time.Duration
	MaxLogtailFetchFailure   int
}

func NewDefaultLogtailServerCfg() *LogtailServerCfg {
	return &LogtailServerCfg{
		RpcMaxMessageSize:        defaultRpcMaxMessageSize,
		RpcPayloadCopyBufferSize: defaultRpcPayloadCopyBufferSize,
		RpcEnableChecksum:        defaultRpcEnableChecksum,
		LogtailCollectInterval:   defaultLogtailCollectInterval,
		ResponseSendTimeout:      defaultResponseSendTimeout,
		MaxLogtailFetchFailure:   defaultMaxLogtailFetchFailure,
	}
}

func (l *LogtailServerCfg) Validate() {
	if l.RpcMaxMessageSize <= 0 {
		l.RpcMaxMessageSize = defaultRpcMaxMessageSize
	}
	if l.RpcPayloadCopyBufferSize <= 0 {
		l.RpcPayloadCopyBufferSize = defaultRpcPayloadCopyBufferSize
	}
	if l.LogtailCollectInterval <= 0 {
		l.LogtailCollectInterval = defaultLogtailCollectInterval
	}
	if l.ResponseSendTimeout <= 0 {
		l.ResponseSendTimeout = defaultResponseSendTimeout
	}
	if l.MaxLogtailFetchFailure <= 0 {
		l.MaxLogtailFetchFailure = defaultMaxLogtailFetchFailure
	}
}
