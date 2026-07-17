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

package motrace

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	MOStatementType = "statement"
	MOLogType       = "log"
	MOErrorType     = "error"
	MORawLogType    = "rawlog"
)

const (
	MaxStatementSize = "MaxStatementSize"
)

// tracerProviderConfig.
type tracerProviderConfig struct {
	enable bool // SetEnable

	// service used to get global config from mo.runtime
	service string // WithService

	// resource contains attributes representing an entity that produces telemetry.
	resource *trace.Resource // withMOVersion, WithNode,

	// disableError
	disableError bool

	batchProcessor BatchProcessor // WithBatchProcessor

	// writerFactory gen writer for CSV output
	writerFactory table.WriterFactory // WithFSWriterFactory, default from export.GetFSWriterFactory4Trace
	// disableSqlWriter
	disableSqlWriter bool // set by WithSQLWriterDisable

	// stmt aggregation
	disableStmtAggregation bool          // set by WithStmtAggregationDisable
	enableStmtMerge        bool          // set by WithStmtMergeDisable
	aggregationWindow      time.Duration // WithAggregationWindow
	selectAggrThreshold    time.Duration // WithSelectThreshold

	sqlExecutor func() ie.InternalExecutor // WithSQLExecutor
	// needInit control table schema create
	needInit bool // WithInitAction

	exportInterval time.Duration //  WithExportInterval
	// longQueryTime unit ns
	longQueryTime int64 //  WithLongQueryTime
	// skipRunningStmt
	skipRunningStmt bool // set by WithSkipRunningStmt

	bufferSizeThreshold int64 // WithBufferSizeThreshold

	cuConfig   config.OBCUConfig // WithCUConfig
	cuConfigV1 config.OBCUConfig // WithCUConfig

	tcpPacket bool // WithTCPPacket

	MaxLogMessageSize int // WithMaxLogMessageSize
	MaxStatementSize  int // WithMaxStatementSize

	// labels for db_logger select target cn as operator.
	labels map[string]string

	mux sync.RWMutex
}

func (cfg *tracerProviderConfig) getNodeResource() *trace.MONodeResource {
	cfg.mux.RLock()
	defer cfg.mux.RUnlock()
	if val, has := cfg.resource.Get("Node"); !has {
		return &trace.MONodeResource{}
	} else {
		return val.(*trace.MONodeResource)
	}
}

func (cfg *tracerProviderConfig) IsEnable() bool {
	cfg.mux.RLock()
	defer cfg.mux.RUnlock()
	return cfg.enable
}

func (cfg *tracerProviderConfig) SetEnable(enable bool) {
	cfg.mux.Lock()
	defer cfg.mux.Unlock()
	cfg.enable = enable
}

func (cfg *tracerProviderConfig) GetSqlExecutor() func() ie.InternalExecutor {
	cfg.mux.RLock()
	defer cfg.mux.RUnlock()
	return cfg.sqlExecutor
}

// TracerProviderOption configures a TracerProvider.
type TracerProviderOption interface {
	apply(*tracerProviderConfig)
}

type tracerProviderOption func(config *tracerProviderConfig)

func (f tracerProviderOption) apply(config *tracerProviderConfig) {
	f(config)
}

func WithService(service string) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.service = service
	}
}

func withMOVersion(v string) tracerProviderOption {
	return func(config *tracerProviderConfig) {
		config.resource.Put("version", v)
	}
}

// WithNode give id as NodeId, t as NodeType
func WithNode(uuid string, t string) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.resource.Put("Node", &trace.MONodeResource{
			NodeUuid: uuid,
			NodeType: t,
		})
	}
}

func EnableTracer(enable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.SetEnable(enable)
	}
}

func WithFSWriterFactory(f table.WriterFactory) tracerProviderOption {
	return tracerProviderOption(func(cfg *tracerProviderConfig) {
		cfg.writerFactory = f
	})
}

func WithExportInterval(secs int) tracerProviderOption {
	return tracerProviderOption(func(cfg *tracerProviderConfig) {
		cfg.exportInterval = time.Second * time.Duration(secs)
	})
}

func WithLongQueryTime(secs float64) tracerProviderOption {
	return tracerProviderOption(func(cfg *tracerProviderConfig) {
		cfg.longQueryTime = int64(float64(time.Second) * secs)
	})
}

// WithLongSpanTime is retained as a compatibility no-op after Span recording
// was retired.
func WithLongSpanTime(_ time.Duration) TracerProviderOption {
	return tracerProviderOption(func(_ *tracerProviderConfig) {})
}

// WithSpanDisable is retained so existing configuration continues to parse.
// Span recording cannot be re-enabled.
func WithSpanDisable(_ bool) tracerProviderOption {
	return func(_ *tracerProviderConfig) {}
}

// EnableSpanProfile is retained as a compatibility no-op.
func EnableSpanProfile(_ bool) tracerProviderOption {
	return func(_ *tracerProviderConfig) {}
}

func WithErrorDisable(disable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.disableError = disable
	}
}

func WithSkipRunningStmt(skip bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.skipRunningStmt = skip
	}
}

func WithSQLWriterDisable(disable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.disableSqlWriter = disable
	}
}

func WithAggregatorDisable(disable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.disableStmtAggregation = disable
	}
}

func WithStmtMergeEnable(enable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.enableStmtMerge = enable
	}
}

func WithCUConfig(cu config.OBCUConfig, cuv1 config.OBCUConfig) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.cuConfig = cu
		cfg.cuConfigV1 = cuv1
	}
}

func WithTCPPacket(count bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.tcpPacket = count
	}
}

func WithMaxLogMessageSize(size int) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.MaxLogMessageSize = size
	}
}

func WithMaxStatementSize(service string) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		if s, exist := runtime.ServiceRuntime(service).GetGlobalVariables(MaxStatementSize); exist {
			cfg.MaxStatementSize = s.(int)
		}
	}
}

func WithLabels(l map[string]string) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.labels = l
	}
}

func WithAggregatorWindow(window time.Duration) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.aggregationWindow = window
	}
}

func WithSelectThreshold(window time.Duration) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.selectAggrThreshold = window
	}
}

func WithBufferSizeThreshold(size int64) tracerProviderOption {
	return tracerProviderOption(func(cfg *tracerProviderConfig) {
		cfg.bufferSizeThreshold = size
	})
}

// DebugMode is retained as a compatibility no-op for the retired Span path.
func DebugMode(_ bool) tracerProviderOption {
	return func(_ *tracerProviderConfig) {}
}

func WithBatchProcessor(p BatchProcessor) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.batchProcessor = p
	}
}

func WithSQLExecutor(f func() ie.InternalExecutor) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.mux.Lock()
		defer cfg.mux.Unlock()
		cfg.sqlExecutor = f
	}
}

func WithInitAction(init bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.mux.Lock()
		defer cfg.mux.Unlock()
		cfg.needInit = init
	}
}
