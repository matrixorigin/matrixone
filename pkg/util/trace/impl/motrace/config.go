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
	"encoding/binary"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	MOStatementType = "statement"
	MOSpanType      = "span"
	MOLogType       = "log"
	MOErrorType     = "error"
	MORawLogType    = "rawlog"
)

// tracerProviderConfig.
type tracerProviderConfig struct {
	// spanProcessors contains collection of SpanProcessors that are processing pipeline
	// for spans in the trace signal.
	// SpanProcessors registered with a TracerProvider and are called at the start
	// and end of a Span's lifecycle, and are called in the order they are
	// registered.
	spanProcessors []trace.SpanProcessor

	enable bool // SetEnable

	// idGenerator is used to generate all Span and Trace IDs when needed.
	idGenerator trace.IDGenerator

	// resource contains attributes representing an entity that produces telemetry.
	resource *trace.Resource // withMOVersion, WithNode,

	// disableSpan
	disableSpan bool
	// disableError
	disableError bool
	// debugMode used in Tracer.Debug
	debugMode bool // DebugMode

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
	// longSpanTime
	longSpanTime time.Duration
	// skipRunningStmt
	skipRunningStmt bool // set by WithSkipRunningStmt

	bufferSizeThreshold int64 // WithBufferSizeThreshold

	cuConfig   config.OBCUConfig // WithCUConfig
	cuConfigV1 config.OBCUConfig // WithCUConfig

	tcpPacket bool // WithTCPPacket

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

func WithLongSpanTime(d time.Duration) tracerProviderOption {
	return tracerProviderOption(func(cfg *tracerProviderConfig) {
		cfg.longSpanTime = d
	})
}

func WithSpanDisable(disable bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.disableSpan = disable
	}
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

func DebugMode(debug bool) tracerProviderOption {
	return func(cfg *tracerProviderConfig) {
		cfg.debugMode = debug
	}
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

var _ trace.IDGenerator = &moIDGenerator{}

type moIDGenerator struct{}

func (M moIDGenerator) NewIDs() (trace.TraceID, trace.SpanID) {
	tid := trace.TraceID{}
	binary.BigEndian.PutUint64(tid[:], util.Fastrand64())
	binary.BigEndian.PutUint64(tid[8:], util.Fastrand64())
	sid := trace.SpanID{}
	binary.BigEndian.PutUint64(sid[:], util.Fastrand64())
	return tid, sid
}

func (M moIDGenerator) NewSpanID() trace.SpanID {
	sid := trace.SpanID{}
	binary.BigEndian.PutUint64(sid[:], util.Fastrand64())
	return sid
}
