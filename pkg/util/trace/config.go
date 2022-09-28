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

package trace

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	InternalExecutor = "InternalExecutor"
	FileService      = "FileService"
)

const (
	MOStatementType = "statement"
	MOSpanType      = "span"
	MORawLogType    = "raw_log"
	MOLogType       = "log"
	MOErrorType     = "error"
)

const (
	B int64 = 1 << (iota * 10)
	KB
	MB
	GB
)

// tracerProviderConfig.
type tracerProviderConfig struct {
	// spanProcessors contains collection of SpanProcessors that are processing pipeline
	// for spans in the trace signal.
	// SpanProcessors registered with a TracerProvider and are called at the start
	// and end of a Span's lifecycle, and are called in the order they are
	// registered.
	spanProcessors []SpanProcessor

	enable bool // see SetEnable

	// idGenerator is used to generate all Span and Trace IDs when needed.
	idGenerator IDGenerator

	// resource contains attributes representing an entity that produces telemetry.
	resource *Resource // see WithMOVersion, WithNode,

	// TODO: can check span's END
	debugMode bool // see DebugMode

	batchProcessMode string // see WithBatchProcessMode

	// writerFactory gen writer for CSV output
	writerFactory export.FSWriterFactory // see WithFSWriterFactory, default from export.GetFSWriterFactory result

	sqlExecutor func() ie.InternalExecutor // see WithSQLExecutor
	// needInit control table schema create
	needInit bool // see WithInitAction

	exportInterval time.Duration // see WithExportInterval
	// longQueryTime unit ns
	longQueryTime int64 // see WithLongQueryTime

	mux sync.RWMutex
}

func (cfg *tracerProviderConfig) getNodeResource() *MONodeResource {
	cfg.mux.RLock()
	defer cfg.mux.RUnlock()
	if val, has := cfg.resource.Get("Node"); !has {
		return &MONodeResource{}
	} else {
		return val.(*MONodeResource)
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

type tracerProviderOptionFunc func(config *tracerProviderConfig)

func (f tracerProviderOptionFunc) apply(config *tracerProviderConfig) {
	f(config)
}

func WithMOVersion(v string) tracerProviderOptionFunc {
	return func(config *tracerProviderConfig) {
		config.resource.Put("version", v)
	}
}

// WithNode give id as NodeId, t as NodeType
func WithNode(uuid string, t string) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.resource.Put("Node", &MONodeResource{
			NodeUuid: uuid,
			NodeType: t,
		})
	}
}

func EnableTracer(enable bool) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.SetEnable(enable)
	}
}

func WithFSWriterFactory(f export.FSWriterFactory) tracerProviderOptionFunc {
	return tracerProviderOptionFunc(func(cfg *tracerProviderConfig) {
		cfg.writerFactory = f
	})
}

func WithExportInterval(secs int) tracerProviderOptionFunc {
	return tracerProviderOptionFunc(func(cfg *tracerProviderConfig) {
		cfg.exportInterval = time.Second * time.Duration(secs)
	})
}

func WithLongQueryTime(secs float64) tracerProviderOptionFunc {
	return tracerProviderOptionFunc(func(cfg *tracerProviderConfig) {
		cfg.longQueryTime = int64(float64(time.Second) * secs)
	})
}

func DebugMode(debug bool) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.debugMode = debug
	}
}

func WithBatchProcessMode(mode string) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.batchProcessMode = mode
	}
}

func WithSQLExecutor(f func() ie.InternalExecutor) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.mux.Lock()
		defer cfg.mux.Unlock()
		cfg.sqlExecutor = f
	}
}

func WithInitAction(init bool) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.mux.Lock()
		defer cfg.mux.Unlock()
		cfg.needInit = init
	}
}

type Uint64IdGenerator struct{}

func (M Uint64IdGenerator) NewIDs() (uint64, uint64) {
	return util.Fastrand64(), util.Fastrand64()
}

func (M Uint64IdGenerator) NewSpanID() uint64 {
	return util.Fastrand64()
}

var _ IDGenerator = &moIDGenerator{}

type moIDGenerator struct{}

func (M moIDGenerator) NewIDs() (TraceID, SpanID) {
	tid := TraceID{}
	binary.BigEndian.PutUint64(tid[:], util.Fastrand64())
	binary.BigEndian.PutUint64(tid[8:], util.Fastrand64())
	sid := SpanID{}
	binary.BigEndian.PutUint64(sid[:], util.Fastrand64())
	return tid, sid
}

func (M moIDGenerator) NewSpanID() SpanID {
	sid := SpanID{}
	binary.BigEndian.PutUint64(sid[:], util.Fastrand64())
	return sid
}

type TraceID [16]byte

var nilTraceID TraceID

// IsZero checks whether the trace TraceID is 0 value.
func (t TraceID) IsZero() bool {
	return bytes.Equal(t[:], nilTraceID[:])
}

func (t TraceID) String() string {
	return uuid.UUID(t).String()
}

type SpanID [8]byte

var nilSpanID SpanID

// SetByUUID use prefix of uuid as value
func (s *SpanID) SetByUUID(id string) {
	if u, err := uuid.Parse(id); err == nil {
		copy(s[:], u[:])
	} else {
		copy(s[:], []byte(id)[:])
	}
}

func (s *SpanID) IsZero() bool {
	return bytes.Equal(s[:], nilSpanID[:])
}

func (s SpanID) String() string {
	return hex.EncodeToString(s[:])
}

var _ zapcore.ObjectMarshaler = (*SpanContext)(nil)

const SpanFieldKey = "span"

func SpanField(sc SpanContext) zap.Field {
	return zap.Object(SpanFieldKey, &sc)
}

func IsSpanField(field zapcore.Field) bool {
	return field.Key == SpanFieldKey
}

// SpanContext contains identifying trace information about a Span.
type SpanContext struct {
	TraceID TraceID `json:"trace_id"`
	SpanID  SpanID  `json:"span_id"`
}

func (c *SpanContext) Size() (n int) {
	return 24
}

func (c *SpanContext) MarshalTo(dAtA []byte) (int, error) {
	l := cap(dAtA)
	if l < c.Size() {
		return -1, io.ErrUnexpectedEOF
	}
	copy(dAtA, c.TraceID[:])
	copy(dAtA[16:], c.SpanID[:])
	return c.Size(), nil
}

func (c *SpanContext) Unmarshal(dAtA []byte) error {
	l := cap(dAtA)
	if l < c.Size() {
		return io.ErrUnexpectedEOF
	}
	copy(c.TraceID[:], dAtA[0:16])
	copy(c.SpanID[:], dAtA[16:24])
	return nil
}

func (c SpanContext) GetIDs() (TraceID, SpanID) {
	return c.TraceID, c.SpanID
}

func (c *SpanContext) Reset() {
	c.TraceID = TraceID{}
	c.SpanID = SpanID{}
}

func (c *SpanContext) IsEmpty() bool {
	return c.TraceID.IsZero() && c.SpanID.IsZero()
}

func (c *SpanContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("TraceId", c.TraceID.String())
	enc.AddString("SpanId", c.SpanID.String())
	return nil
}

func SpanContextWithID(id TraceID) SpanContext {
	return SpanContext{TraceID: id}
}

func SpanContextWithIDs(tid TraceID, sid SpanID) SpanContext {
	return SpanContext{TraceID: tid, SpanID: sid}
}

// SpanConfig is a group of options for a Span.
type SpanConfig struct {
	SpanContext

	// NewRoot identifies a Span as the root Span for a new trace. This is
	// commonly used when an existing trace crosses trust boundaries and the
	// remote parent span context should be ignored for security.
	NewRoot bool `json:"NewRoot"` // see WithNewRoot
	parent  Span `json:"-"`
}

// SpanStartOption applies an option to a SpanConfig. These options are applicable
// only when the span is created.
type SpanStartOption interface {
	applySpanStart(*SpanConfig)
}

type SpanEndOption interface {
	applySpanEnd(*SpanConfig)
}

// SpanOption applies an option to a SpanConfig.
type SpanOption interface {
	SpanStartOption
	SpanEndOption
}

type spanOptionFunc func(*SpanConfig)

func (f spanOptionFunc) applySpanEnd(cfg *SpanConfig) {
	f(cfg)
}

func (f spanOptionFunc) applySpanStart(cfg *SpanConfig) {
	f(cfg)
}

func WithNewRoot(newRoot bool) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.NewRoot = newRoot
	})
}

func WithTraceID(id TraceID) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.TraceID = id
	})
}

func WithSpanID(id SpanID) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.SpanID = id
	})
}

type Resource struct {
	m map[string]any
}

func newResource() *Resource {
	return &Resource{m: make(map[string]any)}
}

func (r *Resource) Put(key string, val any) {
	r.m[key] = val
}

func (r *Resource) Get(key string) (any, bool) {
	val, has := r.m[key]
	return val, has
}

// String need to improve
func (r *Resource) String() string {
	buf, _ := json.Marshal(r.m)
	return string(buf)

}

const NodeTypeStandalone = "Standalone"

type MONodeResource struct {
	NodeUuid string `json:"node_uuid"`
	NodeType string `json:"node_type"`
}
