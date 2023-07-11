// Copyright The OpenTelemetry Authors
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

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2022 Matrix Origin.
//
// Modified the behavior and the interface of the step.

package trace

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"time"
)

type TraceID [16]byte

var NilTraceID TraceID

// IsZero checks whether the trace TraceID is 0 value.
func (t TraceID) IsZero() bool {
	return bytes.Equal(t[:], NilTraceID[:])
}

func (t TraceID) String() string {
	if t.IsZero() {
		return "0"
	}
	return uuid.UUID(t).String()
}

type SpanID [8]byte

var NilSpanID SpanID

// SetByUUID use prefix of uuid as value
func (s *SpanID) SetByUUID(id string) {
	if u, err := uuid.Parse(id); err == nil {
		copy(s[:], u[:])
	} else {
		copy(s[:], []byte(id)[:])
	}
}

func (s *SpanID) IsZero() bool {
	return bytes.Equal(s[:], NilSpanID[:])
}

func (s SpanID) String() string {
	if s.IsZero() {
		return "0"
	}
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

func ContextField(ctx context.Context) zap.Field {
	return SpanField(SpanFromContext(ctx).SpanContext())
}

// SpanContext contains identifying trace information about a Span.
type SpanContext struct {
	TraceID TraceID `json:"trace_id"`
	SpanID  SpanID  `json:"span_id"`
	// Kind default SpanKindInternal
	Kind SpanKind `json:"span_kind"`
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

// Unmarshal with default Kind: SpanKindRemote
func (c *SpanContext) Unmarshal(dAtA []byte) error {
	l := cap(dAtA)
	if l < c.Size() {
		return io.ErrUnexpectedEOF
	}
	copy(c.TraceID[:], dAtA[0:16])
	copy(c.SpanID[:], dAtA[16:24])
	c.Kind = SpanKindRemote
	return nil
}

func (c SpanContext) GetIDs() (TraceID, SpanID) {
	return c.TraceID, c.SpanID
}

func (c *SpanContext) Reset() {
	c.TraceID = NilTraceID
	c.SpanID = NilSpanID
	c.Kind = SpanKindInternal
}

func (c *SpanContext) IsEmpty() bool {
	return c.TraceID.IsZero() && c.SpanID.IsZero()
}

// MarshalLogObject implement zapcore.ObjectMarshaler
func (c *SpanContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if !c.TraceID.IsZero() {
		enc.AddString("trace_id", c.TraceID.String())
	}
	if !c.SpanID.IsZero() {
		enc.AddString("span_id", c.SpanID.String())
	}
	if c.Kind != SpanKindInternal {
		enc.AddString("kind", c.Kind.String())
	}
	return nil
}

func SpanContextWithID(id TraceID, kind SpanKind) SpanContext {
	return SpanContext{TraceID: id, Kind: kind}
}

// SpanContextWithIDs with default Kind: SpanKindInternal
func SpanContextWithIDs(tid TraceID, sid SpanID) SpanContext {
	return SpanContext{TraceID: tid, SpanID: sid, Kind: SpanKindInternal}
}

const (
	FlagProfileGoroutine = 1 << iota
	FlagProfileHeap
	FlagProfileCpu
)

// SpanConfig is a group of options for a Span.
type SpanConfig struct {
	SpanContext

	// NewRoot identifies a Span as the root Span for a new trace. This is
	// commonly used when an existing trace crosses trust boundaries and the
	// remote parent span context should be ignored for security.
	NewRoot bool `json:"NewRoot"` // WithNewRoot
	Parent  Span `json:"-"`

	// LongTimeThreshold set by WithLongTimeThreshold
	LongTimeThreshold time.Duration `json:"-"`
	// profileFlag mark what profile need to do
	profileFlag     uint64
	profileCpuDur   time.Duration // WithProfileCpuSecs
	profileTraceDur time.Duration // WithProfileTraceSecs

	// hungThreshold set by WithHungThreshold
	// It will override Span ctx deadline setting, and start a goroutine to check ctx deadline
	hungThreshold time.Duration
}

const (
	ProfileFlagGoroutine = 1 << iota
	ProfileFlagThreadcreate
	ProfileFlagHeap
	ProfileFlagAllocs
	ProfileFlagBlock
	ProfileFlagMutex
	ProfileFlagCpu
	ProfileFlagTrace
)

func (c *SpanConfig) Reset() {
	c.SpanContext.Reset()
	c.NewRoot = false
	c.Parent = nil
	c.LongTimeThreshold = 0
	c.profileFlag = 0
	c.profileCpuDur = 0
	c.profileTraceDur = 0
	c.hungThreshold = 0
}

func (c *SpanConfig) GetLongTimeThreshold() time.Duration {
	return c.LongTimeThreshold
}

func (c *SpanConfig) HungThreshold() time.Duration {
	return c.hungThreshold
}

// NeedProfile return true if set profileGoroutine, profileHeap, profileCpuDur
func (c *SpanConfig) NeedProfile() bool {
	return c.profileFlag > 0
}

// ProfileGoroutine return the value set by WithProfileGoroutine
func (c *SpanConfig) ProfileGoroutine() bool {
	return c.profileFlag&ProfileFlagGoroutine > 0
}

// ProfileHeap return the value set by WithProfileHeap
func (c *SpanConfig) ProfileHeap() bool {
	return c.profileFlag&ProfileFlagHeap > 0
}

func (c *SpanConfig) ProfileThreadCreate() bool {
	return c.profileFlag&ProfileFlagThreadcreate > 0
}

func (c *SpanConfig) ProfileAllocs() bool {
	return c.profileFlag&ProfileFlagAllocs > 0
}

func (c *SpanConfig) ProfileBlock() bool {
	return c.profileFlag&ProfileFlagBlock > 0
}

func (c *SpanConfig) ProfileMutex() bool {
	return c.profileFlag&ProfileFlagMutex > 0
}

// ProfileCpuSecs return the value set by WithProfileCpuSecs
func (c *SpanConfig) ProfileCpuSecs() time.Duration {
	return c.profileCpuDur
}

// ProfileTraceSecs return the value set by WithProfileTraceSecs
func (c *SpanConfig) ProfileTraceSecs() time.Duration {
	return c.profileTraceDur
}

// SpanStartOption applies an option to a SpanConfig. These options are applicable
// only when the span is created.
type SpanStartOption interface {
	ApplySpanStart(*SpanConfig)
}

type SpanEndOption interface {
	ApplySpanEnd(*SpanConfig)
}

// SpanOption applies an option to a SpanConfig.
type SpanOption interface {
	SpanStartOption
	SpanEndOption
}

type spanOptionFunc func(*SpanConfig)

func (f spanOptionFunc) ApplySpanEnd(cfg *SpanConfig) {
	f(cfg)
}

func (f spanOptionFunc) ApplySpanStart(cfg *SpanConfig) {
	f(cfg)
}

func WithNewRoot(newRoot bool) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.NewRoot = newRoot
	})
}

func WithKind(kind SpanKind) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.Kind = kind
	})
}

// WithLongTimeThreshold set timeout threshold. Span.End will check the Span duration value.
func WithLongTimeThreshold(d time.Duration) SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.LongTimeThreshold = d
	})
}

// WithHungThreshold please be careful to using this option.
// It will create a new goroutine to check hung deadline while calling Tracer.Start().
func WithHungThreshold(d time.Duration) SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.hungThreshold = d
	})
}

// WithProfileGoroutine requests dump pprof/mutex. It will trigger profile.ProfileGoroutine() in Span.End().
// More details in MOSpan.doProfile.
func WithProfileGoroutine() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagGoroutine
	})
}

// WithProfileHeap requests dump pprof/heap. It will trigger profile.ProfileHeap() in Span.End().
// More details in MOSpan.doProfile.
func WithProfileHeap() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagHeap
	})
}

// WithProfileThreadCreate requests dump pprof/threadcreate. It will trigger profile.ProfileThreadcreate() in Span.End().
// More details in MOSpan.doProfile.
func WithProfileThreadCreate() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagThreadcreate
	})
}

// WithProfileAllocs requests dump pprof/allocs. It will trigger profile.ProfileAllocs() in Span.End().
// more details in MOSpan.doProfile.
func WithProfileAllocs() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagAllocs
	})
}

// WithProfileBlock  requests dump pprof/block. It will trigger profile.ProfileBlock() in Span.End().
// More details in MOSpan.doProfile.
func WithProfileBlock() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagBlock
	})
}

// WithProfileMutex  requests dump pprof/mutex. It will trigger profile.ProfileMutex() in Span.End().
// More details in MOSpan.doProfile.
func WithProfileMutex() SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagMutex
	})
}

// WithProfileCpuSecs requests dump pprof/cpu, and specify the time to profile.
// Please carefully to set this value, it is a sync profile.ProfileCPU() op.
// More details in MOSpan.doProfile.
func WithProfileCpuSecs(d time.Duration) SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagCpu
		cfg.profileCpuDur = d
	})
}

// WithProfileTraceSecs requests dump pprof/trace, and specify the time to profile.
// Please carefully to use, it is a sync profile.ProfileTrace() op
// More details in MOSpan.doProfile.
func WithProfileTraceSecs(d time.Duration) SpanStartOption {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.profileFlag |= ProfileFlagTrace
		cfg.profileTraceDur = d
	})
}

type Resource struct {
	m map[string]any
}

func NewResource() *Resource {
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

// SpanKind is the role a Span plays in a Trace.
type SpanKind int

const (
	// SpanKindInternal is a SpanKind for a Span that represents an internal
	// operation within MO.
	SpanKindInternal SpanKind = 0
	// SpanKindStatement is a SpanKind for a Span that represents the operation
	// belong to statement query
	SpanKindStatement SpanKind = 1
	// SpanKindRemote is a SpanKind for a Span that represents the operation
	// cross rpc
	SpanKindRemote SpanKind = 2
	// SpanKindSession is a SpanKind for a Span that represents the operation
	// start from session
	SpanKindSession SpanKind = 3
)

func (k SpanKind) String() string {
	switch k {
	case SpanKindInternal:
		return "internal"
	case SpanKindStatement:
		return "statement"
	case SpanKindRemote:
		return "remote"
	case SpanKindSession:
		return "session"
	default:
		return "unknown"
	}
}

// TracerConfig is a group of options for a Tracer.
type TracerConfig struct {
	Name string
}

// TracerOption applies an option to a TracerConfig.
type TracerOption interface {
	Apply(*TracerConfig)
}

var _ TracerOption = tracerOptionFunc(nil)

type tracerOptionFunc func(*TracerConfig)

func (f tracerOptionFunc) Apply(cfg *TracerConfig) {
	f(cfg)
}

const (
	// FlagsSampled is a bitmask with the sampled bit set. A SpanContext
	// with the sampling bit set means the span is sampled.
	FlagsSampled = TraceFlags(0x01)
)

// TraceFlags contains flags that can be set on a SpanContext.
type TraceFlags byte //nolint:revive // revive complains about stutter of `trace.TraceFlags`.

// IsSampled returns if the sampling bit is set in the TraceFlags.
func (tf TraceFlags) IsSampled() bool {
	return tf&FlagsSampled == FlagsSampled
}

// WithSampled sets the sampling bit in a new copy of the TraceFlags.
func (tf TraceFlags) WithSampled(sampled bool) TraceFlags { // nolint:revive  // sampled is not a control flag.
	if sampled {
		return tf | FlagsSampled
	}

	return tf &^ FlagsSampled
}

// String returns the hex string representation form of TraceFlags.
func (tf TraceFlags) String() string {
	return hex.EncodeToString([]byte{byte(tf)}[:])
}
