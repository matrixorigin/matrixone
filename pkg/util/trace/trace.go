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
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"time"
)

const (
	// FlagsSampled is a bitmask with the sampled bit set. A SpanContext
	// with the sampling bit set means the span is sampled.
	FlagsSampled = TraceFlags(0x01)
)

type TraceID uint64
type SpanID uint64

type defaultSpanKey int

// TracerConfig is a group of options for a Tracer.
type TracerConfig struct {
	Name string

	reminder batchpipe.Reminder
}

// TracerOption applies an option to a TracerConfig.
type TracerOption interface {
	apply(*TracerConfig)
}

type tracerOptionFunc func(*TracerConfig)

func (f tracerOptionFunc) apply(cfg *TracerConfig) {
	f(cfg)
}

func WithReminder(r batchpipe.Reminder) tracerOptionFunc {
	return tracerOptionFunc(func(cfg *TracerConfig) {
		cfg.reminder = r
	})
}

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

// MarshalJSON implements a custom marshal function to encode TraceFlags
// as a hex string.
func (tf TraceFlags) MarshalJSON() ([]byte, error) {
	return json.Marshal(tf.String())
}

// String returns the hex string representation form of TraceFlags.
func (tf TraceFlags) String() string {
	return hex.EncodeToString([]byte{byte(tf)}[:])
}

type MOLogLevel int

var _ batchpipe.HasName = &MOLogModel{}

type MOLogModel struct {
	statementId uint64
	spanId      uint64
	resource    *Resource
	timestamp   util.TimeNano
	codeLine    string // like "util/trace/trace.go:666"
	level       MOLogLevel
	logLine     string
}

func (MOLogModel) GetName() string {
	return "MOLogModel"
}

var gTracerProvider *MOTracerProvider
var gTracer Tracer
var gTraceContext context.Context

func Init(ctx context.Context, opt ...TracerProviderOption) (context.Context, error) {

	gTracerProvider = newMOTracerProvider(opt...)
	config := gTracerProvider.tracerProviderConfig

	gTracer = gTracerProvider.Tracer("MatrixOrigin",
		WithReminder(batchpipe.NewConstantClock(time.Duration(15*time.Second))),
	)

	sc := SpanContext{}
	sc.traceID, sc.spanID = gTracerProvider.idGenerator.NewIDs()

	gTraceContext = ContextWithSpanContext(ctx, sc)

	export.Init()
	// init all batch Process for trace/log/error
	switch {
	case config.batchProcessMode == "singleton":
		export.Register(&MOTracer{}, NewTraceBufferPipeWorker().(batchpipe.PipeImpl[batchpipe.HasName, any]))
	case config.batchProcessMode == "distributed":
		//export.Register(&MOTracer{}, NewTraceBufferPipeWorker())
	}

	return nil, nil
}

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
}

func DefaultContext() context.Context {
	return gTraceContext
}
