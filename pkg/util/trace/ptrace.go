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
	"sync"
)

/*
import (
	"sync/atomic"
)

var (
	gTracer = defaultTracerValue()
)

type tracerProviderHolder struct {
	tp TracerProvider
}

func defaultTracerValue() *atomic.Value {
	v := &atomic.Value{}
	v.Store(tracerProviderHolder{tp: &MOTracerProvider{}})
	return v
}

func GetTracerProvider() TracerProvider {
	return gTracer.Load().(tracerProviderHolder).tp
}

func SetTracerProvider(tp TracerProvider) {
	gTracer.Store(tracerProviderHolder{tp: tp})
}
*/

type SpanProcessor interface {
	OnStart(ctx context.Context, s Span)
	OnEnd(ctx context.Context, s Span)
	Shutdown()
	FLush()
}

type Sampler interface {
}

type IDGenerator interface {
	NewIDs() (TraceID, SpanID)
	NewSpanID() SpanID
}

// tracerProviderConfig.
type tracerProviderConfig struct {
	// processors contains collection of SpanProcessors that are processing pipeline
	// for spans in the trace signal.
	// SpanProcessors registered with a TracerProvider and are called at the start
	// and end of a Span's lifecycle, and are called in the order they are
	// registered.
	processors []SpanProcessor

	// sampler is the default sampler used when creating new spans.
	sampler Sampler

	// idGenerator is used to generate all Span and Trace IDs when needed.
	idGenerator IDGenerator

	// resource contains attributes representing an entity that produces telemetry.
	resource Resource

	enableTracer bool

	debugMode bool // TODO: can check span's END

	batchProcessMode string
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
		config.resource["version"] = v
	}
}

// WithNode give id as NodeId, t as NodeType
func WithNode(id int64, t SpanKind) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.resource["node"] = &MONodeResource{
			NodeID:   id,
			NodeType: t,
		}
	}
}

var _ TracerProvider = &MOTracerProvider{}

type MOTracerProvider struct {
	tracerProviderConfig
}

func newMOTracerProvider(opts ...TracerProviderOption) *MOTracerProvider {
	pTracer := &MOTracerProvider{}
	pTracer.resource = make(Resource)
	for _, opt := range opts {
		opt.apply(&pTracer.tracerProviderConfig)
	}
	return pTracer
}

func (p *MOTracerProvider) Tracer(instrumentationName string, opts ...TracerOption) Tracer {
	if p.enableTracer {
		tracer := &MOTracer{
			TracerConfig: TracerConfig{Name: instrumentationName},
		}
		tracer.spanPool = &sync.Pool{New: func() any {
			return &MOSpan{
				tracer: tracer,
			}
		}}
		for _, opt := range opts {
			opt.apply(&tracer.TracerConfig)
		}
		return tracer
	} else {
		return gNoopTracer
	}
}
