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
	resource *Resource

	enableTracer bool

	debugMode bool // TODO: can check span's END
}

// TracerProviderOption configures a TracerProvider.
type TracerProviderOption interface {
	apply(*tracerProviderConfig)
}

var _ TracerProvider = &MOTracerProvider{}

type MOTracerProvider struct {
	tracerProviderConfig
}

func newMOTracerProvider(opts ...TracerProviderOption) *MOTracerProvider {
	ptracer := &MOTracerProvider{}
	for _, opt := range opts {
		opt.apply(&ptracer.tracerProviderConfig)
	}
	return ptracer
}

func (p *MOTracerProvider) Tracer(instrumentationName string, opts ...TracerOption) Tracer {
	if p.enableTracer {
		tracer := &MOTracer{
			TracerConfig: TracerConfig{Name: instrumentationName},
		}
		for _, opt := range opts {
			opt.apply(&tracer.TracerConfig)
		}
		return tracer
	} else {
		return gNoopTracer
	}
}
