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
	"github.com/matrixorigin/matrixone/pkg/util"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
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

// tracerProviderConfig.
type tracerProviderConfig struct {
	// spanProcessors contains collection of SpanProcessors that are processing pipeline
	// for spans in the trace signal.
	// SpanProcessors registered with a TracerProvider and are called at the start
	// and end of a Span's lifecycle, and are called in the order they are
	// registered.
	spanProcessors []SpanProcessor

	// sampler is the default sampler used when creating new spans.
	sampler Sampler

	// idGenerator is used to generate all Span and Trace IDs when needed.
	idGenerator IDGenerator

	// resource contains attributes representing an entity that produces telemetry.
	resource *Resource // see WithMOVersion, WithNode,

	enableTracer bool // see EnableTracer

	// TODO: can check span's END
	debugMode bool // see DebugMode

	batchProcessMode string // see WithBatchProcessMode

	sqlExecutor func() ie.InternalExecutor // see WithSQLExecutor
}

func (cfg tracerProviderConfig) getNodeResource() *MONodeResource {
	if val, has := cfg.resource.Get("Node"); !has {
		return &MONodeResource{}
	} else {
		return val.(*MONodeResource)
	}
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
func WithNode(id int64, t NodeType) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.resource.Put("Node", &MONodeResource{
			NodeID:   id,
			NodeType: t,
		})
	}
}

func EnableTracer(enable bool) tracerProviderOptionFunc {
	return func(cfg *tracerProviderConfig) {
		cfg.enableTracer = enable
	}
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
		cfg.sqlExecutor = f
	}
}

var _ IDGenerator = &MOTraceIdGenerator{}

type MOTraceIdGenerator struct{}

func (M MOTraceIdGenerator) NewIDs() (TraceID, SpanID) {
	return TraceID(util.Fastrand64()), SpanID(util.Fastrand64())
}

func (M MOTraceIdGenerator) NewSpanID() SpanID {
	return SpanID(util.Fastrand64())
}

var _ TracerProvider = &MOTracerProvider{}

type MOTracerProvider struct {
	tracerProviderConfig
}

func newMOTracerProvider(opts ...TracerProviderOption) *MOTracerProvider {
	pTracer := &MOTracerProvider{}
	pTracer.resource = newResource()
	pTracer.idGenerator = &MOTraceIdGenerator{}
	for _, opt := range opts {
		opt.apply(&pTracer.tracerProviderConfig)
	}
	return pTracer
}

func (p *MOTracerProvider) Tracer(instrumentationName string, opts ...TracerOption) Tracer {
	if !p.enableTracer {
		return noopTracer{}
	}

	tracer := &MOTracer{
		TracerConfig: TracerConfig{Name: instrumentationName},
		provider:     p,
	}
	for _, opt := range opts {
		opt.apply(&tracer.TracerConfig)
	}
	return tracer
}
