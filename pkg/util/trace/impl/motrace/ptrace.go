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

package motrace

import "github.com/matrixorigin/matrixone/pkg/util/trace"

var _ trace.TracerProvider = &MOTracerProvider{}

type MOTracerProvider struct {
	tracerProviderConfig
}

func defaultMOTracerProvider() *MOTracerProvider {
	pTracer := &MOTracerProvider{
		tracerProviderConfig{
			enable:           false,
			resource:         trace.NewResource(),
			idGenerator:      &moIDGenerator{},
			batchProcessMode: FileService,
			batchProcessor:   NoopBatchProcessor{},
		},
	}
	WithNode("node_uuid", trace.NodeTypeStandalone).apply(&pTracer.tracerProviderConfig)
	WithMOVersion("MatrixOne").apply(&pTracer.tracerProviderConfig)
	return pTracer
}

func newMOTracerProvider(opts ...TracerProviderOption) *MOTracerProvider {
	pTracer := defaultMOTracerProvider()
	for _, opt := range opts {
		opt.apply(&pTracer.tracerProviderConfig)
	}
	return pTracer
}

func (p *MOTracerProvider) Tracer(instrumentationName string, opts ...trace.TracerOption) trace.Tracer {
	if !p.IsEnable() {
		return trace.NoopTracer{}
	}

	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: instrumentationName},
		provider:     p,
	}
	for _, opt := range opts {
		opt.Apply(&tracer.TracerConfig)
	}
	return tracer
}
