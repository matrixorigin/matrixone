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
	"context"
	"sync/atomic"
)

// Start a span entity
func Start(ctx context.Context, spanName string, opts ...SpanStartOption) (context.Context, Span) {
	return DefaultTracer().Start(ctx, spanName, opts...)
}

func Debug(ctx context.Context, spanName string, opts ...SpanStartOption) (context.Context, Span) {
	return DefaultTracer().Debug(ctx, spanName, opts...)
}

func Generate(ctx context.Context) context.Context {
	ctx, _ = DefaultTracer().Start(ctx, "generate", WithNewRoot(true))
	return ctx
}

func IsEnable() bool {
	return DefaultTracer().IsEnable()
}

var gTracerHolder atomic.Value

type ITracerHolder interface {
	GetTracer() Tracer
}

type tracerHolder struct {
	tracer Tracer
}

func (h *tracerHolder) GetTracer() Tracer { return h.tracer }

func SetDefaultTracer(tracer Tracer) {
	var holder ITracerHolder = &tracerHolder{tracer: tracer}
	gTracerHolder.Store(holder)
}

func DefaultTracer() Tracer {
	return gTracerHolder.Load().(ITracerHolder).GetTracer()
}

func init() {
	SetDefaultTracer(noopTracerProvider{}.Tracer("init"))
}
