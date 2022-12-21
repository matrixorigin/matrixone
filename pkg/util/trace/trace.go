package trace

import (
	"context"
	"sync/atomic"
)

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return DefaultTracer().Start(ctx, spanName, opts...)
}

func Debug(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return DefaultTracer().Debug(ctx, spanName, opts...)
}

func Generate(ctx context.Context) context.Context {
	ctx, _ = DefaultTracer().Start(ctx, "generate", WithNewRoot(true))
	return ctx
}

var gTracerHolder atomic.Value

type tracerHolder struct {
	tracer Tracer
}

func SetDefaultTracer(tracer Tracer) {
	gTracerHolder.Store(&tracerHolder{tracer: tracer})
}

func DefaultTracer() Tracer {
	return gTracerHolder.Load().(*tracerHolder).tracer
}

func init() {
	SetDefaultTracer(noopTracerProvider{}.Tracer("init"))
}
