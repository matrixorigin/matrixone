package trace

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMOTracer_Start(t1 *testing.T) {
	type fields struct {
		TracerConfig TracerConfig
	}
	type args struct {
		ctx  context.Context
		name string
		opts []SpanOption
	}
	rootCtx := ContextWithSpanContext(context.Background(), SpanContextWithIDs(1, 1))
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantNewRoot      bool
		wantTraceId      TraceID
		wantParentSpanId SpanID
	}{
		{
			name: "normal",
			fields: fields{
				TracerConfig: TracerConfig{Name: "normal"},
			},
			args: args{
				ctx:  rootCtx,
				name: "normal",
				opts: []SpanOption{},
			},
			wantNewRoot:      false,
			wantTraceId:      1,
			wantParentSpanId: 1,
		},
		{
			name: "newRoot",
			fields: fields{
				TracerConfig: TracerConfig{Name: "newRoot"},
			},
			args: args{
				ctx:  rootCtx,
				name: "newRoot",
				opts: []SpanOption{WithNewRoot(true)},
			},
			wantNewRoot:      true,
			wantTraceId:      1,
			wantParentSpanId: 1,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &MOTracer{
				TracerConfig: tt.fields.TracerConfig,
				provider:     gTracerProvider,
			}
			newCtx, span := t.Start(tt.args.ctx, tt.args.name, tt.args.opts...)
			if !tt.wantNewRoot {
				require.Equal(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.Equal(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.Equal(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			} else {
				require.NotEqual(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.NotEqual(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.NotEqual(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			}
		})
	}
}
