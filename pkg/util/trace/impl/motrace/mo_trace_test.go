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

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var _1TxnID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1SesID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1TraceID trace.TraceID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _2TraceID trace.TraceID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2}
var _10F0TraceID trace.TraceID = [16]byte{0x09, 0x87, 0x65, 0x43, 0x21, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0}
var _1SpanID trace.SpanID = [8]byte{0, 0, 0, 0, 0, 0, 0, 1}
var _2SpanID trace.SpanID = [8]byte{0, 0, 0, 0, 0, 0, 0, 2}
var _16SpanID trace.SpanID = [8]byte{0, 0, 0, 0, 0, 0x12, 0x34, 0x56}

func TestMOTracer_Start(t *testing.T) {
	if runtime.GOOS == `linux` {
		t.Skip()
		return
	}
	type fields struct {
		Enable bool
	}
	type args struct {
		ctx  context.Context
		name string
		opts []trace.SpanStartOption
	}
	rootCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextWithIDs(_1TraceID, _1SpanID))
	stmCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextWithID(_1TraceID, trace.SpanKindStatement))
	dAtA := make([]byte, 24)
	span := trace.SpanFromContext(stmCtx)
	c := span.SpanContext()
	cnt, err := c.MarshalTo(dAtA)
	require.Nil(t, err)
	require.Equal(t, 24, cnt)
	sc := &trace.SpanContext{}
	err = sc.Unmarshal(dAtA)
	require.Nil(t, err)
	remoteCtx := trace.ContextWithSpanContext(context.Background(), *sc)
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantNewRoot      bool
		wantTraceId      trace.TraceID
		wantParentSpanId trace.SpanID
		wantKind         trace.SpanKind
	}{
		{
			name:             "normal",
			fields:           fields{Enable: true},
			args:             args{ctx: rootCtx, name: "normal", opts: []trace.SpanStartOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         trace.SpanKindInternal,
		},
		{
			name:             "newRoot",
			fields:           fields{Enable: true},
			args:             args{ctx: rootCtx, name: "newRoot", opts: []trace.SpanStartOption{trace.WithNewRoot(true)}},
			wantNewRoot:      true,
			wantTraceId:      _1TraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         trace.SpanKindInternal,
		},
		{
			name:             "statement",
			fields:           fields{Enable: true},
			args:             args{ctx: stmCtx, name: "newStmt", opts: []trace.SpanStartOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: trace.NilSpanID,
			wantKind:         trace.SpanKindStatement,
		},
		{
			name:             "empty",
			fields:           fields{Enable: true},
			args:             args{ctx: context.Background(), name: "backgroundCtx", opts: []trace.SpanStartOption{}},
			wantNewRoot:      true,
			wantTraceId:      trace.NilTraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         trace.SpanKindInternal,
		},
		{
			name:             "remote",
			fields:           fields{Enable: true},
			args:             args{ctx: remoteCtx, name: "remoteCtx", opts: []trace.SpanStartOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: trace.NilSpanID,
			wantKind:         trace.SpanKindRemote,
		},
	}
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tracer.provider.enable = tt.fields.Enable
			newCtx, span := tracer.Start(tt.args.ctx, tt.args.name, tt.args.opts...)
			if !tt.wantNewRoot {
				require.Equal(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.Equal(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.Equal(t1, tt.wantParentSpanId, trace.SpanFromContext(newCtx).ParentSpanContext().SpanID)
			} else {
				require.NotEqualf(t1, tt.wantTraceId, span.SpanContext().TraceID, "want %s, but got %s", tt.wantTraceId.String(), span.SpanContext().TraceID.String())
				require.NotEqual(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.NotEqual(t1, tt.wantParentSpanId, trace.SpanFromContext(newCtx).ParentSpanContext().SpanID)
			}
			require.Equal(t1, tt.wantKind, trace.SpanFromContext(newCtx).ParentSpanContext().Kind)
			require.Equal(t1, span, trace.SpanFromContext(newCtx))
		})
	}
}

func TestSpanContext_MarshalTo(t *testing.T) {
	type fields struct {
		TraceID trace.TraceID
		SpanID  trace.SpanID
	}
	type args struct {
		dAtA []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      int
		wantBytes []byte
	}{
		{
			name: "normal",
			fields: fields{
				TraceID: trace.NilTraceID,
				SpanID:  _16SpanID,
			},
			args: args{dAtA: make([]byte, 24)},
			want: 24,
			//                1  2  3  4  5  6  7  8, 1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
		{
			name: "not-zero",
			fields: fields{
				TraceID: _10F0TraceID,
				SpanID:  _16SpanID,
			},
			args: args{dAtA: make([]byte, 24)},
			want: 24,
			//                1     2     3     4     5     6     7     8,    1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0x09, 0x87, 0x65, 0x43, 0x21, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &trace.SpanContext{
				TraceID: tt.fields.TraceID,
				SpanID:  tt.fields.SpanID,
				Kind:    trace.SpanKindRemote,
			}
			got, err := c.MarshalTo(tt.args.dAtA)
			require.Equal(t, nil, err)
			require.Equal(t, got, tt.want)
			require.Equal(t, tt.wantBytes, tt.args.dAtA)
			newC := &trace.SpanContext{}
			err = newC.Unmarshal(tt.args.dAtA)
			require.Equal(t, nil, err)
			require.Equal(t, c, newC)
			require.Equal(t, tt.fields.TraceID, newC.TraceID)
			require.Equal(t, tt.fields.SpanID, newC.SpanID)
		})
	}
}

func TestMOSpan_End(t *testing.T) {

	s := gostub.Stub(&freeMOSpan, func(span *MOSpan) {})
	defer s.Reset()

	ctx := context.TODO()
	p := newMOTracerProvider(WithLongSpanTime(time.Hour), EnableTracer(true))
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "test"},
		provider:     p,
	}

	var WG sync.WaitGroup

	// short time span
	var shortTimeSpan trace.Span
	WG.Add(1)
	go func() {
		_, shortTimeSpan = tracer.Start(ctx, "shortTimeSpan")
		defer WG.Done()
		defer shortTimeSpan.End()
	}()
	WG.Wait()
	require.Equal(t, false, shortTimeSpan.(*MOSpan).needRecord)
	require.Equal(t, 0, len(shortTimeSpan.(*MOSpan).ExtraFields))

	// span with LongTimeThreshold
	// and set ExtraFields
	var longTimeSpan trace.Span
	WG.Add(1)
	extraFields := []zap.Field{zap.String("str", "field"), zap.Int64("int", 0)}
	go func() {
		_, longTimeSpan = tracer.Start(ctx, "longTimeSpan", trace.WithLongTimeThreshold(time.Millisecond))
		defer WG.Done()
		defer longTimeSpan.End()

		longTimeSpan.AddExtraFields(extraFields...)

		time.Sleep(10 * time.Millisecond)
	}()
	WG.Wait()
	require.Equal(t, true, longTimeSpan.(*MOSpan).needRecord)
	require.Equal(t, 2, len(longTimeSpan.(*MOSpan).ExtraFields))
	require.Equal(t, true, longTimeSpan.(*MOSpan).doneProfile)
	require.Equal(t, extraFields, longTimeSpan.(*MOSpan).ExtraFields)

	// span with deadline context
	deadlineCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	var deadlineSpan trace.Span
	WG.Add(1)
	go func() {
		_, deadlineSpan = tracer.Start(deadlineCtx, "deadlineCtx")
		defer WG.Done()
		defer deadlineSpan.End()

		time.Sleep(10 * time.Millisecond)
	}()
	WG.Wait()
	require.Equal(t, true, deadlineSpan.(*MOSpan).needRecord)
	require.Equal(t, 1, len(deadlineSpan.(*MOSpan).ExtraFields))
	require.Equal(t, true, deadlineSpan.(*MOSpan).doneProfile)
	require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, deadlineSpan.(*MOSpan).ExtraFields)

	// span with deadline context (plus calling cancel2() before func return)
	deadlineCtx2, cancel2 := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	var deadlineSpan2 trace.Span
	WG.Add(1)
	go func() {
		_, deadlineSpan2 = tracer.Start(deadlineCtx2, "deadlineCtx")
		defer WG.Done()
		defer deadlineSpan2.End()

		time.Sleep(10 * time.Millisecond)
		cancel2()
	}()
	WG.Wait()
	require.Equal(t, true, deadlineSpan2.(*MOSpan).needRecord)
	require.Equal(t, 1, len(deadlineSpan2.(*MOSpan).ExtraFields))
	require.Equal(t, true, deadlineSpan2.(*MOSpan).doneProfile)
	require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, deadlineSpan2.(*MOSpan).ExtraFields)

	// span with deadline context (plus calling cancel2() before func return)
	defer cancel()
	var hungSpan trace.Span
	WG.Add(1)
	go func() {
		_, hungSpan = tracer.Start(ctx, "hungCtx", trace.WithHungThreshold(time.Millisecond))
		defer WG.Done()
		defer hungSpan.End()

		time.Sleep(10 * time.Millisecond)
	}()
	WG.Wait()
	require.Equal(t, true, hungSpan.(*MOSpan).needRecord)
	require.Equal(t, 1, len(hungSpan.(*MOSpan).ExtraFields))
	require.Equal(t, true, hungSpan.(*MOSpan).doneProfile)
	require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, hungSpan.(*MOSpan).ExtraFields)

}

type dummyFileWriterFactory struct{}

func (f *dummyFileWriterFactory) GetRowWriter(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
	return &dummyStringWriter{}
}
func (f *dummyFileWriterFactory) GetWriter(ctx context.Context, fp string) io.WriteCloser {
	selfDir, err := filepath.Abs(".")
	if err != nil {
		panic(err)
	}
	fmt.Printf("root: %s\n", selfDir)
	dirname := path.Dir(fp)
	if dirname != "." && dirname != "./" && dirname != "/" {
		if err := os.Mkdir(dirname, os.ModeType|os.ModePerm); err != nil && !os.IsExist(err) {
			panic(err)
		}
	}
	fw, err := os.Create(fp)
	if err != nil {
		panic(err)
	}
	return fw
}

func TestMOSpan_doProfile(t *testing.T) {
	type fields struct {
		opts   []trace.SpanStartOption
		ctx    context.Context
		tracer *MOTracer
	}

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx := context.TODO()

	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "normal",
			fields: fields{
				opts:   nil,
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "goroutine",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileGoroutine()}, // it will dump file into ETL folder.
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "heap",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileHeap()},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "threadcreate",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileThreadCreate()},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "allocs",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileAllocs()},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "block",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileBlock()},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "mutex",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileMutex()},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "cpu",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileCpuSecs(time.Second)},
				ctx:    ctx,
				tracer: tracer,
			},
		},
		{
			name: "trace",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileTraceSecs(time.Second)},
				ctx:    ctx,
				tracer: tracer,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s := tt.fields.tracer.Start(tt.fields.ctx, "test", tt.fields.opts...)
			s.End()
		})
	}
}
