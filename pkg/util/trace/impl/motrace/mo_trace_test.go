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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var _1StmtID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1TxnID = "00000000-0000-0000-0000-000000000001"
var _1SesID = "00000000-0000-0000-0000-000000000001"
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
	if runtime.NumCPU() < 4 {
		t.Skip("machine's performance too low to handle time sensitive case, issue #11669")
		return
	}

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
	require.Equal(t, false, longTimeSpan.(*MOSpan).doneProfile)
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
	require.Equal(t, false, deadlineSpan.(*MOSpan).doneProfile)
	require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, deadlineSpan.(*MOSpan).ExtraFields)

	// span with deadline context (plus calling cancel2() before func return)
	deadlineCtx2, cancel2 := context.WithTimeout(ctx, time.Millisecond)
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
	require.Equal(t, false, deadlineSpan2.(*MOSpan).doneProfile)
	require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, deadlineSpan2.(*MOSpan).ExtraFields)

	// span with hung option, with Deadline situation
	caseHungOptionWithDeadline := func() {
		var hungSpan trace.Span
		WG.Add(1)
		go func() {
			_, hungSpan = tracer.Start(ctx, "hungCtx", trace.WithHungThreshold(time.Millisecond))
			defer WG.Done()
			defer hungSpan.End()

			time.Sleep(10 * time.Millisecond)
		}()
		WG.Wait()
		require.Equal(t, true, hungSpan.(*MOHungSpan).needRecord)
		require.Equal(t, 1, len(hungSpan.(*MOHungSpan).ExtraFields))
		require.Equal(t, true, hungSpan.(*MOHungSpan).doneProfile)
		require.Equal(t, []zap.Field{zap.Error(context.DeadlineExceeded)}, hungSpan.(*MOHungSpan).ExtraFields)
	}
	caseHungOptionWithDeadline()

	// span with hung option, with NO Deadline situation
	caseHungOptionWithoutDeadline := func() {
		var hungSpan trace.Span
		WG.Add(1)
		go func() {
			_, hungSpan = tracer.Start(ctx, "hungCtx", trace.WithHungThreshold(time.Minute))
			defer WG.Done()
			defer hungSpan.End()

			time.Sleep(10 * time.Millisecond)
		}()
		WG.Wait()
		require.Equal(t, false, hungSpan.(*MOHungSpan).needRecord)
		require.Equal(t, 0, len(hungSpan.(*MOHungSpan).ExtraFields))
		require.Equal(t, false, hungSpan.(*MOHungSpan).doneProfile)
	}
	caseHungOptionWithoutDeadline()

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

	prepareCheckCpu := func(t *testing.T) {
		if runtime.NumCPU() < 4 {
			t.Skip("machine's performance too low to handle time sensitive case, issue #11864")
		}
	}

	tests := []struct {
		name    string
		fields  fields
		prepare func(t *testing.T)
		want    bool
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
			want: true,
		},
		{
			name: "heap",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileHeap()},
				ctx:    ctx,
				tracer: tracer,
			},
			want: true,
		},
		{
			name: "threadcreate",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileThreadCreate()},
				ctx:    ctx,
				tracer: tracer,
			},
			want: true,
		},
		{
			name: "allocs",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileAllocs()},
				ctx:    ctx,
				tracer: tracer,
			},
			want: true,
		},
		{
			name: "block",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileBlock()},
				ctx:    ctx,
				tracer: tracer,
			},
			want: true,
		},
		{
			name: "mutex",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileMutex()},
				ctx:    ctx,
				tracer: tracer,
			},
			want: true,
		},
		{
			name: "cpu",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileCpuSecs(time.Second)},
				ctx:    ctx,
				tracer: tracer,
			},
			prepare: prepareCheckCpu,
			want:    true,
		},
		{
			name: "trace",
			fields: fields{
				opts:   []trace.SpanStartOption{trace.WithProfileTraceSecs(time.Second)},
				ctx:    ctx,
				tracer: tracer,
			},
			prepare: prepareCheckCpu,
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.prepare != nil {
				tt.prepare(t)
			}
			_, s := tt.fields.tracer.Start(tt.fields.ctx, "test", tt.fields.opts...)
			ms, _ := s.(*MOSpan)
			t.Logf("span.LongTimeThreshold: %v", ms.LongTimeThreshold)
			time.Sleep(time.Millisecond)
			s.End()
			t.Logf("span.LongTimeThreshold: %v, duration: %v, needRecord: %v, needProfile: %v, doneProfile: %v",
				ms.LongTimeThreshold, ms.Duration, ms.needRecord, ms.NeedProfile(), ms.doneProfile)
			require.Equal(t, tt.want, s.(*MOSpan).doneProfile)
		})
	}
}

func TestMOHungSpan_EndBeforeDeadline_doProfile(t *testing.T) {

	defer func() {
		err := recover()
		require.Nilf(t, err, "error: %s", err)
	}()

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx := context.TODO()

	var ctrlWG sync.WaitGroup
	ctrlWG.Add(1)

	_, span := tracer.Start(ctx, "test_loop", trace.WithHungThreshold(100*time.Millisecond))
	hungSpan := span.(*MOHungSpan)

	hungSpan.mux.Lock()
	// simulate the act of span.End()
	// TIPs: remove trigger.Stop()
	hungSpan.quitCancel()
	hungSpan.stopped = true
	hungSpan.MOSpan = nil
	time.Sleep(300 * time.Millisecond)
	t.Logf("hungSpan.quitCtx.Err: %s", hungSpan.quitCtx.Err())
	// END > simulate
	hungSpan.mux.Unlock()

	// wait for goroutine to finish
	// should not panic
	time.Sleep(time.Second)
}

func TestContextDeadlineAndCancel(t *testing.T) {
	if runtime.NumCPU() < 4 {
		t.Skip("machine's performance too low to handle time sensitive case")
		return
	}
	quitCtx, quitCancel := context.WithCancel(context.TODO())
	deadlineCtx, deadlineCancel := context.WithTimeout(quitCtx, time.Microsecond)
	defer deadlineCancel()

	time.Sleep(time.Second)
	t.Logf("deadlineCtx.Err: %s", deadlineCtx.Err())
	quitCancel()
	require.Equal(t, context.DeadlineExceeded, deadlineCtx.Err())
}

func TestMOTracer_FSSpanIsEnable(t *testing.T) {

	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	tracer.provider.enable = true

	trace.InitMOCtledSpan()
	trace.SetMoCtledSpanState("local", true, 0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, span := tracer.Start(ctx, "test", trace.WithKind(
		trace.SpanKindLocalFSVis))

	_, ok := span.(trace.NoopSpan)

	assert.True(t, tracer.IsEnable(trace.WithKind(trace.SpanKindLocalFSVis)))
	assert.False(t, ok)
	span.End()

	_, span = tracer.Start(context.Background(), "test", trace.WithKind(
		trace.SpanKindRemoteFSVis))
	_, ok = span.(trace.NoopSpan)
	assert.False(t, tracer.IsEnable(trace.WithKind(trace.SpanKindRemoteFSVis)))
	assert.True(t, ok)
	span.End()

}

func TestMOSpan_NeedRecord(t *testing.T) {
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}

	tracer.provider.enable = true
	tracer.provider.longSpanTime = time.Millisecond * 20
	trace.InitMOCtledSpan()

	// ctx has deadline
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	_, span := tracer.Start(ctx, "normal span")
	time.Sleep(time.Millisecond * 30)
	// deadline not expired, no need to record
	ret, _ := span.(*MOSpan).NeedRecord()
	require.False(t, ret)
	span.End()

	_, span = tracer.Start(ctx, "normal span")
	time.Sleep(time.Second)
	span.(*MOSpan).EndTime = time.Now()
	// ctx expired, record
	ret, _ = span.(*MOSpan).NeedRecord()
	require.True(t, ret)
	span.End()

	trace.SetMoCtledSpanState("statement", true, 0)
	_, span = tracer.Start(ctx, "mo_ctl controlled span",
		trace.WithKind(trace.SpanKindStatement))
	// ctx expired, but not to record
	trace.SetMoCtledSpanState("statement", false, 0)
	ret, _ = span.(*MOSpan).NeedRecord()
	require.False(t, ret)
	span.End()

	trace.SetMoCtledSpanState("statement", true, 0)
	_, span = tracer.Start(ctx, "mo_ctl controlled span",
		trace.WithKind(trace.SpanKindStatement))
	// record
	ret, _ = span.(*MOSpan).NeedRecord()
	require.True(t, ret)
	span.End()

	// it won't record until this trace last more than 1000ms
	trace.SetMoCtledSpanState("statement", true, 1000)
	_, span = tracer.Start(ctx, "mo_ctl controlled span",
		trace.WithKind(trace.SpanKindStatement))

	span.(*MOSpan).Duration = time.Since(span.(*MOSpan).StartTime)

	ret, _ = span.(*MOSpan).NeedRecord()
	require.False(t, ret)
	span.End()

	trace.SetMoCtledSpanState("statement", true, 100)
	_, span = tracer.Start(ctx, "mo_ctl controlled span",
		trace.WithKind(trace.SpanKindStatement))
	time.Sleep(time.Millisecond * 200)
	// record

	span.(*MOSpan).Duration = time.Since(span.(*MOSpan).StartTime)

	ret, _ = span.(*MOSpan).NeedRecord()
	require.True(t, ret)
	span.End()
}

func TestMOCtledKindOverwrite(t *testing.T) {
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	tracer.provider.enable = true

	trace.InitMOCtledSpan()

	fctx, fspan := tracer.Start(context.Background(), "test2", trace.WithKind(trace.SpanKindRemote))
	defer fspan.End()
	require.Equal(t, fspan.SpanContext().Kind, trace.SpanKindRemote)

	trace.SetMoCtledSpanState("statement", true, 0)
	// won't be overwritten
	_, span := tracer.Start(fctx, "test3", trace.WithKind(trace.SpanKindStatement))
	defer span.End()
	require.NotEqual(t, span.SpanContext().Kind, fspan.SpanContext().Kind)
	require.Equal(t, span.SpanContext().Kind, trace.SpanKindStatement)

}

func TestMOCtledKindPassDown(t *testing.T) {
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	tracer.provider.enable = true

	trace.InitMOCtledSpan()
	trace.SetMoCtledSpanState("s3", true, 0)
	specialCtx, specialSpan := tracer.Start(context.Background(), "special span",
		trace.WithKind(trace.SpanKindRemoteFSVis))
	defer specialSpan.End()
	require.Equal(t, specialSpan.SpanContext().Kind, trace.SpanKindRemoteFSVis)

	// won't pass down kind to child
	_, span := tracer.Start(specialCtx, "child span")
	defer span.End()
	require.NotEqual(t, span.SpanContext().Kind, specialSpan.SpanContext().Kind)

}
