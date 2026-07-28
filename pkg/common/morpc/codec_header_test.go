// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/stretchr/testify/assert"
)

type traceCodecIDGenerator struct {
	next byte
}

func (g *traceCodecIDGenerator) NewIDs() (trace.TraceID, trace.SpanID) {
	g.next++
	var traceID trace.TraceID
	traceID[len(traceID)-1] = g.next
	var spanID trace.SpanID
	spanID[len(spanID)-1] = g.next
	return traceID, spanID
}

func (g *traceCodecIDGenerator) NewSpanID() trace.SpanID {
	g.next++
	var spanID trace.SpanID
	spanID[len(spanID)-1] = g.next
	return spanID
}

func TestEncodeContext(t *testing.T) {
	hc := &deadlineContextCodec{}
	out := buf.NewByteBuf(8)

	c, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n, err := hc.Encode(&RPCMessage{Ctx: c}, out)
	assert.Equal(t, 8, n)
	assert.NoError(t, err)
	assert.Equal(t, 8, out.GetWriteIndex())
}

func TestDecodeContext(t *testing.T) {
	hc := &deadlineContextCodec{}
	v := buf.Int64ToBytes(int64(time.Second))
	msg := &RPCMessage{}
	n, err := hc.Decode(msg, v)
	assert.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.NotNil(t, msg.Ctx)
	ts, ok := msg.Ctx.Deadline()
	assert.True(t, ok)
	assert.True(t, !ts.IsZero())
}

func TestEncodeAndDecodeTrace(t *testing.T) {
	hc := &traceCodec{}
	out := buf.NewByteBuf(8)
	span := trace.SpanContextWithIDs(trace.TraceID{}, trace.SpanID{})
	n, err := hc.Encode(&RPCMessage{Ctx: trace.ContextWithSpanContext(context.Background(), span)}, out)
	assert.Equal(t, 1+span.Size(), n)
	assert.NoError(t, err)

	msg := &RPCMessage{}
	_, data := out.ReadBytes(1 + span.Size())

	n, err = hc.Decode(msg, nil)
	assert.Equal(t, 0, n)
	assert.Error(t, err)

	n, err = hc.Decode(msg, data[:1])
	assert.Equal(t, 0, n)
	assert.Error(t, err)

	n, err = hc.Decode(msg, data)
	assert.Equal(t, 1+span.Size(), n)
	assert.NoError(t, err)

	span.Kind = trace.SpanKindRemote
	assert.Equal(t, span, trace.SpanFromContext(msg.Ctx).SpanContext())
}

func TestGeneratedTraceContextSurvivesRPCCodec(t *testing.T) {
	tracer := trace.NewNonRecordingTracer(&traceCodecIDGenerator{})
	ctx, span := tracer.Start(context.Background(), "root", trace.WithNewRoot(true))
	original := span.SpanContext()
	assert.False(t, original.IsEmpty())

	codec := &traceCodec{}
	out := buf.NewByteBuf(1 + original.Size())
	n, err := codec.Encode(&RPCMessage{Ctx: ctx}, out)
	assert.NoError(t, err)
	assert.Equal(t, 1+original.Size(), n)

	_, data := out.ReadBytes(out.Readable())
	msg := &RPCMessage{}
	n, err = codec.Decode(msg, data)
	assert.NoError(t, err)
	assert.Equal(t, 1+original.Size(), n)

	decoded := trace.SpanFromContext(msg.Ctx).SpanContext()
	original.Kind = trace.SpanKindRemote
	assert.Equal(t, original, decoded)
}

func TestEncodeAndDecodeClock(t *testing.T) {
	var n1, n2 int64
	f1 := func() int64 {
		return atomic.LoadInt64(&n1)
	}
	f2 := func() int64 {
		return atomic.LoadInt64(&n2)
	}
	c1 := &hlcCodec{clock: clock.NewHLCClock(f1, 0)}
	c2 := &hlcCodec{clock: clock.NewHLCClock(f2, 0)}

	n1 = 1
	n2 = 2

	out := buf.NewByteBuf(8)
	n, err := c1.Encode(nil, out)
	assert.NoError(t, err)
	assert.Equal(t, 12, n)

	_, data := out.ReadBytes(out.Readable())
	n, err = c2.Decode(nil, data)
	assert.NoError(t, err)
	assert.Equal(t, 12, n)

	now, _ := c2.clock.Now()
	assert.Equal(t, n2, now.PhysicalTime)
}
