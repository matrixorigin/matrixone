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
	"io"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type deadlineContextCodec struct {
}

func (hc *deadlineContextCodec) Encode(msg *RPCMessage, out *buf.ByteBuf) (int, error) {
	if msg.Ctx == nil {
		return 0, nil
	}

	out.WriteInt64(int64(msg.GetTimeout()))
	return 8, nil
}

func (hc *deadlineContextCodec) Decode(msg *RPCMessage, data []byte) (int, error) {
	if len(data) < 8 {
		return 0, io.ErrShortBuffer
	}

	if msg.Ctx == nil {
		msg.Ctx = context.Background()
	}

	msg.timeoutAt = time.Now().Add(time.Duration(buf.Byte2Int64(data)))
	return 8, nil
}

type traceCodec struct {
}

func (hc *traceCodec) Encode(msg *RPCMessage, out *buf.ByteBuf) (int, error) {
	if msg.Ctx == nil {
		return 0, nil
	}

	span := trace.SpanFromContext(msg.Ctx)
	c := span.SpanContext()
	n := c.Size()
	out.MustWriteByte(byte(n))
	idx, _ := setWriterIndexAfterGow(out, n)
	c.MarshalTo(out.RawSlice(idx, idx+n))
	return 1 + n, nil
}

func (hc *traceCodec) Decode(msg *RPCMessage, data []byte) (int, error) {
	if len(data) < 1 {
		return 0, io.ErrShortBuffer
	}

	if len(data) < int(data[0]) {
		return 0, io.ErrShortBuffer
	}

	c := &trace.SpanContext{}
	if err := c.Unmarshal(data[1 : 1+data[0]]); err != nil {
		return 0, err
	}

	if msg.Ctx == nil {
		msg.Ctx = context.Background()
	}
	msg.Ctx = trace.ContextWithSpanContext(msg.Ctx, *c)
	return int(1 + data[0]), nil
}

type hlcCodec struct {
	clock clock.Clock
}

func (hc *hlcCodec) Encode(msg *RPCMessage, out *buf.ByteBuf) (int, error) {
	now, _ := hc.clock.Now()
	out.WriteInt64(now.PhysicalTime)
	out.WriteUint32(now.LogicalTime)
	return 12, nil
}

func (hc *hlcCodec) Decode(msg *RPCMessage, data []byte) (int, error) {
	if len(data) < 12 {
		return 0, io.ErrShortBuffer
	}
	ts := timestamp.Timestamp{}
	ts.PhysicalTime = buf.Byte2Int64(data)
	ts.LogicalTime = buf.Byte2Uint32(data[8:])
	hc.clock.Update(ts)
	return 12, nil
}
