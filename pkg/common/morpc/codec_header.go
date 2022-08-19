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
)

type deadlineContextCodec struct {
}

func (hc *deadlineContextCodec) Encode(msg *RPCMessage, out *buf.ByteBuf) (int, error) {
	if msg.Ctx == nil {
		return 0, nil
	}

	v, err := msg.GetTimeoutFromContext()
	if err != nil {
		return 0, err
	}
	out.WriteInt64(int64(v))
	return 8, nil
}

func (hc *deadlineContextCodec) Decode(msg *RPCMessage, data []byte) (int, error) {
	if len(data) < 8 {
		return 0, io.ErrShortBuffer
	}

	if msg.Ctx == nil {
		msg.Ctx = context.Background()
	}

	msg.Ctx, msg.cancel = context.WithTimeout(msg.Ctx, time.Duration(buf.Byte2Int64(data)))
	return 8, nil
}
