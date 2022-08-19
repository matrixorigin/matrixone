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
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/stretchr/testify/assert"
)

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
