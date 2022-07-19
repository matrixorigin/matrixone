// Copyright 2021 Matrix Origin
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

package rpcserver

import (
	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/util/protoc"
)

var (
	rc = &rpcCodec{}
)

type rpcCodec struct {
}

func NewCodec(maxsize int) (codec.Encoder, codec.Decoder) {
	return length.NewWithSize(rc, rc, 0, 0, 0, maxsize)
}

func (c *rpcCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	v := message.Acquire()
	if err := v.Unmarshal(in.GetMarkedRemindData()); err != nil {
		return false, nil, err
	}
	in.MarkedBytesReaded()
	return true, v, nil
}

func (c *rpcCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	v := data.(*message.Message)
	size := v.Size()
	index := out.GetWriteIndex()
	out.Expansion(size)
	protoc.MustMarshalTo(v, out.RawBuf()[index:index+size])
	if err := out.SetWriterIndex(index + size); err != nil {
		return err
	}
	return nil
}
