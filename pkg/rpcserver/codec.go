package rpcserver

import (
	"matrixone/pkg/rpcserver/message"

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
	var v protoc.PB

	v = message.Acquire()
	if err := v.Unmarshal(in.GetMarkedRemindData()); err != nil {
		return false, nil, err
	}
	in.MarkedBytesReaded()
	return true, v, nil
}

func (c *rpcCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	var v protoc.PB

	v = data.(*message.Message)
	size := v.Size()
	index := out.GetWriteIndex()
	out.Expansion(size)
	protoc.MustMarshalTo(v, out.RawBuf()[index:index+size])
	out.SetWriterIndex(index + size)
	return nil
}
