// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// ProxyProtocolV2Signature is the signature of the Proxy Protocol version 2 header.
	ProxyProtocolV2Signature = "\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"

	ProxyHeaderLength = 16
)

func WithProxyProtocolCodec(c codec.Codec) codec.Codec {
	return &proxyProtocolCodec{Codec: c}
}

type proxyProtocolCodec struct {
	codec.Codec
}

// ProxyHeaderV2 is the structure of the Proxy Protocol version 2 header.
type ProxyHeaderV2 struct {
	Signature          [12]byte
	ProtocolVersionCmd uint8
	ProtocolVersion    uint8
	Length             uint16
}

// ProxyAddr contains the source and target address.
type ProxyAddr struct {
	SourceAddress net.IP
	SourcePort    uint16
	TargetAddress net.IP
	TargetPort    uint16
}

// Decode implements the Codec interface.
func (c *proxyProtocolCodec) Decode(in *buf.ByteBuf) (interface{}, bool, error) {
	proxyAddr, ok, err := parseProxyHeaderV2(in)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return proxyAddr, ok, nil
	}
	return c.Codec.Decode(in)
}

// Encode implements the Codec interface.
func (c *proxyProtocolCodec) Encode(data interface{}, out *buf.ByteBuf, writer io.Writer) error {
	return c.Codec.Encode(data, out, writer)
}

// parseProxyHeader read potential proxy protocol v2 header from the stream
// ref: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
func parseProxyHeaderV2(in *buf.ByteBuf) (*ProxyAddr, bool, error) {
	// Read the Proxy Protocol header.
	if in.Readable() < ProxyHeaderLength {
		return nil, false, nil
	}
	headerBytes := in.PeekN(0, ProxyHeaderLength)

	header := &ProxyHeaderV2{}
	if err := binary.Read(bytes.NewBuffer(headerBytes), binary.BigEndian, header); err != nil {
		return nil, false, nil
	}

	// verify the signature of the header
	if string(header.Signature[:]) != ProxyProtocolV2Signature {
		return nil, false, nil
	}

	// valid proxy header, consume the bytes
	in.Skip(ProxyHeaderLength)

	addr := &ProxyAddr{}
	switch header.Length {
	case 0:
		// LOCAL connection, no source address will be presented
		return addr, true, nil
	case 12, 36:
		if err := readProxyAddr(in, header.Length, addr); err != nil {
			return nil, false, err
		}
		return addr, true, nil
	default:
		return nil, false, moerr.NewInternalErrorNoCtx("invalid source address length %d", header.Length)
	}
}

func readProxyAddr(in *buf.ByteBuf, length uint16, addr *ProxyAddr) error {
	// 4 presents source port (2 bytes) + target port (2 bytes)
	addrLength := (length - 4) / 2
	addr.SourceAddress = make(net.IP, addrLength)
	addr.TargetAddress = make(net.IP, addrLength)
	if err := binary.Read(in, binary.BigEndian, addr.SourceAddress); err != nil {
		return err
	}
	if err := binary.Read(in, binary.BigEndian, addr.TargetAddress); err != nil {
		return err
	}
	if err := binary.Read(in, binary.BigEndian, &addr.SourcePort); err != nil {
		return err
	}
	if err := binary.Read(in, binary.BigEndian, &addr.TargetPort); err != nil {
		return err
	}
	return nil
}
