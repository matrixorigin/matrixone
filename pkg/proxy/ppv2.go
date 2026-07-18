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
	"errors"
	"io"
	"net"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

const (
	// ProxyProtocolV2Signature is the signature of the Proxy Protocol version 2 header.
	ProxyProtocolV2Signature = "\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"

	ProxyHeaderLength = 16
)

const (
	unspec       = 0x00
	tcpOverIPv4  = 0x11
	udpOverIPv4  = 0x12
	tcpOverIPv6  = 0x21
	udpOverIPv6  = 0x22
	unixStream   = 0x31
	unixDatagram = 0x32
)

const (
	ipv4AddrLength = 4
	ipv6AddrLength = 16
)

func WithProxyProtocolCodec(c codec.Codec) codec.Codec {
	return &proxyProtocolCodec{Codec: c}
}

type proxyProtocolCodec struct {
	codec.Codec
}

// mysqlPacketDecodeError retains the header information needed to produce a
// correctly sequenced MySQL error after the underlying codec rejects a payload
// before constructing a Packet.
type mysqlPacketDecodeError struct {
	cause      error
	sequenceID uint8
}

func (e *mysqlPacketDecodeError) Error() string {
	return e.cause.Error()
}

func (e *mysqlPacketDecodeError) Unwrap() error {
	return e.cause
}

// ProxyHeaderV2 is the structure of the Proxy Protocol version 2 header.
type ProxyHeaderV2 struct {
	Signature          [12]byte
	ProtocolVersionCmd uint8
	ProtocolFamily     uint8
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
	proxyAddr, ok, needMore, err := parseProxyHeaderV2(in)
	if err != nil {
		return nil, false, err
	}
	if needMore {
		return nil, false, nil
	}
	if ok {
		return proxyAddr, ok, nil
	}
	var sequenceID uint8
	hasPacketHeader := in.Readable() >= frontend.PacketHeaderLength
	if hasPacketHeader {
		sequenceID = in.PeekN(0, frontend.PacketHeaderLength)[3]
	}
	value, complete, err := c.Codec.Decode(in)
	if hasPacketHeader && errors.Is(err, frontend.ErrPacketTooLarge) {
		err = &mysqlPacketDecodeError{
			cause:      err,
			sequenceID: sequenceID,
		}
	}
	return value, complete, err
}

// Encode implements the Codec interface.
func (c *proxyProtocolCodec) Encode(data interface{}, out *buf.ByteBuf, writer io.Writer) error {
	return c.Codec.Encode(data, out, writer)
}

// parseProxyHeader read potential proxy protocol v2 header from the stream
// ref: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
func parseProxyHeaderV2(in *buf.ByteBuf) (*ProxyAddr, bool, bool, error) {
	// A stream read may stop anywhere in the signature, fixed header, or
	// variable-length body. Keep input buffered while every available signature
	// byte still matches; only a mismatch proves that this is a MySQL packet and
	// allows the caller to delegate to the SQL codec.
	prefixLength := min(in.Readable(), len(ProxyProtocolV2Signature))
	if prefixLength > 0 {
		prefix := in.PeekN(0, prefixLength)
		for i := range prefix {
			if prefix[i] != ProxyProtocolV2Signature[i] {
				return nil, false, false, nil
			}
		}
	}
	if in.Readable() < ProxyHeaderLength {
		return nil, false, true, nil
	}
	headerBytes := in.PeekN(0, ProxyHeaderLength)

	header := &ProxyHeaderV2{}
	if err := binary.Read(bytes.NewBuffer(headerBytes), binary.BigEndian, header); err != nil {
		return nil, false, false, nil
	}

	// verify the signature of the header
	if string(header.Signature[:]) != ProxyProtocolV2Signature {
		return nil, false, false, nil
	}

	// Do not consume the fixed header until the complete declared body is
	// buffered. Decoders are retried with the same ByteBuf after the next socket
	// read, so partial consumption would corrupt the protocol boundary.
	if in.Readable() < ProxyHeaderLength+int(header.Length) {
		return nil, false, true, nil
	}

	// valid proxy header, consume the bytes
	in.Skip(ProxyHeaderLength)

	// According to ppv2:
	//
	// When a sender presents a
	// LOCAL connection, it should not present any address, so it sets this field to
	// zero. Receivers MUST always consider this field to skip the appropriate number
	// of bytes and must not assume zero is presented for LOCAL connections.
	//
	// In practice, the proxy may add extra information (AWS NLB do this) in the proxy header
	// which makes the body length > address length. So here we consume the entire body and
	// read address from it.
	body := make([]byte, header.Length)
	if _, err := io.ReadFull(in, body); err != nil {
		return nil, false, false, moerr.NewInternalErrorNoCtxf("cannot read proxy address, %s", err.Error())
	}
	bodyBuf := bytes.NewBuffer(body)
	addr := &ProxyAddr{}
	switch header.ProtocolFamily {
	case tcpOverIPv4, udpOverIPv4:
		if err := readProxyAddr(bodyBuf, ipv4AddrLength, addr); err != nil {
			return nil, false, false, err
		}
		return addr, true, false, nil
	case tcpOverIPv6, udpOverIPv6:
		if err := readProxyAddr(bodyBuf, ipv6AddrLength, addr); err != nil {
			return nil, false, false, err
		}
		return addr, true, false, nil
	case unspec, unixStream, unixDatagram:
		// The frame is still a consumed PROXY protocol message even when it
		// carries no IP address. Returning complete=true lets the connection
		// state machine record it and reject any later duplicate header.
		return addr, true, false, nil
	default:
		return nil, false, false, moerr.NewInternalErrorNoCtxf("unknown protocol family [%x]", header.ProtocolFamily)
	}
}

func readProxyAddr(in io.Reader, length uint16, addr *ProxyAddr) error {
	addr.SourceAddress = make(net.IP, length)
	addr.TargetAddress = make(net.IP, length)
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
