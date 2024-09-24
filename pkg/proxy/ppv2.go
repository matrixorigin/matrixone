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
		return nil, false, moerr.NewInternalErrorNoCtxf("cannot read proxy address, %s", err.Error())
	}
	bodyBuf := bytes.NewBuffer(body)
	addr := &ProxyAddr{}
	switch header.ProtocolFamily {
	case tcpOverIPv4, udpOverIPv4:
		if err := readProxyAddr(bodyBuf, ipv4AddrLength, addr); err != nil {
			return nil, false, err
		}
		return addr, true, nil
	case tcpOverIPv6, udpOverIPv6:
		if err := readProxyAddr(bodyBuf, ipv6AddrLength, addr); err != nil {
			return nil, false, err
		}
		return addr, true, nil
	case unspec, unixStream, unixDatagram:
		// no address to read
		return addr, false, nil
	default:
		return nil, false, moerr.NewInternalErrorNoCtxf("unknown protocol family [%x]", header.ProtocolFamily)
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
