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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

// writeInitialHandshake sends the initial handshake to client.
func (c *clientConn) writeInitialHandshake() error {
	// TODO(volgariver6): serverVersion is not correct when the config of
	// ParameterUnit.SV.MoVersion is not empty.
	return c.mysqlProto.WritePacket(c.mysqlProto.MakeHandshakePayload())
}

func (c *clientConn) handleHandshakeResp() error {
	// The proxy reads login request from client.
	pack, err := c.readPacket()
	if err != nil {
		return err
	}
	c.mysqlProto.AddSequenceId(1)
	// Save the login packet in client connection, it will be used
	// in the future.
	c.handshakePack = pack

	// Parse the login information and returns whether ssl is needed.
	// Also, we can get connection attributes from client if it sets
	// some.
	//
	// TODO(volgariver6): currently, only connectionAttributes in JDBC URI
	// is supported.
	// SSL is not supported for now.
	_, err = c.mysqlProto.HandleHandshake(c.ctx, pack.Payload)
	if err != nil {
		return err
	}

	// parse tenant information from client login request.
	if err := c.account.parse(c.mysqlProto.GetUserName()); err != nil {
		return err
	}

	c.labelInfo = newLabelInfo(c.account.tenant, c.mysqlProto.GetConnectAttrs())
	return nil
}

func (s *serverConn) parseConnID(p *frontend.Packet) error {
	if len(p.Payload) < 2 {
		return moerr.NewInternalErrorNoCtx("protocol error: payload is too short")
	}
	// From the beginning of payload.
	pos := 0
	// Pass the protocol version.
	pos += 1
	zeroPos := bytes.IndexByte(p.Payload[pos:], 0)
	if zeroPos == -1 {
		return moerr.NewInternalErrorNoCtx("protocol error: cannot get null string")
	}
	// Pass the server version string.
	pos += zeroPos + 1
	if pos+3 >= int(p.Length) {
		return moerr.NewInternalErrorNoCtx("protocol error: cannot parse connection ID")
	}
	s.backendConnID = binary.LittleEndian.Uint32(p.Payload[pos : pos+4])
	return nil
}

// readInitialHandshake reads initial handshake from CN server. The result
// is useless.
func (s *serverConn) readInitialHandshake() error {
	r, err := s.readPacket()
	if err != nil {
		return err
	}
	if err := s.parseConnID(r); err != nil {
		return err
	}
	return nil
}

// writeHandshakeResp writes the auth packet to CN server.
func (s *serverConn) writeHandshakeResp(handshakeResp *frontend.Packet) (*frontend.Packet, error) {
	if err := s.mysqlProto.WritePacket(handshakeResp.Payload); err != nil {
		return nil, err
	}
	// The CN server send a response back to indicate if the auth packet
	// is OK to login.
	data, err := s.readPacket()
	if err != nil {
		return nil, err
	}
	return data, nil
}
