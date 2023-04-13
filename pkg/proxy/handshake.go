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
	"context"
	"crypto/tls"
	"time"

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
	ssl, err := c.mysqlProto.HandleHandshake(c.ctx, pack.Payload)
	if err != nil {
		return err
	}
	if ssl {
		if err = c.upgradeToTLS(); err != nil {
			return err
		}
		return c.handleHandshakeResp()
	}

	// parse tenant information from client login request.
	if err := c.account.parse(c.mysqlProto.GetUserName()); err != nil {
		return err
	}

	c.labelInfo = newLabelInfo(c.account.tenant, c.mysqlProto.GetConnectAttrs())
	return nil
}

// upgradeToTLS upgrades the connection to TLS connection.
func (c *clientConn) upgradeToTLS() error {
	tlsConn := tls.Server(c.conn.RawConn(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return err
	}
	c.conn.UseConn(tlsConn)
	return nil
}

// readInitialHandshake reads initial handshake from CN server. The result
// is useless.
func (s *serverConn) readInitialHandshake() error {
	_, err := s.readPacket()
	if err != nil {
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
