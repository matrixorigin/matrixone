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
	"context"
	"crypto/tls"
	"encoding/binary"
	"time"

	"github.com/fagongzi/goetty/v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

// writeInitialHandshake sends the initial handshake to client.
func (c *clientConn) writeInitialHandshake() error {
	return c.mysqlProto.WritePacket(c.mysqlProto.MakeHandshakePayload())
}

// handleHandshakeResp receives login information from client and saves it
// in proxy end.
func (c *clientConn) handleHandshakeResp() (*protocolMemoryLease, error) {
	deadline := time.Now().Add(c.clientHandshakeTimeout)
	rawConn := c.conn.RawConn()
	deadlineConn := newAbsoluteReadDeadlineConn(rawConn, deadline)
	c.conn.UseConn(deadlineConn)
	c.mysqlProto.UseConn(deadlineConn)
	defer func() {
		deadlineConn.disable()
		// A non-TLS handshake still uses deadlineConn directly, so remove the
		// now-disabled wrapper instead of retaining one object per connection.
		// After a TLS upgrade, tls.Conn owns the wrapped transport and the
		// disabled wrapper remains a transparent part of that stack.
		if c.conn.RawConn() == deadlineConn {
			c.conn.UseConn(rawConn)
			c.mysqlProto.UseConn(rawConn)
		}
	}()
	if err := c.handleHandshakeRespBefore(deadline); err != nil {
		return nil, err
	}
	transientBytes := uint64(0)
	if c.protocolMemoryLimiter != nil {
		transientBytes = c.protocolMemoryLimiter.budget.initialBytes
	}
	lease, err := acquireProtocolMemoryBefore(
		c.ctx,
		c.protocolMemoryLimiter,
		transientBytes,
		deadline,
	)
	if err != nil {
		return nil, err
	}
	if err := c.handoffHandshakeBuffer(lease); err != nil {
		lease.release()
		return nil, err
	}
	return lease, nil
}

// handoffHandshakeBuffer closes the phase-owned goetty input buffer without
// dropping bytes that were read past the final login packet. This applies to
// both plaintext and TLS: for TLS, the same ByteBuf is also referenced by the
// transport's BufferedConn and must be drained and reset before it is freed.
func (c *clientConn) handoffHandshakeBuffer(lease *protocolMemoryLease) error {
	session, ok := c.conn.(goetty.BufferedIOSession)
	if !ok {
		return nil
	}
	input := session.InBuf()
	if input == nil {
		return nil
	}

	readable := input.Readable()
	if readable == 0 {
		input.Reset()
		input.Close()
		return nil
	}
	if readable > c.clientHandshakePacketLimit {
		return frontend.ErrPacketTooLarge
	}

	conn, err := newHandshakeBufferedConn(
		c.conn.RawConn(),
		input.PeekN(0, readable),
		c.sessionAllocator,
		lease,
	)
	if err != nil {
		return err
	}

	// Ownership moves to conn before the tunnel can observe it. Resetting the
	// indices is required because a TLS BufferedConn still holds this ByteBuf;
	// a closed buffer with stale non-zero indices would panic on its next Read.
	input.Skip(readable)
	input.Reset()
	input.Close()
	c.conn.UseConn(conn)
	c.mysqlProto.UseConn(conn)
	return nil
}

func (c *clientConn) handleHandshakeRespBefore(deadline time.Time) error {
	if c.handshakePack != nil {
		return moerr.NewInvalidInputNoCtx("client handshake has already been processed")
	}
	tlsEstablished := false
	for {
		// The proxy reads a protocol phase packet from the client. The first
		// packet may be an SSLRequest; the terminal packet must be a login.
		pack, err := c.readPacketBefore(deadline)
		if err != nil {
			return err
		}
		c.mysqlProto.AddSequenceId(1)

		// SQLCodec copies the decoded payload out of goetty's input buffer. Move
		// the one lifetime-retained copy into the Proxy's shared bounded allocator
		// so ProtocolMemoryLimit covers established connections as well as the
		// fixed client/backend IO buffers.
		payload, err := c.sessionAllocator.Alloc(len(pack.Payload))
		if err != nil {
			return err
		}
		payloadView := payload[:len(pack.Payload)]
		copy(payloadView, pack.Payload)
		ownedPack := &frontend.Packet{
			Length:     pack.Length,
			SequenceID: pack.SequenceID,
			Payload:    payloadView,
		}

		// Parse the phase packet and determine whether it requests TLS.
		ssl, err := c.mysqlProto.HandleHandshake(c.ctx, ownedPack.Payload)
		if err != nil {
			// HandleHandshake may retain a slice of the payload before a later
			// validation fails. Keep clientConn as the cleanup owner until Close.
			c.handshakePack = ownedPack
			c.handshakePayloadAllocation = payload
			return err
		}
		if ssl {
			// An SSLRequest contains no authentication state and is superseded by
			// the login sent inside TLS, so it must not cross the phase.
			c.sessionAllocator.Free(payload)
			if tlsEstablished {
				return moerr.NewInvalidInputNoCtx("duplicate TLS request during client handshake")
			}
			if err = c.upgradeToTLS(deadline); err != nil {
				return err
			}
			tlsEstablished = true
			continue
		}

		// Ownership transfers only for the final login packet. It is used to
		// authenticate fresh backend connections during migration.
		c.handshakePack = ownedPack
		c.handshakePayloadAllocation = payload

		// Parse tenant information from the final login request.
		if err := c.clientInfo.parse(c.mysqlProto.GetUserName()); err != nil {
			return err
		}
		li := &c.clientInfo.labelInfo
		c.clientInfo.labelInfo = newLabelInfo(c.clientInfo.Tenant, li.Labels)

		c.clientInfo.hash, err = c.clientInfo.getHash()
		if err != nil {
			return err
		}
		return nil
	}
}

// upgradeToTLS upgrades the connection to TLS connection.
func (c *clientConn) upgradeToTLS(handshakeDeadline time.Time) error {
	if c.tlsConfig == nil {
		return moerr.NewInternalErrorNoCtx("TLS config is invalid")
	}
	remaining := time.Until(handshakeDeadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	timeout := min(c.tlsConnectTimeout, remaining)
	// TLS handshake packet from client might have been read into the buffer, use a wrapped conn to
	// avoid losing handshake packets.
	tlsConn := tls.Server(c.conn.(goetty.BufferedIOSession).BufferedConn(), c.tlsConfig)
	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseUpgradeToTLS)
	defer cancel()
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		err = moerr.AttachCause(ctx, err)
		return moerr.NewInternalErrorf(ctx, "TLS handshake error: %v", err)
	}
	c.conn.UseConn(tlsConn)
	c.mysqlProto.UseConn(tlsConn)
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
	s.connID = binary.LittleEndian.Uint32(p.Payload[pos : pos+4])
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
