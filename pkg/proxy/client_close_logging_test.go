// Copyright 2026 Matrix Origin
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
	"errors"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type scriptedClientCloseConn struct {
	net.Conn
	closes atomic.Int32
	failAt int32
	err    error
}

func (c *scriptedClientCloseConn) Close() error {
	count := c.closes.Add(1)
	_ = c.Conn.Close()
	if count == c.failAt {
		return c.err
	}
	if count > 1 {
		return net.ErrClosed
	}
	return nil
}

func TestHandlerClientCloseReportsUnexpectedErrors(t *testing.T) {
	closeErr := errors.New("injected client transport close failure")
	for _, tc := range []struct {
		name      string
		failAt    int32
		err       error
		wantPhase string
	}{
		{name: "transport interrupt", failAt: 1, err: closeErr, wantPhase: "interrupt"},
		{name: "final cleanup", failAt: 2, err: closeErr, wantPhase: "cleanup"},
		{name: "reset during close", failAt: 1, err: syscall.ECONNRESET, wantPhase: "interrupt"},
		{name: "already closed", failAt: 1, err: net.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.ErrorLevel)
			local, peer := net.Pipe()
			conn := &scriptedClientCloseConn{Conn: local, failAt: tc.failAt, err: tc.err}
			session := goetty.NewIOSession(goetty.WithSessionConn(1, conn),
				goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())))
			h := &handler{
				ctx:               context.Background(),
				logger:            log.GetServiceLogger(zap.New(core), metadata.ServiceType_PROXY, ""),
				counterSet:        newCounterSet(),
				connectionLimiter: newConnectionLimiter(1, 1),
			}
			done := make(chan error, 1)
			finished := make(chan struct{})
			t.Cleanup(func() {
				_ = local.Close()
				_ = peer.Close()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("handler did not stop during cleanup")
				}
			})
			go func() {
				defer close(finished)
				done <- h.handle(session)
			}()
			require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
			var header [4]byte
			_, err := io.ReadFull(peer, header[:])
			require.NoError(t, err)
			length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
			_, err = io.ReadFull(peer, make([]byte, length))
			require.NoError(t, err)
			require.NoError(t, peer.Close())
			select {
			case err = <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not finish after client disconnect")
			}
			entries := logs.FilterMessage("failed to close tcp connection").All()
			if tc.wantPhase == "" {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, 1)
			require.Contains(t, entries[0].ContextMap()["error"], tc.err.Error())
			require.Equal(t, tc.wantPhase, entries[0].ContextMap()["phase"])
			require.NotZero(t, entries[0].ContextMap()["ConnID"])
		})
	}
}
