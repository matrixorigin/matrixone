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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

type mockServerConn struct {
	conn net.Conn
}

var _ ServerConn = (*mockServerConn)(nil)

func newMockServerConn(conn net.Conn) *mockServerConn {
	m := &mockServerConn{
		conn: conn,
	}
	return m
}

func (s *mockServerConn) RawConn() net.Conn { return s.conn }
func (s *mockServerConn) HandleHandshake(_ *frontend.Packet) (*frontend.Packet, error) {
	return nil, nil
}
func (s *mockServerConn) Close() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}

type testCNServer struct {
	sync.Mutex
	ctx      context.Context
	addr     string
	listener net.Listener
	started  bool
	quit     chan interface{}
	wait     *sync.Cond
	count    int
}

func startTestCNServer(t *testing.T, ctx context.Context, addr string) func() error {
	b := &testCNServer{
		ctx:  ctx,
		addr: addr,
		quit: make(chan interface{}),
		wait: sync.NewCond(&sync.Mutex{}),
	}
	go func() {
		err := b.Start()
		require.NoError(t, err)
	}()
	require.True(t, b.waitCNServerReady())
	return func() error {
		return b.Stop()
	}
}

func (s *testCNServer) waitCNServerReady() bool {
	ctx, cancel := context.WithTimeout(s.ctx, time.Second*3)
	defer cancel()
	tick := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
			s.Lock()
			started := s.started
			s.Unlock()
			_, err := net.Dial("tcp", s.addr)
			if err == nil && started {
				return true
			}
		}
	}
}

func (s *testCNServer) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.Lock()
	s.started = true
	s.Unlock()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return nil
				default:
					return err
				}
			} else {
				go func() {
					s.wait.L.Lock()
					defer s.wait.L.Unlock()
					s.count++
					defer func() { s.count-- }()
					tick := time.NewTicker(time.Millisecond * 100)
					for {
						select {
						case <-tick.C:
							_, _ = conn.Write(makeOKPacket())
						case <-s.quit:
							return
						}
					}
				}()
			}
		}
	}
}

func (s *testCNServer) Stop() error {
	close(s.quit)
	_ = s.listener.Close()
	s.wait.L.Lock()
	defer s.wait.L.Unlock()
	for s.count > 0 {
		s.wait.Wait()
	}
	return nil
}

func TestFakeCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)

	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	addr := "127.0.0.1:38009"
	stopFn := startTestCNServer(t, tp.ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

	li := labelInfo{}
	cn1 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: "127.0.0.1:38009",
	}

	cleanup := testStartClient(t, tp, li, cn1)
	defer cleanup()
}
