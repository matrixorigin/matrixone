// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

// controlledLogtailer makes the server's asynchronous boundaries deterministic:
// tests decide when a callback arrives and can block either subscription phase.
type controlledLogtailer struct {
	mu       sync.Mutex
	callback func(timestamp.Timestamp, timestamp.Timestamp, func(), ...logtail.TableLogtail) error
	now      timestamp.Timestamp
	tableFn  func(context.Context, api.TableID, timestamp.Timestamp, timestamp.Timestamp) (logtail.TableLogtail, func(), error)
}

var _ taelogtail.Logtailer = (*controlledLogtailer)(nil)

func (l *controlledLogtailer) RangeLogtail(
	_ context.Context, _, to timestamp.Timestamp,
) ([]logtail.TableLogtail, []func(), error) {
	return nil, nil, nil
}

func (l *controlledLogtailer) RegisterCallback(
	callback func(timestamp.Timestamp, timestamp.Timestamp, func(), ...logtail.TableLogtail) error,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.callback = callback
}

func (l *controlledLogtailer) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, func(), error) {
	l.mu.Lock()
	fn := l.tableFn
	l.mu.Unlock()
	if fn != nil {
		return fn(ctx, table, from, to)
	}
	return mockLogtail(table, to), nil, nil
}

func (l *controlledLogtailer) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.now, timestamp.Timestamp{}
}

func (l *controlledLogtailer) notify(
	from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail,
) error {
	l.mu.Lock()
	callback := l.callback
	l.mu.Unlock()
	if callback == nil {
		return nil
	}
	return callback(from, to, closeCB, tails...)
}

type testRPCServer struct {
	handler func(context.Context, morpc.RPCMessage, uint64, morpc.ClientSession) error
}

func (s *testRPCServer) Start() error { return nil }
func (s *testRPCServer) Close() error { return nil }
func (s *testRPCServer) RegisterRequestHandler(
	handler func(context.Context, morpc.RPCMessage, uint64, morpc.ClientSession) error,
) {
	s.handler = handler
}

// captureSession has an explicit connection context and preserves writes for
// assertions without binding a unit test to TCP, morpc scheduling, or sleeps.
type captureSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	writes chan morpc.Message
}

func newCaptureSession() *captureSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &captureSession{
		ctx:    ctx,
		cancel: cancel,
		writes: make(chan morpc.Message, 64),
	}
}

func (s *captureSession) Close() error                { s.cancel(); return nil }
func (s *captureSession) SessionCtx() context.Context { return s.ctx }
func (s *captureSession) RemoteAddress() string       { return "test" }
func (s *captureSession) Write(ctx context.Context, message morpc.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.writes <- message:
		return nil
	}
}
func (s *captureSession) AsyncWrite(message morpc.Message) error {
	return s.Write(context.Background(), message)
}
func (*captureSession) CreateCache(context.Context, uint64) (morpc.MessageCache, error) {
	panic("not used in logtail service tests")
}
func (*captureSession) DeleteCache(uint64) { panic("not used in logtail service tests") }
func (*captureSession) GetCache(uint64) (morpc.MessageCache, error) {
	panic("not used in logtail service tests")
}

func newUnitLogtailServer(t *testing.T, logtailer *controlledLogtailer) *LogtailServer {
	return newUnitLogtailServerWithStart(t, logtailer, true)
}

func newUnitLogtailServerWithStart(
	t *testing.T, logtailer *controlledLogtailer, start bool,
) *LogtailServer {
	t.Helper()
	cfg := options.NewDefaultLogtailServerCfg()
	cfg.RpcMaxMessageSize = 1024
	cfg.PullWorkerPoolSize = 1
	cfg.RPCStreamPoisonTime = 20 * time.Millisecond
	cfg.ResponseSendTimeout = time.Second

	server, err := NewLogtailServer(
		"", cfg, logtailer, mockRuntime(),
		func(string, string, *LogtailServer, ...morpc.ServerOption) (morpc.RPCServer, error) {
			return &testRPCServer{}, nil
		},
	)
	require.NoError(t, err)
	if start {
		require.NoError(t, server.Start())
		// The production manager always emits a metadata-only first event to
		// establish the publisher's ordering barrier. Unit fixtures must honor
		// the same producer contract before exercising later phases.
		bootstrapped := make(chan struct{})
		require.NoError(t, logtailer.notify(
			timestamp.Timestamp{}, timestamp.Timestamp{}, func() { close(bootstrapped) },
		))
		select {
		case <-bootstrapped:
		case <-time.After(10 * time.Second):
			t.Fatal("logtail sender did not consume the bootstrap event")
		}
	}
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server
}

func newCaptureStream(session *captureSession) morpcStream {
	return mockMorpcStream(session, 1, 1024)
}
