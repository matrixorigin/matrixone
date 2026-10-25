// Copyright 2021 - 2026 Matrix Origin
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

package morpc

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// observedConnectFactory delegates all connection work to the real transport.
// Its events distinguish returning a completed connect failure from retrying
// the same address without using a short caller deadline as the test oracle.
type observedConnectFactory struct {
	ContextBackendFactory
	calls        atomic.Int32
	firstResult  chan error
	retryStarted chan struct{}
}

func (f *observedConnectFactory) CreateWithContext(
	ctx context.Context,
	remote string,
	opts ...BackendOption,
) (Backend, error) {
	attempt := f.calls.Add(1)
	if attempt == 2 {
		close(f.retryStarted)
	}
	b, err := f.ContextBackendFactory.CreateWithContext(ctx, remote, opts...)
	if attempt == 1 {
		f.firstResult <- err
	}
	return b, err
}

// TestClientOperationsReturnExhaustedConnectBudget exercises a genuine net timeout:
// TCP is accepted, but the peer never completes the TLS handshake. The normal
// goetty factory must terminate its bounded initial connect attempt, allowing
// a caller with a long operation context to re-evaluate the failed endpoint.
func TestClientOperationsReturnExhaustedConnectBudget(t *testing.T) {
	operations := []struct {
		name string
		run  func(RPCClient, context.Context, string) error
	}{
		{"Send", func(c RPCClient, ctx context.Context, remote string) error {
			future, err := c.Send(ctx, remote, newTestMessage(1))
			if future != nil {
				future.Close()
			}
			return err
		}},
		{"NewStream", func(c RPCClient, ctx context.Context, remote string) error {
			stream, err := c.NewStream(ctx, remote, false)
			if stream != nil {
				_ = stream.Close(true)
			}
			return err
		}},
		{"Ping", func(c RPCClient, ctx context.Context, remote string) error {
			return c.Ping(ctx, remote)
		}},
	}
	for _, budget := range []struct {
		name    string
		timeout time.Duration
	}{
		{"context_only", 0},
		{"bounded_create", 5 * time.Second},
	} {
		t.Run(budget.name, func(t *testing.T) {
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					testClientOperationConnectBudget(t, budget.timeout, operation.run)
				})
			}
		})
	}
}

func testClientOperationConnectBudget(
	t *testing.T,
	waitTimeout time.Duration,
	operation func(RPCClient, context.Context, string) error,
) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	stop := make(chan struct{})
	acceptDone := make(chan struct{})
	accepted := make(chan struct{}, 1)
	var connections sync.WaitGroup
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		<-acceptDone
		connections.Wait()
	})
	go func() {
		defer close(acceptDone)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				select {
				case accepted <- struct{}{}:
				default:
				}
				<-stop
			}()
		}
	}()

	factory := &observedConnectFactory{
		ContextBackendFactory: NewGoettyBasedBackendFactory(
			newTestCodec(),
			WithBackendConnectTimeout(50*time.Millisecond),
			WithBackendLogger(zap.NewNop()),
			WithBackendGoettyOptions(goetty.WithSessionTLS(&tls.Config{
				// This peer intentionally never sends a certificate. Verification
				// is irrelevant; the production TLS dial must time out first.
				InsecureSkipVerify: true,
			})),
		).(ContextBackendFactory),
		firstResult:  make(chan error, 1),
		retryStarted: make(chan struct{}),
	}
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientAutoCreateWaitTimeout(waitTimeout),
		WithClientAutoCreateQueueWaitTimeout(0),
		WithClientDisableCircuitBreaker(),
		WithClientLogger(zap.NewNop()),
	)
	require.NoError(t, err)
	manager := newClientGCManager()
	useClientGCManagerForTest(t, rpcClient.(*client), manager)
	t.Cleanup(func() {
		require.NoError(t, rpcClient.Close())
		manager.stop()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		result <- operation(rpcClient, ctx, listener.Addr().String())
	}()
	guard := time.NewTimer(10 * time.Second)
	defer guard.Stop()

	var factoryErr error
	select {
	case factoryErr = <-factory.firstResult:
	case <-guard.C:
		t.Fatal("real transport did not finish its bounded connection attempt")
	}
	require.Error(t, factoryErr)
	select {
	case <-accepted:
	case <-guard.C:
		t.Fatal("connection attempt did not reach the real listener")
	}

	select {
	case operationErr := <-result:
		require.ErrorIs(t, operationErr, factoryErr)
		require.True(t, moerr.IsMoErrCode(operationErr, moerr.ErrRPCTimeout), operationErr)
		require.EqualValues(t, 1, factory.calls.Load())
	case <-factory.retryStarted:
		cancel()
		select {
		case <-result:
		case <-guard.C:
			t.Fatal("operation did not stop after cancellation")
		}
		t.Fatalf("operation retried a fully exhausted initial connect budget instead of returning its error: %v", factoryErr)
	case <-guard.C:
		t.Fatal("operation neither returned the connect failure nor made another attempt")
	}
}
