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

package cnservice

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type gatedBootstrapService struct {
	*testBootService
	ready    atomic.Bool
	failure  atomic.Pointer[bootstrapTestFailure]
	observed chan struct{}
	once     sync.Once
}

type bootstrapTestFailure struct{ err error }

func (b *gatedBootstrapService) UpgradeStatus() (bool, error) {
	b.once.Do(func() { close(b.observed) })
	if failure := b.failure.Load(); failure != nil {
		return false, failure.err
	}
	return b.ready.Load(), nil
}

func TestBootstrapUpgradeWait(t *testing.T) {
	for _, scenario := range []string{"complete", "failure", "terminal", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			boot := &gatedBootstrapService{testBootService: &testBootService{}, observed: make(chan struct{})}
			result := make(chan error, 1)
			s := &service{bootstrapService: boot, bootstrapUpgradeContext: ctx, bootstrapUpgradeResult: result}
			done := make(chan error, 1)
			go func() { done <- s.waitForBootstrapUpgrade() }()
			<-boot.observed
			select {
			case err := <-done:
				t.Fatalf("admission returned before upgrade completion: %v", err)
			default:
			}
			failure := errors.New("upgrade failed")
			switch scenario {
			case "complete":
				boot.ready.Store(true)
			case "failure":
				result <- failure
			case "terminal":
				boot.failure.Store(&bootstrapTestFailure{err: failure})
			case "cancel":
				cancel()
			}
			select {
			case err := <-done:
				switch scenario {
				case "complete":
					require.NoError(t, err)
				case "failure", "terminal":
					require.ErrorIs(t, err, failure)
				case "cancel":
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("upgrade wait did not stop")
			}
		})
	}
}

func TestCloseCancelsInFlightBootstrap(t *testing.T) {
	runtime.RunTest(t.Name(), func(runtime.Runtime) {
		entered := make(chan struct{})
		boot := &testBootService{bootstrapContextHook: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		ctrl := gomock.NewController(t)
		ls := mock_lock.NewMockLockService(ctrl)
		ls.EXPECT().Close().Return(nil).Times(1)
		s := &service{
			cfg:              &Config{UUID: t.Name()},
			logger:           zap.NewNop(),
			stopper:          stopper.NewStopper(t.Name()),
			bootstrapService: boot,
			mo:               &listeningMOServer{listener: listener},
			server:           closeOnlyRPCServer{},
			lockService:      ls,
		}
		started := make(chan error, 1)
		go func() { started <- s.Start() }()
		<-entered
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		select {
		case err := <-started:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Close did not cancel the blocked startup")
		}
		require.NoError(t, <-closed)
		require.Equal(t, 1, boot.closeCount)
		reused, err := net.Listen("tcp", address)
		require.NoError(t, err)
		require.NoError(t, reused.Close())
	})
}

func TestStartContextCancellationCleansUpInFlightBootstrap(t *testing.T) {
	runtime.RunTest(t.Name(), func(runtime.Runtime) {
		entered := make(chan struct{})
		boot := &testBootService{bootstrapContextHook: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		ctrl := gomock.NewController(t)
		ls := mock_lock.NewMockLockService(ctrl)
		ls.EXPECT().Close().Return(nil).Times(1)
		s := &service{
			cfg: &Config{UUID: t.Name()}, logger: zap.NewNop(),
			stopper: stopper.NewStopper(t.Name()), bootstrapService: boot,
			mo: &listeningMOServer{listener: listener}, server: closeOnlyRPCServer{}, lockService: ls,
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started := make(chan error, 1)
		go func() { started <- s.StartContext(ctx) }()
		<-entered
		cancel()
		select {
		case err := <-started:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("StartContext did not stop after cancellation")
		}
		require.Equal(t, 1, boot.closeCount)
		require.False(t, s.viewMetadataIngressReady.Load())
		reused, err := net.Listen("tcp", address)
		require.NoError(t, err)
		require.NoError(t, reused.Close())
	})
}
