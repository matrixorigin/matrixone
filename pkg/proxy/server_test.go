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
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func WithHAKeeperClient(c logservice.ProxyHAKeeperClient) Option {
	return func(s *Server) {
		s.haKeeperClient = c
	}
}

type constructionCleanupHAKeeperClient struct {
	mockHAKeeperClient
	allocationErr error
	closeCount    atomic.Int32
}

func (c *constructionCleanupHAKeeperClient) AllocateIDByKey(context.Context, string) (uint64, error) {
	if c.allocationErr != nil {
		return 0, c.allocationErr
	}
	return 9, nil
}

func (c *constructionCleanupHAKeeperClient) Close() error {
	c.closeCount.Add(1)
	return nil
}

type immediateProxyRevocationClient struct {
	mockHAKeeperClient
	responded chan struct{}
	once      sync.Once
}

func (c *immediateProxyRevocationClient) AllocateIDByKey(context.Context, string) (uint64, error) {
	return 9, nil
}

func (c *immediateProxyRevocationClient) SendProxyHeartbeat(
	context.Context,
	logservicepb.ProxyHeartbeat,
) (logservicepb.CommandBatch, error) {
	c.once.Do(func() { close(c.responded) })
	return logservicepb.CommandBatch{ViewMetadataAdmission: &logservicepb.ViewMetadataAdmission{
		Enabled:    true,
		Epoch:      1,
		Generation: 10,
	}}, nil
}

func TestNewServerClosesHAKeeperClientOnAdmissionInitializationFailure(t *testing.T) {
	client := &constructionCleanupHAKeeperClient{allocationErr: errors.New("allocate generation failed")}
	runtime.SetupServiceBasedRuntime(t.Name(), runtime.DefaultRuntime())
	_, err := NewServer(context.Background(), Config{UUID: t.Name()},
		WithRuntime(runtime.ServiceRuntime(t.Name())), WithHAKeeperClient(client))
	require.ErrorIs(t, err, client.allocationErr)
	require.Equal(t, int32(1), client.closeCount.Load())
}

func TestNewServerClosesHAKeeperClientOnHandlerConstructionFailure(t *testing.T) {
	client := &constructionCleanupHAKeeperClient{}
	runtime.SetupServiceBasedRuntime(t.Name(), runtime.DefaultRuntime())
	_, err := NewServer(context.Background(), Config{
		UUID: t.Name(),
		Plugin: &PluginConfig{
			Backend: "://",
			Timeout: time.Second,
		},
	}, WithRuntime(runtime.ServiceRuntime(t.Name())), WithHAKeeperClient(client))
	require.Error(t, err)
	require.Equal(t, int32(1), client.closeCount.Load())
}

func TestNewServerImmediatelyRevokedReleasesListener(t *testing.T) {
	reserved, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	address := reserved.Addr().String()
	require.NoError(t, reserved.Close())

	runtime.SetupServiceBasedRuntime(t.Name(), runtime.DefaultRuntime())
	client := &immediateProxyRevocationClient{responded: make(chan struct{})}
	s, err := NewServer(context.Background(), Config{
		UUID:          t.Name(),
		ListenAddress: address,
	}, WithRuntime(runtime.ServiceRuntime(t.Name())), WithHAKeeperClient(client))
	require.NoError(t, err)
	<-client.responded
	<-s.viewMetadataAdmissionContext.Done()
	require.NoError(t, s.Close())

	rebound, err := net.Listen("tcp4", address)
	require.NoError(t, err, "construction-time revocation must retain and close the listener owner")
	require.NoError(t, rebound.Close())
}

func TestNewServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	temp := os.TempDir()
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress: "unix://" + listenAddr,
	}
	hc := &mockHAKeeperClient{}
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc))
	require.NoError(t, err)
	require.NotNil(t, s)
	defer func() {
		if s != nil {
			err := s.Close()
			require.NoError(t, err)
		}
	}()
}
