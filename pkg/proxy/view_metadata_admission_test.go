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

package proxy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type admissionProxyApplication struct {
	stopped chan struct{}
	once    sync.Once
}

func (a *admissionProxyApplication) Start() error { return nil }

func (a *admissionProxyApplication) Stop() error {
	a.once.Do(func() { close(a.stopped) })
	return nil
}

func (a *admissionProxyApplication) StopAndWait() error { return a.Stop() }

func (a *admissionProxyApplication) GetSession(uint64) (goetty.IOSession, error) {
	return nil, errors.New("no session")
}

type admissionProxyHAKeeperClient struct {
	logservice.ProxyHAKeeperClient
	id          uint64
	key         string
	deadline    time.Time
	hasDeadline bool
}

func (c *admissionProxyHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	c.key = key
	c.deadline, c.hasDeadline = ctx.Deadline()
	return c.id, nil
}

type admissionRefreshFailureCluster struct {
	clusterservice.MOCluster
	err error
}

func (c *admissionRefreshFailureCluster) Refresh(context.Context) error {
	return c.err
}

func TestProxyAdmissionGenerationInitialization(t *testing.T) {
	client := &admissionProxyHAKeeperClient{id: 18}
	s := &Server{haKeeperClient: client}
	s.config.HAKeeper.HeartbeatTimeout.Duration = 5 * time.Second
	require.NoError(t, s.initViewMetadataAdmission(context.Background()))
	require.Equal(t, proxyViewMetadataAdmissionGenerationKey, client.key)
	require.True(t, client.hasDeadline)
	require.Positive(t, time.Until(client.deadline))
	require.LessOrEqual(t, time.Until(client.deadline), 5*time.Second)
	require.Equal(t, uint64(18), s.viewMetadataAdmissionGeneration)
	require.NotNil(t, s.viewMetadataAdmissionUpdated)
}

func TestProxyAdmissionRefreshesMembershipBeforeEpochAck(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("proxy-admission-test", rt)
	hc := &mockHAKeeperClient{}
	hc.value.ViewMetadataAdmission = &logservicepb.ViewMetadataAdmission{
		Enabled: true,
		Epoch:   5,
	}
	mc := clusterservice.NewMOCluster("proxy-admission-test", hc, time.Hour)
	defer mc.Close()

	s := &Server{
		runtime:                         runtime.ServiceRuntime("proxy-admission-test"),
		viewMetadataAdmissionGeneration: 9,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		handler:                         &handler{moCluster: mc},
	}
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true})
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      5,
			Generation: 9,
			Ready:      false,
		}))
	require.Equal(t, uint64(5), s.viewMetadataObservedEpoch.Load())
	reader := mc.(clusterservice.ViewMetadataAdmissionReader)
	require.Equal(t, uint64(5), reader.GetViewMetadataAdmission().Epoch)

	// A delayed response for the prior process cannot roll the observed epoch
	// or replace the current generation's admission snapshot.
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      6,
			Generation: 8,
			Ready:      true,
		}))
	require.Equal(t, uint64(5), s.viewMetadataObservedEpoch.Load())
	require.False(t, s.viewMetadataAdmission.Load().Ready)
}

func TestProxyAdmissionRevokesIngressForHigherGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	app := &admissionProxyApplication{stopped: make(chan struct{})}
	s := &Server{
		config:                          Config{UUID: "superseded-proxy"},
		runtime:                         runtime.DefaultRuntime(),
		app:                             app,
		viewMetadataAdmissionGeneration: 9,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		viewMetadataAdmissionContext:    ctx,
		viewMetadataAdmissionCancel:     cancel,
	}

	require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      5,
			Generation: 10,
		}))
	require.Equal(t, uint64(10), s.viewMetadataAdmission.Load().Generation)
	select {
	case <-app.stopped:
	default:
		t.Fatal("superseded Proxy ingress and active sessions were not stopped")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestProxyAdmissionWaitsForAuthoritativeResponse(t *testing.T) {
	s := &Server{
		config:                          Config{UUID: "proxy-authoritative-wait"},
		runtime:                         runtime.DefaultRuntime(),
		viewMetadataAdmissionGeneration: 12,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	done := make(chan error, 1)
	go func() {
		done <- s.waitForViewMetadataAdmission(context.Background())
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Proxy admission returned before an authoritative response: %v", err)
	default:
	}

	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), nil))
	require.NoError(t, <-done)
}

func TestProxyAdmissionTimeoutTracksHeartbeatConfiguration(t *testing.T) {
	s := &Server{}
	s.config.HAKeeper.HeartbeatInterval.Duration = 31 * time.Second
	s.config.HAKeeper.HeartbeatTimeout.Duration = 3 * time.Second
	require.Equal(t, 37*time.Second, s.viewMetadataAdmissionTimeout())
}

func TestProxyAdmissionWaitHonorsCallerCancellation(t *testing.T) {
	s := &Server{
		config:                          Config{UUID: "proxy-canceled-wait"},
		viewMetadataAdmissionGeneration: 12,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.waitForViewMetadataAdmission(ctx), context.Canceled)
}

func TestProxyAdmissionDoesNotAckFailedMembershipRefresh(t *testing.T) {
	refreshErr := errors.New("membership refresh failed")
	s := &Server{
		config:                          Config{UUID: "proxy-refresh-failure"},
		runtime:                         runtime.DefaultRuntime(),
		viewMetadataAdmissionGeneration: 14,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		handler: &handler{moCluster: &admissionRefreshFailureCluster{
			err: refreshErr,
		}},
	}
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true})

	err := s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      7,
			Generation: 14,
		})
	require.ErrorIs(t, err, refreshErr)
	require.Zero(t, s.viewMetadataObservedEpoch.Load())
	require.True(t, s.viewMetadataAdmission.Load().Ready)
}
