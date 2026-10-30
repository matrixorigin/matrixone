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

package embed

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestClusterStopCancelsStartupAndAllowsRestart(t *testing.T) {
	isolateClusterPortLeases(t)
	ports, err := acquireClusterPortLease()
	require.NoError(t, err)
	c := &cluster{services: []*operator{{serviceType: metadata.ServiceType_CN}},
		portLease: ports, portLeaseBase: ports.base, portLeaseNext: ports.base}
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	entered := make(chan struct{})
	c.startFn = func(*operator) error {
		ctx := c.startup.context()
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	started := make(chan error, 1)
	go func() { started <- c.Start() }()
	select {
	case <-entered:
	case err := <-started:
		t.Fatalf("startup failed before entering the service: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not begin")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()
	select {
	case err := <-started:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not cancel startup")
	}
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	c.startFn = func(*operator) error { return nil }
	require.NoError(t, c.Start())
	require.NoError(t, c.Stop())
}

func TestStartControlWaitsForAllConcurrentStops(t *testing.T) {
	var control startControl
	first, err := control.begin(t.Context())
	require.NoError(t, err)
	control.stop()
	control.stop()
	require.ErrorIs(t, first.Err(), context.Canceled)
	control.finish(first, first.Err())
	control.stopped()
	_, err = control.begin(t.Context())
	require.Error(t, err)
	control.stopped()
	second, err := control.begin(t.Context())
	require.NoError(t, err)
	control.finish(first, context.Canceled)
	require.NoError(t, second.Err(), "an old failed start must not cancel the next generation")
	control.stop()
	control.stopped()
}

type cancelableStartService struct {
	entered chan struct{}
	closed  chan struct{}
}

func (s *cancelableStartService) Start() error { return s.StartContext(context.Background()) }
func (s *cancelableStartService) StartContext(ctx context.Context) error {
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}
func (s *cancelableStartService) Close() error { close(s.closed); return nil }

func TestOperatorCloseCancelsConstructedServiceStart(t *testing.T) {
	op := &operator{}
	ctx, err := op.startup.begin(t.Context())
	require.NoError(t, err)
	svc := &cancelableStartService{entered: make(chan struct{}), closed: make(chan struct{})}
	started := make(chan error, 1)
	go func() {
		op.Lock()
		err := op.startConstructedServiceLocked(svc)
		op.Unlock()
		op.startup.finish(ctx, err)
		started <- err
	}()
	select {
	case <-svc.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("service startup did not begin")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- op.Close() }()
	select {
	case err := <-started:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("operator close did not cancel service startup")
	}
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("operator close did not finish")
	}
	select {
	case <-svc.closed:
	default:
		t.Fatal("constructed service was not closed")
	}
}
