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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type bootstrapHAKeeperClient struct {
	*mockHAKeeperClient
	getClusterState func(context.Context) (logpb.CheckerState, error)
}

const bootstrapTestTimeout = 10 * time.Second

func (c *bootstrapHAKeeperClient) GetClusterState(ctx context.Context) (logpb.CheckerState, error) {
	return c.getClusterState(ctx)
}

func TestBootstrapRetriesTransientHAKeeperError(t *testing.T) {
	calls := 0
	c := &bootstrapHAKeeperClient{
		mockHAKeeperClient: &mockHAKeeperClient{},
		getClusterState: func(context.Context) (logpb.CheckerState, error) {
			calls++
			if calls == 1 {
				return logpb.CheckerState{}, errors.New("temporary HAKeeper error")
			}
			return logpb.CheckerState{
				TaskTableUser: logpb.TaskTableUser{
					Username: "u1",
					Password: "p1",
				},
			}, nil
		},
	}
	h := &handler{
		haKeeperClient: c,
		sqlWorker:      newSQLWorker(),
	}

	require.NoError(t, h.bootstrapWithTimeout(
		context.Background(),
		time.Millisecond,
		time.Second,
	))
	require.Equal(t, 2, calls)
}

func TestBootstrapReturnsContextCancellation(t *testing.T) {
	called := make(chan struct{})
	c := &bootstrapHAKeeperClient{
		mockHAKeeperClient: &mockHAKeeperClient{},
		getClusterState: func(ctx context.Context) (logpb.CheckerState, error) {
			close(called)
			<-ctx.Done()
			return logpb.CheckerState{}, ctx.Err()
		},
	}
	h := &handler{
		haKeeperClient: c,
		sqlWorker:      newSQLWorker(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errC := make(chan error, 1)
	go func() {
		errC <- h.bootstrapWithTimeout(ctx, time.Millisecond, time.Second)
	}()

	select {
	case <-called:
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("HAKeeper request was not started")
	}
	cancel()
	select {
	case err := <-errC:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("bootstrap did not return after context cancellation")
	}
}

func TestBootstrapStopsWithStopper(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan error, 1)
	c := &bootstrapHAKeeperClient{
		mockHAKeeperClient: &mockHAKeeperClient{},
		getClusterState: func(ctx context.Context) (logpb.CheckerState, error) {
			close(requestStarted)
			<-ctx.Done()
			requestCanceled <- ctx.Err()
			return logpb.CheckerState{}, ctx.Err()
		},
	}
	h := &handler{
		haKeeperClient: c,
		sqlWorker:      newSQLWorker(),
	}
	st := stopper.NewStopper("test-proxy-bootstrap")
	defer st.Stop()
	require.NoError(t, runBootstrapTask(context.Background(), st, h))

	select {
	case <-requestStarted:
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("HAKeeper request was not started")
	}

	stopDone := make(chan struct{})
	go func() {
		st.Stop()
		close(stopDone)
	}()

	select {
	case err := <-requestCanceled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("HAKeeper request context was not canceled")
	}
	select {
	case <-stopDone:
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("stopper did not wait for bootstrap task termination")
	}
}

func TestBootstrapReturnsTimeoutAfterPersistentHAKeeperError(t *testing.T) {
	permanentErr := errors.New("HAKeeper unavailable")
	calls := 0
	c := &bootstrapHAKeeperClient{
		mockHAKeeperClient: &mockHAKeeperClient{},
		getClusterState: func(context.Context) (logpb.CheckerState, error) {
			calls++
			return logpb.CheckerState{}, permanentErr
		},
	}
	h := &handler{
		haKeeperClient: c,
		sqlWorker:      newSQLWorker(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errC := make(chan error, 1)
	go func() {
		errC <- h.bootstrapWithTimeout(
			ctx,
			time.Millisecond,
			50*time.Millisecond,
		)
	}()
	var err error
	select {
	case err = <-errC:
	case <-time.After(bootstrapTestTimeout):
		t.Fatal("bootstrap did not return after its timeout")
	}

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, permanentErr)
	require.Positive(t, calls)
}

func TestBootstrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	c := mockHAKeeperClient{}
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	cfg := Config{
		RebalanceInterval: toml.Duration{Duration: time.Second},
	}
	cfg.Cluster.RefreshInterval = toml.Duration{Duration: defaultRefreshInterval}
	h, err := newProxyHandler(ctx, rt, cfg, st, nil, &c, true)
	require.NoError(t, err)
	require.NoError(t, h.bootstrap(ctx))

	u, err := db_holder.GetSQLWriterDBUser()
	require.NoError(t, err)
	require.Equal(t, db_holder.MOLoggerUser, u.UserName)
	f := db_holder.GetSQLWriterDBAddressFunc()
	require.NotNil(t, f)
	addr, err := f(ctx, true)
	require.Error(t, err)
	require.Equal(t, "", addr)
}
