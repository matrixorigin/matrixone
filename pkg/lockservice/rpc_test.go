// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			releaseResponse(resp)
		},
	)
}

func TestSetRestartServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_SetRestartService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.SetRestartService.OK = true
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					SetRestartService: lock.SetRestartServiceRequest{ServiceID: "s1"},
					Method:            lock.Method_SetRestartService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.True(t, resp.SetRestartService.OK)
			releaseResponse(resp)
		},
	)
}

func TestCanRestartServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_CanRestartService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.CanRestartService.OK = true
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					CanRestartService: lock.CanRestartServiceRequest{ServiceID: "s1"},
					Method:            lock.Method_CanRestartService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.True(t, resp.CanRestartService.OK)
			releaseResponse(resp)
		},
	)
}

func TestRemainTxnServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_RemainTxnInService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.RemainTxnInService.RemainTxn = -1
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					RemainTxnInService: lock.RemainTxnInServiceRequest{ServiceID: "s1"},
					Method:             lock.Method_RemainTxnInService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.Equal(t, int32(-1), resp.RemainTxnInService.RemainTxn)
			releaseResponse(resp)
		},
	)
}

func TestRPCSendErrBackendCannotConnect(t *testing.T) {
	runRPCServerNoCloseTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			err := s.Close()
			require.NoError(t, err)
			_, err = c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
		},
	)
}

func TestRPCSendWithNotSupport(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			_, err := c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
		},
	)
}

func TestMOErrorCanHandled(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, moerr.NewDeadLockDetectedNoCtx(), cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected))
		},
	)
}

func TestRequestCanBeFilter(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			require.Equal(t, err, ctx.Err())
		},
		WithServerMessageFilter(func(r *lock.Request) bool { return false }),
	)
}

func TestLockTableBindChanged(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &lock.LockTable{ServiceID: "s1"}
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.NoError(t, err)
			require.NotNil(t, resp.NewBind)
			assert.Equal(t, lock.LockTable{ServiceID: "s1"}, *resp.NewBind)
			releaseResponse(resp)
		},
	)
}

func TestNewClientWithMOCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(testSockets[7:]))

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID:          "mock",
					LockServiceAddress: testSockets,
				},
			},
			[]metadata.TNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	var newClientFailed bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				newClientFailed = true
			}
		}()
		_, err := NewClient(morpc.Config{})
		if err != nil {
			newClientFailed = true
		}
	}()
	require.True(t, newClientFailed, "new LockService Client without a process-level cluster nor a custom cluster should fail")
	c, err := NewClient(morpc.Config{}, WithMOCluster(cluster))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func runRPCTests(
	t *testing.T,
	fn func(Client, Server),
	opts ...ServerOption) {
	reuse.RunReuseTests(func() {
		defer leaktest.AfterTest(t)()
		testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
		assert.NoError(t, os.RemoveAll(testSockets[7:]))

		runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
		cluster := clusterservice.NewMOCluster(
			nil,
			0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(
				[]metadata.CNService{
					{
						ServiceID:          "s1",
						LockServiceAddress: testSockets,
					},
					{
						ServiceID:          "s2",
						LockServiceAddress: testSockets,
					},
				},
				[]metadata.TNService{
					{
						LockServiceAddress: testSockets,
					},
				}))
		runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

		s, err := NewServer(testSockets, morpc.Config{}, opts...)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, s.Close())
		}()
		require.NoError(t, s.Start())

		c, err := NewClient(morpc.Config{})
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		fn(c, s)
	})
}

func runRPCServerNoCloseTests(
	t *testing.T,
	fn func(Client, Server),
	opts ...ServerOption) {
	defer leaktest.AfterTest(t)()
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(testSockets[7:]))

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID:          "s1",
					LockServiceAddress: testSockets,
				},
				{
					ServiceID:          "s2",
					LockServiceAddress: testSockets,
				},
			},
			[]metadata.TNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	s, err := NewServer(testSockets, morpc.Config{}, opts...)
	require.NoError(t, err)
	require.NoError(t, s.Start())

	c, err := NewClient(morpc.Config{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	fn(c, s)
}
