// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type closeFailureRPC struct {
	morpc.MethodBasedServer[*pb.Request, *pb.Response]
	err   error
	calls int
}

func (r *closeFailureRPC) Close() error { r.calls++; return r.err }

func TestShardServerCloseStopsWorkersOnRPCError(t *testing.T) {
	sentinel := errors.New("RPC close refused")
	rpc := &closeFailureRPC{err: sentinel}
	s := &server{rpc: rpc, stopper: stopper.NewStopper(t.Name())}
	// The fixture must still join its worker if the production Close misses Stop.
	t.Cleanup(s.stopper.Stop)
	entered, done := make(chan struct{}), make(chan struct{})
	require.NoError(t, s.stopper.RunTask(func(ctx context.Context) { close(entered); <-ctx.Done(); close(done) }))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("shard server worker did not start")
	}
	require.Same(t, sentinel, s.Close())
	select {
	case <-done:
	default:
		t.Fatal("shard server Close returned before worker stopped")
	}
	require.Same(t, sentinel, s.Close())
	require.Equal(t, 1, rpc.calls)
	require.ErrorIs(t, s.stopper.RunTask(func(context.Context) {}), stopper.ErrUnavailable)
}

func TestShardServerConstructorUnwind(t *testing.T) {
	runtime.RunTest(t.Name(), func(rt runtime.Runtime) {
		cluster := clusterservice.NewMOCluster(t.Name(), nil, 0, clusterservice.WithDisableRefresh())
		rt.SetGlobalVariables(runtime.ClusterService, cluster)
		defer cluster.Close()
		var owner ShardServer
		defer func() {
			if owner != nil {
				require.NoError(t, owner.Close())
			}
		}()
		sentinel := errors.New("shard option refused")
		require.PanicsWithValue(t, sentinel, func() {
			NewShardServer(Config{ServiceID: t.Name()}, rt.Logger(), func(value ShardServer) { owner = value }, func(s *server) {
				require.Same(t, s, owner)
				panic(sentinel)
			})
		})
		require.NotNil(t, owner)
		require.ErrorIs(t, owner.(*server).stopper.RunTask(func(context.Context) {}), stopper.ErrUnavailable)
	})
}
