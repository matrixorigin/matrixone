// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package changedtables

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

type watchTopologySnapshot struct {
	state logpb.CheckerState
	err   error
}

func (s watchTopologySnapshot) GetClusterState(context.Context) (logpb.CheckerState, error) {
	return s.state, s.err
}

func TestChangedTableTNRoute(t *testing.T) {
	configured := metadata.TNShardRecord{ShardID: 1, LogShardID: 11}
	secondShard := metadata.TNShardRecord{ShardID: 2, LogShardID: 22}
	current := logpb.TNStoreInfo{
		Tick:           100,
		ServiceAddress: "current:1000",
		Shards:         []logpb.TNShardInfo{{ShardID: 1, ReplicaID: 101}},
	}
	older := logpb.TNStoreInfo{
		Tick:           99,
		ServiceAddress: "older:1000",
		Shards:         []logpb.TNShardInfo{{ShardID: 1, ReplicaID: 100}},
	}
	tied := older
	tied.Tick = current.Tick
	wrongShard := current
	wrongShard.Shards = []logpb.TNShardInfo{{ShardID: 2, ReplicaID: 201}}
	sameStore := current
	sameStore.Shards = []logpb.TNShardInfo{current.Shards[0], wrongShard.Shards[0]}
	noAddress := current
	noAddress.ServiceAddress = ""
	noReplica := current
	noReplica.Shards = []logpb.TNShardInfo{{ShardID: 1}}
	want := metadata.TNShard{TNShardRecord: configured, ReplicaID: 101, Address: current.ServiceAddress}

	for _, tc := range []struct {
		name       string
		configured []metadata.TNShardRecord
		stores     map[string]logpb.TNStoreInfo
		want       metadata.TNShard
	}{
		{name: "unconfigured", stores: map[string]logpb.TNStoreInfo{"current": current}},
		{name: "no routes", configured: []metadata.TNShardRecord{configured}},
		{name: "single route", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": current}, want: want},
		{name: "configured multiple shards with only one advertisement", configured: []metadata.TNShardRecord{configured, secondShard}, stores: map[string]logpb.TNStoreInfo{"current": current}},
		{name: "multiple shards on one TN", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": sameStore}},
		{name: "mismatched shard on another TN", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": current, "wrong": wrongShard}},
		{name: "replica replaced on same logical shard", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": current, "older": older}, want: want},
		{name: "ambiguous latest tick", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": current, "tied": tied}},
		{name: "latest advertisement has no address", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": noAddress, "older": older}},
		{name: "latest advertisement has no replica", configured: []metadata.TNShardRecord{configured}, stores: map[string]logpb.TNStoreInfo{"current": noReplica, "older": older}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keeper := watchTopologySnapshot{state: logpb.CheckerState{
				ClusterInfo: logpb.ClusterInfo{TNShards: tc.configured},
				TNState:     logpb.TNState{Stores: tc.stores},
			}}
			target, err := changedTableTN(t.Context(), keeper)
			if tc.want.ShardID != 0 {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			}
			require.Equal(t, tc.want, target)
		})
	}
}

func TestChangedTableTNTopologyUnavailable(t *testing.T) {
	_, err := changedTableTN(t.Context(), nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)

	unavailable := errors.New("HAKeeper read failed")
	target, err := changedTableTN(t.Context(), watchTopologySnapshot{err: unavailable})
	require.ErrorIs(t, err, unavailable)
	require.Zero(t, target)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	target, err = changedTableTN(ctx, watchTopologySnapshot{})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, target)
}
