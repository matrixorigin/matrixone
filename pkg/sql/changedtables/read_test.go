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
	currentRoute := metadata.TNShard{TNShardRecord: configured, ReplicaID: 101, Address: current.ServiceAddress}
	olderRoute := metadata.TNShard{TNShardRecord: configured, ReplicaID: 100, Address: older.ServiceAddress}
	singleShard := []metadata.TNShardRecord{configured}

	for _, tc := range []struct {
		name        string
		configured  []metadata.TNShardRecord
		stores      map[string]logpb.TNStoreInfo
		want        metadata.TNShard
		recheckCode uint16
	}{
		{name: "unconfigured", stores: map[string]logpb.TNStoreInfo{"current": current}, recheckCode: moerr.ErrNotSupported},
		{name: "no routes", configured: singleShard, recheckCode: moerr.ErrChangeWatchRetry},
		{name: "single route", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": current}, want: currentRoute},
		{name: "configured multiple shards with only one advertisement", configured: []metadata.TNShardRecord{configured, secondShard}, stores: map[string]logpb.TNStoreInfo{"current": current},
			recheckCode: moerr.ErrNotSupported},
		{name: "multiple shards on one TN", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": sameStore}, recheckCode: moerr.ErrNotSupported},
		{name: "mismatched shard on another TN", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": current, "wrong": wrongShard},
			recheckCode: moerr.ErrNotSupported},
		{name: "replica replaced on same logical shard", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": current, "older": older}, want: currentRoute},
		{name: "another replica becomes the sole route", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"older": older},
			want: olderRoute, recheckCode: moerr.ErrChangeWatchRetry},
		{name: "ambiguous latest tick", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": current, "tied": tied},
			recheckCode: moerr.ErrChangeWatchRetry},
		{name: "latest advertisement has no address", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": noAddress, "older": older},
			recheckCode: moerr.ErrChangeWatchRetry},
		{name: "latest advertisement has no replica", configured: singleShard, stores: map[string]logpb.TNStoreInfo{"current": noReplica, "older": older},
			recheckCode: moerr.ErrChangeWatchRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keeper := watchTopologySnapshot{state: logpb.CheckerState{
				ClusterInfo: logpb.ClusterInfo{TNShards: tc.configured},
				TNState:     logpb.TNState{Stores: tc.stores},
			}}
			target, err := changedTableTN(t.Context(), keeper, nil)
			if tc.want.ShardID != 0 {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			}
			require.Equal(t, tc.want, target)

			rechecked, err := changedTableTN(t.Context(), keeper, &currentRoute)
			if tc.recheckCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.recheckCode), "expected %d, got %v", tc.recheckCode, err)
				require.Zero(t, rechecked)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, rechecked)
			}
		})
	}
}

func TestChangedTableTNTopologyUnavailable(t *testing.T) {
	prior := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 1, LogShardID: 11},
		ReplicaID:     101,
		Address:       "current:1000",
	}
	for _, tc := range []struct {
		name     string
		previous *metadata.TNShard
	}{
		{name: "initial"},
		{name: "recheck", previous: &prior},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := changedTableTN(t.Context(), nil, tc.previous)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)

			unavailable := errors.New("HAKeeper read failed")
			target, err := changedTableTN(t.Context(), watchTopologySnapshot{err: unavailable}, tc.previous)
			require.ErrorIs(t, err, unavailable)
			require.Zero(t, target)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			target, err = changedTableTN(ctx, watchTopologySnapshot{}, tc.previous)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, target)
		})
	}
}
