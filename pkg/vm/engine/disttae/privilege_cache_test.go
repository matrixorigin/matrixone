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

package disttae

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeCacheVersionRejectsUnprovenCatalog(t *testing.T) {
	ctx := context.Background()
	snapshot := timestamp.Timestamp{PhysicalTime: 20}
	ctrl := gomock.NewController(t)
	cli := mock_frontend.NewMockTxnClient(ctrl)
	cli.EXPECT().GetLatestSnapshot(ctx, timestamp.Timestamp{}).Return(snapshot, nil).AnyTimes()
	e := &Engine{cli: cli, partitions: make(map[[2]uint64]*logtailreplay.Partition)}
	e.catalog.Store(cache.NewCatalog())
	e.pClient.eng = e
	e.pClient.receivedLogTailTime.ready.Store(true)
	e.pClient.subscribed = subscribedTable{eng: e, m: make(map[uint64]*subEntry)}
	workspace := &Transaction{proc: testutil.NewProc(t)}
	defer workspace.proc.Free()
	for i, name := range []string{catalog.MO_DATABASE, catalog.MO_TABLES, "mo_user", "mo_role", "mo_user_grant", "mo_role_grant", "mo_role_privs"} {
		id := uint64(i + 1)
		account := uint32(0)
		if i >= 2 {
			id += 10
			account = 7
			insertCatalogTableAt(t, e, workspace, account, catalog.MO_CATALOG_ID, id, 1, catalog.MO_CATALOG, name, types.BuildTS(5, 0))
		}
		e.pClient.subscribed.m[id] = &subEntry{dbID: catalog.MO_CATALOG_ID, state: Subscribed}
		state, done := e.GetOrCreateLatestPart(ctx, uint64(account), catalog.MO_CATALOG_ID, id).MutateState()
		state.UpdateDuration(types.TS{}, types.MaxTs())
		state.UpdateAppliedTo(types.BuildTS(10, 0))
		done()
	}
	read := func() PrivilegeCacheVersion {
		t.Helper()
		version, minimum, err := e.GetPrivilegeCacheVersion(ctx, 7, timestamp.Timestamp{})
		require.NoError(t, err)
		require.Equal(t, snapshot.Prev(), minimum)
		return version
	}
	original := read()
	require.True(t, original != (PrivilegeCacheVersion{}))
	require.True(t, original == read())
	e.pClient.subscribed.setTablePendingUpdate(1, 1, timestamp.Timestamp{PhysicalTime: 15})
	require.True(t, read() == (PrivilegeCacheVersion{}), "pending catalog application must not authorize reuse")
	e.pClient.subscribed.clearTablePendingUpdate(1, 1, timestamp.Timestamp{PhysicalTime: 15})
	require.True(t, original == read())
	e.pClient.receivedLogTailTime.ready.Store(false)
	unready, observed, err := e.GetPrivilegeCacheVersion(ctx, 7, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot, observed)
	require.True(t, unready == (PrivilegeCacheVersion{}), "reconnect admission is closed")
	e.pClient.receivedLogTailTime.ready.Store(true)
	delete(e.partitions, [2]uint64{1, 1})
	state, done := e.GetOrCreateLatestPart(ctx, 0, 1, 1).MutateState()
	state.UpdateDuration(types.TS{}, types.MaxTs())
	state.UpdateAppliedTo(types.BuildTS(10, 0))
	done()
	rebuilt := read()
	require.True(t, rebuilt != (PrivilegeCacheVersion{}))
	require.True(t, original != rebuilt, "reconstruction at the same watermark changes identity")
	state, done = e.GetOrCreateLatestPart(ctx, 0, 1, 1).MutateState()
	state.UpdateAppliedTo(types.TimestampToTS(snapshot))
	done()
	require.True(t, read() == (PrivilegeCacheVersion{}), "a future catalog cannot prove an older snapshot")
}

func TestPrivilegeCacheVersionPreservesSnapshotErrors(t *testing.T) {
	for _, failure := range []error{errors.New("snapshot unavailable"), context.Canceled, nil} {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		cli := mock_frontend.NewMockTxnClient(ctrl)
		minimum := timestamp.Timestamp{PhysicalTime: 10}
		cli.EXPECT().GetLatestSnapshot(ctx, minimum).Return(timestamp.Timestamp{}, failure)
		eng := &Engine{cli: cli}
		eng.pClient.receivedLogTailTime.ready.Store(true)
		version, observed, err := eng.GetPrivilegeCacheVersion(ctx, 7, minimum)
		if failure == nil {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, failure)
		}
		require.Equal(t, minimum, observed)
		require.Equal(t, PrivilegeCacheVersion{}, version, "an unknown snapshot cannot authorize cache reuse")
	}
}
