// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"errors"
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/require"
)

type optimizerStatsStoreStub struct {
	stats      *pb.StatsInfo
	refreshErr error
	key        pb.StatsInfoKey
	mode       string
	getCalled  bool
	getSync    bool
}

func (s *optimizerStatsStoreStub) RefreshWithMode(_ context.Context, key pb.StatsInfoKey, mode string) error {
	s.key = key
	s.mode = mode
	return s.refreshErr
}

func (s *optimizerStatsStoreStub) Get(_ context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	s.getCalled = true
	s.key = key
	s.getSync = sync
	return s.stats
}

func TestRefreshTableStatsDefinesPublicationBoundary(t *testing.T) {
	key := pb.StatsInfoKey{TableID: 42, DbName: "db", TableName: "events"}
	t.Run("success", func(t *testing.T) {
		fresh := &pb.StatsInfo{TableCnt: 1_000_000}
		store := &optimizerStatsStoreStub{stats: fresh}

		got, err := refreshTableStats(context.Background(), key, store)
		require.NoError(t, err)
		require.Same(t, fresh, got)
		require.Equal(t, key, store.key)
		require.Equal(t, "auto", store.mode)
		require.True(t, store.getCalled)
		require.False(t, store.getSync)
	})

	t.Run("refresh failure is not published", func(t *testing.T) {
		wantErr := errors.New("refresh failed")
		store := &optimizerStatsStoreStub{refreshErr: wantErr}

		got, err := refreshTableStats(context.Background(), key, store)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
		require.False(t, store.getCalled)
	})
}

func TestOptimizerStatsRefreshAdmissionIsTableScopedAndCancelable(t *testing.T) {
	gs := &GlobalStats{}
	gs.initStatsRefreshAdmission()
	key := pb.StatsInfoKey{AccId: 1, TableID: 42}

	release, err := gs.acquireStatsRefresh(context.Background(), key)
	require.NoError(t, err)
	defer release()

	otherKey := key
	otherKey.TableID++
	releaseOther, err := gs.acquireStatsRefresh(context.Background(), otherKey)
	require.NoError(t, err, "unrelated tables must not queue behind this table")
	releaseOther()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	releaseCanceled, err := gs.acquireStatsRefresh(canceled, key)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, releaseCanceled)
}

func TestCoordinateStatsUpdateCancellationReleasesUpdateGeneration(t *testing.T) {
	gs := &GlobalStats{}
	gs.initStatsRefreshAdmission()
	gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	key := pb.StatsInfoKey{AccId: 1, TableID: 42}

	release, err := gs.acquireStatsRefresh(context.Background(), key)
	require.NoError(t, err)
	defer release()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	gs.coordinateStatsUpdate(pb.StatsInfoKeyWithContext{Ctx: canceled, Key: key})

	gs.updatingMu.Lock()
	record := gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.NotNil(t, record)
	require.False(t, record.inProgress,
		"cancellation while waiting for refresh admission must close the update generation")
}
