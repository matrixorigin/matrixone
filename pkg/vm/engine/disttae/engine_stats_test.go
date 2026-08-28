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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

type optimizerStatsStoreStub struct {
	stats      *pb.StatsInfo
	refreshErr error
	key        pb.StatsInfoKey
	mode       string
}

func (s *optimizerStatsStoreStub) refreshStatsWithMode(
	_ context.Context,
	key pb.StatsInfoKey,
	mode string,
) (*pb.StatsInfo, error) {
	s.key = key
	s.mode = mode
	return s.stats, s.refreshErr
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
	})

	t.Run("refresh failure is not published", func(t *testing.T) {
		wantErr := errors.New("refresh failed")
		store := &optimizerStatsStoreStub{refreshErr: wantErr}

		got, err := refreshTableStats(context.Background(), key, store)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
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
	generation := gs.currentOrCreateUpdateRecord(key)
	gs.coordinateStatsUpdateJob(statsUpdateJob{
		wrapKey:        pb.StatsInfoKeyWithContext{Ctx: canceled, Key: key},
		expectedRecord: generation,
	})

	gs.updatingMu.Lock()
	record := gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.NotNil(t, record)
	require.False(t, record.inProgress,
		"cancellation while waiting for refresh admission must close the update generation")
}

func TestCompleteStatsRefreshKeepsMetadataInsideAdmission(t *testing.T) {
	gs := &GlobalStats{}
	gs.initStatsRefreshAdmission()
	gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	key := pb.StatsInfoKey{AccId: 1, TableID: 42}

	generation := &updateRecord{inProgress: true}
	gs.updatingMu.updating[key] = generation
	oldRelease, err := gs.acquireStatsRefresh(context.Background(), key)
	require.NoError(t, err)

	newerDone := make(chan error, 1)
	go func() {
		newRelease, acquireErr := gs.acquireStatsRefresh(context.Background(), key)
		if acquireErr != nil {
			newerDone <- acquireErr
			return
		}
		gs.markAutomaticUpdateComplete(key, generation, true, 100, 0.5)
		newRelease()
		newerDone <- nil
	}()

	// Waiting here after releasing forces the newer refresh to commit between
	// release and any code that might incorrectly update the old baseline late.
	var newerErr error
	gs.completeStatsRefresh(key, generation, true, 1, 1.0, func() {
		oldRelease()
		newerErr = <-newerDone
	})
	require.NoError(t, newerErr)

	gs.updatingMu.Lock()
	record := *gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.False(t, record.inProgress)
	require.Equal(t, int64(100), record.baseObjectCount)
	require.Equal(t, 0.5, record.samplingRatio)
}

func TestCoordinateStatsUpdateSubscribeFailurePreservesLastPublishedStats(t *testing.T) {
	key := pb.StatsInfoKey{
		AccId: 1, DatabaseID: 10, TableID: 42, DbName: "db", TableName: "events",
	}
	newStats := func() *pb.StatsInfo {
		return &pb.StatsInfo{TableCnt: 1_000_000}
	}
	newGlobalStats := func() *GlobalStats {
		gs := &GlobalStats{engine: &Engine{}}
		gs.initStatsRefreshAdmission()
		gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
		gs.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
		gs.mu.cond = sync.NewCond(&gs.mu)
		return gs
	}

	fault.Enable()
	t.Cleanup(func() { fault.Disable() })
	removeFault, err := objectio.InjectLogging(
		objectio.FJ_CNSubscribeTableFail, key.DbName, key.TableName, 0, true,
	)
	require.NoError(t, err)
	t.Cleanup(removeFault)

	t.Run("retain last successful publication", func(t *testing.T) {
		gs := newGlobalStats()
		lastGood := newStats()
		gs.mu.statsInfoMap[key] = lastGood

		generation := gs.currentOrCreateUpdateRecord(key)
		gs.coordinateStatsUpdateJob(statsUpdateJob{
			wrapKey:        pb.StatsInfoKeyWithContext{Ctx: context.Background(), Key: key},
			expectedRecord: generation,
		})

		gs.mu.Lock()
		got, exists := gs.mu.statsInfoMap[key]
		gs.mu.Unlock()
		require.True(t, exists)
		require.Same(t, lastGood, got,
			"a failed automatic refresh must not erase the last successful publication")
	})

	t.Run("complete first failed generation", func(t *testing.T) {
		gs := newGlobalStats()

		generation := gs.currentOrCreateUpdateRecord(key)
		gs.coordinateStatsUpdateJob(statsUpdateJob{
			wrapKey:        pb.StatsInfoKeyWithContext{Ctx: context.Background(), Key: key},
			expectedRecord: generation,
		})

		gs.mu.Lock()
		got, exists := gs.mu.statsInfoMap[key]
		gs.mu.Unlock()
		require.True(t, exists,
			"the first failed automatic generation must still wake synchronous waiters")
		require.Nil(t, got)
	})
}

func TestExplicitStatsRefreshSubscribeFailureDoesNotRetainGeneration(t *testing.T) {
	key := pb.StatsInfoKey{
		AccId: 1, DatabaseID: 10, TableID: 42, DbName: "db", TableName: "events",
	}
	gs := &GlobalStats{engine: &Engine{}}
	gs.initStatsRefreshAdmission()
	gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)

	fault.Enable()
	t.Cleanup(func() { fault.Disable() })
	removeFault, err := objectio.InjectLogging(
		objectio.FJ_CNSubscribeTableFail, key.DbName, key.TableName, 0, true,
	)
	require.NoError(t, err)
	t.Cleanup(removeFault)

	stats, err := gs.refreshStatsWithMode(context.Background(), key, "auto")
	require.Error(t, err)
	require.Nil(t, stats)

	gs.updatingMu.Lock()
	_, retained := gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.False(t, retained,
		"a failed subscription has no cleanup owner and must not create a generation")
}

func TestInitialStatsGetSubscribeFailureDoesNotQueueOwnerlessGeneration(t *testing.T) {
	key := pb.StatsInfoKey{
		AccId: 1, DatabaseID: 10, TableID: 42, DbName: "db", TableName: "events",
	}
	gs := &GlobalStats{
		engine:       &Engine{},
		updateC:      make(chan statsUpdateJob, 1),
		queueWatcher: newQueueWatcher(),
	}
	gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	gs.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	gs.mu.cond = sync.NewCond(&gs.mu)

	fault.Enable()
	t.Cleanup(func() { fault.Disable() })
	removeFault, err := objectio.InjectLogging(
		objectio.FJ_CNSubscribeTableFail, key.DbName, key.TableName, 0, true,
	)
	require.NoError(t, err)
	t.Cleanup(removeFault)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan *pb.StatsInfo, 1)
	go func() { result <- gs.Get(ctx, key, true) }()

	select {
	case got := <-result:
		require.Nil(t, got)
	case <-gs.updateC:
		cancel()
		<-result
		t.Fatal("a failed initial subscription queued work without a cleanup owner")
	case <-time.After(time.Second):
		cancel()
		t.Fatal("stats get did not terminate after subscription failure")
	}

	gs.updatingMu.Lock()
	_, retained := gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.False(t, retained)
	require.Empty(t, gs.updateC)
}
