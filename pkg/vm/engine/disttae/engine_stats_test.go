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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type optimizerStatsStoreStub struct {
	stats      *pb.StatsInfo
	refreshErr error
	key        pb.StatsInfoKey
	mode       string
	options    engine.StatsRefreshOptions
}

func (s *optimizerStatsStoreStub) refreshStatsWithMode(
	_ context.Context,
	key pb.StatsInfoKey,
	mode string,
	options engine.StatsRefreshOptions,
) (*pb.StatsInfo, error) {
	s.key = key
	s.mode = mode
	s.options = options
	return s.stats, s.refreshErr
}

func TestRefreshTableStatsDefinesPublicationBoundary(t *testing.T) {
	key := pb.StatsInfoKey{TableID: 42, DbName: "db", TableName: "events"}
	t.Run("success", func(t *testing.T) {
		fresh := &pb.StatsInfo{TableCnt: 1_000_000}
		store := &optimizerStatsStoreStub{stats: fresh}
		options := engine.StatsRefreshOptions{ColumnNDVs: map[string]float64{"url": 900_000}}

		got, err := refreshTableStats(context.Background(), key, options, store)
		require.NoError(t, err)
		require.Same(t, fresh, got)
		require.Equal(t, key, store.key)
		require.Equal(t, "auto", store.mode)
		require.Equal(t, options, store.options)
	})

	t.Run("refresh failure is not published", func(t *testing.T) {
		wantErr := errors.New("refresh failed")
		store := &optimizerStatsStoreStub{refreshErr: wantErr}

		got, err := refreshTableStats(context.Background(), key, engine.StatsRefreshOptions{}, store)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
	})
}

func TestApplyStatsRefreshOptions(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name:    "events",
		Version: 7,
		Cols:    []*planpb.ColDef{{Name: "url"}, {Name: "kind"}, {Name: "__mo_fake_pk_col", Hidden: true}},
	}

	t.Run("table-wide row count and NDV replace missing metadata atomically", func(t *testing.T) {
		rowCount := float64(4)
		stats := &pb.StatsInfo{}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			TableRowCount:   &rowCount,
			ColumnNDVs:      map[string]float64{"url": 3, "kind": 2},
		})
		require.NoError(t, err)
		require.Equal(t, rowCount, stats.TableCnt)
		require.Equal(t, map[string]float64{
			"url": 3, "kind": 2, "__mo_fake_pk_col": 4,
		}, stats.NdvMap)
		require.Equal(t, tableDef.Name, stats.TableName)
	})

	t.Run("empty table remains a completed usable observation", func(t *testing.T) {
		rowCount := float64(0)
		stats := &pb.StatsInfo{}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			TableRowCount:   &rowCount,
			ColumnNDVs:      map[string]float64{"url": 0},
		})
		require.NoError(t, err)
		require.Zero(t, stats.TableCnt)
		require.Equal(t, tableDef.Name, stats.TableName)
		require.True(t, plan2.StatsInfoUsable(stats))
	})

	t.Run("selected NDV is capped and unselected metadata is retained", func(t *testing.T) {
		stats := &pb.StatsInfo{
			TableCnt: 1_000,
			NdvMap:   map[string]float64{"url": 10, "kind": 7},
		}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			ColumnNDVs:      map[string]float64{"url": 1_100},
		})
		require.NoError(t, err)
		require.Equal(t, float64(1_000), stats.NdvMap["url"])
		require.Equal(t, float64(7), stats.NdvMap["kind"])
	})

	t.Run("exact row count restores retained count bounds", func(t *testing.T) {
		rowCount := float64(4)
		stats := &pb.StatsInfo{
			TableCnt:   1_000,
			NdvMap:     map[string]float64{"url": 900, "kind": 7},
			NullCntMap: map[string]uint64{"url": 800, "kind": 3},
		}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			TableRowCount:   &rowCount,
			ColumnNDVs:      map[string]float64{"url": 3},
		})
		require.NoError(t, err)
		require.Equal(t, rowCount, stats.TableCnt)
		require.Equal(t, map[string]float64{
			"url": 3, "kind": 4, "__mo_fake_pk_col": 4,
		}, stats.NdvMap)
		require.Equal(t, map[string]uint64{"url": 4, "kind": 3}, stats.NullCntMap)
	})

	t.Run("schema replacement is rejected atomically", func(t *testing.T) {
		stats := &pb.StatsInfo{TableCnt: 100, NdvMap: map[string]float64{"kind": 7}}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(6),
			ColumnNDVs:      map[string]float64{"kind": 8},
		})
		require.Error(t, err)
		require.Equal(t, float64(100), stats.TableCnt)
		require.Equal(t, map[string]float64{"kind": 7}, stats.NdvMap)
	})

	t.Run("invalid catalog column is an error rather than a panic", func(t *testing.T) {
		stats := &pb.StatsInfo{TableCnt: 100}
		err := applyStatsRefreshOptions(stats, &planpb.TableDef{
			Name: "broken", Cols: []*planpb.ColDef{nil},
		}, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(0),
			ColumnNDVs:      map[string]float64{"kind": 8},
		})
		require.Error(t, err)
	})

	t.Run("missing schema version is rejected", func(t *testing.T) {
		stats := &pb.StatsInfo{TableCnt: 100}
		err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
			ColumnNDVs: map[string]float64{"kind": 8},
		})
		require.ErrorContains(t, err, "without its schema version")
	})

	t.Run("missing candidate statistics is rejected", func(t *testing.T) {
		err := applyStatsRefreshOptions(nil, tableDef, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			ColumnNDVs:      map[string]float64{"kind": 8},
		})
		require.ErrorContains(t, err, "without table statistics")
	})

	t.Run("missing table definition is rejected", func(t *testing.T) {
		err := applyStatsRefreshOptions(&pb.StatsInfo{}, nil, engine.StatsRefreshOptions{
			TableDefVersion: uint32Pointer(7),
			ColumnNDVs:      map[string]float64{"kind": 8},
		})
		require.ErrorContains(t, err, "without table statistics")
	})

	for _, test := range []struct {
		name     string
		column   string
		ndv      float64
		rowCount *float64
	}{
		{name: "unknown column", column: "missing", ndv: 1},
		{name: "negative", column: "url", ndv: -1},
		{name: "NaN", column: "url", ndv: math.NaN()},
		{name: "positive infinity", column: "url", ndv: math.Inf(1)},
		{name: "negative row count", column: "url", ndv: 1, rowCount: float64Pointer(-1)},
		{name: "NaN row count", column: "url", ndv: 1, rowCount: float64Pointer(math.NaN())},
		{name: "fractional row count", column: "url", ndv: 1, rowCount: float64Pointer(1.5)},
		{name: "infinite row count", column: "url", ndv: 1, rowCount: float64Pointer(math.Inf(1))},
	} {
		t.Run(test.name+" is rejected atomically", func(t *testing.T) {
			stats := &pb.StatsInfo{TableCnt: 100, NdvMap: map[string]float64{"kind": 7}}
			err := applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
				TableDefVersion: uint32Pointer(7),
				TableRowCount:   test.rowCount,
				ColumnNDVs:      map[string]float64{"kind": 8, test.column: test.ndv},
			})
			require.Error(t, err)
			require.Equal(t, map[string]float64{"kind": 7}, stats.NdvMap)
		})
	}
}

func TestVersionedStatsPublicationLinearizesWithCatalogChange(t *testing.T) {
	keyWithoutOwner := pb.StatsInfoKey{DatabaseID: 1, TableID: 2}
	published, err := (&GlobalStats{}).publishStatsForGenerationAtTableVersion(
		context.Background(), keyWithoutOwner, nil, nil, 0,
	)
	require.ErrorContains(t, err, "without an engine")
	require.False(t, published)

	published, err = (&GlobalStats{engine: &Engine{}}).publishStatsForGenerationAtTableVersion(
		context.Background(), keyWithoutOwner, nil, nil, 0,
	)
	require.ErrorContains(t, err, "without a catalog cache")
	require.False(t, published)

	runTest(t, func(ctx context.Context, e *Engine) {
		const (
			databaseID = uint64(1000)
			tableID    = uint64(1001)
		)
		insertTable(t, e, databaseID, tableID, "db", "events")
		key := pb.StatsInfoKey{
			DatabaseID: databaseID,
			TableID:    tableID,
			DbName:     "db",
			TableName:  "events",
		}
		generation := e.globalStats.currentOrCreateUpdateRecord(key)

		alterTuple, err := catalog.GenCreateTableTuple(catalog.Table{
			AccountId:    key.AccId,
			DatabaseId:   databaseID,
			TableId:      tableID,
			DatabaseName: key.DbName,
			TableName:    key.TableName,
			Version:      1,
		}, e.mp, types.NewPacker())
		require.NoError(t, err)
		_, err = fillRandomRowidAndZeroTs(alterTuple, e.mp)
		require.NoError(t, err)
		defer alterTuple.Clean(e.mp)

		type validationPoint struct {
			key     pb.StatsInfoKey
			version uint32
		}
		validated := make(chan validationPoint, 1)
		allowPublish := make(chan struct{})
		e.globalStats.beforeVersionedStatsPublish = func(gotKey pb.StatsInfoKey, version uint32) {
			validated <- validationPoint{key: gotKey, version: version}
			<-allowPublish
		}

		type publishResult struct {
			published bool
			err       error
		}
		firstStats := &pb.StatsInfo{TableCnt: 10}
		publishDone := make(chan publishResult, 1)
		go func() {
			published, err := e.globalStats.publishStatsForGenerationAtTableVersion(
				context.Background(), key, generation, firstStats, 0,
			)
			publishDone <- publishResult{published: published, err: err}
		}()
		point := <-validated
		require.Equal(t, key, point.key)
		require.Equal(t, uint32(0), point.version)

		alterAttempted := make(chan struct{})
		alterDone := make(chan struct{})
		go func() {
			close(alterAttempted)
			e.GetLatestCatalogCache().InsertTable(alterTuple)
			close(alterDone)
		}()
		<-alterAttempted
		select {
		case <-alterDone:
			close(allowPublish)
			t.Fatal("ALTER crossed the schema-validation/publication boundary")
		default:
		}

		close(allowPublish)
		result := <-publishDone
		require.NoError(t, result.err)
		require.True(t, result.published)
		<-alterDone
		e.globalStats.beforeVersionedStatsPublish = nil
		require.Equal(t, uint32(1), e.GetLatestCatalogCache().GetTableById(
			key.AccId, key.DatabaseID, key.TableID).Version)

		staleStats := &pb.StatsInfo{TableCnt: 99}
		published, err := e.globalStats.publishStatsForGenerationAtTableVersion(
			context.Background(), key, generation, staleStats, 0,
		)
		require.ErrorContains(t, err, "current version 1")
		require.False(t, published)
		e.globalStats.mu.Lock()
		require.Same(t, firstStats, e.globalStats.mu.statsInfoMap[key])
		require.Equal(t, uint32(0), e.globalStats.mu.tableDefVersions[key])
		e.globalStats.mu.Unlock()
		require.Same(t, firstStats,
			e.globalStats.GetAtTableVersion(ctx, key, false, 0))
		require.Nil(t, e.globalStats.GetAtTableVersion(ctx, key, false, 1),
			"a plan for the altered schema must not consume the old observation")
		subscribed := false
		e.globalStats.beforeSubscribeTable = func(pb.StatsInfoKey) { subscribed = true }
		require.Same(t, firstStats, e.globalStats.Get(ctx, key, false),
			"a local diagnostic reader may inspect the published observation")
		require.Nil(t, e.StatsForRemote(ctx, key),
			"a remote reader cannot safely export a bound observation")
		require.False(t, subscribed,
			"a non-blocking incompatible read must fail before subscription or remote I/O")
		e.globalStats.beforeSubscribeTable = nil

		missingKey := key
		missingKey.TableID++
		published, err = e.globalStats.publishStatsForGenerationAtTableVersion(
			context.Background(), missingKey, generation, staleStats, 0,
		)
		require.ErrorContains(t, err, "no longer exists")
		require.False(t, published)

		published, err = e.globalStats.publishStatsForGenerationAtTableVersion(
			context.Background(), key, nil, nil, 1,
		)
		require.NoError(t, err)
		require.False(t, published)

		secondStats := &pb.StatsInfo{TableCnt: 20}
		published, err = e.globalStats.publishStatsForGenerationAtTableVersion(
			context.Background(), key, generation, secondStats, 1,
		)
		require.NoError(t, err)
		require.True(t, published)
		require.Same(t, secondStats,
			e.globalStats.GetAtTableVersion(ctx, key, false, 1))
		require.Nil(t, e.globalStats.GetAtTableVersion(ctx, key, false, 0),
			"an old snapshot must not consume the replacement observation")

		t.Run("cancellation at final publication fence", func(t *testing.T) {
			requestCtx, cancelRequest := context.WithCancelCause(context.Background())
			defer cancelRequest(nil)
			wantCancellation := errors.New("cancel statistics publication")
			publicationReached := make(chan validationPoint, 1)
			allowPublication := make(chan struct{})
			var releasePublicationOnce sync.Once
			releasePublication := func() {
				releasePublicationOnce.Do(func() { close(allowPublication) })
			}
			t.Cleanup(releasePublication)
			e.globalStats.beforeVersionedStatsPublish = func(gotKey pb.StatsInfoKey, version uint32) {
				publicationReached <- validationPoint{key: gotKey, version: version}
				<-allowPublication
			}
			canceledStats := &pb.StatsInfo{TableCnt: 30}
			canceledPublishDone := make(chan publishResult, 1)
			go func() {
				published, err := e.globalStats.publishStatsForGenerationAtTableVersion(
					requestCtx, key, generation, canceledStats, 1,
				)
				canceledPublishDone <- publishResult{published: published, err: err}
			}()
			var cancellationPoint validationPoint
			select {
			case cancellationPoint = <-publicationReached:
			case <-time.After(5 * time.Second):
				t.Fatal("statistics publication did not reach the final version fence")
			}
			require.Equal(t, key, cancellationPoint.key)
			require.Equal(t, uint32(1), cancellationPoint.version)
			cancelRequest(wantCancellation)
			releasePublication()
			var canceledResult publishResult
			select {
			case canceledResult = <-canceledPublishDone:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled statistics publication did not finish")
			}
			require.ErrorIs(t, canceledResult.err, wantCancellation)
			require.False(t, canceledResult.published)
			e.globalStats.beforeVersionedStatsPublish = nil
			e.globalStats.mu.Lock()
			require.Same(t, secondStats, e.globalStats.mu.statsInfoMap[key],
				"cancellation before the final cache swap must preserve last-good statistics")
			require.Equal(t, uint32(1), e.globalStats.mu.tableDefVersions[key],
				"cancellation before the final cache swap must preserve the schema binding")
			e.globalStats.mu.Unlock()
		})
	})
}

func float64Pointer(value float64) *float64 {
	return &value
}

func uint32Pointer(value uint32) *uint32 {
	return &value
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

func TestOptimizerStatsRefreshAdmissionRejectsStoppedOwner(t *testing.T) {
	ownerCtx, stopOwner := context.WithCancel(context.Background())
	stopOwner()
	gs := &GlobalStats{ctx: ownerCtx}
	gs.initStatsRefreshAdmission()

	release, err := gs.acquireStatsRefresh(
		context.Background(), pb.StatsInfoKey{AccId: 1, TableID: 42})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, release,
		"a stopped GlobalStats owner must not transfer a refresh admission token")
}

func TestStatsRefreshContextPreservesRequestAndObservesOwnerStop(t *testing.T) {
	type requestValueKey struct{}
	ownerCtx, stopOwner := context.WithCancel(context.Background())
	requestCtx := context.WithValue(context.Background(), requestValueKey{}, "request-value")
	gs := &GlobalStats{ctx: ownerCtx}

	refreshCtx, stopRefresh, err := gs.newStatsRefreshContext(requestCtx)
	require.NoError(t, err)
	t.Cleanup(stopRefresh)
	require.Equal(t, "request-value", refreshCtx.Value(requestValueKey{}))

	stopOwner()
	select {
	case <-refreshCtx.Done():
		require.ErrorIs(t, context.Cause(refreshCtx), context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("owner shutdown did not cancel downstream refresh work")
	}
}

func TestStatsRefreshContextClosesOwnerWatcherRegistrationRace(t *testing.T) {
	ownerCtx := newDelayedLifecycleContext()
	ownerCtx.cancelOnRegister = true
	gs := &GlobalStats{ctx: ownerCtx}

	refreshCtx, stopRefresh, err := gs.newStatsRefreshContext(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, refreshCtx)
	require.Nil(t, stopRefresh)
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

func TestCompleteAutomaticStatsRefreshKeepsMetadataInsideAdmission(t *testing.T) {
	gs := &GlobalStats{}
	gs.initStatsRefreshAdmission()
	gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	gs.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	gs.mu.cond = sync.NewCond(&gs.mu)
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
	gs.completeAutomaticStatsRefresh(
		key, generation, &pb.StatsInfo{}, true, 1, 1.0, func() {
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

	stats, err := gs.refreshStatsWithMode(context.Background(), key, "auto", engine.StatsRefreshOptions{})
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
