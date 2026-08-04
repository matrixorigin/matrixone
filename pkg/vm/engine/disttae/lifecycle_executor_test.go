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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioio "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestLifecycleCutoffUsesEvaluationTimezoneAndGrace(t *testing.T) {
	evaluation := time.Date(2026, 7, 31, 22, 0, 0, 0, time.UTC)
	cutoff, encoded, err := lifecycleCutoff(
		evaluation,
		90,
		2,
		"Asia/Shanghai",
		types.T_date,
	)
	require.NoError(t, err)
	require.Equal(t, "2026-05-01T06:00:00+08:00", cutoff.Format(time.RFC3339))
	require.Equal(t, int64(types.DateFromCalendar(2026, 5, 1)), encoded)
}

func TestLifecycleObjectExpirationUsesOnlyLifecycleSortKeyProof(t *testing.T) {
	stats := objectio.NewObjectStats()
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(100))
	zoneMap.Update(types.Timestamp(200))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zoneMap))

	whole, skip := lifecycleObjectExpirationByZoneMap(
		*stats,
		0,
		0,
		types.T_timestamp,
		201,
	)
	require.True(t, whole)
	require.False(t, skip)

	whole, skip = lifecycleObjectExpirationByZoneMap(
		*stats,
		0,
		0,
		types.T_timestamp,
		100,
	)
	require.False(t, whole)
	require.True(t, skip)

	whole, skip = lifecycleObjectExpirationByZoneMap(
		*stats,
		1,
		0,
		types.T_timestamp,
		201,
	)
	require.False(t, whole)
	require.False(t, skip)
}

func TestResolveLifecycleTAEFileServiceRoutesUnqualifiedObjectIO(t *testing.T) {
	ctx := context.Background()
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	fileServices, err := fileservice.NewFileServices("", shared)
	require.NoError(t, err)
	defer fileServices.Close(ctx)
	require.Empty(t, fileServices.Name())

	taeFS, err := resolveLifecycleTAEFileService(fileServices)
	require.NoError(t, err)
	require.Equal(t, defines.SharedFileServiceName, taeFS.Name())

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixed(
		value.Vecs[0], types.Timestamp(100), false, mp,
	))
	value.SetRowCount(1)
	defer value.Clean(mp)

	writer := objectioio.ConstructWriter(
		0,
		[]uint16{0},
		-1,
		false,
		false,
		taeFS,
	)
	_, err = writer.WriteBatch(value)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, blocks)
	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(
		&stats,
		uint32(len(blocks)),
	))

	// This is the production failure mode: an unqualified TAE Object name
	// cannot be routed through the aggregate whose default service is empty.
	_, err = fileServices.StatFile(ctx, stats.ObjectLocation().Name().String())
	require.ErrorContains(t, err, "service  not found")

	zoneMap, err := loadLifecycleObjectColumnZoneMap(ctx, taeFS, stats, 0)
	require.NoError(t, err)
	require.True(t, zoneMap.IsInited())
	require.NoError(t, (lifecyclepkg.SQLSyncProtectionClient{
		FileService: taeFS,
	}).StatExact(ctx, []objectio.ObjectStats{stats}))
}

func TestClassifyLifecycleDiscoveryPageLoadsNonSortKeyZoneMap(t *testing.T) {
	source := lifecyclePlanTestSourceWithMeta(t, 128<<20, 32, 64)
	snapshot := types.BuildTS(100, 1)
	next := lifecyclepkg.DiscoveryCursor{Snapshot: snapshot, Wrapped: true}
	completedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	page := lifecyclepkg.DiscoveryPage{
		Candidates: []lifecyclepkg.Candidate{{
			Snapshot: snapshot,
			Source:   source,
		}},
		Next:                next,
		MetaBytes:           64,
		CompletedFullScanAt: completedAt,
	}
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(100))
	zoneMap.Update(types.Timestamp(200))
	loads := 0

	inputs, gotNext, gotCompletedAt, err := classifyLifecycleDiscoveryPage(
		context.Background(),
		page,
		0,
		1,
		7,
		types.T_timestamp,
		201,
		1024,
		func(
			_ context.Context,
			_ objectio.ObjectStats,
			seqnum uint16,
		) (objectio.ZoneMap, error) {
			loads++
			require.Equal(t, uint16(7), seqnum)
			return zoneMap, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, loads)
	require.Len(t, inputs, 1)
	require.True(t, inputs[0].Whole)
	require.Equal(t, next, gotNext)
	require.Equal(t, completedAt, gotCompletedAt)
}

func TestClassifyLifecycleDiscoveryPageStopsAtMetadataBudgetPrefix(t *testing.T) {
	first := lifecyclePlanTestSourceWithMeta(t, 128<<20, 10, 20)
	second := lifecyclePlanTestSourceWithMeta(t, 128<<20, 10, 20)
	snapshot := types.BuildTS(100, 1)
	page := lifecyclepkg.DiscoveryPage{
		Candidates: []lifecyclepkg.Candidate{
			{Snapshot: snapshot, Source: first},
			{Snapshot: snapshot, Source: second},
		},
		Next: lifecyclepkg.DiscoveryCursor{
			Snapshot:      snapshot,
			Wrapped:       true,
			HasLastObject: false,
		},
		MetaBytes:           50,
		CompletedFullScanAt: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
	}
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(100))
	loads := 0

	inputs, next, completedAt, err := classifyLifecycleDiscoveryPage(
		context.Background(),
		page,
		0,
		1,
		7,
		types.T_timestamp,
		200,
		100,
		func(
			context.Context,
			objectio.ObjectStats,
			uint16,
		) (objectio.ZoneMap, error) {
			loads++
			return zoneMap, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, loads)
	require.Len(t, inputs, 1)
	require.True(t, next.HasLastObject)
	require.Equal(t, *first.ObjectShortName(), next.LastObjectName)
	require.True(t, completedAt.IsZero())
}

func TestClassifyLifecycleDiscoveryPageRejectsOversizeMetadataBeforeLoad(t *testing.T) {
	source := lifecyclePlanTestSourceWithMeta(t, 128<<20, 32, 64)
	loads := 0

	_, _, _, err := classifyLifecycleDiscoveryPage(
		context.Background(),
		lifecyclepkg.DiscoveryPage{
			Candidates: []lifecyclepkg.Candidate{{Source: source}},
			MetaBytes:  1,
		},
		0,
		1,
		7,
		types.T_timestamp,
		200,
		100,
		func(
			context.Context,
			objectio.ObjectStats,
			uint16,
		) (objectio.ZoneMap, error) {
			loads++
			return nil, nil
		},
	)
	require.ErrorContains(t, err, "RESOURCE_BLOCKED")
	require.Zero(t, loads)
}

func TestClassifyLifecycleDiscoveryPageSortKeyDoesNotLoadMetadata(t *testing.T) {
	source := lifecyclePlanTestSourceWithMeta(t, 128<<20, 32, 64)
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(100))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		&source.ObjectStats,
		zoneMap,
	))
	snapshot := types.BuildTS(100, 1)
	next := lifecyclepkg.DiscoveryCursor{Snapshot: snapshot, Wrapped: true}
	loads := 0

	inputs, gotNext, _, err := classifyLifecycleDiscoveryPage(
		context.Background(),
		lifecyclepkg.DiscoveryPage{
			Candidates: []lifecyclepkg.Candidate{{
				Snapshot: snapshot,
				Source:   source,
			}},
			Next:      next,
			MetaBytes: 1,
		},
		0,
		0,
		7,
		types.T_timestamp,
		200,
		100,
		func(
			context.Context,
			objectio.ObjectStats,
			uint16,
		) (objectio.ZoneMap, error) {
			loads++
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Zero(t, loads)
	require.Len(t, inputs, 1)
	require.True(t, inputs[0].Whole)
	require.Equal(t, next, gotNext)
}

func TestLifecycleDiscoveryCursorTreatsCorruptionAsResettableHint(t *testing.T) {
	snapshot := types.BuildTS(123, 4)
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 7).Short()
	cursor := lifecycleDiscoveryCursor(lifecyclepkg.Binding{
		ScanSnapshotHex:       hex.EncodeToString(snapshot[:]),
		ScanLastObjectNameHex: hex.EncodeToString(name[:]),
	})
	require.Equal(t, snapshot, cursor.Snapshot)
	require.True(t, cursor.HasLastObject)
	require.Equal(t, *name, cursor.LastObjectName)

	cursor = lifecycleDiscoveryCursor(lifecyclepkg.Binding{
		ScanSnapshotHex:       "bad",
		ScanLastObjectNameHex: "bad",
	})
	require.True(t, cursor.Snapshot.IsEmpty())
	require.False(t, cursor.HasLastObject)
}

func TestLifecycleDiscoveryRequestCarriesFullScanFairness(t *testing.T) {
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	lastFullScan := now.Add(-time.Hour)
	snapshot := types.BuildTS(123, 4)
	cursor := lifecyclepkg.DiscoveryCursor{Snapshot: types.BuildTS(100, 1)}
	request := lifecycleDiscoveryRequest(
		lifecyclepkg.Binding{LastFullScanAt: lastFullScan},
		snapshot,
		now,
		cursor,
	)
	require.Equal(t, snapshot, request.Snapshot)
	require.Equal(t, now, request.Now)
	require.Equal(t, cursor, request.Cursor)
	require.Equal(t, lastFullScan, request.LastFullScanAt)
	require.Equal(t, 24*time.Hour, request.FullScanInterval)
	require.Equal(t, lifecycleDiscoveryPageObjects, request.Limits.MaxObjects)
	require.Equal(t, uint64(lifecycleDiscoveryMetaBytes), request.Limits.MaxMetaBytes)
}

func TestLifecycleRewriteSlotBoundsLocalRewriteConcurrency(t *testing.T) {
	slots := make(chan struct{}, 1)
	releaseFirst, err := tryAcquireLifecycleRewriteSlot(
		context.Background(),
		slots,
	)
	require.NoError(t, err)

	_, err = tryAcquireLifecycleRewriteSlot(context.Background(), slots)
	require.ErrorContains(t, err, "RESOURCE_BLOCKED")

	releaseFirst()
	releaseSecond, err := tryAcquireLifecycleRewriteSlot(
		context.Background(),
		slots,
	)
	require.NoError(t, err)
	releaseSecond()
}

func TestLifecycleObjectOutcomeAdvancesBindingOnlyAfterFinalCommit(t *testing.T) {
	binding := lifecyclepkg.Binding{Version: 7}

	recorded := applyLifecycleObjectOutcome(&binding, false)
	require.False(t, recorded)
	require.Equal(t, uint64(7), binding.Version)

	recorded = applyLifecycleObjectOutcome(&binding, true)
	require.True(t, recorded)
	require.Equal(t, uint64(8), binding.Version)
}

func TestLifecycleDeferredObjectErrorDoesNotBlockPage(t *testing.T) {
	require.True(t, isLifecycleDeferredObjectError(
		fmt.Errorf("MIXED_LAYOUT_BLOCKED: amplification exceeded"),
	))
	require.True(t, isLifecycleDeferredObjectError(
		fmt.Errorf("RESOURCE_BLOCKED: rewrite window exhausted"),
	))
	require.False(t, isLifecycleDeferredObjectError(
		fmt.Errorf("Lifecycle source Object identity changed"),
	))
}

func TestPlanLifecycleObjectTasksBatchesWholeAndKeepsMixedSingleton(t *testing.T) {
	wholeA := lifecyclePlanTestSource(t, 128<<20)
	wholeB := lifecyclePlanTestSource(t, 256<<20)
	mixed := lifecyclePlanTestSource(t, 512<<20)
	wholeC := lifecyclePlanTestSource(t, 64<<20)

	plans := planLifecycleObjectTasks([]lifecycleObjectPlanInput{
		{Source: wholeA, Whole: true},
		{Source: wholeB, Whole: true},
		{Source: mixed},
		{Source: wholeC, Whole: true},
	})

	require.Len(t, plans, 3)
	require.True(t, plans[0].Whole)
	require.Len(t, plans[0].Sources, 2)
	require.Equal(t, uint64(384<<20), plans[0].SourceBytes)
	require.False(t, plans[1].Whole)
	require.Len(t, plans[1].Sources, 1)
	require.Equal(t, uint64(512<<20), plans[1].SourceBytes)
	require.True(t, plans[2].Whole)
	require.Len(t, plans[2].Sources, 1)
}

func TestPlanLifecycleObjectTasksBoundsWholeSourceCountAndBytes(t *testing.T) {
	inputs := make([]lifecycleObjectPlanInput, 0, 65)
	for range 65 {
		inputs = append(inputs, lifecycleObjectPlanInput{
			Source: lifecyclePlanTestSource(t, 1),
			Whole:  true,
		})
	}
	plans := planLifecycleObjectTasks(inputs)
	require.Len(t, plans, 2)
	require.Len(t, plans[0].Sources, lifecycleWholeBatchMaxSources)
	require.Len(t, plans[1].Sources, 1)

	plans = planLifecycleObjectTasks([]lifecycleObjectPlanInput{
		{Source: lifecyclePlanTestSource(t, 3<<30), Whole: true},
		{Source: lifecyclePlanTestSource(t, 3<<30), Whole: true},
	})
	require.Len(t, plans, 2)
	require.LessOrEqual(
		t,
		plans[0].SourceBytes,
		uint64(lifecycleWholeBatchMaxSourceBytes),
	)
	require.LessOrEqual(
		t,
		plans[1].SourceBytes,
		uint64(lifecycleWholeBatchMaxSourceBytes),
	)
}

func lifecyclePlanTestSource(t *testing.T, sourceBytes uint32) objectio.ObjectEntry {
	t.Helper()
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(
		&objectID,
		false,
		true,
		false,
	)
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, sourceBytes))
	return objectio.ObjectEntry{ObjectStats: *stats}
}

func lifecyclePlanTestSourceWithMeta(
	t *testing.T,
	sourceBytes uint32,
	metaBytes uint32,
	metaLogicalBytes uint32,
) objectio.ObjectEntry {
	t.Helper()
	source := lifecyclePlanTestSource(t, sourceBytes)
	require.NoError(t, objectio.SetObjectStatsExtent(
		&source.ObjectStats,
		objectio.NewExtent(0, 0, metaBytes, metaLogicalBytes),
	))
	return source
}

func TestLifecycleCoordinatorRunSlotDoesNotQueueDuplicateRun(t *testing.T) {
	slots := make(chan struct{}, 1)
	releaseFirst, acquired := tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.True(t, acquired)

	_, acquired = tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.False(t, acquired)

	releaseFirst()
	releaseSecond, acquired := tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.True(t, acquired)
	releaseSecond()
}

func TestLifecycleCoordinatorDefaultsBoundClusterChildren(t *testing.T) {
	config := lifecycleCoordinatorConfig()
	require.Equal(t, 2, config.MaxClusterChildren)
	require.Equal(t, 1, config.MaxTableChildren)
}

func TestLifecycleCleanupRootTimeoutUsesRemainingSweepBudget(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	timeout, ok := lifecycleCleanupRootTimeout(
		now,
		now.Add(30*time.Second),
	)
	require.True(t, ok)
	require.Equal(t, 30*time.Second, timeout)

	timeout, ok = lifecycleCleanupRootTimeout(
		now,
		now.Add(5*time.Minute),
	)
	require.True(t, ok)
	require.Equal(t, lifecycleTemporaryCleanupTimeout, timeout)

	_, ok = lifecycleCleanupRootTimeout(now, now)
	require.False(t, ok)
}

func TestLifecycleCleanupPhasesReserveTimeForLaterWork(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Minute)
	require.Equal(
		t,
		now.Add(20*time.Second),
		lifecycleCleanupPhaseDeadline(now, deadline, 3),
	)
	require.Equal(
		t,
		now.Add(30*time.Second),
		lifecycleCleanupPhaseDeadline(now, deadline, 2),
	)
	require.Equal(t, deadline, lifecycleCleanupPhaseDeadline(now, deadline, 1))
}

func TestLifecycleReconcileCursorAdvancesPastAttemptedRoots(t *testing.T) {
	roots := []lifecyclepkg.CleanupRoot{
		{RootID: "01"},
		{RootID: "02"},
		{RootID: "03"},
	}
	require.Equal(t, "old", lifecycleNextReconcileCursor(
		"old", "03", roots, 0,
	))
	require.Equal(t, "02", lifecycleNextReconcileCursor(
		"old", "03", roots, 2,
	))
	require.Equal(t, "03", lifecycleNextReconcileCursor(
		"old", "03", roots, 3,
	))
}

func TestLifecycleMetadataCompactionRunsOnBoundedMaintenanceCadence(t *testing.T) {
	now := time.Date(2026, 8, 1, 0, 5, 0, 0, time.UTC)
	require.True(t, lifecycleMetadataCompactionDue(time.Time{}, now))
	require.False(t, lifecycleMetadataCompactionDue(
		now.Add(-lifecycleMetadataCompactionInterval+time.Nanosecond),
		now,
	))
	require.True(t, lifecycleMetadataCompactionDue(
		now.Add(-lifecycleMetadataCompactionInterval),
		now,
	))
}

func TestLifecycleCleanupSweepReleasesPublishedTTLTemporaryOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	now := time.Now().UTC()
	root := lifecyclepkg.CleanupRoot{
		RootID:               "2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f",
		AttemptID:            "e091026d-114b-44f9-81f3-326bf6481446",
		Mode:                 lifecyclepkg.CleanupModeTTLRewrite,
		OwnerAccountID:       17,
		LogicalTableID:       42,
		PhysicalTableID:      43,
		ExecutorEpoch:        7,
		WorkerDeadline:       now.Add(time.Minute),
		TAENamespace:         "shared/2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f/e091026d-114b-44f9-81f3-326bf6481446",
		BookingPrefix:        "shared/2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f/e091026d-114b-44f9-81f3-326bf6481446/booking",
		ReservedCleanupBytes: 1 << 20,
		SourceSetDigest:      [32]byte{2},
		State:                lifecyclepkg.CleanupRootPublished,
		StateVersion:         3,
		CleanupAfter:         now,
	}
	cleaned := root
	cleaned.TemporaryCleanupDone = true
	cleaned.StateVersion++
	step := 0
	sqlExecutor := &restoreTxnSQLExecutor{execute: func(
		sql string,
		option executor.StatementOption,
	) (executor.Result, error) {
		require.Equal(t, uint32(0), option.AccountID())
		lower := strings.ToLower(sql)
		step++
		switch step {
		case 1:
			require.Contains(t, lower, "state='published' and temporary_cleanup_done=false")
			return lifecycleExecutorCleanupRootResult(t, mp, root), nil
		case 2:
			require.Contains(t, lower, "state in ('registered','uploading','verified','finalizing','commit_unknown','published')")
			return executor.Result{Mp: mp}, nil
		case 3:
			require.Contains(t, lower, "state in ('delete_pending','deleting')")
			return executor.Result{Mp: mp}, nil
		case 4:
			require.Contains(t, lower, "temporary_cleanup_done=true")
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		case 5:
			require.Contains(t, lower, "where root_id=unhex")
			return lifecycleExecutorCleanupRootResult(t, mp, cleaned), nil
		case 6:
			require.Contains(t, lower, "set state='delete_pending'")
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		default:
			t.Fatalf("unexpected cleanup SQL %s", sql)
			return executor.Result{}, nil
		}
	}}
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { shared.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cursor, complete, err := sweepLifecycleCleanupRoots(
		ctx,
		sqlExecutor,
		shared,
		nil,
		"",
	)
	require.NoError(t, err)
	require.True(t, complete)
	require.Empty(t, cursor)
	require.Equal(t, 6, step)
}

func TestLifecycleDisabledContinuesMaintenanceAndSkipsBindingScan(t *testing.T) {
	fake := &disabledLifecycleSQLExecutor{
		t:  t,
		mp: mpool.MustNewZero(),
	}
	run := LifecycleTaskExecutorFactory(nil, nil, fake, nil, nil)
	require.NoError(t, run(context.Background(), &task.AsyncTask{}))
	require.NotEmpty(t, fake.queries)
	sawAccountPage := false
	sawMetadataCompaction := false
	for _, query := range fake.queries {
		if strings.Contains(query, "mo_account") {
			sawAccountPage = true
		}
		if strings.Contains(query, "delete from mo_catalog.mo_lifecycle_cleanup_roots") {
			sawMetadataCompaction = true
		}
		require.NotContains(t, query, "mo_lifecycle_bindings")
		require.NotContains(t, query, "mo_lifecycle_restore_attempts")
	}
	require.True(t, sawAccountPage)
	require.True(t, sawMetadataCompaction)
}

func TestLifecycleEnabledCoordinatorCompletesEmptyBindingPage(t *testing.T) {
	fake := &disabledLifecycleSQLExecutor{
		t:       t,
		mp:      mpool.MustNewZero(),
		enabled: true,
	}
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { shared.Close(context.Background()) })
	services, err := fileservice.NewFileServices("", shared)
	require.NoError(t, err)
	t.Cleanup(func() { services.Close(context.Background()) })

	run := LifecycleTaskExecutorFactory(nil, nil, fake, services, nil)
	require.NoError(t, run(context.Background(), &task.AsyncTask{}))
	sawBindingAccountPage := false
	for _, query := range fake.queries {
		if strings.Contains(query, "from mo_catalog.mo_account") &&
			strings.Contains(query, "order by account_id") {
			sawBindingAccountPage = true
		}
	}
	require.True(t, sawBindingAccountPage)
}

func TestLifecycleCoordinatorStopsAfterFirstMaintenanceFailure(t *testing.T) {
	expected := errors.New("cleanup root catalog unavailable")
	fake := &failingLifecycleSQLExecutor{
		t:   t,
		err: expected,
	}
	run := LifecycleTaskExecutorFactory(nil, nil, fake, nil, nil)
	err := run(context.Background(), &task.AsyncTask{})
	require.ErrorIs(t, err, expected)
	require.Equal(t, 1, fake.calls)
}

func TestLifecycleBindingExecutorCompletesEmptyObjectPage(t *testing.T) {
	ctrl := gomock.NewController(t)
	engineMock := mock_frontend.NewMockEngine(ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	operator := mock_frontend.NewMockTxnOperator(ctrl)
	mp := mpool.MustNewZero()
	fakeSQL := &disabledLifecycleSQLExecutor{
		t:       t,
		mp:      mp,
		enabled: true,
	}

	tableDef := &plan.TableDef{
		TblId:   43,
		Name:    "events",
		DbName:  "history",
		Version: 3,
		Cols: []*plan.ColDef{
			{
				ColId:   1,
				Name:    "id",
				Seqnum:  0,
				NotNull: true,
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
			{
				ColId:   2,
				Name:    "created_at",
				Seqnum:  1,
				NotNull: true,
				Typ: plan.Type{
					Id:          int32(types.T_timestamp),
					NotNullable: true,
				},
			},
		},
	}
	snapshot := types.BuildTS(100, 1)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	table := &emptyLifecycleRunTable{
		def: tableDef,
		page: lifecyclepkg.DiscoveryPage{
			Next: lifecyclepkg.DiscoveryCursor{
				Snapshot: snapshot,
				Wrapped:  true,
			},
			StartedFullScanAt:   now,
			CompletedFullScanAt: now,
		},
	}

	engineMock.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{})
	txnClient.EXPECT().New(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(operator, nil)
	engineMock.EXPECT().New(gomock.Any(), operator).Return(nil)
	engineMock.EXPECT().GetRelationById(
		gomock.Any(), operator, uint64(43),
	).Return("history", "events", table, nil)
	operator.EXPECT().SnapshotTS().Return(snapshot.ToTimestamp())
	operator.EXPECT().Rollback(gomock.Any()).Return(nil)

	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { shared.Close(context.Background()) })

	schemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	runner := lifecycleBindingExecutor{
		engine:       engineMock,
		txnClient:    txnClient,
		sqlExecutor:  fakeSQL,
		taeFS:        shared,
		release:      lifecyclepkg.SQLReleaseConfig{Executor: fakeSQL},
		pager:        lifecyclepkg.SQLBindingPager{Executor: fakeSQL},
		rewriteSlots: make(chan struct{}, 1),
		now:          func() time.Time { return now },
		epoch:        7,
	}
	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Minute)
	defer cancelRun()
	err = runner.run(runCtx, lifecyclepkg.Binding{
		ID:                    "00112233445566778899aabbccddeeff",
		AccountID:             17,
		DatabaseID:            41,
		LogicalTableID:        43,
		PhysicalTableID:       43,
		Generation:            1,
		LifecycleColumnID:     2,
		SchemaDigest:          hex.EncodeToString(schemaDigest[:]),
		Action:                "DELETE",
		ExpireAfterDays:       7,
		EvaluationTimezone:    "UTC",
		Version:               1,
		LastFullScanAt:        now.Add(-48 * time.Hour),
		ScanSnapshotHex:       "",
		ScanLastObjectNameHex: "",
	})
	require.NoError(t, err)
	require.Equal(t, 1, table.discoveryCalls)
	require.True(t, slices.ContainsFunc(fakeSQL.queries, func(query string) bool {
		return strings.Contains(query, "update mo_catalog.mo_lifecycle_bindings") &&
			strings.Contains(query, "last_full_scan_at=utc_timestamp()")
	}))
}

func TestLifecycleBindingExecutorDefersMixedObjectBeforeSideEffectsWhenRewriteBusy(t *testing.T) {
	ctrl := gomock.NewController(t)
	engineMock := mock_frontend.NewMockEngine(ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	operator := mock_frontend.NewMockTxnOperator(ctrl)
	mp := mpool.MustNewZero()
	fakeSQL := &disabledLifecycleSQLExecutor{t: t, mp: mp, enabled: true}
	tableDef := &plan.TableDef{
		TblId:   43,
		Name:    "events",
		DbName:  "history",
		Version: 3,
		Cols: []*plan.ColDef{
			{
				ColId:  1,
				Name:   "id",
				Seqnum: 0,
				Typ:    plan.Type{Id: int32(types.T_int64)},
			},
			{
				ColId:  2,
				Name:   "created_at",
				Seqnum: 1,
				Typ:    plan.Type{Id: int32(types.T_timestamp)},
			},
		},
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	_, encodedCutoff, err := lifecycleCutoff(
		now,
		7,
		0,
		"UTC",
		types.T_timestamp,
	)
	require.NoError(t, err)
	source := lifecyclePlanTestSource(t, 128<<20)
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(encodedCutoff - 1))
	zoneMap.Update(types.Timestamp(encodedCutoff + 1))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		&source.ObjectStats,
		zoneMap,
	))
	snapshot := types.BuildTS(100, 1)
	table := &emptyLifecycleRunTable{
		def:            tableDef,
		sortKeyOrdinal: 1,
		page: lifecyclepkg.DiscoveryPage{
			Candidates: []lifecyclepkg.Candidate{{Snapshot: snapshot, Source: source}},
			Next: lifecyclepkg.DiscoveryCursor{
				Snapshot: snapshot,
				Wrapped:  true,
			},
			StartedFullScanAt:   now,
			CompletedFullScanAt: now,
		},
	}

	engineMock.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{})
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(operator, nil)
	engineMock.EXPECT().New(gomock.Any(), operator).Return(nil)
	engineMock.EXPECT().GetRelationById(
		gomock.Any(), operator, uint64(43),
	).Return("history", "events", table, nil)
	operator.EXPECT().SnapshotTS().Return(snapshot.ToTimestamp())
	operator.EXPECT().Rollback(gomock.Any()).Return(nil)
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { shared.Close(context.Background()) })
	rewriteSlots := make(chan struct{}, 1)
	rewriteSlots <- struct{}{}
	runner := lifecycleBindingExecutor{
		engine:       engineMock,
		txnClient:    txnClient,
		sqlExecutor:  fakeSQL,
		taeFS:        shared,
		release:      lifecyclepkg.SQLReleaseConfig{Executor: fakeSQL},
		pager:        lifecyclepkg.SQLBindingPager{Executor: fakeSQL},
		rewriteSlots: rewriteSlots,
		now:          func() time.Time { return now },
		epoch:        7,
	}
	schemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Minute)
	defer cancelRun()
	runErr := runner.run(runCtx, lifecyclepkg.Binding{
		ID:                 "00112233445566778899aabbccddeeff",
		AccountID:          17,
		DatabaseID:         41,
		LogicalTableID:     43,
		PhysicalTableID:    43,
		Generation:         1,
		LifecycleColumnID:  2,
		SchemaDigest:       hex.EncodeToString(schemaDigest[:]),
		Action:             "DELETE",
		ExpireAfterDays:    7,
		EvaluationTimezone: "UTC",
		Version:            1,
		LastFullScanAt:     now.Add(-48 * time.Hour),
	})
	require.ErrorContains(t, runErr, "RESOURCE_BLOCKED")
	require.True(t, lifecyclepkg.IsLifecycleDeferred(runErr))
	require.Equal(t, 1, table.discoveryCalls)
}

func TestLifecycleBindingExecutorFailsWholeObjectBeforeRetireWhenProtectionSelectionFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	engineMock := mock_frontend.NewMockEngine(ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	operator := mock_frontend.NewMockTxnOperator(ctrl)
	mp := mpool.MustNewZero()
	fakeSQL := &disabledLifecycleSQLExecutor{t: t, mp: mp, enabled: true}
	tableDef := &plan.TableDef{
		TblId:   43,
		Name:    "events",
		DbName:  "history",
		Version: 3,
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
			{ColId: 2, Name: "created_at", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_timestamp)}},
		},
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	_, encodedCutoff, err := lifecycleCutoff(now, 7, 0, "UTC", types.T_timestamp)
	require.NoError(t, err)
	source := lifecyclePlanTestSource(t, 128<<20)
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(encodedCutoff - 2))
	zoneMap.Update(types.Timestamp(encodedCutoff - 1))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		&source.ObjectStats,
		zoneMap,
	))
	snapshot := types.BuildTS(100, 1)
	expected := errors.New("protection selection failed")
	table := &emptyLifecycleRunTable{
		def:                 tableDef,
		sortKeyOrdinal:      1,
		selectProtectionErr: expected,
		page: lifecyclepkg.DiscoveryPage{
			Candidates: []lifecyclepkg.Candidate{{Snapshot: snapshot, Source: source}},
			Next: lifecyclepkg.DiscoveryCursor{
				Snapshot: snapshot,
				Wrapped:  true,
			},
			StartedFullScanAt:   now,
			CompletedFullScanAt: now,
		},
	}
	engineMock.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{})
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(operator, nil)
	engineMock.EXPECT().New(gomock.Any(), operator).Return(nil)
	engineMock.EXPECT().GetRelationById(
		gomock.Any(), operator, uint64(43),
	).Return("history", "events", table, nil)
	operator.EXPECT().SnapshotTS().Return(snapshot.ToTimestamp())
	operator.EXPECT().Rollback(gomock.Any()).Return(nil)
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { shared.Close(context.Background()) })
	runner := lifecycleBindingExecutor{
		engine:       engineMock,
		txnClient:    txnClient,
		sqlExecutor:  fakeSQL,
		taeFS:        shared,
		release:      lifecyclepkg.SQLReleaseConfig{Executor: fakeSQL},
		pager:        lifecyclepkg.SQLBindingPager{Executor: fakeSQL},
		rewriteSlots: make(chan struct{}, 1),
		now:          func() time.Time { return now },
		epoch:        7,
	}
	schemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Minute)
	defer cancelRun()
	runErr := runner.run(runCtx, lifecyclepkg.Binding{
		ID:                 "00112233445566778899aabbccddeeff",
		AccountID:          17,
		DatabaseID:         41,
		LogicalTableID:     43,
		PhysicalTableID:    43,
		Generation:         1,
		LifecycleColumnID:  2,
		SchemaDigest:       hex.EncodeToString(schemaDigest[:]),
		Action:             "DELETE",
		ExpireAfterDays:    7,
		EvaluationTimezone: "UTC",
		Version:            1,
		LastFullScanAt:     now.Add(-48 * time.Hour),
	})
	require.ErrorIs(t, runErr, expected)
	require.Equal(t, 1, table.discoveryCalls)
}

type emptyLifecycleRunTable struct {
	engine.Relation
	def                 *plan.TableDef
	page                lifecyclepkg.DiscoveryPage
	discoveryCalls      int
	sortKeyOrdinal      int
	selectProtectionErr error
}

func (table *emptyLifecycleRunTable) GetTableDef(context.Context) *plan.TableDef {
	return table.def
}

func (table *emptyLifecycleRunTable) LifecycleDiscoverObjectPage(
	_ context.Context,
	request lifecyclepkg.DiscoveryRequest,
) (lifecyclepkg.DiscoveryPage, error) {
	table.discoveryCalls++
	if request.Snapshot.IsEmpty() {
		return lifecyclepkg.DiscoveryPage{}, errors.New("empty discovery snapshot")
	}
	return table.page, nil
}

func (table *emptyLifecycleRunTable) LifecycleSortKeyOrdinal() int {
	if table.sortKeyOrdinal == 0 {
		return -1
	}
	return table.sortKeyOrdinal
}

func (*emptyLifecycleRunTable) LifecycleReadObject(
	context.Context,
	types.TS,
	objectio.ObjectStats,
	uint64,
	lifecyclepkg.ExactBlockConsumer,
) (lifecyclepkg.ObjectScanReport, error) {
	panic("empty page must not read an Object")
}

func (*emptyLifecycleRunTable) LifecycleRewriteObject(
	context.Context,
	LifecycleRewriteOptions,
) (LifecycleRewriteResult, error) {
	panic("empty page must not rewrite an Object")
}

func (table *emptyLifecycleRunTable) LifecycleSelectProtectionSet(
	context.Context,
	types.TS,
	[]objectio.ObjectEntry,
	logtailreplay.LifecycleTombstoneSelectionLimits,
) (lifecyclepkg.ProtectionSet, error) {
	if table.selectProtectionErr != nil {
		return lifecyclepkg.ProtectionSet{}, table.selectProtectionErr
	}
	panic("empty page must not select a protection set")
}

func lifecycleExecutorCleanupRootResult(
	t *testing.T,
	mp *mpool.MPool,
	root lifecyclepkg.CleanupRoot,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(27)
	stringsByColumn := map[int]string{
		0:  strings.ReplaceAll(root.RootID, "-", ""),
		1:  strings.ReplaceAll(root.AttemptID, "-", ""),
		2:  string(root.Mode),
		7:  root.WorkerDeadline.Format("2006-01-02 15:04:05.999999"),
		8:  root.ArchiveNamespace,
		9:  root.CredentialHandle,
		10: root.ArchivePrefix,
		11: root.ManifestKey,
		12: hex.EncodeToString(root.ManifestDigest[:]),
		13: root.TAENamespace,
		14: root.SegmentID,
		15: root.BookingPrefix,
		18: hex.EncodeToString(root.SourceSetDigest[:]),
		19: root.FinalTxnID,
		20: string(root.State),
		22: root.CleanupAfter.Format("2006-01-02 15:04:05.999999"),
		26: root.LastError,
	}
	if !root.QuiescenceSince.IsZero() {
		stringsByColumn[24] = root.QuiescenceSince.Format("2006-01-02 15:04:05.999999")
	}
	if !root.LastListAt.IsZero() {
		stringsByColumn[25] = root.LastListAt.Format("2006-01-02 15:04:05.999999")
	}
	numbers := map[int]uint64{
		3:  uint64(root.OwnerAccountID),
		4:  root.LogicalTableID,
		5:  root.PhysicalTableID,
		6:  root.ExecutorEpoch,
		16: uint64(root.OrdinalUpperBound),
		17: root.ReservedCleanupBytes,
		21: root.StateVersion,
	}
	for column := range value.Vecs {
		switch {
		case column == 23:
			value.Vecs[column] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column], root.TemporaryCleanupDone, false, mp,
			))
		case numbers[column] != 0 || column == 3 || column == 4 ||
			column == 5 || column == 6 || column == 16 || column == 17 ||
			column == 21:
			value.Vecs[column] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column], numbers[column], false, mp,
			))
		default:
			value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
			nullValue := (column == 24 && root.QuiescenceSince.IsZero()) ||
				(column == 25 && root.LastListAt.IsZero())
			require.NoError(t, vector.AppendBytes(
				value.Vecs[column], []byte(stringsByColumn[column]), nullValue, mp,
			))
		}
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}

type disabledLifecycleSQLExecutor struct {
	t       *testing.T
	mp      *mpool.MPool
	enabled bool
	queries []string
}

func (fake *disabledLifecycleSQLExecutor) Exec(
	ctx context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	_, hasDeadline := ctx.Deadline()
	require.True(fake.t, hasDeadline)
	fake.queries = append(fake.queries, strings.ToLower(sql))
	if strings.Contains(strings.ToLower(sql), "mo_feature_registry") {
		value := batch.NewWithSize(2)
		value.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		value.Vecs[1] = vector.NewVec(types.T_json.ToType())
		require.NoError(fake.t, vector.AppendFixed(
			value.Vecs[0], fake.enabled, false, fake.mp,
		))
		scope, err := types.ParseStringToByteJson(`{"archive_stages":[]}`)
		require.NoError(fake.t, err)
		require.NoError(fake.t, vector.AppendByteJson(
			value.Vecs[1], scope, false, fake.mp,
		))
		value.SetRowCount(1)
		return executor.Result{
			Batches: []*batch.Batch{value},
			Mp:      fake.mp,
		}, nil
	}
	if strings.Contains(
		strings.ToLower(sql),
		"update mo_catalog.mo_lifecycle_bindings",
	) {
		return executor.Result{AffectedRows: 1, Mp: fake.mp}, nil
	}
	return executor.Result{Mp: fake.mp}, nil
}

type failingLifecycleSQLExecutor struct {
	t     *testing.T
	err   error
	calls int
}

func (fake *failingLifecycleSQLExecutor) Exec(
	ctx context.Context,
	_ string,
	_ executor.Options,
) (executor.Result, error) {
	fake.calls++
	_, hasDeadline := ctx.Deadline()
	require.True(fake.t, hasDeadline)
	return executor.Result{}, fake.err
}

func (*failingLifecycleSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected Lifecycle failure transaction")
}

func (*disabledLifecycleSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected Lifecycle disabled transaction")
}
