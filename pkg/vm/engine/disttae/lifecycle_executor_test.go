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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioio "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
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

type disabledLifecycleSQLExecutor struct {
	t       *testing.T
	mp      *mpool.MPool
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
			value.Vecs[0], false, false, fake.mp,
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
