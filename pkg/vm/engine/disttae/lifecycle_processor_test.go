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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestLifecycleProcessorWholePublishesOnlyAfterVerifiedArchive(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	committed := false
	fixture.task.OnFinalCommit = func() { committed = true }
	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Equal(
		t,
		"lifecycle/root/attempt",
		root.ArchivePrefix,
	)
	require.NotEmpty(t, root.ManifestKey)
	require.NotEmpty(t, root.ManifestDigest)
	require.Equal(t, 1, fixture.finalizer.calls)
	require.True(t, committed)
	require.Equal(t, root.RootID, fixture.finalizer.request.Control.RootId)
	require.Equal(t, root.AttemptID, fixture.finalizer.request.Control.AttemptId)
	require.Equal(
		t,
		fixture.task.Sources[0].ObjectStats.Marshal(),
		fixture.finalizer.request.Control.DataSourceObjectStats[0],
	)
	require.Contains(
		t,
		fixture.finalizer.request.SyncProtectionJobID,
		root.AttemptID+"-",
	)
	require.True(t, fixture.protection.released)
	require.GreaterOrEqual(t, fixture.store.puts, 3)
}

func TestLifecycleProcessorWholeAllSnapshotDeletedIsNoop(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	committed := false
	fixture.task.OnFinalCommit = func() { committed = true }
	fixture.task.Table.(*lifecycleProcessorTable).snapshotDeleted =
		nulls.Build(2, 0, 1)

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Empty(t, root.LastError)
	require.Zero(t, fixture.finalizer.calls)
	require.False(t, committed)
}

func TestLifecycleProcessorMixedWithoutExpiredRowsIsNoop(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	committed := false
	fixture.task.OnFinalCommit = func() { committed = true }
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nulls.NewWithSize(2), nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	fixture.task.Table.(*lifecycleProcessorTable).rewrite = true

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Empty(t, root.LastError)
	require.Zero(t, fixture.finalizer.calls)
	require.False(t, committed)
}

func TestLifecycleProcessorWholeBatchUsesOneRootAndOneDataset(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	secondID := objectio.NewObjectid()
	secondStats := objectio.NewObjectStatsWithObjectID(
		&secondID,
		false,
		true,
		true,
	)
	require.NoError(t, objectio.SetObjectStatsRowCnt(secondStats, 2))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(secondStats, 1))
	require.NoError(t, objectio.SetObjectStatsOriginSize(secondStats, 1024))
	fixture.task.Sources = append(fixture.task.Sources, objectio.ObjectEntry{
		ObjectStats: *secondStats,
		CreateTime:  fixture.task.Sources[0].CreateTime,
	})

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Len(t, fixture.roots.snapshot(), 1)
	require.Equal(t, 1, fixture.finalizer.calls)
	require.Len(t, fixture.finalizer.request.Control.DataSourceObjectStats, 2)
	require.Equal(t, uint64(4), fixture.finalizer.request.Manifest.RowCount)
}

func TestLifecycleProcessorRejectsWholeBatchAboveCertifiedSourceBytes(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	require.NoError(t, objectio.SetObjectStatsOriginSize(
		&fixture.task.Sources[0].ObjectStats,
		3<<30,
	))
	secondID := objectio.NewObjectid()
	secondStats := objectio.NewObjectStatsWithObjectID(
		&secondID,
		false,
		true,
		true,
	)
	require.NoError(t, objectio.SetObjectStatsRowCnt(secondStats, 2))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(secondStats, 1))
	require.NoError(t, objectio.SetObjectStatsOriginSize(secondStats, 3<<30))
	fixture.task.Sources = append(fixture.task.Sources, objectio.ObjectEntry{
		ObjectStats: *secondStats,
		CreateTime:  fixture.task.Sources[0].CreateTime,
	})

	err := fixture.processor.validateTask(context.Background(), fixture.task)
	require.ErrorContains(t, err, "source bytes")
}

func TestLifecycleArchivePurgeEligibilityUsesFrozenCutoff(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Cutoff = fixture.task.Now.Add(-92 * 24 * time.Hour)
	fixture.task.PurgeAfter = 365 * 24 * time.Hour

	_, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		fixture.task.Cutoff.Add(fixture.task.PurgeAfter),
		fixture.finalizer.request.PurgeEligibleAt,
	)
}

func TestLifecycleWholeCommitControlOmitsRewriteState(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Whole = true
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8

	control := fixture.processor.commitControl(
		fixture.task,
		lifecyclepkg.CleanupRoot{
			RootID:          "root",
			AttemptID:       "attempt",
			SourceSetDigest: lifecycleSourceDigest(fixture.task.Sources),
		},
		LifecycleRewriteResult{
			CreatedObjectStats:      [][]byte{{1}},
			TransferBookingLocation: []string{"booking"},
			TransferMappingDigest:   [32]byte{3},
			MergeLevel:              4,
		},
	)

	require.Equal(t, api.LifecycleCommitEntry_Whole, control.RetireMode)
	require.Empty(t, control.CreatedObjectStats)
	require.Empty(t, control.TransferBookingLocations)
	require.Empty(t, control.TransferMappingDigest)
	require.Zero(t, control.MaxDeltaRows)
	require.Zero(t, control.MaxDeltaBytes)
	require.Zero(t, control.MaxDeltaBlocks)
	require.Zero(t, control.MergeLevel)
}

func TestLifecycleFinalizerRejectsUnverifiedOrMismatchedArchive(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	_, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	request := fixture.finalizer.request
	require.NoError(t, validateLifecycleFinalizeRequest(request))

	request.Manifest.VerificationStatus = "SOURCE_ENCODED"
	require.ErrorContains(
		t,
		validateLifecycleFinalizeRequest(request),
		"full-readback verified",
	)
	request.Manifest.VerificationStatus = "FULL_READBACK_VERIFIED"
	request.Control.AttemptId = "different-attempt"
	require.ErrorContains(
		t,
		validateLifecycleFinalizeRequest(request),
		"identity mismatch",
	)
}

func TestLifecycleProcessorFaultAfterReadbackAbandonsRootWithoutFinalize(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.processor.Faults = lifecycleTestFaults{
		lifecyclepkg.FaultAfterFullReadback: errors.New("injected readback crash"),
	}
	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "injected readback crash")
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Zero(t, fixture.finalizer.calls)
	require.True(t, fixture.protection.released)
}

func TestLifecycleProcessorPreCommitFaultMatrixNeverRetiresSource(t *testing.T) {
	points := []lifecyclepkg.FaultPoint{
		lifecyclepkg.FaultAfterRootRegister,
		lifecyclepkg.FaultAfterProtection,
		lifecyclepkg.FaultBeforeSourceRead,
		lifecyclepkg.FaultBeforePayloadPut,
		lifecyclepkg.FaultAfterPayloadPut,
		lifecyclepkg.FaultBeforeManifestPut,
		lifecyclepkg.FaultAfterManifestPut,
		lifecyclepkg.FaultBeforeFullReadback,
		lifecyclepkg.FaultAfterFullReadback,
		lifecyclepkg.FaultAfterPayloadWrite,
		lifecyclepkg.FaultBeforeFinalCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			fixture := newLifecycleProcessorFixture(t)
			fixture.processor.Faults =
				lifecyclepkg.NewProgrammableFaultInjector(
					map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
						point: lifecyclepkg.FailOnHit(1, "injected-"+string(point)),
					},
				)

			root, err := fixture.processor.ProcessArchiveObject(
				context.Background(),
				fixture.task,
			)
			require.ErrorContains(t, err, "injected-"+string(point))
			require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
			require.Zero(t, fixture.finalizer.calls)
			persisted := fixture.roots.snapshot()[root.RootID]
			require.Equal(t, lifecyclepkg.CleanupRootDeletePending, persisted.State)
		})
	}
}

func TestLifecycleProcessorPostCommitFaultKeepsPublishedOwner(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.processor.Faults = lifecyclepkg.NewProgrammableFaultInjector(
		map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
			lifecyclepkg.FaultAfterFinalCommit: lifecyclepkg.FailOnHit(
				1,
				"final-commit-response-lost",
			),
		},
	)

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "final-commit-response-lost")
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Equal(t, 1, fixture.finalizer.calls)
	persisted := fixture.roots.snapshot()[root.RootID]
	require.Equal(t, lifecyclepkg.CleanupRootPublished, persisted.State)
}

func TestLifecycleProcessorRootCASResponseLossLeavesDurableRetryableOwner(
	t *testing.T,
) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.processor.Faults = lifecyclepkg.NewProgrammableFaultInjector(
		map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
			lifecyclepkg.FaultAfterRootCAS: lifecyclepkg.FailOnHit(
				1,
				"root-cas-response-lost",
			),
		},
	)

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "root-cas-response-lost")
	require.Equal(t, lifecyclepkg.CleanupRootUploading, root.State)
	persisted := fixture.roots.snapshot()[root.RootID]
	require.Equal(
		t,
		lifecyclepkg.CleanupRootUploading,
		persisted.State,
	)
	require.Zero(t, fixture.finalizer.calls)
	require.Zero(t, fixture.store.puts)
}

func TestLifecycleProcessorCommitUnknownKeepsRootFailClosed(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.finalizer.err = moerr.NewTxnUnknown(
		context.Background(),
		"lifecycle-final",
	)
	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnUnknown))
	require.Equal(t, lifecyclepkg.CleanupRootCommitUnknown, root.State)
	require.False(t, lifecyclepkg.CanSweepCleanupRoot(root))
}

func TestLifecycleProcessorProtectionLossFailsClosedBeforeSourceRead(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.protection.statErr = errors.New("SyncProtection lost after TN restart")

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "SyncProtection lost")
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.True(t, fixture.protection.released)
	require.Zero(t, fixture.store.puts)
	require.Zero(t, fixture.finalizer.calls)
}

func TestLifecycleProcessorAttemptUsesAbsoluteDeadline(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Deadline = time.Now().Add(20 * time.Millisecond)
	table := fixture.task.Table.(*lifecycleProcessorTable)
	table.waitForContext = true

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Zero(t, fixture.store.puts)
	require.Zero(t, fixture.finalizer.calls)
	require.True(t, fixture.protection.released)
}

func TestLifecycleProtectionReleaseContextIsBoundedAndDetached(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := lifecycleProtectionReleaseContext(parent)
	defer cancel()

	require.NoError(t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(
		t,
		time.Now().Add(lifecycleProtectionReleaseTimeout),
		deadline,
		time.Second,
	)
}

func TestLifecycleTemporaryCleanupContextIsBoundedAndDetached(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := lifecycleTemporaryCleanupContext(parent)
	defer cancel()

	require.NoError(t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(
		t,
		time.Now().Add(lifecycleTemporaryCleanupTimeout),
		deadline,
		time.Second,
	)
}

func TestLifecycleRollbackContextIsBoundedAndDetached(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := lifecycleRollbackContext(parent)
	defer cancel()

	require.NoError(t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(
		t,
		time.Now().Add(lifecycleRollbackTimeout),
		deadline,
		time.Second,
	)
}

func TestLifecycleProcessorCleanupCannotStartBeforeWorkerDeadline(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.processor.Config.CleanupGrace = time.Second
	fixture.processor.Faults = lifecyclepkg.NewProgrammableFaultInjector(
		map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
			lifecyclepkg.FaultAfterRootRegister: lifecyclepkg.FailOnHit(
				1,
				"stop after durable root",
			),
		},
	)
	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "stop after durable root")
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Equal(t, fixture.task.Deadline, root.CleanupAfter)
}

func TestLifecycleProcessorCleanupRootPrecedesFirstProviderPut(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.store.beforePut = func(key string) error {
		roots := fixture.roots.snapshot()
		require.Len(t, roots, 1)
		for _, root := range roots {
			require.Contains(t, key, "/"+root.RootID+"/"+root.AttemptID+"/")
			require.Equal(t, lifecyclepkg.CleanupRootUploading, root.State)
		}
		return nil
	}
	_, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
}

func TestLifecycleProcessorMixedUsesSingleSourceRewriteProducer(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	table := fixture.task.Table.(*lifecycleProcessorTable)
	table.rewrite = true
	fixture.temporary.objects["lifecycle-staging/root/attempt/booking/booking-000000"] =
		[]byte("booking")

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Equal(
		t,
		api.LifecycleCommitEntry_Rewrite,
		fixture.finalizer.request.Control.RetireMode,
	)
	require.Len(t, fixture.finalizer.request.Control.CreatedObjectStats, 1)
	require.Len(t, fixture.finalizer.request.Control.TransferBookingLocations, 1)
	require.Equal(t, 1, table.rewriteCalls)
	require.Empty(t, root.SegmentID)
	require.Zero(t, root.OrdinalUpperBound)
	require.Empty(t, root.BookingPrefix)
	require.True(t, root.TemporaryCleanupDone)
	require.Empty(t, fixture.temporary.objects)
}

func TestLifecycleProcessorRewriteStagingFaultsNeverFinalize(t *testing.T) {
	for _, point := range []lifecyclepkg.FaultPoint{
		lifecyclepkg.FaultBeforeRewriteStaging,
		lifecyclepkg.FaultAfterRewriteStaging,
	} {
		t.Run(string(point), func(t *testing.T) {
			fixture := newLifecycleProcessorFixture(t)
			fixture.task.Whole = false
			fixture.task.Classifier = func(
				context.Context,
				*batch.Batch,
				*nulls.Nulls,
			) (*nulls.Nulls, error) {
				expired := &nulls.Nulls{}
				expired.Add(0)
				return expired, nil
			}
			fixture.task.DeltaRows = 100
			fixture.task.DeltaBytes = 1 << 20
			fixture.task.DeltaBlocks = 8
			fixture.task.MaxCreatedObjects = 16
			table := fixture.task.Table.(*lifecycleProcessorTable)
			table.rewrite = true
			fixture.processor.Faults =
				lifecyclepkg.NewProgrammableFaultInjector(
					map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
						point: lifecyclepkg.FailOnHit(
							1,
							"injected-"+string(point),
						),
					},
				)

			root, err := fixture.processor.ProcessArchiveObject(
				context.Background(),
				fixture.task,
			)
			require.ErrorContains(t, err, "injected-"+string(point))
			require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
			require.Zero(t, fixture.finalizer.calls)
			if point == lifecyclepkg.FaultBeforeRewriteStaging {
				require.Zero(t, table.rewriteCalls)
			} else {
				require.Equal(t, 1, table.rewriteCalls)
			}
		})
	}
}

func TestLifecycleProcessorReservesRewriteBytesBeforeRootAndSourceRead(
	t *testing.T,
) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Whole = false
	fixture.task.Table.(*lifecycleProcessorTable).rewrite = true
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	admission, err := lifecyclepkg.NewRewriteAdmission(
		lifecyclepkg.RewriteReleaseProfile{
			Window:                   time.Hour,
			MaxAmplification:         20,
			MaxSourceBytesPerAccount: 512,
			MaxSourceBytesPerCluster: 512,
		},
	)
	require.NoError(t, err)
	fixture.processor.RewriteAdmission = admission

	_, err = fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "Rewrite byte window exhausted")
	require.Empty(t, fixture.roots.snapshot())
	require.Zero(t, fixture.task.Table.(*lifecycleProcessorTable).rewriteCalls)
	require.Zero(t, fixture.store.puts)
}

func TestLifecycleProcessorChecksCleanupCapacityBeforeRootCreation(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.roots.capacityErr = errors.New(
		"RESOURCE_BLOCKED: Lifecycle Cleanup Root capacity exhausted",
	)

	_, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "Cleanup Root capacity exhausted")
	require.Empty(t, fixture.roots.snapshot())
	require.Zero(t, fixture.store.puts)
	require.Zero(t, fixture.finalizer.calls)
}

func TestLifecycleProcessorMixedTemporaryCleanupFailureKeepsPublishedPayload(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	fixture.task.Table.(*lifecycleProcessorTable).rewrite = true
	fixture.temporary.listErr = errors.New("temporary store unavailable")

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Empty(t, root.SegmentID)
	require.Zero(t, root.OrdinalUpperBound)
	require.False(t, root.TemporaryCleanupDone)
	require.NotEmpty(t, root.BookingPrefix)
	require.Contains(t, root.LastError, "temporary store unavailable")
	require.NotEmpty(t, fixture.store.objects)
}

func TestLifecycleProcessorArchiveRewriteWithoutLiveRowsKeepsRewriteRootOwner(
	t *testing.T,
) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		expired.Add(1)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	table := fixture.task.Table.(*lifecycleProcessorTable)
	table.rewrite = true
	table.noLiveRows = true

	root, err := fixture.processor.ProcessArchiveObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Equal(t, lifecyclepkg.CleanupModeArchiveRewrite, root.Mode)
	require.Equal(
		t,
		api.LifecycleCommitEntry_Whole,
		fixture.finalizer.request.Control.RetireMode,
	)
	require.Empty(t, fixture.finalizer.request.Control.CreatedObjectStats)
	require.Empty(t, fixture.finalizer.request.Control.TransferBookingLocations)
	require.True(t, root.TemporaryCleanupDone)
	require.Empty(t, root.BookingPrefix)
	require.Empty(t, root.SegmentID)
	require.Equal(
		t,
		lifecyclepkg.CleanupModeArchiveRewrite,
		fixture.roots.snapshot()[root.RootID].Mode,
	)
}

func TestLifecycleProcessorWholeTTLUsesReceiptWithoutExternalSideEffects(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.DatasetID = ""
	fixture.task.ReceiptID = "receipt"
	fixture.task.Classifier = func(
		_ context.Context,
		value *batch.Batch,
		snapshotDeleted *nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		for row := 0; row < value.RowCount(); row++ {
			if snapshotDeleted == nil ||
				!snapshotDeleted.Contains(uint64(row)) {
				expired.Add(uint64(row))
			}
		}
		return expired, nil
	}

	root, err := fixture.processor.ProcessTTLObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Empty(t, root.RootID)
	require.Empty(t, fixture.roots.snapshot())
	require.Zero(t, fixture.store.puts)
	require.Equal(t, 1, fixture.finalizer.calls)
	require.Equal(t, "receipt", fixture.finalizer.request.Control.ReceiptId)
	require.Empty(t, fixture.finalizer.request.Control.DatasetId)
	require.Nil(t, fixture.finalizer.request.Manifest)
	require.Positive(t, fixture.finalizer.request.ExpiredRows)
}

func TestLifecycleProcessorTTLPreCommitFaultsNeverRetireSource(t *testing.T) {
	for _, whole := range []bool{true, false} {
		for _, point := range []lifecyclepkg.FaultPoint{
			lifecyclepkg.FaultAfterProtection,
			lifecyclepkg.FaultBeforeSourceRead,
			lifecyclepkg.FaultBeforeFinalCommit,
		} {
			t.Run(
				fmt.Sprintf("whole=%t/%s", whole, point),
				func(t *testing.T) {
					fixture := newLifecycleProcessorFixture(t)
					fixture.task.DatasetID = ""
					fixture.task.ReceiptID = "receipt"
					fixture.task.Whole = whole
					fixture.task.Classifier = func(
						context.Context,
						*batch.Batch,
						*nulls.Nulls,
					) (*nulls.Nulls, error) {
						expired := &nulls.Nulls{}
						expired.Add(0)
						if whole {
							expired.Add(1)
						}
						return expired, nil
					}
					if !whole {
						fixture.task.DeltaRows = 100
						fixture.task.DeltaBytes = 1 << 20
						fixture.task.DeltaBlocks = 8
						fixture.task.MaxCreatedObjects = 16
						fixture.task.Table.(*lifecycleProcessorTable).rewrite = true
					}
					fixture.processor.Faults =
						lifecyclepkg.NewProgrammableFaultInjector(
							map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
								point: lifecyclepkg.FailOnHit(
									1,
									"injected-"+string(point),
								),
							},
						)

					root, err := fixture.processor.ProcessTTLObject(
						context.Background(),
						fixture.task,
					)
					require.ErrorContains(t, err, "injected-"+string(point))
					require.Zero(t, fixture.finalizer.calls)
					if point == lifecyclepkg.FaultBeforeFinalCommit && !whole {
						require.Equal(
							t,
							lifecyclepkg.CleanupRootDeletePending,
							root.State,
						)
					} else {
						require.Empty(t, root.RootID)
					}
				},
			)
		}
	}
}

func TestLifecycleProcessorTTLPostCommitFaultKeepsPublishedOwner(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.DatasetID = ""
	fixture.task.ReceiptID = "receipt"
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	fixture.task.Table.(*lifecycleProcessorTable).rewrite = true
	fixture.processor.Faults = lifecyclepkg.NewProgrammableFaultInjector(
		map[lifecyclepkg.FaultPoint]lifecyclepkg.FaultAction{
			lifecyclepkg.FaultAfterFinalCommit: lifecyclepkg.FailOnHit(
				1,
				"ttl-final-commit-response-lost",
			),
		},
	)

	root, err := fixture.processor.ProcessTTLObject(
		context.Background(),
		fixture.task,
	)
	require.ErrorContains(t, err, "ttl-final-commit-response-lost")
	require.Equal(t, 1, fixture.finalizer.calls)
	require.Equal(t, lifecyclepkg.CleanupRootPublished, root.State)
	require.Empty(t, root.SegmentID)
	require.Equal(
		t,
		lifecyclepkg.CleanupRootPublished,
		fixture.roots.snapshot()[root.RootID].State,
	)
}

func TestLifecycleProcessorMixedTTLRewritesWithoutArchivePayload(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.DatasetID = ""
	fixture.task.ReceiptID = "receipt"
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	fixture.task.Table.(*lifecycleProcessorTable).rewrite = true
	fixture.temporary.objects["lifecycle-staging/root/attempt/booking/booking-000000"] =
		[]byte("booking")

	root, err := fixture.processor.ProcessTTLObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.True(t, root.TemporaryCleanupDone)
	require.Empty(t, root.SegmentID)
	require.Empty(t, root.BookingPrefix)
	require.Zero(t, fixture.store.puts)
	require.Equal(t, "receipt", fixture.finalizer.request.Control.ReceiptId)
	require.Empty(t, fixture.finalizer.request.Control.DatasetId)
	require.Positive(t, fixture.finalizer.request.ExpiredRows)
}

func TestLifecycleProcessorMixedTTLWithoutLiveRowsFinalizesAsWhole(t *testing.T) {
	fixture := newLifecycleProcessorFixture(t)
	fixture.task.DatasetID = ""
	fixture.task.ReceiptID = "receipt"
	fixture.task.Whole = false
	fixture.task.Classifier = func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		expired := &nulls.Nulls{}
		expired.Add(0)
		return expired, nil
	}
	fixture.task.DeltaRows = 100
	fixture.task.DeltaBytes = 1 << 20
	fixture.task.DeltaBlocks = 8
	fixture.task.MaxCreatedObjects = 16
	table := fixture.task.Table.(*lifecycleProcessorTable)
	table.rewrite = true
	table.noLiveRows = true

	root, err := fixture.processor.ProcessTTLObject(
		context.Background(),
		fixture.task,
	)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, root.State)
	require.Equal(
		t,
		api.LifecycleCommitEntry_Whole,
		fixture.finalizer.request.Control.RetireMode,
	)
	require.Empty(t, fixture.finalizer.request.Control.CreatedObjectStats)
	require.Empty(t, fixture.finalizer.request.Control.TransferBookingLocations)
	require.Positive(t, fixture.finalizer.request.ExpiredRows)
	require.NoError(t, validateLifecycleFinalizeRequest(fixture.finalizer.request))
}

type lifecycleProcessorFixture struct {
	processor  LifecycleProcessor
	task       LifecycleObjectTask
	roots      *lifecycleProcessorRootStore
	store      *lifecycleProcessorArchiveStore
	protection *lifecycleProcessorProtection
	finalizer  *lifecycleProcessorFinalizer
	temporary  *lifecycleProcessorCleanupStore
}

func newLifecycleProcessorFixture(t *testing.T) *lifecycleProcessorFixture {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	snapshot := types.BuildTS(now.UnixNano(), 1)
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, true)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, 1024))
	source := objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(now.Add(-time.Hour).UnixNano(), 0),
	}
	schema := lifecyclepkg.SchemaDescriptor{
		FormatVersion:      1,
		SourceTableID:      43,
		SourceTableVersion: 2,
		SourceDatabaseName: "db",
		SourceTableName:    "events",
		Columns: []lifecyclepkg.SchemaColumn{
			{
				Ordinal:        0,
				SourceColumnID: 1,
				Name:           "id",
				TypeID:         int32(types.T_int64),
				NotNull:        true,
			},
			{
				Ordinal:        1,
				SourceColumnID: 2,
				Name:           "name",
				TypeID:         int32(types.T_varchar),
				Width:          64,
			},
		},
	}
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	value := lifecycleProcessorBatch(t)
	table := &lifecycleProcessorTable{batch: value}
	roots := newLifecycleProcessorRootStore()
	store := newLifecycleProcessorArchiveStore()
	protection := &lifecycleProcessorProtection{}
	finalizer := &lifecycleProcessorFinalizer{}
	temporary := &lifecycleProcessorCleanupStore{
		objects: make(map[string][]byte),
	}
	ids := []string{"root", "attempt", "txn", "write"}
	idIndex := 0
	processor := LifecycleProcessor{
		Config: LifecycleProcessorConfig{
			TAENamespace:          "shared",
			MaxRestoreChunkRows:   1024,
			MaxChunkBytes:         1 << 20,
			MaxActiveCleanupRoots: 1024,
			MaxActiveCleanupBytes: 1 << 40,
			CleanupGrace:          time.Minute,
		},
		Roots:           roots,
		CleanupCapacity: roots,
		Store:           store,
		Protection:      protection,
		Finalizer:       finalizer,
		TemporaryStore:  temporary,
		ID: func() string {
			value := ids[idIndex]
			idIndex++
			return value
		},
	}
	task := LifecycleObjectTask{
		Binding: lifecyclepkg.Binding{
			ID:              "binding",
			AccountID:       1,
			DatabaseID:      42,
			LogicalTableID:  43,
			PhysicalTableID: 43,
			Generation:      7,
			Version:         11,
			SchemaDigest:    hex.EncodeToString(schemaDigest[:]),
		},
		Table:               table,
		Sources:             []objectio.ObjectEntry{source},
		SourceSnapshot:      snapshot,
		Schema:              schema,
		SchemaDigest:        schemaDigest,
		BindingSchemaDigest: schemaDigest,
		Whole:               true,
		ArchiveTarget: lifecyclepkg.FrozenArchiveTarget{
			FormatVersion:     1,
			StageID:           9,
			Provider:          "s3",
			Region:            "us-east-1",
			BucketOrContainer: "archive",
			ImmutablePrefix:   "tenant-1",
			CredentialHandle:  "default",
		},
		DatasetID:                  "dataset",
		Cutoff:                     now.Add(-90 * 24 * time.Hour),
		PurgeAfter:                 365 * 24 * time.Hour,
		Now:                        now,
		Deadline:                   now.Add(time.Minute),
		ExecutorEpoch:              1,
		MaxCertifiedBlockReadBytes: 256 << 20,
		TargetObjectSize:           128 << 20,
		ProtectionLimits: logtailreplay.LifecycleTombstoneSelectionLimits{
			MaxScannedObjects:  16,
			MaxSelectedObjects: 16,
			MaxMetaBytes:       1 << 20,
		},
	}
	return &lifecycleProcessorFixture{
		processor:  processor,
		task:       task,
		roots:      roots,
		store:      store,
		protection: protection,
		finalizer:  finalizer,
		temporary:  temporary,
	}
}

func lifecycleProcessorBatch(t *testing.T) *batch.Batch {
	t.Helper()
	mp := mpool.MustNewZero()
	value := batch.New([]string{"id", "name"})
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	nameType := types.T_varchar.ToType()
	nameType.Width = 64
	value.Vecs[1] = vector.NewVec(nameType)
	require.NoError(t, vector.AppendFixed(value.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(value.Vecs[0], int64(2), false, mp))
	require.NoError(t, vector.AppendBytes(value.Vecs[1], []byte("one"), false, mp))
	require.NoError(t, vector.AppendBytes(value.Vecs[1], []byte("two"), false, mp))
	value.SetRowCount(2)
	t.Cleanup(func() { value.Clean(mp) })
	return value
}

type lifecycleProcessorTable struct {
	batch           *batch.Batch
	snapshotDeleted *nulls.Nulls
	rewrite         bool
	noLiveRows      bool
	waitForContext  bool
	rewriteCalls    int
}

func TestLifecycleCleanupReservationUsesPhysicalBookingRowWidth(t *testing.T) {
	source := lifecyclePlanTestSource(t, 1)
	require.NoError(t, objectio.SetObjectStatsRowCnt(&source.ObjectStats, 10))
	reserved, archive, err := lifecycleCleanupReservation(
		LifecycleObjectTask{
			Sources:                    []objectio.ObjectEntry{source},
			MaxCreatedObjects:          1,
			TargetObjectSize:           1,
			MaxCertifiedBlockReadBytes: 1,
		},
		false,
	)
	require.NoError(t, err)
	require.Zero(t, archive)
	require.Equal(
		t,
		uint64(2)+lifecycleBookingPhysicalOverhead+10*uint64(15),
		reserved,
	)
}

func (table *lifecycleProcessorTable) LifecycleReadObject(
	ctx context.Context,
	_ types.TS,
	source objectio.ObjectStats,
	_ uint64,
	consume lifecyclepkg.ExactBlockConsumer,
) (lifecyclepkg.ObjectScanReport, error) {
	report := lifecyclepkg.NewObjectScanReport(
		source.BlkCnt(),
		uint64(source.Rows()),
	)
	if table.waitForContext {
		<-ctx.Done()
		return report, ctx.Err()
	}
	if err := report.ObservePhysicalBlock(
		table.batch.RowCount(),
		table.snapshotDeleted,
	); err != nil {
		return report, err
	}
	return report, consume(table.batch, table.snapshotDeleted)
}

func (table *lifecycleProcessorTable) LifecycleRewriteObject(
	ctx context.Context,
	options LifecycleRewriteOptions,
) (LifecycleRewriteResult, error) {
	if !table.rewrite {
		return LifecycleRewriteResult{}, errors.New("unexpected Rewrite")
	}
	table.rewriteCalls++
	if err := options.BeforeLiveWrite(ctx, options.LiveSegmentID); err != nil {
		return LifecycleRewriteResult{}, err
	}
	expired, err := options.Classify(ctx, table.batch, nil)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	if err := options.Archive(ctx, table.batch, expired); err != nil {
		return LifecycleRewriteResult{}, err
	}
	if table.noLiveRows {
		report := lifecyclepkg.NewObjectScanReport(1, uint64(table.batch.RowCount()))
		if err := report.ObserveClassifiedBlock(
			table.batch.RowCount(),
			nil,
			expired,
		); err != nil {
			return LifecycleRewriteResult{}, err
		}
		return LifecycleRewriteResult{ScanReport: report}, ErrLifecycleRewriteHasNoLiveRows
	}
	createdID := objectio.NewObjectidWithSegmentIDAndNum(
		&options.LiveSegmentID,
		0,
	)
	created := objectio.NewObjectStatsWithObjectID(
		createdID,
		false,
		true,
		true,
	)
	if err := objectio.SetObjectStatsRowCnt(created, 1); err != nil {
		return LifecycleRewriteResult{}, err
	}
	booking, err := options.BookingPath(0)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	report := lifecyclepkg.NewObjectScanReport(1, uint64(table.batch.RowCount()))
	if err := report.ObserveClassifiedBlock(
		table.batch.RowCount(),
		nil,
		expired,
	); err != nil {
		return LifecycleRewriteResult{}, err
	}
	if report.ExpiredRows == 0 {
		return LifecycleRewriteResult{ScanReport: report}, ErrLifecycleNoExpiredRows
	}
	if report.LiveRows == 0 {
		return LifecycleRewriteResult{ScanReport: report}, ErrLifecycleRewriteHasNoLiveRows
	}
	return LifecycleRewriteResult{
		CreatedObjectStats:      [][]byte{created.Clone().Marshal()},
		TransferBookingLocation: []string{booking},
		TransferMappingDigest:   [32]byte{1},
		MergeLevel:              1,
		ScanReport:              report,
	}, nil
}

func (table *lifecycleProcessorTable) LifecycleDiscoverObjectPage(
	context.Context,
	lifecyclepkg.DiscoveryRequest,
) (lifecyclepkg.DiscoveryPage, error) {
	return lifecyclepkg.DiscoveryPage{}, nil
}

func (table *lifecycleProcessorTable) LifecycleSelectProtectionSet(
	_ context.Context,
	_ types.TS,
	sources []objectio.ObjectEntry,
	_ logtailreplay.LifecycleTombstoneSelectionLimits,
) (lifecyclepkg.ProtectionSet, error) {
	data := make([]objectio.ObjectStats, len(sources))
	for index := range sources {
		data[index] = sources[index].ObjectStats
	}
	return lifecyclepkg.ProtectionSet{
		DataSources:         data,
		ProtectedObjects:    data,
		SourceSetDigest:     lifecycleSourceDigest(sources),
		ProtectionSetDigest: sha256ObjectStats(data),
	}, nil
}

func (*lifecycleProcessorTable) LifecycleSortKeyOrdinal() int {
	return -1
}

func sha256ObjectStats(stats []objectio.ObjectStats) [32]byte {
	var result [32]byte
	for _, value := range stats {
		for index := range result {
			result[index] ^= value[index%len(value)]
		}
	}
	return result
}

type lifecycleProcessorProtection struct {
	released bool
	statErr  error
}

func (client *lifecycleProcessorProtection) Register(
	_ context.Context,
	jobID string,
	_ []objectio.ObjectStats,
	_ time.Time,
) error {
	if jobID == "" {
		return errors.New("empty job")
	}
	return nil
}

func (client *lifecycleProcessorProtection) StatExact(
	context.Context,
	[]objectio.ObjectStats,
) error {
	return client.statErr
}

func (*lifecycleProcessorProtection) Renew(
	context.Context,
	string,
	time.Time,
) error {
	return nil
}

func (client *lifecycleProcessorProtection) Release(context.Context, string) error {
	client.released = true
	return nil
}

type lifecycleProcessorFinalizer struct {
	calls   int
	request LifecycleFinalizeRequest
	err     error
}

func (finalizer *lifecycleProcessorFinalizer) Finalize(
	_ context.Context,
	request LifecycleFinalizeRequest,
) error {
	finalizer.calls++
	finalizer.request = request
	if err := validateLifecycleFinalizeRequest(request); err != nil {
		return err
	}
	return finalizer.err
}

type lifecycleProcessorArchiveStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	puts      int
	beforePut func(string) error
}

func newLifecycleProcessorArchiveStore() *lifecycleProcessorArchiveStore {
	return &lifecycleProcessorArchiveStore{
		objects: make(map[string][]byte),
	}
}

func (store *lifecycleProcessorArchiveStore) Put(
	_ context.Context,
	key string,
	value []byte,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.beforePut != nil {
		if err := store.beforePut(key); err != nil {
			return err
		}
	}
	if previous, exists := store.objects[key]; exists &&
		string(previous) != string(value) {
		return fmt.Errorf("immutable collision")
	}
	store.objects[key] = append([]byte(nil), value...)
	store.puts++
	return nil
}

func (store *lifecycleProcessorArchiveStore) Get(
	_ context.Context,
	key string,
) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, exists := store.objects[key]
	if !exists {
		return nil, fmt.Errorf("not found")
	}
	return append([]byte(nil), value...), nil
}

func (store *lifecycleProcessorArchiveStore) Stat(
	_ context.Context,
	key string,
) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, exists := store.objects[key]
	if !exists {
		return 0, fmt.Errorf("not found")
	}
	return int64(len(value)), nil
}

func (store *lifecycleProcessorArchiveStore) GetExact(
	ctx context.Context,
	key string,
	size int64,
) ([]byte, error) {
	value, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if int64(len(value)) != size {
		return nil, fmt.Errorf("size changed")
	}
	return value, nil
}

type lifecycleProcessorRootStore struct {
	mu          sync.Mutex
	roots       map[string]lifecyclepkg.CleanupRoot
	capacityErr error
}

func newLifecycleProcessorRootStore() *lifecycleProcessorRootStore {
	return &lifecycleProcessorRootStore{
		roots: make(map[string]lifecyclepkg.CleanupRoot),
	}
}

func (store *lifecycleProcessorRootStore) Register(
	_ context.Context,
	root lifecyclepkg.CleanupRoot,
) error {
	if err := lifecyclepkg.ValidateCleanupRoot(root); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.roots[root.RootID]; exists {
		return errors.New("duplicate root")
	}
	store.roots[root.RootID] = root
	return nil
}

func (store *lifecycleProcessorRootStore) Get(
	_ context.Context,
	rootID string,
) (lifecyclepkg.CleanupRoot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	root, exists := store.roots[rootID]
	if !exists {
		return lifecyclepkg.CleanupRoot{}, errors.New("missing root")
	}
	return root, nil
}

func (store *lifecycleProcessorRootStore) HasUnresolvedSource(
	_ context.Context,
	ownerAccountID uint32,
	physicalTableID uint64,
	sourceSetDigest [32]byte,
) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, root := range store.roots {
		if root.OwnerAccountID != ownerAccountID ||
			root.PhysicalTableID != physicalTableID ||
			root.SourceSetDigest != sourceSetDigest {
			continue
		}
		if root.State == lifecyclepkg.CleanupRootFinalizing ||
			root.State == lifecyclepkg.CleanupRootCommitUnknown {
			return true, nil
		}
	}
	return false, nil
}

func (store *lifecycleProcessorRootStore) CheckCreateCapacity(
	_ context.Context,
	_ int,
	_ uint64,
	_ uint64,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.capacityErr
}

func (store *lifecycleProcessorRootStore) Transition(
	_ context.Context,
	rootID string,
	attemptID string,
	executorEpoch uint64,
	from lifecyclepkg.CleanupRootState,
	expectedVersion uint64,
	to lifecyclepkg.CleanupRootState,
) (lifecyclepkg.CleanupRoot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	root, exists := store.roots[rootID]
	if !exists ||
		root.AttemptID != attemptID ||
		root.ExecutorEpoch != executorEpoch ||
		root.State != from ||
		root.StateVersion != expectedVersion {
		return lifecyclepkg.CleanupRoot{}, errors.New("root CAS failed")
	}
	root.State = to
	root.StateVersion++
	store.roots[rootID] = root
	return root, nil
}

func (store *lifecycleProcessorRootStore) UpdateCleanup(
	_ context.Context,
	root lifecyclepkg.CleanupRoot,
	expectedVersion uint64,
) (lifecyclepkg.CleanupRoot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.roots[root.RootID]
	if !exists ||
		current.AttemptID != root.AttemptID ||
		current.State != root.State ||
		current.StateVersion != expectedVersion {
		return lifecyclepkg.CleanupRoot{}, errors.New("root update CAS failed")
	}
	root.StateVersion++
	store.roots[root.RootID] = root
	return root, nil
}

func (store *lifecycleProcessorRootStore) snapshot() map[string]lifecyclepkg.CleanupRoot {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make(map[string]lifecyclepkg.CleanupRoot, len(store.roots))
	for key, value := range store.roots {
		result[key] = value
	}
	return result
}

type lifecycleTestFaults map[lifecyclepkg.FaultPoint]error

func (faults lifecycleTestFaults) Inject(
	_ context.Context,
	point lifecyclepkg.FaultPoint,
) error {
	return faults[point]
}

type lifecycleProcessorCleanupStore struct {
	objects map[string][]byte
	listErr error
}

func (store *lifecycleProcessorCleanupStore) List(
	_ context.Context,
	prefix string,
) ([]string, error) {
	if store.listErr != nil {
		return nil, store.listErr
	}
	keys := make([]string, 0)
	for key := range store.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (store *lifecycleProcessorCleanupStore) Delete(
	_ context.Context,
	key string,
) error {
	delete(store.objects, key)
	return nil
}
