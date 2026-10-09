// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

var (
	watermarkOwnerNumber = regexp.MustCompile(`([0-9]+) AS owner_generation`)
	watermarkOwnerClaim  = regexp.MustCompile(`GREATEST\(owner_generation, ([0-9]+)\)`)
)

type takeoverCheckpointExecutor struct {
	ownerGeneration  uint64
	durableWatermark types.TS
	onCheckpoint     func()
	onQuery          func()
	sourceGeneration uint64
	queries          int
	claims           int
}

type legacyWatermarkProgressExecutor struct {
	found      bool
	watermark  string
	generation uint64
}

func (e *legacyWatermarkProgressExecutor) Exec(context.Context, string, ie.SessionOverrideOptions) error {
	return strconv.ErrSyntax
}

func (e *legacyWatermarkProgressExecutor) Query(_ context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	if !strings.HasPrefix(sql, "SELECT watermark, source_table_id FROM `mo_catalog`.`mo_cdc_watermark`") {
		return &InternalExecResultForTest{err: strconv.ErrSyntax}
	}
	if !e.found {
		return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{}}
	}
	return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: [][]interface{}{{e.watermark, strconv.FormatUint(e.generation, 10)}}}}
}

func (*legacyWatermarkProgressExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func TestGetWatermarkProgressIfExistsDistinguishesLegacyResume(t *testing.T) {
	key := &WatermarkKey{AccountId: 1, TaskId: "legacy", DBName: "db", TableName: "table"}

	withProgress := &legacyWatermarkProgressExecutor{found: true, watermark: types.BuildTS(200, 3).ToString(), generation: 0}
	updater := NewCDCWatermarkUpdater(t.Name()+"-with-progress", withProgress)
	wm, generation, found, err := updater.GetWatermarkProgressIfExists(context.Background(), key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, types.BuildTS(200, 3), wm)
	require.Zero(t, generation)

	withoutProgress := &legacyWatermarkProgressExecutor{}
	updater = NewCDCWatermarkUpdater(t.Name()+"-without-progress", withoutProgress)
	wm, generation, found, err = updater.GetWatermarkProgressIfExists(context.Background(), key)
	require.NoError(t, err)
	require.False(t, found)
	require.True(t, wm.IsEmpty())
	require.Zero(t, generation)
}

func (e *takeoverCheckpointExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` SET owner_generation") {
		e.claims++
		match := watermarkOwnerClaim.FindStringSubmatch(sql)
		if len(match) != 2 {
			return strconv.ErrSyntax
		}
		candidate, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return err
		}
		if candidate > e.ownerGeneration {
			e.ownerGeneration = candidate
		}
		return nil
	}
	if strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` AS w") {
		if e.onCheckpoint != nil {
			callback := e.onCheckpoint
			e.onCheckpoint = nil
			callback()
		}
		match := watermarkOwnerNumber.FindStringSubmatch(sql)
		if len(match) != 2 {
			return strconv.ErrSyntax
		}
		candidate, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return err
		}
		if candidate == e.ownerGeneration {
			e.durableWatermark = types.BuildTS(200, 0)
		}
		return nil
	}
	return strconv.ErrSyntax
}

// Freeze the result before invoking the callback: it represents a SELECT that
// completed before the interleaved publication, not a reread of mutable state.
func (e *takeoverCheckpointExecutor) Query(_ context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	e.queries++
	generation := e.sourceGeneration
	if generation == 0 {
		generation = 11
	}
	var data [][]interface{}
	switch {
	case strings.HasPrefix(sql, "SELECT owner_generation, watermark, source_table_id"):
		data = [][]interface{}{{strconv.FormatUint(e.ownerGeneration, 10), e.durableWatermark.ToString(), strconv.FormatUint(generation, 10)}}
	case strings.HasPrefix(sql, "SELECT watermark, source_table_id FROM `mo_catalog`.`mo_cdc_watermark`"):
		data = [][]interface{}{{e.durableWatermark.ToString(), strconv.FormatUint(generation, 10)}}
	default:
		return &InternalExecResultForTest{err: strconv.ErrSyntax}
	}
	if e.onQuery != nil {
		callback := e.onQuery
		e.onQuery = nil
		callback()
	}
	return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: data}}
}

func (*takeoverCheckpointExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

type restartDeletedCheckpointExecutor struct {
	rowExists        bool
	ownerGeneration  uint64
	durableWatermark types.TS
	beforeCheckpoint func()
}

func (e *restartDeletedCheckpointExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	isInsert := strings.HasPrefix(sql, "INSERT INTO `mo_catalog`.`mo_cdc_watermark`")
	isUpdate := strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` AS w")
	if !isInsert && !isUpdate {
		return strconv.ErrSyntax
	}
	if e.beforeCheckpoint != nil {
		callback := e.beforeCheckpoint
		e.beforeCheckpoint = nil
		callback()
	}
	match := watermarkOwnerNumber.FindStringSubmatch(sql)
	if len(match) != 2 {
		return strconv.ErrSyntax
	}
	candidate, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return err
	}
	if !e.rowExists {
		if isInsert {
			e.rowExists = true
			e.ownerGeneration = candidate
			e.durableWatermark = types.BuildTS(200, 0)
		}
		return nil
	}
	if candidate == e.ownerGeneration {
		e.durableWatermark = types.BuildTS(200, 0)
	}
	return nil
}

func (e *restartDeletedCheckpointExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	return &InternalExecResultForTest{err: strconv.ErrSyntax}
}

func (*restartDeletedCheckpointExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func TestRestartDeletedWatermarkCannotBeRecreatedByStableCheckpoint(t *testing.T) {
	ctx := context.Background()
	owner := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
	store := &restartDeletedCheckpointExecutor{
		rowExists:       true,
		ownerGeneration: owner.GenerationToken(),
	}
	updater := NewCDCWatermarkUpdater(t.Name(), store)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}
	watermark := types.BuildTS(200, 0)
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(ctx, owner, 11), key, &watermark))
	store.beforeCheckpoint = func() {
		// The async writer has already passed its owner precheck. Model RESTART
		// committing the deliberate watermark deletion before this SQL executes.
		store.rowExists = false
		store.ownerGeneration = 0
		store.durableWatermark = types.TS{}
	}
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(ctx))
	_, err := updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.False(t, store.rowExists, "a delayed stable checkpoint recreated the RESTART-deleted row")
	require.True(t, store.durableWatermark.IsEmpty())
}

func TestSnapshotTakeoverRejectsPreviousOwnerCheckpoint(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)
	pool := fileservice.NewPool(1, func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() }, func(p *types.Packer) { p.Close() })
	sink := newTransactionalSnapshotSinker()
	store := &takeoverCheckpointExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), store)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}
	S, T := types.BuildTS(100, 0), types.BuildTS(200, 0)
	ownerA := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
	ownerB := NewOwnerFenceForGeneration(time.UnixMicro(200), func(context.Context) error { return nil })
	_, _, err = updater.ClaimWatermarkOwner(ctx, key, ownerA)
	require.NoError(t, err)

	tmA := NewTransactionManager(sink, updater, 1, "task1", "db1", "table1")
	tmA.SetOwnerFence(ownerA)
	tmA.SetWatermarkGeneration(11)
	dpA := NewDataProcessor(sink, tmA, mp, pool, 1, 0, 1, 0, true, 1, "task1", "db1", "table1")
	defer dpA.Cleanup()
	dpA.SetTransactionRange(types.TS{}, S)
	require.NoError(t, dpA.ProcessChange(ctx, &ChangeData{Type: ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, mp, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}, S)}))
	require.NoError(t, dpA.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))
	dpA.SetTransactionRange(S, T)
	require.NoError(t, dpA.ProcessChange(ctx, &ChangeData{Type: ChangeTypeTailDone,
		DeleteBatch: buildBatch(t, mp, []int32{1}, T)}))
	require.NoError(t, dpA.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))
	require.NotContains(t, sink.durableKeys(), int32(1))

	store.onCheckpoint = func() {
		watermark, generation, err := updater.ClaimWatermarkOwner(ctx, key, ownerB)
		require.NoError(t, err)
		_, staleCommitting := updater.cacheCommitting[*key]
		require.False(t, staleCommitting, "takeover must remove the previous owner's higher-priority local cache")
		require.True(t, watermark.IsEmpty())
		require.Equal(t, uint64(11), generation)

		tmB := NewTransactionManager(sink, updater, 1, "task1", "db1", "table1")
		tmB.SetOwnerFence(ownerB)
		tmB.SetWatermarkGeneration(11)
		dpB := NewDataProcessor(sink, tmB, mp, pool, 1, 0, 1, 0, true, 1, "task1", "db1", "table1")
		defer dpB.Cleanup()
		dpB.SetTransactionRange(types.TS{}, S)
		for k := int32(1); k <= 9; k++ {
			require.NoError(t, dpB.ProcessChange(ctx, &ChangeData{Type: ChangeTypeSnapshot,
				InsertBatch: buildBatch(t, mp, []int32{k}, S)}))
		}
		require.NoError(t, tmB.EnsureCleanup(ctx))
	}
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(ctx))
	_, err = updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.True(t, store.durableWatermark.IsEmpty(), "the previous owner's checkpoint must lose to takeover admission")
	cached := updater.cacheCommitted[*key]
	require.True(t, cached.IsEmpty(), "the rejected checkpoint must not poison this CN's cache")
	require.Contains(t, sink.durableKeys(), int32(1), "the replacement really committed a partial replay")
}

func TestSnapshotOwnerGenerationCannotMoveBackward(t *testing.T) {
	ctx := context.Background()
	store := &takeoverCheckpointExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), store)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}
	older := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
	newer := NewOwnerFenceForGeneration(time.UnixMicro(200), func(context.Context) error { return nil })

	_, _, err := updater.ClaimWatermarkOwner(ctx, key, newer)
	require.NoError(t, err)
	_, _, err = updater.ClaimWatermarkOwner(ctx, key, older)
	require.Error(t, err)
	require.True(t, IsOwnerFenceLostError(err))
	require.Equal(t, uint64(200), store.ownerGeneration)
	require.Same(t, newer, updater.activeWatermarkFence[*key])
}

func TestDelayedOwnerAdmissionCannotReplaceNewerLocalFence(t *testing.T) {
	ctx := context.Background()
	store := &takeoverCheckpointExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), store)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}
	older := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
	newer := NewOwnerFenceForGeneration(time.UnixMicro(200), func(context.Context) error { return nil })

	// Model an old admission that completed its durable read before a newer
	// owner published locally, but reached the local publication point later.
	updater.Lock()
	require.True(t, updater.activateWatermarkFenceLocked(*key, newer))
	require.False(t, updater.activateWatermarkFenceLocked(*key, older))
	updater.Unlock()
	require.Same(t, newer, updater.activeWatermarkFence[*key])
	require.NoError(t, newer.Check(ctx))
}

// A remote claim wins after the local fence check: a remote claim wins after the local fence check.
func TestRemoteTakeoverMustNotPromoteNoOpCheckpoint(t *testing.T) {
	for _, read := range []string{"typed-read", "owner-claim"} {
		for _, evict := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/evict=%t", read, evict), func(t *testing.T) {
				ctx := context.Background()
				durable, attempted := types.BuildTS(100, 0), types.BuildTS(200, 0)
				store := &takeoverCheckpointExecutor{durableWatermark: durable}
				updater := NewCDCWatermarkUpdater(t.Name(), store, WithCustomizedScheduleJob(func(job *UpdaterJob) error { job.DoneWithErr(nil); return nil }))
				updater.Start()
				t.Cleanup(updater.Stop)
				key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}
				oldOwner := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
				_, _, err := updater.ClaimWatermarkOwner(ctx, key, oldOwner)
				require.NoError(t, err)
				require.NoError(t, updater.UpdateWatermarkOnly(WithWatermarkOwnerFence(ctx, oldOwner, 11), key, &attempted))
				// The other CN has a different updater: only durable ownership changes here.
				store.onCheckpoint = func() { store.ownerGeneration = 200 }
				updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(ctx))
				_, err = updater.execBatchUpdateWM()
				require.NoError(t, err)
				require.Equal(t, durable, store.durableWatermark, "guarded UPDATE must be a no-op")
				require.Equal(t, attempted, updater.cacheCommitted[*key], "exercise the real optimistic publication path")
				if evict {
					require.NoError(t, updater.EvictTaskLocalStateForOwner(ctx, key.AccountId, key.TaskId, oldOwner.GenerationToken()))
				}
				var got types.TS
				var generation uint64
				if read == "typed-read" {
					got, generation, err = updater.GetWatermarkProgress(ctx, key)
				} else {
					newOwner := NewOwnerFenceForGeneration(time.UnixMicro(300), func(context.Context) error { return nil })
					got, generation, err = updater.ClaimWatermarkOwner(ctx, key, newOwner)
				}
				require.NoError(t, err)
				require.Equal(t, durable, got, "a rejected old-owner checkpoint is not a durable replay boundary")
				require.Equal(t, uint64(11), generation)
				require.Equal(t, durable, updater.cacheCommitted[*key])
				require.Empty(t, updater.progressReads)
			})
		}
	}

}

func TestDurableProgressPublicationInterleavings(t *testing.T) {
	for _, claim := range []bool{false, true} {
		for _, scenario := range []string{"ack", "ack-then-noop", "replacement", "retirement-reuse", "eviction", "deletion", "cancel", "two-conflicts", "unrelated-key"} {
			t.Run(fmt.Sprintf("claim=%t/%s", claim, scenario), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "t"}
				store := &takeoverCheckpointExecutor{durableWatermark: types.BuildTS(100, 0)}
				u := NewCDCWatermarkUpdater(t.Name(), store, WithCustomizedScheduleJob(func(job *UpdaterJob) error { job.DoneWithErr(nil); return nil }))
				owner := NewOwnerFenceForGeneration(time.UnixMicro(100), func(context.Context) error { return nil })
				_, _, err := u.ClaimWatermarkOwner(ctx, key, owner)
				require.NoError(t, err)
				store.queries, store.claims = 0, 0
				confirmed := types.BuildTS(20, 0)
				acknowledge := func() {
					store.durableWatermark, store.sourceGeneration = confirmed, 12
					require.NoError(t, u.finishTargetAcknowledgement(ctx, key, owner, confirmed, 12))
				}
				store.onQuery = func() {
					switch scenario {
					case "ack", "ack-then-noop", "two-conflicts":
						acknowledge()
						if scenario == "ack-then-noop" {
							// A remote claim rejects a later buffered checkpoint. The cache is
							// optimistic again even though the earlier ACK was confirmed.
							attempted := types.BuildTS(200, 0)
							require.NoError(t, u.UpdateWatermarkOnly(WithWatermarkOwnerFence(ctx, owner, 12), key, &attempted))
							store.onCheckpoint = func() { store.ownerGeneration = 200 }
							u.committingBuffer = append(u.committingBuffer, NewCommittingWMJob(ctx))
							_, err := u.execBatchUpdateWM()
							require.NoError(t, err)
							require.Equal(t, attempted, u.cacheCommitted[*key])
						}
						if scenario == "two-conflicts" {
							store.onQuery = acknowledge
						}
					case "replacement":
						store.durableWatermark = confirmed
						replacement := NewOwnerFenceForGeneration(time.UnixMicro(200), func(context.Context) error { return nil })
						_, _, err := u.ClaimWatermarkOwner(ctx, key, replacement)
						require.NoError(t, err)
					case "retirement-reuse":
						u.Lock()
						u.retireWatermarkProgressLocked(*key)
						u.Unlock()
						store.durableWatermark = confirmed
						_, _, err := u.GetWatermarkProgress(ctx, key)
						require.NoError(t, err)
					case "eviction":
						// No cache tier or active fence remains: cleanup must discover
						// the key from the outstanding-read inventory alone.
						u.Lock()
						delete(u.cacheCommitted, *key)
						delete(u.cacheCommittedGeneration, *key)
						delete(u.activeWatermarkFence, *key)
						u.Unlock()
						require.NoError(t, u.EvictTaskLocalStateForOwner(ctx, key.AccountId, key.TaskId, owner.GenerationToken()))
					case "deletion":
						u.MarkTaskDeleted(key.TaskId)
						u.Lock()
						u.retireWatermarkProgressLocked(*key)
						u.Unlock()
					case "cancel":
						cancel()
					case "unrelated-key":
						other := *key
						other.TableName = "other"
						u.Lock()
						require.True(t, u.activateWatermarkFenceLocked(other, owner))
						u.Unlock()
						require.NoError(t, u.finishTargetAcknowledgement(ctx, &other, owner, confirmed, 12))
					}
				}
				var got types.TS
				var generation uint64
				if claim {
					got, generation, err = u.ClaimWatermarkOwner(ctx, key, owner)
				} else {
					got, generation, err = u.GetWatermarkProgress(ctx, key)
				}
				switch {
				case scenario == "cancel":
					require.ErrorIs(t, err, context.Canceled)
				case scenario == "deletion" || scenario == "eviction" || scenario == "retirement-reuse" || scenario == "two-conflicts":
					require.True(t, IsRetryableSnapshotEpochError(err), "%v", err)
				case claim && (scenario == "replacement" || scenario == "ack-then-noop"):
					require.True(t, IsOwnerFenceLostError(err), "%v", err)
				default:
					require.NoError(t, err)
					if scenario == "unrelated-key" {
						require.Equal(t, types.BuildTS(100, 0), got)
						require.Equal(t, uint64(11), generation)
						require.Equal(t, 1, store.queries)
					} else {
						require.Equal(t, confirmed, got)
						expectedGeneration := store.sourceGeneration
						if expectedGeneration == 0 {
							expectedGeneration = 11
						}
						require.Equal(t, expectedGeneration, generation)
					}
				}
				if scenario == "deletion" || scenario == "eviction" {
					require.NotContains(t, u.cacheCommitted, *key)
				}
				if scenario == "retirement-reuse" {
					require.Equal(t, confirmed, u.cacheCommitted[*key])
				}
				if scenario == "two-conflicts" {
					require.Equal(t, 2, store.queries)
					require.Equal(t, confirmed, u.cacheCommitted[*key])
				}
				if scenario != "replacement" {
					require.LessOrEqual(t, store.queries, 2)
				}
				if claim && scenario != "replacement" {
					require.Equal(t, 1, store.claims, "reread must not repeat the owner UPDATE")
				}
				require.Empty(t, u.progressReads)
			})
		}
	}
}
