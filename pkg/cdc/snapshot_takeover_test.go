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
}

func (e *takeoverCheckpointExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` SET owner_generation") {
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

func (e *takeoverCheckpointExecutor) Query(_ context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	switch {
	case strings.HasPrefix(sql, "SELECT owner_generation, watermark, source_table_id"):
		return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: [][]interface{}{{strconv.FormatUint(e.ownerGeneration, 10), e.durableWatermark.ToString(), "11"}}}}
	case strings.HasPrefix(sql, "SELECT watermark, source_table_id FROM `mo_catalog`.`mo_cdc_watermark`"):
		return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: [][]interface{}{{e.durableWatermark.ToString(), "11"}}}}
	default:
		return &InternalExecResultForTest{err: strconv.ErrSyntax}
	}
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
