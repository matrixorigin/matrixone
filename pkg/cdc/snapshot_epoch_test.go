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
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type snapshotEpochTestExecutor struct {
	mu                sync.Mutex
	epochs            map[uint64]string
	waitForTwoInserts chan struct{}
	barrierOnce       sync.Once
	waitForPreflights chan struct{}
	preflightOnce     sync.Once
	preflightCount    int
	insertErr         error
	commitOnInsertErr bool
	queryErr          error
}

var (
	snapshotEpochSelectID = regexp.MustCompile("source_table_id = ([0-9]+)$")
	snapshotEpochDeleteID = regexp.MustCompile("source_table_id < ([0-9]+)$")
	snapshotEpochOtherID  = regexp.MustCompile("source_table_id <> ([0-9]+) ORDER BY source_table_id DESC LIMIT 1$")
	snapshotEpochInsert   = regexp.MustCompile(
		"VALUES \\([0-9]+, '[^']*', '[^']*', '[^']*', ([0-9]+), '([^']+)'\\)(?: ON DUPLICATE KEY UPDATE.*)?$")
)

func newSnapshotEpochTestExecutor() *snapshotEpochTestExecutor {
	return &snapshotEpochTestExecutor{epochs: make(map[uint64]string)}
}

func (e *snapshotEpochTestExecutor) Exec(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) error {
	switch {
	case strings.HasPrefix(sql, "DELETE FROM") && strings.Contains(sql, "mo_cdc_snapshot"):
		match := snapshotEpochDeleteID.FindStringSubmatch(sql)
		if len(match) != 2 {
			return strconv.ErrSyntax
		}
		currentTableID, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return err
		}
		e.mu.Lock()
		for tableID := range e.epochs {
			if tableID < currentTableID {
				delete(e.epochs, tableID)
			}
		}
		e.mu.Unlock()
		return nil
	case strings.HasPrefix(sql, "INSERT INTO") && strings.Contains(sql, "mo_cdc_snapshot"):
		match := snapshotEpochInsert.FindStringSubmatch(sql)
		if len(match) != 3 {
			return strconv.ErrSyntax
		}
		tableID, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return err
		}
		e.mu.Lock()
		if e.insertErr != nil && !e.commitOnInsertErr {
			err := e.insertErr
			e.mu.Unlock()
			return err
		}
		if _, ok := e.epochs[tableID]; !ok {
			e.epochs[tableID] = match[2]
		}
		if e.waitForTwoInserts != nil && len(e.epochs) == 2 {
			e.barrierOnce.Do(func() { close(e.waitForTwoInserts) })
		}
		err = e.insertErr
		e.mu.Unlock()
		return err
	default:
		return strconv.ErrSyntax
	}
}

func (e *snapshotEpochTestExecutor) Query(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	if e.queryErr != nil {
		return &InternalExecResultForTest{err: e.queryErr}
	}
	if match := snapshotEpochOtherID.FindStringSubmatch(sql); len(match) == 2 {
		tableID, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return &InternalExecResultForTest{err: err}
		}
		e.mu.Lock()
		var highestOtherID uint64
		for existingID := range e.epochs {
			if existingID != tableID && existingID > highestOtherID {
				highestOtherID = existingID
			}
		}
		preflightBarrier := e.waitForPreflights
		if preflightBarrier != nil && len(e.epochs) == 0 {
			e.preflightCount++
			if e.preflightCount == 2 {
				e.preflightOnce.Do(func() { close(preflightBarrier) })
			}
		}
		e.mu.Unlock()
		if preflightBarrier != nil && highestOtherID == 0 {
			<-preflightBarrier
		}
		if highestOtherID != 0 {
			return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{
				Data: [][]interface{}{{strconv.FormatUint(highestOtherID, 10)}},
			}}
		}
		return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{}}
	}
	match := snapshotEpochSelectID.FindStringSubmatch(sql)
	if len(match) != 2 {
		return &InternalExecResultForTest{err: strconv.ErrSyntax}
	}
	tableID, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return &InternalExecResultForTest{err: err}
	}
	e.mu.Lock()
	epoch, ok := e.epochs[tableID]
	barrier := e.waitForTwoInserts
	e.mu.Unlock()
	if ok && barrier != nil {
		<-barrier
	}
	data := [][]interface{}{}
	if ok {
		data = append(data, []interface{}{epoch})
	}
	return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: data}}
}

func (e *snapshotEpochTestExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func (e *snapshotEpochTestExecutor) epoch(tableID uint64) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	epoch, ok := e.epochs[tableID]
	return epoch, ok
}

func (e *snapshotEpochTestExecutor) epochCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.epochs)
}

func TestInitialSnapshotEpochPersistsPerTableGeneration(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	key := &WatermarkKey{
		AccountId: 7,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	first := types.BuildTS(100, 3)
	later := types.BuildTS(900, 8)

	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	epoch, changed, err := updater.GetOrCreateInitialSnapshotEpochForGeneration(
		context.Background(), key, 11, first)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, first, epoch)
	stored, ok := executor.epoch(11)
	require.True(t, ok)
	require.Equal(t, first.ToString(), stored)

	// A restart after an intermediate target commit must not select the newer
	// candidate. A new updater models the new executor process with no cache.
	restarted := NewCDCWatermarkUpdater(t.Name()+"-restart", executor)
	epoch, changed, err = restarted.GetOrCreateInitialSnapshotEpochForGeneration(
		context.Background(), key, 11, later)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, first, epoch)
	stored, ok = executor.epoch(11)
	require.True(t, ok)
	require.Equal(t, first.ToString(), stored)

	// Recreating or truncating the table changes its source ID. The retired
	// generation remains as an immutable retry anchor while the new generation
	// receives a fresh epoch. Task cancellation/drop cleans up both rows.
	epoch, changed, err = restarted.GetOrCreateInitialSnapshotEpochForGeneration(
		context.Background(), key, 12, later)
	require.NoError(t, err)
	require.True(t, changed, "a fresh owner must reset target rows from the retired generation")
	require.Equal(t, later, epoch)
	stored, ok = executor.epoch(12)
	require.True(t, ok)
	require.Equal(t, later.ToString(), stored)
	stored, ok = executor.epoch(11)
	require.True(t, ok)
	require.Equal(t, first.ToString(), stored)
}

func TestInitialSnapshotEpochStateDistinguishesCreateFromRestart(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	candidate := types.BuildTS(100, 3)

	state, err := updater.GetOrCreateInitialSnapshotEpochState(
		context.Background(), key, 11, candidate)
	require.NoError(t, err)
	require.True(t, state.Created)
	require.False(t, state.HasOtherGeneration)
	require.Equal(t, candidate, state.Epoch)

	state, err = updater.GetOrCreateInitialSnapshotEpochState(
		context.Background(), key, 11, types.BuildTS(900, 8))
	require.NoError(t, err)
	require.False(t, state.Created)
	require.False(t, state.HasOtherGeneration)
	require.Equal(t, candidate, state.Epoch)
}

func TestInitialSnapshotEpochMissingWithProgressRemainsRejected(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	candidate := types.BuildTS(300, 0)

	for attempt := 0; attempt < 2; attempt++ {
		_, err := updater.GetOrCreateInitialSnapshotEpochStateForProgress(
			context.Background(), key, 11, candidate, types.BuildTS(200, 0), 11)
		require.ErrorContains(t, err, "metadata is missing")
		_, exists := executor.epoch(11)
		require.False(t, exists, "a rejected admission must not create metadata trusted by its retry")
	}
}

func TestInitialSnapshotEpochAllowsNewSourceGenerationProgress(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	_, err := updater.GetOrCreateInitialSnapshotEpochState(
		context.Background(), key, 10, types.BuildTS(100, 0))
	require.NoError(t, err)

	state, err := updater.GetOrCreateInitialSnapshotEpochStateForProgress(
		context.Background(), key, 11, types.BuildTS(300, 0), types.BuildTS(200, 0), 10)
	require.NoError(t, err)
	require.True(t, state.Created)
	require.True(t, state.HasOtherGeneration)
	require.Equal(t, types.BuildTS(300, 0), state.Epoch)
}

func TestInitialSnapshotEpochRejectsStaleSourceBeforeInsert(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}

	_, err := updater.GetOrCreateInitialSnapshotEpochStateForProgress(
		context.Background(), key, 11, types.BuildTS(300, 0), types.BuildTS(200, 0), 12)
	require.Error(t, err)
	require.True(t, IsRetryableSnapshotEpochError(err))
	_, exists := executor.epoch(11)
	require.False(t, exists, "a stale source view must not add a retired epoch row")
}

func TestInitialSnapshotEpochAmbiguousInsertRecovery(t *testing.T) {
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	candidate := types.BuildTS(100, 3)

	t.Run("committed response lost", func(t *testing.T) {
		executor := newSnapshotEpochTestExecutor()
		executor.insertErr = errors.New("connection lost")
		executor.commitOnInsertErr = true
		updater := NewCDCWatermarkUpdater(t.Name(), executor)
		state, err := updater.GetOrCreateInitialSnapshotEpochState(context.Background(), key, 11, candidate)
		require.NoError(t, err)
		require.True(t, state.Created)
		require.Equal(t, candidate, state.Epoch)
	})

	t.Run("not committed remains retryable", func(t *testing.T) {
		executor := newSnapshotEpochTestExecutor()
		executor.insertErr = errors.New("write rejected")
		updater := NewCDCWatermarkUpdater(t.Name(), executor)
		_, err := updater.GetOrCreateInitialSnapshotEpoch(context.Background(), key, 11, candidate)
		require.Error(t, err)
		require.True(t, IsRetryableSnapshotEpochError(err))
	})
}

func TestInitialSnapshotEpochBackendReadFailureClassification(t *testing.T) {
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	candidate := types.BuildTS(100, 3)

	t.Run("backend read is retryable", func(t *testing.T) {
		backendErr := errors.New("catalog unavailable")
		executor := newSnapshotEpochTestExecutor()
		executor.queryErr = backendErr
		updater := NewCDCWatermarkUpdater(t.Name(), executor)
		_, err := updater.GetOrCreateInitialSnapshotEpoch(
			context.Background(), key, 11, candidate)
		require.ErrorIs(t, err, backendErr)
		require.True(t, IsRetryableSnapshotEpochError(err))
	})

	t.Run("caller cancellation remains control flow", func(t *testing.T) {
		executor := newSnapshotEpochTestExecutor()
		executor.queryErr = context.Canceled
		updater := NewCDCWatermarkUpdater(t.Name(), executor)
		_, err := updater.GetOrCreateInitialSnapshotEpoch(
			context.Background(), key, 11, candidate)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, IsRetryableSnapshotEpochError(err))
	})
}

func TestInitialSnapshotEpochOverlappingGenerationsDoNotEraseRetryAnchors(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	executor.waitForTwoInserts = make(chan struct{})
	executor.waitForPreflights = make(chan struct{})
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}

	type result struct {
		epoch   types.TS
		changed bool
		err     error
	}
	results := make(chan result, 2)
	for tableID, candidate := range map[uint64]types.TS{
		11: types.BuildTS(100, 3),
		12: types.BuildTS(900, 8),
	} {
		go func() {
			epoch, changed, err := updater.GetOrCreateInitialSnapshotEpochForGeneration(
				context.Background(), key, tableID, candidate)
			results <- result{epoch: epoch, changed: changed, err: err}
		}()
	}

	seen := make(map[string]struct{}, 2)
	for range 2 {
		result := <-results
		require.NoError(t, result.err)
		require.True(t, result.changed)
		seen[result.epoch.ToString()] = struct{}{}
	}
	require.Contains(t, seen, types.BuildTS(100, 3).ToString())
	require.Contains(t, seen, types.BuildTS(900, 8).ToString())
	for tableID, expected := range map[uint64]types.TS{
		11: types.BuildTS(100, 3),
		12: types.BuildTS(900, 8),
	} {
		stored, ok := executor.epoch(tableID)
		require.True(t, ok)
		require.Equal(t, expected.ToString(), stored)
	}
}

func TestInitialSnapshotEpochCompactionIsMonotonicAndBounded(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}

	for tableID := uint64(11); tableID <= 40; tableID++ {
		_, _, err := updater.GetOrCreateInitialSnapshotEpochForGeneration(
			context.Background(), key, tableID, types.BuildTS(int64(tableID*100), 0))
		require.NoError(t, err)
		require.NoError(t, updater.DeleteInitialSnapshotGenerationsBefore(
			context.Background(), key, tableID))
		require.Equal(t, 1, executor.epochCount(), "completed generation must recycle retired epochs")
	}

	// A delayed older owner may run cleanup after generation 41 is published,
	// but its one-way predicate cannot delete the newer retry anchor.
	_, _, err := updater.GetOrCreateInitialSnapshotEpochForGeneration(
		context.Background(), key, 41, types.BuildTS(4100, 0))
	require.NoError(t, err)
	require.NoError(t, updater.DeleteInitialSnapshotGenerationsBefore(
		context.Background(), key, 40))
	_, newerExists := executor.epoch(41)
	require.True(t, newerExists)
	require.Equal(t, 2, executor.epochCount())

	require.NoError(t, updater.DeleteInitialSnapshotGenerationsBefore(
		context.Background(), key, 41))
	require.Equal(t, 1, executor.epochCount())
}

func TestInitialSnapshotEpochDetectsNewerGeneration(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}

	_, _, err := updater.GetOrCreateInitialSnapshotEpochForGeneration(
		context.Background(), key, 12, types.BuildTS(1200, 0))
	require.NoError(t, err)
	_, err = updater.GetOrCreateInitialSnapshotEpochState(
		context.Background(), key, 11, types.BuildTS(1100, 0))
	require.True(t, IsRetryableSnapshotEpochError(err))
	_, staleInserted := executor.epoch(11)
	require.False(t, staleInserted,
		"a known-stale detector must not grow per-table snapshot metadata")

	// A lower row may still exist from an old binary or from the narrow race
	// between the preflight and a concurrent newer insert. It must fail closed.
	executor.mu.Lock()
	executor.epochs[11] = types.BuildTS(1100, 0).ToString()
	executor.mu.Unlock()
	state, err := updater.GetOrCreateInitialSnapshotEpochState(
		context.Background(), key, 11, types.BuildTS(1101, 0))
	require.NoError(t, err)
	require.True(t, state.HasOtherGeneration)
	require.True(t, state.HasNewerGeneration,
		"a stale source-catalog view must not treat a future retry anchor as retired")
}

func TestInitialSnapshotEpochRejectsInvalidCandidate(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), newSnapshotEpochTestExecutor())
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}

	_, err := updater.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 0, types.BuildTS(1, 0))
	require.Error(t, err)
	_, err = updater.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 1, types.TS{})
	require.Error(t, err)
}

func TestSnapshotEpochSQLUsesEscapedKeys(t *testing.T) {
	key := &WatermarkKey{AccountId: 1, TaskId: "t'ask", DBName: "d'b", TableName: "t'bl"}
	sql := CDCSQLBuilder.GetSnapshotEpochSQL(key, 9)
	require.Contains(t, sql, "task_id = 't''ask'")
	require.Contains(t, sql, "db_name = 'd''b'")
	require.Contains(t, sql, "table_name = 't''bl'")
	require.Contains(t, CDCSQLBuilder.InsertSnapshotEpochSQL(key, 9, types.BuildTS(10, 1)), "ON DUPLICATE KEY UPDATE snapshot_epoch = snapshot_epoch")
	require.Contains(t, CDCSQLBuilder.ClaimWatermarkOwnerSQL(key, 123), "GREATEST(owner_generation, 123)")
	require.Contains(t, CDCSQLBuilder.GetWatermarkOwnerProgressSQL(key), "SELECT owner_generation, watermark, source_table_id")
	require.Contains(t, CDCSQLBuilder.GetHighestOtherSnapshotGenerationSQL(key, 9), "ORDER BY source_table_id DESC")
	require.Contains(t, CDCSQLBuilder.DeleteSnapshotEpochGenerationsBeforeSQL(key, 9), "source_table_id < 9")
	require.Contains(t, CDCSQLBuilder.DeleteOrphanSnapshotEpochSQL(), "LEFT JOIN `mo_catalog`.`mo_cdc_task`")
}

func TestBufferedWatermarkFromSupersededOwnerIsDropped(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(200, 1)
	fenceChecks := 0
	ctx := WithWatermarkOwnerFence(context.Background(), NewOwnerFenceForGeneration(time.UnixMicro(1), func(ctx context.Context) error {
		fenceChecks++
		return moerr.NewInvalidTask(ctx, "old-cn", 1)
	}), 11)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key, &watermark))
	updater.commitFailureCount[*key] = 2
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.Equal(t, 1, fenceChecks)
	_, exists := updater.cacheCommitted[*key]
	require.False(t, exists)
	_, hasFailures := updater.commitFailureCount[*key]
	require.False(t, hasFailures,
		"a replacement owner must not inherit the retired claim's circuit state")
}

func TestBufferedWatermarksValidateSharedOwnerOnce(t *testing.T) {
	executor := newSnapshotEpochTestExecutor()
	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	watermark := types.BuildTS(200, 1)
	fenceChecks := 0
	fence := NewOwnerFenceForGeneration(time.UnixMicro(1), func(ctx context.Context) error {
		fenceChecks++
		return moerr.NewInvalidTask(ctx, "old-cn", 1)
	})

	for _, table := range []string{"t1", "t2"} {
		key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: table}
		require.NoError(t, updater.UpdateWatermarkOnly(
			WithWatermarkOwnerFence(context.Background(), fence, 11), key, &watermark))
	}
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.Equal(t, 1, fenceChecks)
	require.Empty(t, updater.cacheCommitted)
}

func TestBufferedWatermarkKeepsNewestClaimForEqualTimestamp(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), newSnapshotEpochTestExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(200, 1)
	oldFence := NewOwnerFenceForGeneration(time.Unix(100, 0), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(time.Unix(200, 0), func(context.Context) error { return nil })

	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), oldFence, 11), key, &watermark))
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), newFence, 11), key, &watermark))
	require.Same(t, newFence, updater.cacheUncommittedFence[*key])

	// A late callback from the old in-process generation must not replace the
	// newer fence for the same idempotent watermark.
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), oldFence, 11), key, &watermark))
	require.Same(t, newFence, updater.cacheUncommittedFence[*key])
}
