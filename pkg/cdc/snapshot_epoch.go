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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const snapshotEpochPersistenceTimeout = 20 * time.Second

// RetryableSnapshotEpochError means no durable epoch was visible after an
// ambiguous catalog write. Retrying is safe: a successful but delayed INSERT
// is discovered by the read-before-write path, while an uncommitted INSERT
// simply attempts the same claim again.
type RetryableSnapshotEpochError struct {
	err error
}

// InitialSnapshotEpochState describes the durable source generation selected
// for one logical CDC table. HasNewerGeneration distinguishes a retired epoch
// from a stale source-catalog view. Created is needed by restart admission: a
// stable task with an existing watermark but no epoch row has lost protocol
// metadata and must not silently guess that the current source generation is
// safe.
type InitialSnapshotEpochState struct {
	Epoch              types.TS
	HasOtherGeneration bool
	HasNewerGeneration bool
	Created            bool
}

func (e *RetryableSnapshotEpochError) Error() string { return e.err.Error() }
func (e *RetryableSnapshotEpochError) Unwrap() error { return e.err }

func IsRetryableSnapshotEpochError(err error) bool {
	var retryable *RetryableSnapshotEpochError
	return errors.As(err, &retryable)
}

func NewRetryableSnapshotEpochError(err error) error {
	if err == nil || IsRetryableSnapshotEpochError(err) {
		return err
	}
	return &RetryableSnapshotEpochError{err: err}
}

func classifySnapshotEpochBackendError(err error) error {
	if err == nil || IsRetryableSnapshotEpochError(err) || errors.Is(err, context.Canceled) {
		return err
	}
	return NewRetryableSnapshotEpochError(err)
}

// GetOrCreateInitialSnapshotEpoch returns the durable epoch for one source
// table generation. It must complete before a reader may make a partial target
// commit. Reusing the same source table ID preserves the old epoch across
// retries. Different source table IDs are retained until the newer generation
// has successfully reset target state. At that point the newer owner may
// delete only lower table IDs; a delayed old owner can therefore never delete
// a replacement's retry anchor.
func (u *CDCWatermarkUpdater) GetOrCreateInitialSnapshotEpoch(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
) (types.TS, error) {
	epoch, _, err := u.GetOrCreateInitialSnapshotEpochForGeneration(ctx, key, sourceTableID, candidate)
	return epoch, err
}

// GetOrCreateInitialSnapshotEpochForGeneration also reports whether a durable
// epoch exists for another incarnation of the same logical table. Callers that
// must distinguish retired from newer generations use the state-returning API.
func (u *CDCWatermarkUpdater) GetOrCreateInitialSnapshotEpochForGeneration(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
) (types.TS, bool, error) {
	state, err := u.GetOrCreateInitialSnapshotEpochState(ctx, key, sourceTableID, candidate)
	return state.Epoch, state.HasOtherGeneration, err
}

// GetOrCreateInitialSnapshotEpochState persists or loads the exact table
// generation epoch and reports whether this call created it. Callers must use
// this on every stable-task pipeline start, not only while the watermark is
// empty: the epoch also makes stale-history recovery and table recreation safe
// after a process restart.
func (u *CDCWatermarkUpdater) GetOrCreateInitialSnapshotEpochState(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
) (InitialSnapshotEpochState, error) {
	return u.getOrCreateInitialSnapshotEpochState(
		ctx, key, sourceTableID, candidate, types.TS{}, 0, false)
}

// GetOrCreateInitialSnapshotEpochStateForProgress refuses to manufacture an
// epoch beside progress whose source image is unknown. The check is performed
// before INSERT, so retrying the failed admission cannot turn its own rejected
// candidate into trusted metadata.
func (u *CDCWatermarkUpdater) GetOrCreateInitialSnapshotEpochStateForProgress(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
	watermark types.TS,
	watermarkGeneration uint64,
) (InitialSnapshotEpochState, error) {
	return u.getOrCreateInitialSnapshotEpochState(
		ctx, key, sourceTableID, candidate, watermark, watermarkGeneration, true)
}

func (u *CDCWatermarkUpdater) getOrCreateInitialSnapshotEpochState(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
	watermark types.TS,
	watermarkGeneration uint64,
	validateProgress bool,
) (InitialSnapshotEpochState, error) {
	if sourceTableID == 0 || candidate.IsEmpty() || !candidate.Valid() {
		return InitialSnapshotEpochState{}, moerr.NewInternalErrorf(
			ctx, "invalid CDC snapshot epoch candidate %s for source table %d",
			candidate.ToString(), sourceTableID)
	}
	if validateProgress && watermarkGeneration > sourceTableID {
		return InitialSnapshotEpochState{}, NewRetryableSnapshotEpochError(
			moerr.NewInternalErrorf(ctx,
				"CDC source table generation %d is older than durable watermark generation %d for %s",
				sourceTableID, watermarkGeneration, key.String()))
	}

	if epoch, ok, err := u.readInitialSnapshotEpoch(ctx, key, sourceTableID); err != nil {
		return InitialSnapshotEpochState{}, err
	} else if ok {
		otherGeneration, changed, err := u.highestOtherInitialSnapshotGeneration(
			ctx, key, sourceTableID)
		return InitialSnapshotEpochState{
			Epoch:              epoch,
			HasOtherGeneration: changed,
			HasNewerGeneration: changed && otherGeneration > sourceTableID,
		}, err
	}
	// Do not manufacture a retry anchor for a catalog view that is already
	// known to be stale. The post-insert check below is still required for a
	// concurrent newer claim between this preflight and the INSERT.
	otherGeneration, changed, err := u.highestOtherInitialSnapshotGeneration(
		ctx, key, sourceTableID)
	if err != nil {
		return InitialSnapshotEpochState{}, err
	} else if changed && otherGeneration > sourceTableID {
		return InitialSnapshotEpochState{}, NewRetryableSnapshotEpochError(
			moerr.NewInternalErrorf(
				ctx,
				"CDC source table generation %d is older than durable snapshot generation %d for %s",
				sourceTableID, otherGeneration, key.String(),
			))
	}
	if validateProgress && !watermark.IsEmpty() &&
		(watermarkGeneration == sourceTableID ||
			(watermarkGeneration == 0 && !changed)) {
		return InitialSnapshotEpochState{}, moerr.NewInternalErrorf(
			ctx,
			"CDC stable snapshot metadata is missing for %s generation %d with watermark %s",
			key.String(), sourceTableID, watermark.ToString(),
		)
	}
	persistCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkUpdate)
	defer cancel()
	persistCtx = defines.AttachAccountId(persistCtx, catalog.System_Account)

	// The no-op duplicate update plus the primary key makes concurrent
	// claim/restart attempts for this generation converge on one epoch.
	if err := u.ie.Exec(
		persistCtx,
		CDCSQLBuilder.InsertSnapshotEpochSQL(key, sourceTableID, candidate),
		ie.SessionOverrideOptions{},
	); err != nil {
		// The INSERT result can be ambiguous. Resolve a committed-but-lost
		// response immediately; otherwise explicitly ask the detector to retry.
		if epoch, ok, readErr := u.readInitialSnapshotEpoch(ctx, key, sourceTableID); readErr == nil && ok {
			otherGeneration, changed, changedErr := u.highestOtherInitialSnapshotGeneration(
				ctx, key, sourceTableID)
			return InitialSnapshotEpochState{
				Epoch:              epoch,
				HasOtherGeneration: changed,
				HasNewerGeneration: changed && otherGeneration > sourceTableID,
				Created:            true,
			}, changedErr
		} else if readErr != nil {
			return InitialSnapshotEpochState{}, classifySnapshotEpochBackendError(
				errors.Join(err, readErr))
		}
		return InitialSnapshotEpochState{}, classifySnapshotEpochBackendError(err)
	}

	epoch, ok, err := u.readInitialSnapshotEpoch(ctx, key, sourceTableID)
	if err != nil {
		return InitialSnapshotEpochState{}, err
	}
	if !ok {
		return InitialSnapshotEpochState{}, &RetryableSnapshotEpochError{err: moerr.NewInternalErrorf(
			ctx, "CDC snapshot epoch was not durable for %s generation %d",
			key.String(), sourceTableID)}
	}
	otherGeneration, changed, err = u.highestOtherInitialSnapshotGeneration(
		ctx, key, sourceTableID)
	return InitialSnapshotEpochState{
		Epoch:              epoch,
		HasOtherGeneration: changed,
		HasNewerGeneration: changed && otherGeneration > sourceTableID,
		Created:            true,
	}, err
}

func (u *CDCWatermarkUpdater) highestOtherInitialSnapshotGeneration(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
) (uint64, bool, error) {
	readCtx, cancel := context.WithTimeoutCause(ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkRead)
	defer cancel()
	readCtx = defines.AttachAccountId(readCtx, catalog.System_Account)
	res := u.ie.Query(
		readCtx,
		CDCSQLBuilder.GetHighestOtherSnapshotGenerationSQL(key, sourceTableID),
		ie.SessionOverrideOptions{},
	)
	if err := res.Error(); err != nil {
		return 0, false, classifySnapshotEpochBackendError(err)
	}
	if res.RowCount() == 0 {
		return 0, false, nil
	}
	if res.RowCount() != 1 {
		return 0, false, moerr.NewInternalErrorf(
			ctx, "duplicate highest CDC snapshot generation result for %s", key.String())
	}
	otherGeneration, err := res.GetUint64(readCtx, 0, 0)
	if err != nil {
		return 0, false, err
	}
	return otherGeneration, true, nil
}

// DeleteInitialSnapshotGenerationsBefore compacts retired retry anchors after
// target initialization for sourceTableID has completed. MatrixOne table IDs
// are monotonic, so the one-way predicate is the generation fence: stale owner
// G can delete rows below G, but never G+1 or any later replacement.
//
// The operation is idempotent. An ambiguous result is returned as retryable;
// the next pipeline attempt can safely issue the same DELETE again.
func (u *CDCWatermarkUpdater) DeleteInitialSnapshotGenerationsBefore(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
) error {
	if sourceTableID == 0 {
		return moerr.NewInternalErrorf(ctx, "invalid CDC source table generation %d", sourceTableID)
	}

	deleteCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkUpdate)
	defer cancel()
	deleteCtx = defines.AttachAccountId(deleteCtx, catalog.System_Account)
	if err := u.ie.Exec(
		deleteCtx,
		CDCSQLBuilder.DeleteSnapshotEpochGenerationsBeforeSQL(key, sourceTableID),
		ie.SessionOverrideOptions{},
	); err != nil {
		return classifySnapshotEpochBackendError(err)
	}
	return nil
}

func (u *CDCWatermarkUpdater) readInitialSnapshotEpoch(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
) (types.TS, bool, error) {
	readCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkRead)
	defer cancel()
	readCtx = defines.AttachAccountId(readCtx, catalog.System_Account)
	res := u.ie.Query(
		readCtx,
		CDCSQLBuilder.GetSnapshotEpochSQL(key, sourceTableID),
		ie.SessionOverrideOptions{},
	)
	if err := res.Error(); err != nil {
		return types.TS{}, false, classifySnapshotEpochBackendError(err)
	}
	if res.RowCount() == 0 {
		return types.TS{}, false, nil
	}
	if res.RowCount() != 1 {
		return types.TS{}, false, moerr.NewInternalErrorf(
			ctx, "duplicate CDC snapshot epochs for %s generation %d",
			key.String(), sourceTableID)
	}
	epochText, err := res.GetString(readCtx, 0, 0)
	if err != nil {
		return types.TS{}, false, err
	}
	epoch, err := parseInitialSnapshotEpoch(epochText)
	if err != nil {
		return types.TS{}, false, moerr.NewInternalErrorf(
			ctx, "invalid CDC snapshot epoch %q for %s generation %d: %v",
			epochText, key.String(), sourceTableID, err)
	}
	return epoch, true, nil
}

func parseInitialSnapshotEpoch(value string) (types.TS, error) {
	physicalText, logicalText, ok := strings.Cut(value, "-")
	if !ok || physicalText == "" || logicalText == "" {
		return types.TS{}, moerr.NewInvalidInputNoCtx("expected physical-logical timestamp")
	}
	physical, err := strconv.ParseInt(physicalText, 10, 64)
	if err != nil || physical <= 0 {
		return types.TS{}, moerr.NewInvalidInputNoCtx("invalid physical timestamp")
	}
	logical, err := strconv.ParseUint(logicalText, 10, 32)
	if err != nil {
		return types.TS{}, moerr.NewInvalidInputNoCtx("invalid logical timestamp")
	}
	return types.BuildTS(physical, uint32(logical)), nil
}

func (b cdcSQLBuilder) GetSnapshotEpochSQL(key *WatermarkKey, sourceTableID uint64) string {
	return fmt.Sprintf("SELECT snapshot_epoch FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s' AND source_table_id = %d", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID)
}

func (b cdcSQLBuilder) GetHighestOtherSnapshotGenerationSQL(key *WatermarkKey, sourceTableID uint64) string {
	return fmt.Sprintf("SELECT source_table_id FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s' AND source_table_id <> %d ORDER BY source_table_id DESC LIMIT 1", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID)
}

func (b cdcSQLBuilder) InsertSnapshotEpochSQL(key *WatermarkKey, sourceTableID uint64, epoch types.TS) string {
	return fmt.Sprintf("INSERT INTO `mo_catalog`.`mo_cdc_snapshot` (account_id, task_id, db_name, table_name, source_table_id, snapshot_epoch) VALUES (%d, '%s', '%s', '%s', %d, '%s') ON DUPLICATE KEY UPDATE snapshot_epoch = snapshot_epoch", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID, epoch.ToString())
}

func (b cdcSQLBuilder) DeleteSnapshotEpochGenerationsBeforeSQL(
	key *WatermarkKey,
	sourceTableID uint64,
) string {
	return fmt.Sprintf("DELETE FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s' AND source_table_id < %d", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID)
}

func (b cdcSQLBuilder) DeleteSnapshotEpochSQL(accountID uint64, taskID string) string {
	return fmt.Sprintf("DELETE FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s'", accountID, escapeSQLString(taskID))
}

func (b cdcSQLBuilder) DeleteOrphanSnapshotEpochSQL() string {
	return "DELETE s FROM `mo_catalog`.`mo_cdc_snapshot` AS s " +
		"LEFT JOIN `mo_catalog`.`mo_cdc_task` AS t " +
		"ON t.account_id = s.account_id AND t.task_id = s.task_id " +
		"WHERE t.task_id IS NULL"
}
