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
// for one logical CDC table. Created is needed by restart admission: a stable
// task with an existing watermark but no epoch row has lost protocol metadata
// and must not silently guess that the current source generation is safe.
type InitialSnapshotEpochState struct {
	Epoch              types.TS
	HasOtherGeneration bool
	Created            bool
}

func (e *RetryableSnapshotEpochError) Error() string { return e.err.Error() }
func (e *RetryableSnapshotEpochError) Unwrap() error { return e.err }

func IsRetryableSnapshotEpochError(err error) bool {
	var retryable *RetryableSnapshotEpochError
	return errors.As(err, &retryable)
}

// GetOrCreateInitialSnapshotEpoch returns the durable epoch for one source
// table generation. It must complete before a reader may make a partial target
// commit. Reusing the same source table ID preserves the old epoch across
// retries. Different source table IDs are intentionally retained side by side:
// an old owner can overlap a replacement owner, so neither generation may
// delete the other's retry anchor. Terminal task cleanup removes all epochs.
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
// epoch exists for an older incarnation of the same logical table. Since every
// bounded target commit is preceded by epoch persistence, that fact is the
// durable signal that a fresh owner must reset the target before replaying the
// new generation.
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
	if sourceTableID == 0 || candidate.IsEmpty() || !candidate.Valid() {
		return InitialSnapshotEpochState{}, moerr.NewInternalErrorf(
			ctx, "invalid CDC snapshot epoch candidate %s for source table %d",
			candidate.ToString(), sourceTableID)
	}

	if epoch, ok, err := u.readInitialSnapshotEpoch(ctx, key, sourceTableID); err != nil {
		return InitialSnapshotEpochState{}, err
	} else if ok {
		changed, err := u.hasOtherInitialSnapshotGeneration(ctx, key, sourceTableID)
		return InitialSnapshotEpochState{
			Epoch: epoch, HasOtherGeneration: changed,
		}, err
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
			changed, changedErr := u.hasOtherInitialSnapshotGeneration(ctx, key, sourceTableID)
			return InitialSnapshotEpochState{
				Epoch: epoch, HasOtherGeneration: changed, Created: true,
			}, changedErr
		} else if readErr != nil {
			return InitialSnapshotEpochState{}, &RetryableSnapshotEpochError{err: errors.Join(err, readErr)}
		}
		return InitialSnapshotEpochState{}, &RetryableSnapshotEpochError{err: err}
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
	changed, err := u.hasOtherInitialSnapshotGeneration(ctx, key, sourceTableID)
	return InitialSnapshotEpochState{
		Epoch: epoch, HasOtherGeneration: changed, Created: true,
	}, err
}

func (u *CDCWatermarkUpdater) hasOtherInitialSnapshotGeneration(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
) (bool, error) {
	readCtx, cancel := context.WithTimeoutCause(ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkRead)
	defer cancel()
	readCtx = defines.AttachAccountId(readCtx, catalog.System_Account)
	res := u.ie.Query(readCtx, CDCSQLBuilder.HasOtherSnapshotGenerationSQL(key, sourceTableID), ie.SessionOverrideOptions{})
	if err := res.Error(); err != nil {
		return false, err
	}
	return res.RowCount() > 0, nil
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
		return types.TS{}, false, err
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

func (b cdcSQLBuilder) HasOtherSnapshotGenerationSQL(key *WatermarkKey, sourceTableID uint64) string {
	return fmt.Sprintf("SELECT source_table_id FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s' AND source_table_id <> %d LIMIT 1", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID)
}

func (b cdcSQLBuilder) InsertSnapshotEpochSQL(key *WatermarkKey, sourceTableID uint64, epoch types.TS) string {
	return fmt.Sprintf("INSERT INTO `mo_catalog`.`mo_cdc_snapshot` (account_id, task_id, db_name, table_name, source_table_id, snapshot_epoch) VALUES (%d, '%s', '%s', '%s', %d, '%s') ON DUPLICATE KEY UPDATE snapshot_epoch = snapshot_epoch", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID, epoch.ToString())
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
