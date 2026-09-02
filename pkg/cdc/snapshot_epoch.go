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

// GetOrCreateInitialSnapshotEpoch returns the durable epoch for one source
// table generation. It must complete before a reader may make a partial target
// commit. Reusing the same source table ID preserves the old epoch across
// retries; a new table ID replaces the retired generation before publishing its
// reader.
func (u *CDCWatermarkUpdater) GetOrCreateInitialSnapshotEpoch(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
	candidate types.TS,
) (types.TS, error) {
	if sourceTableID == 0 || candidate.IsEmpty() || !candidate.Valid() {
		return types.TS{}, moerr.NewInternalErrorf(
			ctx, "invalid CDC snapshot epoch candidate %s for source table %d",
			candidate.ToString(), sourceTableID)
	}

	if epoch, ok, err := u.readInitialSnapshotEpoch(ctx, key, sourceTableID); err != nil {
		return types.TS{}, err
	} else if ok {
		if err := u.deleteRetiredInitialSnapshotEpochs(ctx, key, sourceTableID); err != nil {
			return types.TS{}, err
		}
		return epoch, nil
	}

	persistCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkUpdate)
	defer cancel()
	persistCtx = defines.AttachAccountId(persistCtx, catalog.System_Account)

	// Insert before deleting retired generations. The no-op duplicate update plus
	// the primary key makes concurrent claim/restart attempts converge on one epoch;
	// no contender can erase the winning row between its initial read and write.
	if err := u.ie.Exec(
		persistCtx,
		CDCSQLBuilder.InsertSnapshotEpochSQL(key, sourceTableID, candidate),
		ie.SessionOverrideOptions{},
	); err != nil {
		// The INSERT result can be ambiguous. A retry first reads the durable row
		// and therefore cannot select another epoch after a successful commit.
		return types.TS{}, err
	}

	epoch, ok, err := u.readInitialSnapshotEpoch(ctx, key, sourceTableID)
	if err != nil {
		return types.TS{}, err
	}
	if !ok {
		return types.TS{}, moerr.NewInternalErrorf(
			ctx, "CDC snapshot epoch was not durable for %s generation %d",
			key.String(), sourceTableID)
	}
	if err := u.deleteRetiredInitialSnapshotEpochs(ctx, key, sourceTableID); err != nil {
		return types.TS{}, err
	}
	return epoch, nil
}

func (u *CDCWatermarkUpdater) deleteRetiredInitialSnapshotEpochs(
	ctx context.Context,
	key *WatermarkKey,
	sourceTableID uint64,
) error {
	cleanupCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkUpdate)
	defer cancel()
	cleanupCtx = defines.AttachAccountId(cleanupCtx, catalog.System_Account)
	return u.ie.Exec(
		cleanupCtx,
		CDCSQLBuilder.DeleteRetiredSnapshotEpochsSQL(key, sourceTableID),
		ie.SessionOverrideOptions{},
	)
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

func (b cdcSQLBuilder) DeleteRetiredSnapshotEpochsSQL(key *WatermarkKey, sourceTableID uint64) string {
	return fmt.Sprintf("DELETE FROM `mo_catalog`.`mo_cdc_snapshot` WHERE account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s' AND source_table_id <> %d", key.AccountId, escapeSQLString(key.TaskId), escapeSQLString(key.DBName), escapeSQLString(key.TableName), sourceTableID)
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
