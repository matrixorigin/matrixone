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

package iceberg

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type DeferredMappingUpdate struct {
	AccountID                uint32
	DatabaseID               uint64
	TableID                  uint64
	LastSnapshotID           string
	LastMetadataLocationHash string
	ExpectedVersion          uint64
}

func (d *DAO) ApplyDeferredMappingUpdate(ctx context.Context, update DeferredMappingUpdate) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateDeferredMappingUpdate(ctx, update); err != nil {
		return err
	}
	return d.exec.Exec(ctx, BuildDeferredMappingUpdateSQL(update))
}

func ValidateDeferredMappingUpdate(ctx context.Context, update DeferredMappingUpdate) error {
	if update.AccountID == 0 || update.DatabaseID == 0 || update.TableID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg deferred update requires account_id, db_id, and table_id")
	}
	if update.LastSnapshotID == "" || update.LastMetadataLocationHash == "" {
		return moerr.NewInvalidInput(ctx, "iceberg deferred update requires last snapshot and metadata location hash")
	}
	if update.ExpectedVersion == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg deferred update requires optimistic expected version")
	}
	return nil
}

func BuildDeferredMappingUpdateSQL(update DeferredMappingUpdate) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set last_snapshot_id = %s, last_metadata_location_hash = %s, updated_at = utc_timestamp, version = version + 1 where account_id = %d and db_id = %d and table_id = %d and version = %d",
		TableTables,
		quoteSQLString(update.LastSnapshotID),
		quoteSQLString(update.LastMetadataLocationHash),
		update.AccountID,
		update.DatabaseID,
		update.TableID,
		update.ExpectedVersion,
	)
}
