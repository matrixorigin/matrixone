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

package lifecycle

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const lifecycleAccountPageSize = 64

// SQLBindingPager reads only explicit Binding rows. It first pages account IDs
// from the system catalog and then issues one indexed Binding query in each
// tenant. Ordinary tables are never enumerated.
type SQLBindingPager struct {
	Executor executor.SQLExecutor
}

// SaveCursor persists only a scheduling hint. The final transaction never
// trusts it; exact metadata and TN CAS remain the retirement proof.
func (pager SQLBindingPager) SaveCursor(
	ctx context.Context,
	binding Binding,
	cursor DiscoveryCursor,
	fullScanAt time.Time,
) (Binding, error) {
	if pager.Executor == nil ||
		len(binding.ID) != 32 ||
		binding.AccountID == 0 ||
		binding.Version == 0 ||
		cursor.Snapshot.IsEmpty() {
		return binding, moerr.NewInvalidInput(
			ctx,
			"Lifecycle cursor update is incomplete",
		)
	}
	lastObject := "null"
	if cursor.HasLastObject {
		lastObject = fmt.Sprintf(
			"unhex('%s')",
			hex.EncodeToString(cursor.LastObjectName[:]),
		)
	}
	fullScanUpdate := ""
	if !fullScanAt.IsZero() {
		fullScanUpdate = ",last_full_scan_at=utc_timestamp()"
	}
	result, err := pager.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_bindings
set scan_snapshot_ts=unhex('%s'),scan_last_object_name=%s,
scan_wrapped=%t%s,version=version+1,updated_at=utc_timestamp()
where binding_id=unhex('%s') and state='ACTIVE' and version=%d`,
			hex.EncodeToString(cursor.Snapshot[:]),
			lastObject,
			cursor.Wrapped,
			fullScanUpdate,
			binding.ID,
			binding.Version,
		),
		executor.Options{}.WithAccountID(binding.AccountID),
	)
	if err != nil {
		return binding, err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return binding, moerr.NewInternalErrorNoCtxf("Lifecycle cursor CAS failed")
	}
	binding.ScanSnapshotHex = hex.EncodeToString(cursor.Snapshot[:])
	binding.ScanLastObjectNameHex = ""
	if cursor.HasLastObject {
		binding.ScanLastObjectNameHex = hex.EncodeToString(
			cursor.LastObjectName[:],
		)
	}
	binding.ScanWrapped = cursor.Wrapped
	if !fullScanAt.IsZero() {
		binding.LastFullScanAt = fullScanAt.UTC().Truncate(time.Microsecond)
	}
	binding.Version++
	return binding, nil
}

func (pager SQLBindingPager) NextActiveBindings(
	ctx context.Context,
	cursor BindingCursor,
	limit int,
) ([]Binding, BindingCursor, bool, error) {
	if pager.Executor == nil || limit <= 0 {
		return nil, cursor, false, moerr.NewInvalidInput(
			ctx,
			"Lifecycle SQL Binding pager is not configured",
		)
	}
	accountResult, err := pager.Executor.Exec(
		ctx,
		fmt.Sprintf(
			"select cast(account_id as bigint unsigned) from mo_catalog.mo_account where account_id >= %d order by account_id limit %d",
			cursor.AccountID,
			lifecycleAccountPageSize,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return nil, cursor, false, err
	}
	defer accountResult.Close()
	accountIDs, err := readLifecycleAccountIDs(accountResult)
	if err != nil {
		return nil, cursor, false, err
	}
	if len(accountIDs) == 0 {
		return nil, cursor, true, nil
	}

	result := make([]Binding, 0, limit)
	next := cursor
	for _, accountID := range accountIDs {
		afterBinding := ""
		if accountID == cursor.AccountID {
			afterBinding = cursor.BindingID
		}
		page, err := pager.readAccountBindings(
			ctx,
			accountID,
			afterBinding,
			limit-len(result),
		)
		if err != nil {
			return nil, cursor, false, err
		}
		result = append(result, page...)
		if len(page) > 0 {
			next = BindingCursor{
				AccountID: accountID,
				BindingID: page[len(page)-1].ID,
			}
		}
		if len(result) == limit {
			return result, next, false, nil
		}
		next = BindingCursor{AccountID: accountID + 1}
	}
	if len(accountIDs) == lifecycleAccountPageSize {
		return result, next, false, nil
	}
	return result, next, true, nil
}

func (pager SQLBindingPager) readAccountBindings(
	ctx context.Context,
	accountID uint32,
	afterBinding string,
	limit int,
) ([]Binding, error) {
	if limit <= 0 {
		return nil, nil
	}
	predicate := ""
	if afterBinding != "" {
		if len(afterBinding) != 32 || strings.IndexFunc(afterBinding, func(value rune) bool {
			return !((value >= '0' && value <= '9') ||
				(value >= 'a' && value <= 'f') ||
				(value >= 'A' && value <= 'F'))
		}) >= 0 {
			return nil, moerr.NewInternalError(ctx, "invalid Lifecycle Binding cursor")
		}
		predicate = fmt.Sprintf(" and binding_id > unhex('%s')", afterBinding)
	}
	sql := fmt.Sprintf(
		`select hex(binding_id),database_id,logical_table_id,physical_table_id,
binding_generation,hex(schema_digest),lifecycle_column_id,action,
expire_after_days,late_arrival_grace_days,evaluation_timezone,
coalesce(stage_id,0),coalesce(purge_after_days,0),
coalesce(hex(stage_identity_digest),''),coalesce(hex(scan_snapshot_ts),''),
coalesce(hex(scan_last_object_name),''),
scan_wrapped,state
 ,version,last_full_scan_at
from mo_catalog.mo_lifecycle_bindings
where state = 'ACTIVE'%s
order by binding_id limit %d`,
		predicate,
		limit,
	)
	queryResult, err := pager.Executor.Exec(
		ctx,
		sql,
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return nil, err
	}
	defer queryResult.Close()
	return decodeLifecycleBindings(queryResult, accountID)
}

func readLifecycleAccountIDs(result executor.Result) ([]uint32, error) {
	accountIDs := make([]uint32, 0)
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle account query returned %d columns", len(columns))
			return false
		}
		for row := 0; row < rows; row++ {
			value, err := lifecycleUint64At(columns[0], row)
			if err != nil || value > uint64(^uint32(0)) {
				if err == nil {
					err = moerr.NewInternalErrorNoCtxf("Lifecycle account ID %d overflows uint32", value)
				}
				decodeErr = err
				return false
			}
			accountIDs = append(accountIDs, uint32(value))
		}
		return true
	})
	return accountIDs, decodeErr
}

func decodeLifecycleBindings(
	result executor.Result,
	accountID uint32,
) ([]Binding, error) {
	bindings := make([]Binding, 0)
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 20 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Binding query returned %d columns", len(columns))
			return false
		}
		for row := 0; row < rows; row++ {
			numbers := make([]uint64, 0, 10)
			for _, column := range []int{1, 2, 3, 4, 6, 8, 9, 11, 12, 18} {
				value, err := lifecycleUint64At(columns[column], row)
				if err != nil {
					decodeErr = err
					return false
				}
				numbers = append(numbers, value)
			}
			binding := Binding{
				ID:                    strings.ToLower(columns[0].GetStringAt(row)),
				AccountID:             accountID,
				DatabaseID:            numbers[0],
				LogicalTableID:        numbers[1],
				PhysicalTableID:       numbers[2],
				Generation:            numbers[3],
				SchemaDigest:          strings.ToLower(columns[5].GetStringAt(row)),
				LifecycleColumnID:     numbers[4],
				Action:                columns[7].GetStringAt(row),
				ExpireAfterDays:       uint32(numbers[5]),
				LateArrivalGraceDays:  uint32(numbers[6]),
				EvaluationTimezone:    columns[10].GetStringAt(row),
				StageID:               numbers[7],
				StageIdentityDigest:   strings.ToLower(columns[13].GetStringAt(row)),
				PurgeAfterDays:        uint32(numbers[8]),
				ScanSnapshotHex:       strings.ToLower(columns[14].GetStringAt(row)),
				ScanLastObjectNameHex: strings.ToLower(columns[15].GetStringAt(row)),
				ScanWrapped:           vector.GetFixedAtNoTypeCheck[bool](columns[16], row),
				State:                 columns[17].GetStringAt(row),
				Version:               numbers[9],
			}
			if !columns[19].GetNulls().Contains(uint64(row)) {
				lastFullScanAt := vector.GetFixedAtNoTypeCheck[types.Timestamp](
					columns[19],
					row,
				)
				binding.LastFullScanAt = lastFullScanAt.
					ToDatetime(time.UTC).
					ConvertToGoTime(time.UTC)
			}
			if len(binding.ID) != 32 ||
				len(binding.SchemaDigest) != 64 ||
				(binding.Action == "ARCHIVE" && len(binding.StageIdentityDigest) != 64) {
				decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Binding persistent identity is corrupt")
				return false
			}
			bindings = append(bindings, binding)
		}
		return true
	})
	return bindings, decodeErr
}

func lifecycleUint64At(value *vector.Vector, row int) (uint64, error) {
	switch value.GetType().Oid {
	case types.T_uint8:
		return uint64(vector.GetFixedAtNoTypeCheck[uint8](value, row)), nil
	case types.T_uint16:
		return uint64(vector.GetFixedAtNoTypeCheck[uint16](value, row)), nil
	case types.T_uint32:
		return uint64(vector.GetFixedAtNoTypeCheck[uint32](value, row)), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](value, row), nil
	case types.T_int8:
		typed := vector.GetFixedAtNoTypeCheck[int8](value, row)
		if typed >= 0 {
			return uint64(typed), nil
		}
	case types.T_int16:
		typed := vector.GetFixedAtNoTypeCheck[int16](value, row)
		if typed >= 0 {
			return uint64(typed), nil
		}
	case types.T_int32:
		typed := vector.GetFixedAtNoTypeCheck[int32](value, row)
		if typed >= 0 {
			return uint64(typed), nil
		}
	case types.T_int64:
		typed := vector.GetFixedAtNoTypeCheck[int64](value, row)
		if typed >= 0 {
			return uint64(typed), nil
		}
	}
	return 0, moerr.NewInternalErrorNoCtxf(
		"Lifecycle expected non-negative integer, got %s",
		value.GetType().Oid,
	)
}
