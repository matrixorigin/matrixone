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

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const lifecycleSQLTimestampLayout = "2006-01-02 15:04:05.999999"

// SQLCleanupRootRepository keeps external-object ownership in the system
// account so tenant DROP does not orphan Provider writes. All mutations are
// single-row CAS statements; ordinary transactions and tables never access it.
type SQLCleanupRootRepository struct {
	Executor executor.SQLExecutor
}

func (repository SQLCleanupRootRepository) Register(
	ctx context.Context,
	root CleanupRoot,
) error {
	if repository.Executor == nil {
		return fmt.Errorf("Lifecycle Cleanup Root SQL executor is nil")
	}
	if err := ValidateCleanupRoot(root); err != nil {
		return err
	}
	rootID, err := lifecycleSQLUUID(root.RootID)
	if err != nil {
		return err
	}
	attemptID, err := lifecycleSQLUUID(root.AttemptID)
	if err != nil {
		return err
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`insert into mo_catalog.mo_lifecycle_cleanup_roots(
root_id,attempt_id,mode,owner_account_id,logical_table_id,physical_table_id,
executor_epoch,worker_lease_deadline,archive_namespace_blob,credential_handle,
archive_prefix,manifest_key,manifest_digest,tae_namespace_blob,segment_id,
booking_prefix,ordinal_upper_bound,reserved_cleanup_bytes,source_set_digest,final_txn_id,state,
state_version,cleanup_after,temporary_cleanup_done,quiescence_since,last_list_at,
last_error,created_at,updated_at)
values(unhex('%s'),unhex('%s'),%s,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,
unhex('%s'),%s,%s,%d,%s,%t,%s,%s,%s,utc_timestamp(),utc_timestamp())`,
			rootID,
			attemptID,
			lifecycleSQLQuote(string(root.Mode)),
			root.OwnerAccountID,
			root.LogicalTableID,
			root.PhysicalTableID,
			root.ExecutorEpoch,
			lifecycleSQLTime(root.WorkerDeadline),
			lifecycleSQLNullableString(root.ArchiveNamespace),
			lifecycleSQLNullableString(root.CredentialHandle),
			lifecycleSQLNullableString(root.ArchivePrefix),
			lifecycleSQLNullableString(root.ManifestKey),
			lifecycleSQLNullableDigest(root.ManifestDigest),
			lifecycleSQLNullableString(root.TAENamespace),
			lifecycleSQLNullableString(root.SegmentID),
			lifecycleSQLNullableString(root.BookingPrefix),
			root.OrdinalUpperBound,
			root.ReservedCleanupBytes,
			hex.EncodeToString(root.SourceSetDigest[:]),
			lifecycleSQLNullableString(root.FinalTxnID),
			lifecycleSQLQuote(string(root.State)),
			root.StateVersion,
			lifecycleSQLTime(root.CleanupAfter),
			root.TemporaryCleanupDone,
			lifecycleSQLNullableTime(root.QuiescenceSince),
			lifecycleSQLNullableTime(root.LastListAt),
			lifecycleSQLNullableString(root.LastError),
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return fmt.Errorf(
			"Lifecycle Cleanup Root insert affected %d rows",
			result.AffectedRows,
		)
	}
	return nil
}

func (repository SQLCleanupRootRepository) Get(
	ctx context.Context,
	rootID string,
) (CleanupRoot, error) {
	if repository.Executor == nil {
		return CleanupRoot{}, fmt.Errorf("Lifecycle Cleanup Root SQL executor is nil")
	}
	encodedID, err := lifecycleSQLUUID(rootID)
	if err != nil {
		return CleanupRoot{}, err
	}
	result, err := repository.Executor.Exec(
		ctx,
		lifecycleCleanupRootSelect+
			fmt.Sprintf(" where root_id=unhex('%s')", encodedID),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return CleanupRoot{}, err
	}
	defer result.Close()
	roots, err := decodeLifecycleCleanupRoots(result)
	if err != nil {
		return CleanupRoot{}, err
	}
	if len(roots) != 1 {
		return CleanupRoot{}, fmt.Errorf(
			"Lifecycle Cleanup Root %s returned %d rows",
			rootID,
			len(roots),
		)
	}
	return roots[0], nil
}

func (repository SQLCleanupRootRepository) HasUnresolvedSource(
	ctx context.Context,
	ownerAccountID uint32,
	physicalTableID uint64,
	_ [32]byte,
) (bool, error) {
	if repository.Executor == nil ||
		ownerAccountID == 0 ||
		physicalTableID == 0 {
		return false, fmt.Errorf(
			"Lifecycle unresolved source query is incomplete",
		)
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select root_id from mo_catalog.mo_lifecycle_cleanup_roots
where owner_account_id=%d and physical_table_id=%d
and state in ('FINALIZING','COMMIT_UNKNOWN') limit 1`,
			ownerAccountID,
			physicalTableID,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return false, err
	}
	defer result.Close()
	return lifecycleResultHasRows(result), nil
}

func (repository SQLCleanupRootRepository) CheckCreateCapacity(
	ctx context.Context,
	maxActiveRoots int,
	maxReservedBytes uint64,
	requestedBytes uint64,
) error {
	if repository.Executor == nil || maxActiveRoots <= 0 ||
		maxReservedBytes == 0 || requestedBytes == 0 {
		return fmt.Errorf("Lifecycle Cleanup Root capacity check is incomplete")
	}
	result, err := repository.Executor.Exec(
		ctx,
		`select cast(count(*) as bigint unsigned),
cast(coalesce(sum(reserved_cleanup_bytes),0) as bigint unsigned)
from mo_catalog.mo_lifecycle_cleanup_roots
where state in ('REGISTERED','UPLOADING','VERIFIED','FINALIZING',
'COMMIT_UNKNOWN','DELETE_PENDING','DELETING')
or (state='PUBLISHED' and temporary_cleanup_done=false)`,
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	activeRoots, reservedBytes, err := decodeCleanupCapacity(result)
	if err != nil {
		return err
	}
	metricv2.LifecycleActiveCleanupRootGauge.Set(float64(activeRoots))
	metricv2.LifecycleReservedCleanupBytesGauge.Set(float64(reservedBytes))
	if activeRoots >= uint64(maxActiveRoots) ||
		reservedBytes >= maxReservedBytes ||
		requestedBytes > maxReservedBytes-reservedBytes {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"cleanup_roots",
		).Inc()
		return fmt.Errorf(
			"RESOURCE_BLOCKED: Lifecycle Cleanup Root capacity exhausted",
		)
	}
	return nil
}

func decodeCleanupCapacity(result executor.Result) (uint64, uint64, error) {
	var activeRoots uint64
	var reservedBytes uint64
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 2 || rowsRead+rows != 1 {
			decodeErr = fmt.Errorf("Lifecycle Cleanup Root capacity query is invalid")
			return false
		}
		activeRoots, decodeErr = lifecycleUint64At(columns[0], 0)
		if decodeErr == nil {
			reservedBytes, decodeErr = lifecycleUint64At(columns[1], 0)
		}
		rowsRead += rows
		return decodeErr == nil
	})
	if decodeErr != nil {
		return 0, 0, decodeErr
	}
	if rowsRead != 1 {
		return 0, 0, fmt.Errorf(
			"Lifecycle Cleanup Root capacity query returned %d rows",
			rowsRead,
		)
	}
	return activeRoots, reservedBytes, nil
}

func (repository SQLCleanupRootRepository) Transition(
	ctx context.Context,
	rootID string,
	attemptID string,
	executorEpoch uint64,
	from CleanupRootState,
	expectedVersion uint64,
	to CleanupRootState,
) (CleanupRoot, error) {
	if !validateCleanupRootTransition(from, to) {
		return CleanupRoot{}, fmt.Errorf(
			"invalid Lifecycle Cleanup Root transition %s -> %s",
			from,
			to,
		)
	}
	current, err := repository.Get(ctx, rootID)
	if err != nil {
		return CleanupRoot{}, err
	}
	if current.AttemptID != attemptID ||
		current.ExecutorEpoch != executorEpoch ||
		current.State != from ||
		current.StateVersion != expectedVersion {
		return CleanupRoot{}, fmt.Errorf("Lifecycle Cleanup Root transition CAS failed")
	}
	rootHex, _ := lifecycleSQLUUID(rootID)
	attemptHex, err := lifecycleSQLUUID(attemptID)
	if err != nil {
		return CleanupRoot{}, err
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_cleanup_roots
set state=%s,state_version=state_version+1,updated_at=utc_timestamp()
where root_id=unhex('%s') and attempt_id=unhex('%s') and executor_epoch=%d
and state=%s and state_version=%d`,
			lifecycleSQLQuote(string(to)),
			rootHex,
			attemptHex,
			executorEpoch,
			lifecycleSQLQuote(string(from)),
			expectedVersion,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return CleanupRoot{}, err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return CleanupRoot{}, fmt.Errorf("Lifecycle Cleanup Root transition CAS failed")
	}
	current.State = to
	current.StateVersion++
	metricv2.LifecycleRootTransitionCounter.WithLabelValues(
		string(from),
		string(to),
	).Inc()
	return current, nil
}

func (repository SQLCleanupRootRepository) UpdateCleanup(
	ctx context.Context,
	root CleanupRoot,
	expectedVersion uint64,
) (CleanupRoot, error) {
	if repository.Executor == nil {
		return CleanupRoot{}, fmt.Errorf("Lifecycle Cleanup Root SQL executor is nil")
	}
	rootHex, err := lifecycleSQLUUID(root.RootID)
	if err != nil {
		return CleanupRoot{}, err
	}
	attemptHex, err := lifecycleSQLUUID(root.AttemptID)
	if err != nil {
		return CleanupRoot{}, err
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_cleanup_roots set
worker_lease_deadline=%s,manifest_key=%s,manifest_digest=%s,final_txn_id=%s,
segment_id=%s,booking_prefix=%s,ordinal_upper_bound=%d,
cleanup_after=%s,temporary_cleanup_done=%t,quiescence_since=%s,last_list_at=%s,last_error=%s,
state_version=state_version+1,updated_at=utc_timestamp()
where root_id=unhex('%s') and attempt_id=unhex('%s') and state=%s
and state_version=%d`,
			lifecycleSQLTime(root.WorkerDeadline),
			lifecycleSQLNullableString(root.ManifestKey),
			lifecycleSQLNullableDigest(root.ManifestDigest),
			lifecycleSQLNullableString(root.FinalTxnID),
			lifecycleSQLNullableString(root.SegmentID),
			lifecycleSQLNullableString(root.BookingPrefix),
			root.OrdinalUpperBound,
			lifecycleSQLTime(root.CleanupAfter),
			root.TemporaryCleanupDone,
			lifecycleSQLNullableTime(root.QuiescenceSince),
			lifecycleSQLNullableTime(root.LastListAt),
			lifecycleSQLNullableString(root.LastError),
			rootHex,
			attemptHex,
			lifecycleSQLQuote(string(root.State)),
			expectedVersion,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return CleanupRoot{}, err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return CleanupRoot{}, fmt.Errorf("Lifecycle Cleanup Root update CAS failed")
	}
	root.StateVersion++
	return root, nil
}

func (repository SQLCleanupRootRepository) ListSweepable(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]CleanupRoot, error) {
	if repository.Executor == nil || now.IsZero() || limit <= 0 {
		return nil, fmt.Errorf("Lifecycle Cleanup Root sweep query is incomplete")
	}
	result, err := repository.Executor.Exec(
		ctx,
		lifecycleCleanupRootSelect+fmt.Sprintf(
			` where state in ('DELETE_PENDING','DELETING')
and cleanup_after <= %s order by cleanup_after,root_id limit %d`,
			lifecycleSQLTime(now),
			limit,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return decodeLifecycleCleanupRoots(result)
}

func (repository SQLCleanupRootRepository) ListPublishedTemporary(
	ctx context.Context,
	limit int,
) ([]CleanupRoot, error) {
	if repository.Executor == nil || limit <= 0 {
		return nil, fmt.Errorf(
			"Lifecycle published temporary cleanup query is incomplete",
		)
	}
	result, err := repository.Executor.Exec(
		ctx,
		lifecycleCleanupRootSelect+fmt.Sprintf(
			` where state='PUBLISHED' and temporary_cleanup_done=false
order by updated_at,root_id limit %d`,
			limit,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return decodeLifecycleCleanupRoots(result)
}

func (repository SQLCleanupRootRepository) ListReconcileable(
	ctx context.Context,
	afterRootID string,
	limit int,
) ([]CleanupRoot, string, bool, error) {
	if repository.Executor == nil || limit <= 0 {
		return nil, afterRootID, false, fmt.Errorf(
			"Lifecycle Cleanup Root reconcile query is incomplete",
		)
	}
	const states = "'REGISTERED','UPLOADING','VERIFIED','FINALIZING','COMMIT_UNKNOWN','PUBLISHED'"
	where := " where state in (" + states + ")"
	if afterRootID != "" {
		encoded, err := lifecycleSQLUUID(afterRootID)
		if err != nil {
			return nil, afterRootID, false, err
		}
		where += fmt.Sprintf(" and root_id > unhex('%s')", encoded)
	}
	query := func(condition string) ([]CleanupRoot, error) {
		result, err := repository.Executor.Exec(
			ctx,
			lifecycleCleanupRootSelect+condition+
				fmt.Sprintf(" order by root_id limit %d", limit),
			executor.Options{}.WithAccountID(catalog.System_Account),
		)
		if err != nil {
			return nil, err
		}
		defer result.Close()
		return decodeLifecycleCleanupRoots(result)
	}
	roots, err := query(where)
	if err != nil {
		return nil, afterRootID, false, err
	}
	wrapped := false
	if len(roots) == 0 && afterRootID != "" {
		roots, err = query(" where state in (" + states + ")")
		if err != nil {
			return nil, afterRootID, false, err
		}
		wrapped = true
	}
	next := afterRootID
	if len(roots) > 0 {
		next = roots[len(roots)-1].RootID
	}
	return roots, next, wrapped, nil
}

const lifecycleCleanupRootSelect = `select
hex(root_id),hex(attempt_id),mode,owner_account_id,logical_table_id,
physical_table_id,executor_epoch,cast(worker_lease_deadline as varchar),
archive_namespace_blob,credential_handle,archive_prefix,manifest_key,
hex(manifest_digest),tae_namespace_blob,segment_id,booking_prefix,
ordinal_upper_bound,reserved_cleanup_bytes,hex(source_set_digest),final_txn_id,state,state_version,
cast(cleanup_after as varchar),temporary_cleanup_done,cast(quiescence_since as varchar),
cast(last_list_at as varchar),last_error
from mo_catalog.mo_lifecycle_cleanup_roots`

func decodeLifecycleCleanupRoots(
	result executor.Result,
) ([]CleanupRoot, error) {
	roots := make([]CleanupRoot, 0)
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 27 {
			decodeErr = fmt.Errorf(
				"Lifecycle Cleanup Root query returned %d columns",
				len(columns),
			)
			return false
		}
		for row := 0; row < rows; row++ {
			rootID, err := lifecycleUUIDFromHex(columns[0].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			attemptID, err := lifecycleUUIDFromHex(columns[1].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			numbers := make([]uint64, 0, 7)
			for _, column := range []int{3, 4, 5, 6, 16, 17, 21} {
				number, numberErr := lifecycleUint64At(columns[column], row)
				if numberErr != nil {
					decodeErr = numberErr
					return false
				}
				numbers = append(numbers, number)
			}
			workerDeadline, err := lifecycleParseSQLTime(
				columns[7].GetStringAt(row),
			)
			if err != nil {
				decodeErr = err
				return false
			}
			cleanupAfter, err := lifecycleParseSQLTime(
				columns[22].GetStringAt(row),
			)
			if err != nil {
				decodeErr = err
				return false
			}
			temporaryCleanupDone, err := lifecycleBoolAt(columns[23], row)
			if err != nil {
				decodeErr = err
				return false
			}
			root := CleanupRoot{
				RootID:               rootID,
				AttemptID:            attemptID,
				Mode:                 CleanupMode(columns[2].GetStringAt(row)),
				OwnerAccountID:       uint32(numbers[0]),
				LogicalTableID:       numbers[1],
				PhysicalTableID:      numbers[2],
				ExecutorEpoch:        numbers[3],
				WorkerDeadline:       workerDeadline,
				ArchiveNamespace:     lifecycleNullableString(columns[8], row),
				CredentialHandle:     lifecycleNullableString(columns[9], row),
				ArchivePrefix:        lifecycleNullableString(columns[10], row),
				ManifestKey:          lifecycleNullableString(columns[11], row),
				TAENamespace:         lifecycleNullableString(columns[13], row),
				SegmentID:            lifecycleNullableString(columns[14], row),
				BookingPrefix:        lifecycleNullableString(columns[15], row),
				OrdinalUpperBound:    uint32(numbers[4]),
				ReservedCleanupBytes: numbers[5],
				FinalTxnID:           lifecycleNullableString(columns[19], row),
				State:                CleanupRootState(columns[20].GetStringAt(row)),
				StateVersion:         numbers[6],
				CleanupAfter:         cleanupAfter,
				TemporaryCleanupDone: temporaryCleanupDone,
				LastError:            lifecycleNullableString(columns[26], row),
			}
			if err := lifecycleDecodeDigest(
				columns[12].GetStringAt(row),
				&root.ManifestDigest,
				true,
			); err != nil {
				decodeErr = err
				return false
			}
			if err := lifecycleDecodeDigest(
				columns[18].GetStringAt(row),
				&root.SourceSetDigest,
				false,
			); err != nil {
				decodeErr = err
				return false
			}
			if !columns[24].GetNulls().Contains(uint64(row)) {
				root.QuiescenceSince, err = lifecycleParseSQLTime(
					columns[24].GetStringAt(row),
				)
				if err != nil {
					decodeErr = err
					return false
				}
			}
			if !columns[25].GetNulls().Contains(uint64(row)) {
				root.LastListAt, err = lifecycleParseSQLTime(
					columns[25].GetStringAt(row),
				)
				if err != nil {
					decodeErr = err
					return false
				}
			}
			roots = append(roots, root)
		}
		return true
	})
	return roots, decodeErr
}

func lifecycleBoolAt(value *vector.Vector, row int) (bool, error) {
	if value.GetType().Oid != types.T_bool {
		return false, fmt.Errorf(
			"Lifecycle expected bool, got %s",
			value.GetType().Oid,
		)
	}
	return vector.GetFixedAtNoTypeCheck[bool](value, row), nil
}

func lifecycleSQLUUID(value string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("invalid Lifecycle UUID %q: %w", value, err)
	}
	return hex.EncodeToString(parsed[:]), nil
}

func lifecycleUUIDFromHex(value string) (string, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return "", fmt.Errorf("invalid persisted Lifecycle UUID %q", value)
	}
	parsed, err := uuid.FromBytes(decoded)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func lifecycleSQLQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func lifecycleSQLNullableString(value string) string {
	if value == "" {
		return "null"
	}
	return lifecycleSQLQuote(value)
}

func lifecycleSQLNullableDigest(value [32]byte) string {
	var zero [32]byte
	if value == zero {
		return "null"
	}
	return "unhex('" + hex.EncodeToString(value[:]) + "')"
}

func lifecycleSQLTime(value time.Time) string {
	return lifecycleSQLQuote(
		value.UTC().Truncate(time.Microsecond).Format(lifecycleSQLTimestampLayout),
	)
}

func lifecycleSQLNullableTime(value time.Time) string {
	if value.IsZero() {
		return "null"
	}
	return lifecycleSQLTime(value)
}

func lifecycleParseSQLTime(value string) (time.Time, error) {
	for _, layout := range []string{
		lifecycleSQLTimestampLayout,
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid Lifecycle SQL timestamp %q", value)
}

func lifecycleNullableString(value *vector.Vector, row int) string {
	if value.GetNulls().Contains(uint64(row)) {
		return ""
	}
	return value.GetStringAt(row)
}

// SQLCleanupReconcileCatalog resolves only Lifecycle-owned metadata. It does
// not attempt to provide a stronger transaction terminal-status service than
// ordinary MO.
type SQLCleanupReconcileCatalog struct {
	Executor executor.SQLExecutor
}

func (catalogAdapter SQLCleanupReconcileCatalog) MatchingPublication(
	ctx context.Context,
	root CleanupRoot,
	_ time.Time,
) (CleanupPublicationState, error) {
	if catalogAdapter.Executor == nil {
		return CleanupPublicationMissing, fmt.Errorf(
			"Lifecycle Cleanup reconcile SQL executor is nil",
		)
	}
	rootID, err := lifecycleSQLUUID(root.RootID)
	if err != nil {
		return CleanupPublicationMissing, err
	}
	attemptID, err := lifecycleSQLUUID(root.AttemptID)
	if err != nil {
		return CleanupPublicationMissing, err
	}
	var sql string
	switch root.Mode {
	case CleanupModeArchiveWhole, CleanupModeArchiveRewrite:
		sql = fmt.Sprintf(
			`select state from mo_catalog.mo_lifecycle_datasets
where root_id=unhex('%s') and attempt_id=unhex('%s') limit 1`,
			rootID,
			attemptID,
		)
	case CleanupModeTTLRewrite:
		sql = fmt.Sprintf(
			`select 'PUBLISHED' from mo_catalog.mo_lifecycle_ttl_receipts
where root_id=unhex('%s') and attempt_id=unhex('%s') limit 1`,
			rootID,
			attemptID,
		)
	default:
		return CleanupPublicationMissing, fmt.Errorf(
			"unknown Lifecycle Cleanup Root mode %s",
			root.Mode,
		)
	}
	result, err := catalogAdapter.Executor.Exec(
		ctx,
		sql,
		executor.Options{}.WithAccountID(root.OwnerAccountID),
	)
	if err != nil {
		// DROP ACCOUNT removes the tenant Catalog before the system-owned Root
		// is reclaimed. Confirm that case in the system account so owner-driven
		// cleanup can converge for a PUBLISHED Root. A COMMIT_UNKNOWN Root still
		// remains fail-closed: missing tenant metadata is not proof of abort.
		accountExists, accountErr := catalogAdapter.accountExists(
			ctx,
			root.OwnerAccountID,
		)
		if accountErr != nil {
			return CleanupPublicationMissing, fmt.Errorf(
				"Lifecycle publication query failed: %w; account lookup failed: %v",
				err,
				accountErr,
			)
		}
		if !accountExists {
			return CleanupPublicationMissing, nil
		}
		return CleanupPublicationMissing, err
	}
	defer result.Close()
	state := ""
	rowsRead := 0
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rowsRead+rows > 1 {
			state = "CORRUPT"
			return false
		}
		if rows == 1 {
			state = columns[0].GetStringAt(0)
			rowsRead++
		}
		return true
	})
	switch state {
	case "":
		return CleanupPublicationMissing, nil
	case "PUBLISHED":
		return CleanupPublicationPublished, nil
	case "DELETE_PENDING", "DELETING", "PURGED":
		return CleanupPublicationDeletePending, nil
	default:
		return CleanupPublicationMissing, fmt.Errorf(
			"Lifecycle matching publication has invalid state %q",
			state,
		)
	}
}

func (catalogAdapter SQLCleanupReconcileCatalog) OwnerExists(
	ctx context.Context,
	root CleanupRoot,
) (bool, error) {
	if catalogAdapter.Executor == nil {
		return false, fmt.Errorf("Lifecycle Cleanup reconcile SQL executor is nil")
	}
	result, err := catalogAdapter.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select rel_id from mo_catalog.mo_tables
where account_id=%d and rel_id=%d limit 1`,
			root.OwnerAccountID,
			root.PhysicalTableID,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return false, err
	}
	defer result.Close()
	return lifecycleResultHasRows(result), nil
}

func (catalogAdapter SQLCleanupReconcileCatalog) RequestCleanup(
	ctx context.Context,
	root CleanupRoot,
	now time.Time,
) (bool, error) {
	if catalogAdapter.Executor == nil || now.IsZero() {
		return false, fmt.Errorf("Lifecycle Cleanup reconcile request is incomplete")
	}
	if root.Mode == CleanupModeTTLRewrite {
		return true, nil
	}
	ownerExists, err := catalogAdapter.OwnerExists(ctx, root)
	if err != nil {
		return false, err
	}
	accountExists, err := catalogAdapter.accountExists(ctx, root.OwnerAccountID)
	if err != nil {
		return false, err
	}
	if !accountExists {
		// Tenant Catalog is already gone. Root is the remaining physical
		// owner and may reclaim its immutable Provider namespace.
		return true, nil
	}
	rootID, err := lifecycleSQLUUID(root.RootID)
	if err != nil {
		return false, err
	}
	attemptID, err := lifecycleSQLUUID(root.AttemptID)
	if err != nil {
		return false, err
	}
	eligibility := ""
	if ownerExists {
		eligibility = " and purge_eligible_at <= " + lifecycleSQLTime(now)
	}
	result, err := catalogAdapter.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_datasets
set state='DELETE_PENDING',version=version+1,updated_at=utc_timestamp()
where root_id=unhex('%s') and attempt_id=unhex('%s') and state='PUBLISHED'
and restore_lease_id is null%s`,
			rootID,
			attemptID,
			eligibility,
		),
		executor.Options{}.WithAccountID(root.OwnerAccountID),
	)
	if err != nil {
		return false, err
	}
	affected := result.AffectedRows
	result.Close()
	if affected == 1 {
		return true, nil
	}
	if affected > 1 {
		return false, fmt.Errorf(
			"Lifecycle Dataset cleanup updated %d rows",
			affected,
		)
	}
	publication, err := catalogAdapter.MatchingPublication(ctx, root, now)
	if err != nil {
		return false, err
	}
	if publication == CleanupPublicationDeletePending {
		return true, nil
	}
	if publication == CleanupPublicationMissing && !ownerExists {
		return true, nil
	}
	return false, nil
}

// FinalizeCleanup closes the tenant-visible Purge only after the Cleanup
// Sweeper has proved the immutable Provider namespace empty for the complete
// quiescence window. It is idempotent because a lost response may leave the
// Dataset already PURGED while the Root is still DELETING.
func (catalogAdapter SQLCleanupReconcileCatalog) FinalizeCleanup(
	ctx context.Context,
	root CleanupRoot,
) error {
	if catalogAdapter.Executor == nil {
		return fmt.Errorf("Lifecycle Cleanup finalize SQL executor is nil")
	}
	if root.Mode == CleanupModeTTLRewrite {
		return nil
	}
	accountExists, err := catalogAdapter.accountExists(ctx, root.OwnerAccountID)
	if err != nil {
		return err
	}
	if !accountExists {
		// DROP ACCOUNT already removed the tenant Catalog. The system-owned Root
		// remains authoritative for physical cleanup and may become CLEANED.
		return nil
	}
	rootID, err := lifecycleSQLUUID(root.RootID)
	if err != nil {
		return err
	}
	attemptID, err := lifecycleSQLUUID(root.AttemptID)
	if err != nil {
		return err
	}
	result, err := catalogAdapter.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_datasets
set state='PURGED',version=version+1,updated_at=utc_timestamp()
where root_id=unhex('%s') and attempt_id=unhex('%s')
and state in ('DELETE_PENDING','DELETING')`,
			rootID,
			attemptID,
		),
		executor.Options{}.WithAccountID(root.OwnerAccountID),
	)
	if err != nil {
		return err
	}
	affected := result.AffectedRows
	result.Close()
	if affected == 1 {
		return nil
	}
	if affected > 1 {
		return fmt.Errorf(
			"Lifecycle Dataset cleanup finalized %d rows",
			affected,
		)
	}
	result, err = catalogAdapter.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select state from mo_catalog.mo_lifecycle_datasets
where root_id=unhex('%s') and attempt_id=unhex('%s') limit 1`,
			rootID,
			attemptID,
		),
		executor.Options{}.WithAccountID(root.OwnerAccountID),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	state := ""
	rowsRead := 0
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rowsRead+rows > 1 {
			state = "CORRUPT"
			return false
		}
		if rows == 1 {
			state = columns[0].GetStringAt(0)
			rowsRead++
		}
		return true
	})
	if rowsRead == 0 && state == "" {
		// A failed Archive attempt reaches DELETE_PENDING before any Dataset is
		// published. Once its Root-owned namespace has passed the physical
		// quiescence check, there is no tenant publication left to finalize.
		return nil
	}
	if state == "PURGED" {
		return nil
	}
	return fmt.Errorf(
		"Lifecycle Dataset cleanup is not final, state %q",
		state,
	)
}

func (catalogAdapter SQLCleanupReconcileCatalog) accountExists(
	ctx context.Context,
	accountID uint32,
) (bool, error) {
	result, err := catalogAdapter.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select account_id from mo_catalog.mo_account
where account_id=%d limit 1`,
			accountID,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return false, err
	}
	defer result.Close()
	return lifecycleResultHasRows(result), nil
}

func lifecycleResultHasRows(result executor.Result) bool {
	found := false
	result.ReadRows(func(rows int, _ []*vector.Vector) bool {
		found = rows > 0
		return false
	})
	return found
}

func lifecycleDecodeDigest(
	value string,
	target *[32]byte,
	nullable bool,
) error {
	if value == "" && nullable {
		return nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(target) {
		return fmt.Errorf("invalid Lifecycle digest %q", value)
	}
	copy(target[:], decoded)
	return nil
}
