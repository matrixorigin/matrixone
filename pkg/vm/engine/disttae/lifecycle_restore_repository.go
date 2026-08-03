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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
)

const (
	lifecycleRestoreInitializeTimeout        = 30 * time.Second
	lifecycleRestoreAdmissionLockWaitTimeout = 5 * time.Second
)

type lifecycleRestoreEngine interface {
	GetRelationById(
		context.Context,
		client.TxnOperator,
		uint64,
	) (string, string, engine.Relation, error)
}

// SQLRestoreRepository implements every Restore ownership transition using
// ordinary MO transactions. It adds no tagged entry or private transaction
// state machine.
type SQLRestoreRepository struct {
	AccountID                        uint32
	TargetDatabaseName               string
	Executor                         executor.SQLExecutor
	Engine                           lifecycleRestoreEngine
	MPool                            *mpool.MPool
	AutoIncrement                    incrservice.AutoIncrementService
	Roots                            lifecyclepkg.CleanupRootRepository
	MaxRestoreStagingBytesPerAccount uint64
}

func (repository SQLRestoreRepository) Initialize(
	ctx context.Context,
	request lifecyclepkg.RestoreInitializeRequest,
) (attempt lifecyclepkg.RestoreAttempt, err error) {
	if err := repository.validate(); err != nil {
		return attempt, err
	}
	if err := repository.validateRestoreAdmission(request.Dataset.LogicalBytes); err != nil {
		return attempt, err
	}
	restoreID, err := lifecycleCatalogUUID(request.Attempt.RestoreID)
	if err != nil {
		return attempt, err
	}
	datasetID, err := lifecycleCatalogUUID(request.Dataset.DatasetID)
	if err != nil {
		return attempt, err
	}
	leaseID, err := lifecycleCatalogUUID(request.Attempt.LeaseID)
	if err != nil {
		return attempt, err
	}
	initializeCtx, cancelInitialize := context.WithTimeout(
		ctx,
		lifecycleRestoreInitializeTimeout,
	)
	defer cancelInitialize()
	err = repository.Executor.ExecTxn(
		initializeCtx,
		func(txn executor.TxnExecutor) error {
			if lockErr := repository.lockRestoreAccount(txn); lockErr != nil {
				return lockErr
			}
			existing, found, readErr := repository.getAttempt(
				initializeCtx,
				func(sql string) (executor.Result, error) {
					return txn.Exec(
						sql,
						executor.StatementOption{}.WithAccountID(repository.AccountID),
					)
				},
				request.Attempt.RestoreID,
			)
			if readErr != nil {
				return readErr
			}
			if found {
				if existing.DatasetID != request.Dataset.DatasetID ||
					existing.LeaseID != request.Attempt.LeaseID ||
					existing.HiddenName != request.Attempt.HiddenName ||
					existing.TargetName != request.Attempt.TargetName {
					return fmt.Errorf("Lifecycle Restore initialization identity mismatch")
				}
				attempt = existing
				return nil
			}
			if admissionErr := repository.checkRestoreAccountAdmission(
				txn,
				request.Dataset.LogicalBytes,
			); admissionErr != nil {
				return admissionErr
			}
			result, execErr := txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_datasets
set restore_lease_id=unhex('%s'),restore_deadline=%s,
access_generation=access_generation+1,version=version+1,updated_at=utc_timestamp()
where dataset_id=unhex('%s') and state='PUBLISHED' and version=%d
and restore_lease_id is null`,
					leaseID,
					lifecycleCatalogTime(request.Attempt.Deadline),
					datasetID,
					request.Dataset.Version,
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			affected := result.AffectedRows
			result.Close()
			if affected != 1 {
				return lifecyclepkg.ErrRestoreInProgress
			}
			result, execErr = txn.Exec(
				request.HiddenCreateSQL,
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			result.Close()
			stagingTableID, execErr := repository.lookupHiddenTableID(
				txn,
				request.Attempt.StagingDatabaseID,
				request.Attempt.HiddenName,
			)
			if execErr != nil {
				return execErr
			}
			request.Attempt.StagingTableID = stagingTableID
			result, execErr = txn.Exec(
				fmt.Sprintf(
					`insert into mo_catalog.mo_lifecycle_restore_attempts(
restore_id,dataset_id,lease_id,deadline,staging_database_id,staging_table_id,
hidden_name,target_database_id,target_name,state,next_chunk_ordinal,
restored_rows,verified_content_hash,last_error,updated_at)
values(unhex('%s'),unhex('%s'),unhex('%s'),%s,%d,%d,%s,%d,%s,
'IMPORTING',0,0,null,null,utc_timestamp())`,
					restoreID,
					datasetID,
					leaseID,
					lifecycleCatalogTime(request.Attempt.Deadline),
					request.Attempt.StagingDatabaseID,
					stagingTableID,
					lifecycleCatalogQuote(request.Attempt.HiddenName),
					request.Attempt.TargetDatabaseID,
					lifecycleCatalogQuote(request.Attempt.TargetName),
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			defer result.Close()
			if result.AffectedRows != 1 {
				return fmt.Errorf("Lifecycle Restore Attempt insert failed")
			}
			attempt = request.Attempt
			attempt.State = "IMPORTING"
			return nil
		},
		executor.Options{}.
			WithAccountID(repository.AccountID).
			WithLockWaitTimeout(lifecycleRestoreAdmissionLockWaitTimeout),
	)
	return attempt, err
}

func (repository SQLRestoreRepository) validateRestoreAdmission(
	requestedBytes uint64,
) error {
	if requestedBytes == 0 ||
		repository.MaxRestoreStagingBytesPerAccount == 0 {
		return fmt.Errorf("Lifecycle Restore staging limits are invalid")
	}
	return nil
}

// lockRestoreAccount serializes capacity checks only for the requesting
// account. Cross-account Restore initialization remains independent; the
// cluster byte limit is a certification/monitoring boundary rather than a
// distributed transaction invariant, so Lifecycle needs no quota table or
// cluster-wide feature-row lock.
func (repository SQLRestoreRepository) lockRestoreAccount(
	txn executor.TxnExecutor,
) error {
	result, err := txn.Exec(
		fmt.Sprintf(
			`select cast(account_id as bigint unsigned) from mo_catalog.mo_account
where account_id=%d for update`,
			repository.AccountID,
		),
		executor.StatementOption{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	accountID, err := decodeLifecycleRestoreUint64(result, "owner account lock")
	if err != nil {
		return err
	}
	if accountID != uint64(repository.AccountID) {
		return fmt.Errorf("Lifecycle Restore owner account no longer exists")
	}
	return nil
}

func (repository SQLRestoreRepository) checkRestoreAccountAdmission(
	txn executor.TxnExecutor,
	requestedBytes uint64,
) error {
	accountBytes, err := restoreActiveLogicalBytes(txn, repository.AccountID)
	if err != nil {
		return err
	}
	if accountBytes > math.MaxUint64-requestedBytes ||
		accountBytes+requestedBytes > repository.MaxRestoreStagingBytesPerAccount {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"restore_account_bytes",
		).Inc()
		return fmt.Errorf(
			"RESOURCE_BLOCKED: account Restore staging bytes exhausted",
		)
	}
	return nil
}

func restoreActiveLogicalBytes(
	txn executor.TxnExecutor,
	accountID uint32,
) (uint64, error) {
	result, err := txn.Exec(
		`select cast(coalesce(sum(d.logical_bytes),0) as bigint unsigned)
from mo_catalog.mo_lifecycle_restore_attempts a
join mo_catalog.mo_lifecycle_datasets d on d.dataset_id=a.dataset_id
where a.state in ('IMPORTING','PUBLISHING')`,
		executor.StatementOption{}.WithAccountID(accountID),
	)
	if err != nil {
		return 0, err
	}
	defer result.Close()
	return decodeLifecycleRestoreUint64(result, "staging usage")
}

func decodeLifecycleRestoreUint64(
	result executor.Result,
	name string,
) (uint64, error) {
	var value uint64
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rowsRead+rows != 1 {
			decodeErr = fmt.Errorf("Lifecycle Restore %s row is invalid", name)
			return false
		}
		value = vector.GetFixedAtNoTypeCheck[uint64](columns[0], 0)
		rowsRead += rows
		return true
	})
	if decodeErr != nil {
		return 0, decodeErr
	}
	if rowsRead != 1 {
		return 0, fmt.Errorf("Lifecycle Restore %s row is missing", name)
	}
	return value, nil
}

func (repository SQLRestoreRepository) GetAttempt(
	ctx context.Context,
	restoreID string,
) (lifecyclepkg.RestoreAttempt, error) {
	if err := repository.validateReader(); err != nil {
		return lifecyclepkg.RestoreAttempt{}, err
	}
	attempt, found, err := repository.getAttempt(
		ctx,
		func(sql string) (executor.Result, error) {
			return repository.Executor.Exec(
				ctx,
				sql,
				executor.Options{}.WithAccountID(repository.AccountID),
			)
		},
		restoreID,
	)
	if err != nil {
		return lifecyclepkg.RestoreAttempt{}, err
	}
	if !found {
		return lifecyclepkg.RestoreAttempt{}, fmt.Errorf(
			"Lifecycle Restore Attempt %s does not exist",
			restoreID,
		)
	}
	return attempt, nil
}

// FindResumable returns either the one still-live importing attempt or a DONE
// attempt whose published target still has the same physical table identity.
// The latter makes a retry after a lost publish response idempotent without
// preventing a later Restore after that target table has been dropped.
func (repository SQLRestoreRepository) FindResumable(
	ctx context.Context,
	datasetID string,
	targetDatabaseID uint64,
	targetName string,
) (lifecyclepkg.RestoreAttempt, bool, error) {
	if err := repository.validateReader(); err != nil {
		return lifecyclepkg.RestoreAttempt{}, false, err
	}
	if targetDatabaseID == 0 || targetName == "" {
		return lifecyclepkg.RestoreAttempt{}, false, fmt.Errorf(
			"Lifecycle Restore resume target is incomplete",
		)
	}
	encodedDatasetID, err := lifecycleCatalogUUID(datasetID)
	if err != nil {
		return lifecyclepkg.RestoreAttempt{}, false, err
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select hex(a.restore_id),hex(a.dataset_id),hex(a.lease_id),
date_format(a.deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),
a.staging_database_id,a.staging_table_id,a.hidden_name,a.target_database_id,
a.target_name,a.state,a.next_chunk_ordinal,a.restored_rows,
coalesce(hex(a.verified_content_hash),'')
from mo_catalog.mo_lifecycle_restore_attempts a
where a.dataset_id=unhex('%s') and a.target_database_id=%d and a.target_name=%s
and ((a.state='IMPORTING' and a.deadline>utc_timestamp() and exists (
       select 1 from mo_catalog.mo_tables h
       where h.rel_id=a.staging_table_id
         and h.reldatabase_id=a.staging_database_id
         and h.relname=a.hidden_name)) or
     (a.state='DONE' and a.verified_content_hash is not null and exists (
       select 1 from mo_catalog.mo_tables t
       where t.rel_id=a.staging_table_id
         and t.reldatabase_id=a.target_database_id
         and t.relname=a.target_name)))
order by a.updated_at desc,a.restore_id limit 2`,
			encodedDatasetID,
			targetDatabaseID,
			lifecycleCatalogQuote(targetName),
		),
		executor.Options{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return lifecyclepkg.RestoreAttempt{}, false, err
	}
	defer result.Close()
	return repository.decodeAttemptResult(result)
}

func (repository SQLRestoreRepository) ImportChunk(
	ctx context.Context,
	attempt lifecyclepkg.RestoreAttempt,
	receipt lifecyclepkg.RestoreChunkReceipt,
	schema lifecyclepkg.SchemaDescriptor,
	rows [][]lifecyclepkg.CanonicalCell,
) (updated lifecyclepkg.RestoreAttempt, err error) {
	if err := repository.validate(); err != nil {
		return updated, err
	}
	restoreID, err := lifecycleCatalogUUID(attempt.RestoreID)
	if err != nil {
		return updated, err
	}
	leaseID, err := lifecycleCatalogUUID(attempt.LeaseID)
	if err != nil {
		return updated, err
	}
	err = repository.Executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			existing, found, readErr := repository.getChunkReceipt(
				txn,
				attempt.RestoreID,
				receipt.ChunkOrdinal,
			)
			if readErr != nil {
				return readErr
			}
			if found {
				if existing.ChunkDigest != receipt.ChunkDigest {
					return fmt.Errorf("Lifecycle Restore Chunk digest corruption")
				}
				current, ok, getErr := repository.getAttempt(
					ctx,
					func(sql string) (executor.Result, error) {
						return txn.Exec(
							sql,
							executor.StatementOption{}.WithAccountID(repository.AccountID),
						)
					},
					attempt.RestoreID,
				)
				if getErr != nil {
					return getErr
				}
				if !ok {
					return fmt.Errorf("Lifecycle Restore Attempt disappeared")
				}
				updated = current
				return nil
			}
			current, found, readErr := repository.getAttempt(
				ctx,
				func(sql string) (executor.Result, error) {
					return txn.Exec(
						sql,
						executor.StatementOption{}.WithAccountID(repository.AccountID),
					)
				},
				attempt.RestoreID,
			)
			if readErr != nil {
				return readErr
			}
			if !found ||
				current.State != "IMPORTING" ||
				current.LeaseID != attempt.LeaseID ||
				current.NextChunkOrdinal != receipt.ChunkOrdinal ||
				!current.Deadline.After(time.Now()) {
				return fmt.Errorf("Lifecycle Restore Chunk lease or ordinal CAS failed")
			}
			_, _, relation, getErr := repository.Engine.GetRelationById(
				ctx,
				txn.Txn(),
				current.StagingTableID,
			)
			if getErr != nil {
				return getErr
			}
			value, batchErr := lifecyclepkg.CanonicalRowsToBatch(
				ctx,
				schema,
				rows,
				repository.MPool,
			)
			if batchErr != nil {
				return batchErr
			}
			defer value.Clean(repository.MPool)
			schemaDigest, digestErr := schema.Digest()
			if digestErr != nil {
				return digestErr
			}
			if verifyErr := lifecyclepkg.VerifyRestoreBatch(
				ctx,
				schemaDigest,
				value,
				receipt.RowCount,
				receipt.LogicalBytes,
				receipt.CanonicalContentHash,
			); verifyErr != nil {
				return verifyErr
			}
			if prepareErr := prepareLifecycleRestoreWriteBatch(
				ctx,
				value,
				relation.GetTableDef(ctx),
				repository.AutoIncrement,
				txn.Txn(),
				repository.MPool,
			); prepareErr != nil {
				return prepareErr
			}
			writeErr := relation.Write(ctx, value)
			if writeErr != nil {
				return writeErr
			}
			result, execErr := txn.Exec(
				fmt.Sprintf(
					`insert into mo_catalog.mo_lifecycle_restore_chunks(
restore_id,chunk_ordinal,file_ordinal,row_group_ordinal,chunk_digest,
row_count,logical_bytes,canonical_content_hash,created_at)
values(unhex('%s'),%d,%d,%d,unhex('%s'),%d,%d,unhex('%s'),
utc_timestamp())`,
					restoreID,
					receipt.ChunkOrdinal,
					receipt.FileOrdinal,
					receipt.RowGroupOrdinal,
					hex.EncodeToString(receipt.ChunkDigest[:]),
					receipt.RowCount,
					receipt.LogicalBytes,
					hex.EncodeToString(receipt.CanonicalContentHash[:]),
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			result.Close()
			result, execErr = txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_restore_attempts
set next_chunk_ordinal=next_chunk_ordinal+1,
restored_rows=restored_rows+%d,updated_at=utc_timestamp()
where restore_id=unhex('%s') and lease_id=unhex('%s')
and state='IMPORTING' and next_chunk_ordinal=%d and deadline>utc_timestamp()`,
					receipt.RowCount,
					restoreID,
					leaseID,
					receipt.ChunkOrdinal,
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			defer result.Close()
			if result.AffectedRows != 1 {
				return fmt.Errorf("Lifecycle Restore Chunk progress CAS failed")
			}
			current.NextChunkOrdinal++
			current.RestoredRows += receipt.RowCount
			updated = current
			return nil
		},
		executor.Options{}.WithAccountID(repository.AccountID),
	)
	return updated, err
}

// prepareLifecycleRestoreWriteBatch reuses MO's existing auto-increment
// service to populate the fake primary key added by ordinary CREATE TABLE.
// Archive values remain unchanged; only the target table's internal fake-PK
// vector is added before Relation.Write.
func prepareLifecycleRestoreWriteBatch(
	ctx context.Context,
	value *batch.Batch,
	tableDef *plan.TableDef,
	autoIncrement incrservice.AutoIncrementService,
	txn client.TxnOperator,
	mp *mpool.MPool,
) error {
	if value == nil || tableDef == nil || autoIncrement == nil || mp == nil {
		return fmt.Errorf("Lifecycle Restore write preparation is incomplete")
	}
	if tableDef.Pkey == nil ||
		tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		return fmt.Errorf("Lifecycle Restore staging table has no MO fake primary key")
	}
	if len(value.Attrs) != len(value.Vecs) {
		return fmt.Errorf("Lifecycle Restore staging schema does not match Archive columns")
	}
	writableColumns := 0
	rowIDSeen := false
	for _, column := range tableDef.Cols {
		if column.Name == catalog.Row_ID {
			if rowIDSeen || !column.Hidden ||
				types.T(column.Typ.Id) != types.T_Rowid {
				return fmt.Errorf("Lifecycle Restore staging row ID definition is invalid")
			}
			rowIDSeen = true
			continue
		}
		writableColumns++
	}
	if !rowIDSeen || writableColumns != len(value.Vecs)+1 {
		return fmt.Errorf("Lifecycle Restore staging schema does not match Archive columns")
	}

	logical := make(map[string]int, len(value.Attrs))
	for index, attribute := range value.Attrs {
		key := strings.ToLower(attribute)
		if _, exists := logical[key]; exists {
			return fmt.Errorf("Lifecycle Restore Batch has duplicate column %s", attribute)
		}
		logical[key] = index
	}
	fullAttributes := make([]string, writableColumns)
	fullVectors := make([]*vector.Vector, writableColumns)
	fakeIndex := -1
	writeIndex := 0
	for _, column := range tableDef.Cols {
		if column.Name == catalog.Row_ID {
			continue
		}
		if column.Name == catalog.FakePrimaryKeyColName {
			if fakeIndex != -1 || !column.Hidden || !column.Typ.AutoIncr ||
				types.T(column.Typ.Id) != types.T_uint64 {
				return fmt.Errorf("Lifecycle Restore fake primary key definition is invalid")
			}
			fakeIndex = writeIndex
			writeIndex++
			continue
		}
		if column.Hidden {
			return fmt.Errorf(
				"Lifecycle Restore staging table has unexpected hidden column %s",
				column.Name,
			)
		}
		logicalIndex, exists := logical[strings.ToLower(column.Name)]
		if !exists || value.Vecs[logicalIndex] == nil {
			return fmt.Errorf("Lifecycle Restore staging column %s is missing", column.Name)
		}
		actualType := value.Vecs[logicalIndex].GetType()
		if !actualType.Eq(vector.ProtoTypeToType(column.Typ)) {
			return fmt.Errorf("Lifecycle Restore staging column %s type changed", column.Name)
		}
		fullAttributes[writeIndex] = column.Name
		fullVectors[writeIndex] = value.Vecs[logicalIndex]
		writeIndex++
	}
	if fakeIndex == -1 {
		return fmt.Errorf("Lifecycle Restore staging fake primary key is missing")
	}

	fakeVector := vector.NewVec(types.T_uint64.ToType())
	rows := value.RowCount()
	nulls := make([]bool, rows)
	for index := range nulls {
		nulls[index] = true
	}
	if err := vector.AppendFixedList(
		fakeVector,
		make([]uint64, rows),
		nulls,
		mp,
	); err != nil {
		fakeVector.Free(mp)
		return err
	}
	fullAttributes[fakeIndex] = catalog.FakePrimaryKeyColName
	fullVectors[fakeIndex] = fakeVector
	value.Attrs = fullAttributes
	value.Vecs = fullVectors

	if _, err := autoIncrement.InsertValues(
		ctx,
		tableDef.TblId,
		tableDef.AutoIncrEpoch,
		txn,
		value.Vecs,
		rows,
		int64(rows),
	); err != nil {
		return err
	}
	if fakeVector.Length() != rows || fakeVector.GetNulls().Any() {
		return fmt.Errorf("Lifecycle Restore fake primary key generation is incomplete")
	}
	return nil
}

func (repository SQLRestoreRepository) ListChunkReceipts(
	ctx context.Context,
	restoreID string,
) ([]lifecyclepkg.RestoreChunkReceipt, error) {
	encoded, err := lifecycleCatalogUUID(restoreID)
	if err != nil {
		return nil, err
	}
	result, err := repository.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select chunk_ordinal,file_ordinal,row_group_ordinal,
hex(chunk_digest),row_count,logical_bytes,hex(canonical_content_hash)
from mo_catalog.mo_lifecycle_restore_chunks
where restore_id=unhex('%s') order by chunk_ordinal`,
			encoded,
		),
		executor.Options{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	receipts := make([]lifecyclepkg.RestoreChunkReceipt, 0)
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 7 {
			decodeErr = fmt.Errorf("Lifecycle Restore Chunk query is invalid")
			return false
		}
		for row := 0; row < rows; row++ {
			digest, err := lifecycleRestoreDigest(columns[3].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			contentHash, err := lifecycleRestoreDigest(columns[6].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			receipts = append(receipts, lifecyclepkg.RestoreChunkReceipt{
				RestoreID:            restoreID,
				ChunkOrdinal:         vector.GetFixedAtNoTypeCheck[uint64](columns[0], row),
				FileOrdinal:          vector.GetFixedAtNoTypeCheck[uint32](columns[1], row),
				RowGroupOrdinal:      vector.GetFixedAtNoTypeCheck[uint32](columns[2], row),
				ChunkDigest:          digest,
				RowCount:             vector.GetFixedAtNoTypeCheck[uint64](columns[4], row),
				LogicalBytes:         vector.GetFixedAtNoTypeCheck[uint64](columns[5], row),
				CanonicalContentHash: contentHash,
			})
		}
		return true
	})
	return receipts, decodeErr
}

func (repository SQLRestoreRepository) Publish(
	ctx context.Context,
	attempt lifecyclepkg.RestoreAttempt,
	verifiedHash [sha256.Size]byte,
	schema lifecyclepkg.SchemaDescriptor,
	autoIncrementMaxima []lifecyclepkg.AutoIncrementMax,
) error {
	restoreID, err := lifecycleCatalogUUID(attempt.RestoreID)
	if err != nil {
		return err
	}
	leaseID, err := lifecycleCatalogUUID(attempt.LeaseID)
	if err != nil {
		return err
	}
	datasetID, err := lifecycleCatalogUUID(attempt.DatasetID)
	if err != nil {
		return err
	}
	return repository.Executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			current, found, readErr := repository.getAttempt(
				ctx,
				func(sql string) (executor.Result, error) {
					return txn.Exec(
						sql,
						executor.StatementOption{}.
							WithAccountID(repository.AccountID),
					)
				},
				attempt.RestoreID,
			)
			if readErr != nil {
				return readErr
			}
			if !found {
				return fmt.Errorf("Lifecycle Restore Attempt disappeared")
			}
			if current.State == "DONE" &&
				current.VerifiedHash == verifiedHash {
				return nil
			}
			if current.State != "IMPORTING" ||
				current.LeaseID != attempt.LeaseID ||
				current.StagingDatabaseID != attempt.StagingDatabaseID ||
				current.StagingTableID != attempt.StagingTableID ||
				current.HiddenName != attempt.HiddenName ||
				current.TargetDatabaseID != attempt.TargetDatabaseID ||
				current.TargetName != attempt.TargetName ||
				current.NextChunkOrdinal != attempt.NextChunkOrdinal ||
				current.RestoredRows != attempt.RestoredRows {
				return fmt.Errorf("Lifecycle Restore publish identity changed")
			}
			databaseID, name, tableID, identityErr :=
				repository.lookupTableIdentity(
					txn,
					current.StagingTableID,
				)
			if identityErr != nil {
				return identityErr
			}
			if identityErr = validateLifecycleRestoreHiddenIdentity(
				current,
				databaseID,
				name,
				tableID,
			); identityErr != nil {
				return identityErr
			}
			result, execErr := txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_restore_attempts
set state='PUBLISHING',verified_content_hash=unhex('%s'),
updated_at=utc_timestamp()
where restore_id=unhex('%s') and lease_id=unhex('%s')
and state='IMPORTING' and deadline>utc_timestamp()
and next_chunk_ordinal=%d and restored_rows=%d`,
					hex.EncodeToString(verifiedHash[:]),
					restoreID,
					leaseID,
					attempt.NextChunkOrdinal,
					attempt.RestoredRows,
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			affected := result.AffectedRows
			result.Close()
			if affected != 1 {
				current, found, getErr := repository.getAttempt(
					ctx,
					func(sql string) (executor.Result, error) {
						return txn.Exec(
							sql,
							executor.StatementOption{}.WithAccountID(repository.AccountID),
						)
					},
					attempt.RestoreID,
				)
				if getErr == nil && found && current.State == "DONE" &&
					current.VerifiedHash == verifiedHash {
					return nil
				}
				return fmt.Errorf("Lifecycle Restore publish CAS failed")
			}
			if repository.AutoIncrement != nil {
				for _, maximum := range autoIncrementMaxima {
					columnName, offset, maximumErr :=
						lifecycleRestoreAutoIncrementOffset(ctx, schema, maximum)
					if maximumErr != nil {
						return maximumErr
					}
					if setErr := repository.AutoIncrement.SetOffset(
						ctx,
						attempt.StagingTableID,
						columnName,
						offset,
						txn.Txn(),
					); setErr != nil {
						return setErr
					}
				}
			} else if len(autoIncrementMaxima) > 0 {
				return fmt.Errorf("Lifecycle auto-increment service is unavailable")
			}
			result, execErr = txn.Exec(
				fmt.Sprintf(
					"rename table %s.%s to %s.%s",
					lifecycleRestoreIdentifier(repository.TargetDatabaseName),
					lifecycleRestoreIdentifier(attempt.HiddenName),
					lifecycleRestoreIdentifier(repository.TargetDatabaseName),
					lifecycleRestoreIdentifier(attempt.TargetName),
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			result.Close()
			result, execErr = txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_restore_attempts
set state='DONE',updated_at=utc_timestamp()
where restore_id=unhex('%s') and lease_id=unhex('%s')
and state='PUBLISHING' and staging_table_id=%d`,
					restoreID,
					leaseID,
					attempt.StagingTableID,
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			if result.AffectedRows != 1 {
				result.Close()
				return fmt.Errorf("Lifecycle Restore DONE CAS failed")
			}
			result.Close()
			result, execErr = txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_datasets
set restore_lease_id=null,restore_deadline=null,
access_generation=access_generation+1,version=version+1,
updated_at=utc_timestamp()
where dataset_id=unhex('%s') and state='PUBLISHED'
and restore_lease_id=unhex('%s')`,
					datasetID,
					leaseID,
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			defer result.Close()
			if result.AffectedRows != 1 {
				return fmt.Errorf("Lifecycle Restore Dataset lease release failed")
			}
			return nil
		},
		executor.Options{}.WithAccountID(repository.AccountID),
	)
}

func lifecycleRestoreAutoIncrementOffset(
	ctx context.Context,
	schema lifecyclepkg.SchemaDescriptor,
	maximum lifecyclepkg.AutoIncrementMax,
) (string, uint64, error) {
	if int(maximum.ColumnOrdinal) >= len(schema.Columns) ||
		!schema.Columns[maximum.ColumnOrdinal].AutoIncrement {
		return "", 0, fmt.Errorf("Lifecycle auto-increment maximum is corrupt")
	}
	offset, err := strconv.ParseUint(maximum.Value, 10, 64)
	if err != nil {
		return "", 0, err
	}
	column := schema.Columns[maximum.ColumnOrdinal]
	if err = incrservice.ValidateAutoColumnOffset(
		ctx,
		types.T(column.TypeID),
		offset,
	); err != nil {
		return "", 0, err
	}
	return column.Name, offset, nil
}

func (repository SQLRestoreRepository) CleanupHidden(
	ctx context.Context,
	restoreID string,
) error {
	if err := repository.validateReader(); err != nil {
		return err
	}
	attempt, err := repository.GetAttempt(ctx, restoreID)
	if err != nil {
		return err
	}
	if attempt.State == "DONE" {
		return nil
	}
	encoded, _ := lifecycleCatalogUUID(restoreID)
	datasetID, err := lifecycleCatalogUUID(attempt.DatasetID)
	if err != nil {
		return err
	}
	leaseID, err := lifecycleCatalogUUID(attempt.LeaseID)
	if err != nil {
		return err
	}
	return repository.Executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			current, found, readErr := repository.getAttempt(
				ctx,
				func(sql string) (executor.Result, error) {
					return txn.Exec(
						sql,
						executor.StatementOption{}.
							WithAccountID(repository.AccountID),
					)
				},
				restoreID,
			)
			if readErr != nil {
				return readErr
			}
			if !found {
				return fmt.Errorf("Lifecycle Restore Attempt disappeared")
			}
			if current.State == "DONE" {
				return nil
			}
			if repository.TargetDatabaseName == "" {
				return repository.failRestoreAttemptAndReleaseLease(
					txn,
					encoded,
					datasetID,
					leaseID,
					current,
				)
			}
			databaseID, name, tableID, lookupErr := repository.lookupTableIdentity(
				txn,
				current.StagingTableID,
			)
			if lookupErr != nil {
				return lookupErr
			}
			if identityErr := validateLifecycleRestoreHiddenIdentity(
				current,
				databaseID,
				name,
				tableID,
			); identityErr != nil {
				return identityErr
			}
			result, execErr := txn.Exec(
				fmt.Sprintf(
					`update mo_catalog.mo_lifecycle_restore_attempts
set state='FAILED',updated_at=utc_timestamp()
where restore_id=unhex('%s') and state<>'DONE'
and staging_table_id=%d and hidden_name=%s`,
					encoded,
					current.StagingTableID,
					lifecycleCatalogQuote(current.HiddenName),
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr != nil {
				return execErr
			}
			if result.AffectedRows != 1 {
				result.Close()
				return fmt.Errorf("Lifecycle Restore cleanup CAS failed")
			}
			result.Close()
			result, execErr = txn.Exec(
				fmt.Sprintf(
					"drop table %s.%s",
					lifecycleRestoreIdentifier(repository.TargetDatabaseName),
					lifecycleRestoreIdentifier(current.HiddenName),
				),
				executor.StatementOption{}.WithAccountID(repository.AccountID),
			)
			if execErr == nil {
				result.Close()
			}
			if execErr != nil {
				return execErr
			}
			return repository.releaseRestoreDatasetLease(
				txn,
				datasetID,
				leaseID,
			)
		},
		executor.Options{}.WithAccountID(repository.AccountID),
	)
}

func (repository SQLRestoreRepository) failRestoreAttemptAndReleaseLease(
	txn executor.TxnExecutor,
	restoreID string,
	datasetID string,
	leaseID string,
	attempt lifecyclepkg.RestoreAttempt,
) error {
	result, err := txn.Exec(
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_restore_attempts
set state='FAILED',updated_at=utc_timestamp()
where restore_id=unhex('%s') and state<>'DONE'
and staging_table_id=%d and hidden_name=%s`,
			restoreID,
			attempt.StagingTableID,
			lifecycleCatalogQuote(attempt.HiddenName),
		),
		executor.StatementOption{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return err
	}
	affected := result.AffectedRows
	result.Close()
	if affected != 1 {
		return fmt.Errorf("Lifecycle Restore cleanup CAS failed")
	}
	return repository.releaseRestoreDatasetLease(txn, datasetID, leaseID)
}

func (repository SQLRestoreRepository) releaseRestoreDatasetLease(
	txn executor.TxnExecutor,
	datasetID string,
	leaseID string,
) error {
	result, err := txn.Exec(
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_datasets
set restore_lease_id=null,restore_deadline=null,
access_generation=access_generation+1,version=version+1,
updated_at=utc_timestamp()
where dataset_id=unhex('%s') and state='PUBLISHED'
and restore_lease_id=unhex('%s')`,
			datasetID,
			leaseID,
		),
		executor.StatementOption{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return fmt.Errorf("Lifecycle Restore Dataset lease cleanup failed")
	}
	return nil
}

func (repository SQLRestoreRepository) RequestPurge(
	ctx context.Context,
	dataset lifecyclepkg.RestoreDataset,
	now time.Time,
) error {
	encoded, err := lifecycleCatalogUUID(dataset.DatasetID)
	if err != nil {
		return err
	}
	if dataset.State == "PUBLISHED" {
		result, updateErr := repository.Executor.Exec(
			ctx,
			fmt.Sprintf(
				`update mo_catalog.mo_lifecycle_datasets
set state='DELETE_PENDING',access_generation=access_generation+1,
version=version+1,updated_at=utc_timestamp()
where dataset_id=unhex('%s') and state='PUBLISHED' and version=%d
and purge_eligible_at<=%s
and restore_lease_id is null`,
				encoded,
				dataset.Version,
				lifecycleCatalogTime(now),
			),
			executor.Options{}.WithAccountID(repository.AccountID),
		)
		if updateErr != nil {
			return updateErr
		}
		affected := result.AffectedRows
		result.Close()
		if affected != 1 {
			return lifecyclepkg.ErrRestoreInProgress
		}
	} else if dataset.State != "DELETE_PENDING" {
		return fmt.Errorf(
			"Lifecycle Dataset state %s cannot be purged",
			dataset.State,
		)
	}
	if repository.Roots != nil && dataset.RootID != "" {
		root, rootErr := repository.Roots.Get(ctx, dataset.RootID)
		if rootErr != nil {
			return rootErr
		}
		if root.State == lifecyclepkg.CleanupRootPublished {
			_, rootErr = repository.Roots.Transition(
				ctx,
				root.RootID,
				root.AttemptID,
				root.ExecutorEpoch,
				root.State,
				root.StateVersion,
				lifecyclepkg.CleanupRootDeletePending,
			)
		}
		if rootErr != nil {
			return rootErr
		}
	}
	return nil
}

func (repository SQLRestoreRepository) validate() error {
	if repository.AccountID == 0 ||
		repository.TargetDatabaseName == "" ||
		repository.Executor == nil ||
		repository.Engine == nil ||
		repository.MPool == nil {
		return fmt.Errorf("Lifecycle SQL Restore repository is incomplete")
	}
	return nil
}

func (repository SQLRestoreRepository) validateReader() error {
	if repository.AccountID == 0 || repository.Executor == nil {
		return fmt.Errorf("Lifecycle SQL Restore reader is incomplete")
	}
	return nil
}

func (repository SQLRestoreRepository) lookupHiddenTableID(
	txn executor.TxnExecutor,
	databaseID uint64,
	hiddenName string,
) (uint64, error) {
	result, err := txn.Exec(
		fmt.Sprintf(
			`select rel_id from mo_catalog.mo_tables
where reldatabase_id=%d and relname=%s`,
			databaseID,
			lifecycleCatalogQuote(hiddenName),
		),
		executor.StatementOption{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return 0, err
	}
	defer result.Close()
	var tableID uint64
	rows := 0
	result.ReadRows(func(count int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rows+count != 1 {
			return false
		}
		tableID = vector.GetFixedAtNoTypeCheck[uint64](columns[0], 0)
		rows += count
		return true
	})
	if rows != 1 || tableID == 0 {
		return 0, fmt.Errorf("Lifecycle hidden Restore table was not created")
	}
	return tableID, nil
}

func (repository SQLRestoreRepository) lookupTableIdentity(
	txn executor.TxnExecutor,
	tableID uint64,
) (uint64, string, uint64, error) {
	result, err := txn.Exec(
		fmt.Sprintf(
			`select reldatabase_id,relname,rel_id
from mo_catalog.mo_tables where rel_id=%d`,
			tableID,
		),
		executor.StatementOption{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return 0, "", 0, err
	}
	defer result.Close()
	var databaseID uint64
	var name string
	var actualID uint64
	rows := 0
	result.ReadRows(func(count int, columns []*vector.Vector) bool {
		if len(columns) != 3 || rows+count != 1 {
			return false
		}
		databaseID = vector.GetFixedAtNoTypeCheck[uint64](columns[0], 0)
		name = columns[1].GetStringAt(0)
		actualID = vector.GetFixedAtNoTypeCheck[uint64](columns[2], 0)
		rows += count
		return true
	})
	if rows != 1 {
		return 0, "", 0, fmt.Errorf(
			"Lifecycle Restore table identity is unknown",
		)
	}
	return databaseID, name, actualID, nil
}

func validateLifecycleRestoreHiddenIdentity(
	attempt lifecyclepkg.RestoreAttempt,
	databaseID uint64,
	tableName string,
	tableID uint64,
) error {
	if databaseID != attempt.StagingDatabaseID ||
		tableName != attempt.HiddenName ||
		tableID != attempt.StagingTableID {
		return fmt.Errorf("Lifecycle Restore hidden table identity changed")
	}
	return nil
}

func (repository SQLRestoreRepository) getAttempt(
	ctx context.Context,
	exec func(string) (executor.Result, error),
	restoreID string,
) (lifecyclepkg.RestoreAttempt, bool, error) {
	encoded, err := lifecycleCatalogUUID(restoreID)
	if err != nil {
		return lifecyclepkg.RestoreAttempt{}, false, err
	}
	result, err := exec(fmt.Sprintf(
		`select hex(restore_id),hex(dataset_id),hex(lease_id),
date_format(deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),
staging_database_id,staging_table_id,hidden_name,target_database_id,
target_name,state,next_chunk_ordinal,restored_rows,
coalesce(hex(verified_content_hash),'')
from mo_catalog.mo_lifecycle_restore_attempts
where restore_id=unhex('%s')`,
		encoded,
	))
	if err != nil {
		return lifecyclepkg.RestoreAttempt{}, false, err
	}
	defer result.Close()
	return repository.decodeAttemptResult(result)
}

func (repository SQLRestoreRepository) decodeAttemptResult(
	result executor.Result,
) (lifecyclepkg.RestoreAttempt, bool, error) {
	var attempt lifecyclepkg.RestoreAttempt
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 13 || rowsRead+rows != 1 {
			decodeErr = fmt.Errorf("Lifecycle Restore Attempt row is invalid")
			return false
		}
		deadline, err := time.ParseInLocation(
			"2006-01-02 15:04:05.999999",
			columns[3].GetStringAt(0),
			time.UTC,
		)
		if err != nil {
			decodeErr = err
			return false
		}
		attempt = lifecyclepkg.RestoreAttempt{
			RestoreID:          lifecycleRestoreUUID(columns[0].GetStringAt(0)),
			DatasetID:          lifecycleRestoreUUID(columns[1].GetStringAt(0)),
			LeaseID:            lifecycleRestoreUUID(columns[2].GetStringAt(0)),
			Deadline:           deadline,
			StagingDatabaseID:  vector.GetFixedAtNoTypeCheck[uint64](columns[4], 0),
			StagingTableID:     vector.GetFixedAtNoTypeCheck[uint64](columns[5], 0),
			HiddenName:         columns[6].GetStringAt(0),
			TargetDatabaseID:   vector.GetFixedAtNoTypeCheck[uint64](columns[7], 0),
			TargetDatabaseName: repository.TargetDatabaseName,
			TargetName:         columns[8].GetStringAt(0),
			State:              columns[9].GetStringAt(0),
			NextChunkOrdinal:   vector.GetFixedAtNoTypeCheck[uint64](columns[10], 0),
			RestoredRows:       vector.GetFixedAtNoTypeCheck[uint64](columns[11], 0),
		}
		hashText := columns[12].GetStringAt(0)
		if hashText != "" {
			attempt.VerifiedHash, decodeErr = lifecycleRestoreDigest(hashText)
			if decodeErr != nil {
				return false
			}
		}
		rowsRead += rows
		return true
	})
	if decodeErr != nil {
		return lifecyclepkg.RestoreAttempt{}, false, decodeErr
	}
	return attempt, rowsRead == 1, nil
}

func (repository SQLRestoreRepository) getChunkReceipt(
	txn executor.TxnExecutor,
	restoreID string,
	ordinal uint64,
) (lifecyclepkg.RestoreChunkReceipt, bool, error) {
	encoded, err := lifecycleCatalogUUID(restoreID)
	if err != nil {
		return lifecyclepkg.RestoreChunkReceipt{}, false, err
	}
	result, err := txn.Exec(
		fmt.Sprintf(
			`select hex(chunk_digest) from mo_catalog.mo_lifecycle_restore_chunks
where restore_id=unhex('%s') and chunk_ordinal=%d`,
			encoded,
			ordinal,
		),
		executor.StatementOption{}.WithAccountID(repository.AccountID),
	)
	if err != nil {
		return lifecyclepkg.RestoreChunkReceipt{}, false, err
	}
	defer result.Close()
	var receipt lifecyclepkg.RestoreChunkReceipt
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rowsRead+rows != 1 {
			decodeErr = fmt.Errorf("Lifecycle Restore Chunk row is invalid")
			return false
		}
		receipt.RestoreID = restoreID
		receipt.ChunkOrdinal = ordinal
		receipt.ChunkDigest, decodeErr = lifecycleRestoreDigest(
			columns[0].GetStringAt(0),
		)
		rowsRead += rows
		return decodeErr == nil
	})
	return receipt, rowsRead == 1, decodeErr
}

func lifecycleRestoreDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, fmt.Errorf("invalid Lifecycle digest")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func lifecycleRestoreUUID(value string) string {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return ""
	}
	parsed, err := uuid.FromBytes(decoded)
	if err != nil {
		return ""
	}
	return parsed.String()
}

func lifecycleRestoreIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
