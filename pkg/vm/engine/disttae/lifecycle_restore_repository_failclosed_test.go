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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_incr "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_incr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestSQLRestoreRepositoryReadersFailClosedOnCatalogCorruption(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	failure := errors.New("catalog unavailable")
	errorExecutor := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, failure
	})
	repository := SQLRestoreRepository{
		AccountID:          17,
		TargetDatabaseName: "history",
		Executor:           errorExecutor,
	}
	restoreID := "11111111-1111-1111-1111-111111111111"
	datasetID := "22222222-2222-2222-2222-222222222222"

	_, err := (SQLRestoreRepository{}).GetAttempt(ctx, restoreID)
	require.ErrorContains(t, err, "reader is incomplete")
	_, err = repository.GetAttempt(ctx, "invalid")
	require.ErrorContains(t, err, "invalid Lifecycle Catalog UUID")
	_, err = repository.GetAttempt(ctx, restoreID)
	require.ErrorIs(t, err, failure)

	missing := repository
	missing.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{Mp: mp}, nil
	})
	_, err = missing.GetAttempt(ctx, restoreID)
	require.ErrorContains(t, err, "does not exist")

	_, _, err = repository.FindResumable(ctx, datasetID, 0, "")
	require.ErrorContains(t, err, "target is incomplete")
	_, _, err = repository.FindResumable(ctx, "invalid", 7, "events")
	require.ErrorContains(t, err, "invalid Lifecycle Catalog UUID")
	_, _, err = repository.FindResumable(ctx, datasetID, 7, "events")
	require.ErrorIs(t, err, failure)
	_, found, err := missing.FindResumable(ctx, datasetID, 7, "events")
	require.NoError(t, err)
	require.False(t, found)

	wrongColumns := repository
	wrongColumns.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		return lifecycleRestoreStringRows(t, mp, "only-one-column"), nil
	})
	_, err = wrongColumns.GetAttempt(ctx, restoreID)
	require.ErrorContains(t, err, "Attempt row is invalid")

	badDeadline := repository
	badDeadline.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		result := lifecycleRestoreAttemptRows(t, mp, "IMPORTING")
		require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[3], 0, []byte("invalid"), mp))
		return result, nil
	})
	_, err = badDeadline.GetAttempt(ctx, restoreID)
	require.Error(t, err)

	badHash := repository
	badHash.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		result := lifecycleRestoreAttemptRows(t, mp, "DONE")
		require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[12], 0, []byte("bad"), mp))
		return result, nil
	})
	_, err = badHash.GetAttempt(ctx, restoreID)
	require.ErrorContains(t, err, "invalid Lifecycle digest")
}

func TestSQLRestoreChunkReceiptReaderRejectsCorruptRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	restoreID := "11111111-1111-1111-1111-111111111111"
	failure := errors.New("catalog unavailable")

	_, err := (SQLRestoreRepository{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, failure },
	)}).ListChunkReceipts(ctx, "invalid")
	require.ErrorContains(t, err, "invalid Lifecycle Catalog UUID")
	_, err = (SQLRestoreRepository{AccountID: 17, Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, failure },
	)}).ListChunkReceipts(ctx, restoreID)
	require.ErrorIs(t, err, failure)

	for _, test := range []struct {
		name   string
		mutate func(executor.Result)
		match  string
	}{
		{"columns", func(result executor.Result) {
			result.Batches[0].Vecs = result.Batches[0].Vecs[:1]
		}, "query is invalid"},
		{"chunk-digest", func(result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[3], 0, []byte("bad"), mp))
		}, "invalid Lifecycle digest"},
		{"content-hash", func(result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[6], 0, []byte("bad"), mp))
		}, "invalid Lifecycle digest"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := SQLRestoreRepository{AccountID: 17}
			repository.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
				result := lifecycleRestoreChunkReceiptRows(t, mp)
				test.mutate(result)
				return result, nil
			})
			_, err := repository.ListChunkReceipts(ctx, restoreID)
			require.ErrorContains(t, err, test.match)
		})
	}
}

func TestPrepareLifecycleRestoreWriteBatchRejectsSchemaDrift(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	controller := gomock.NewController(t)
	increments := mock_incr.NewMockAutoIncrementService(controller)
	tableDef := lifecycleRestoreTableDefForTest()
	value := lifecycleRestoreWriteBatchForTest(t, mp)

	require.Error(t, prepareLifecycleRestoreWriteBatch(ctx, nil, tableDef, increments, nil, mp))
	require.Error(t, prepareLifecycleRestoreWriteBatch(ctx, value, nil, increments, nil, mp))
	require.Error(t, prepareLifecycleRestoreWriteBatch(ctx, value, tableDef, nil, nil, mp))
	require.Error(t, prepareLifecycleRestoreWriteBatch(ctx, value, tableDef, increments, nil, nil))

	withoutPrimary := *tableDef
	withoutPrimary.Pkey = nil
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, lifecycleRestoreWriteBatchForTest(t, mp), &withoutPrimary, increments, nil, mp,
	), "no MO fake primary key")

	badAttrs := lifecycleRestoreWriteBatchForTest(t, mp)
	badAttrs.Attrs = append(badAttrs.Attrs, "extra")
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, badAttrs, tableDef, increments, nil, mp,
	), "schema does not match")

	duplicateAttrs := lifecycleRestoreWriteBatchForTest(t, mp)
	duplicateAttrs.Attrs = []string{"payload", "PAYLOAD"}
	duplicateAttrs.Vecs = append(duplicateAttrs.Vecs, duplicateAttrs.Vecs[0])
	duplicateDef := lifecycleRestoreTableDefForTest()
	duplicateDef.Cols = append(duplicateDef.Cols[:1], &plan.ColDef{
		Name: "other", Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
	}, duplicateDef.Cols[1], duplicateDef.Cols[2])
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, duplicateAttrs, duplicateDef, increments, nil, mp,
	), "duplicate column")

	missing := lifecycleRestoreWriteBatchForTest(t, mp)
	missing.Attrs[0] = "other"
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, missing, tableDef, increments, nil, mp,
	), "column payload is missing")

	wrongType := lifecycleRestoreWriteBatchForTest(t, mp)
	wrongType.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(wrongType.Vecs[0], uint64(1), false, mp))
	wrongType.SetRowCount(1)
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, wrongType, tableDef, increments, nil, mp,
	), "type changed")

	unexpectedHidden := lifecycleRestoreTableDefForTest()
	unexpectedHidden.Cols[0].Hidden = true
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, lifecycleRestoreWriteBatchForTest(t, mp), unexpectedHidden, increments, nil, mp,
	), "unexpected hidden column")

	invalidFake := lifecycleRestoreTableDefForTest()
	invalidFake.Cols[1].Typ.AutoIncr = false
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, lifecycleRestoreWriteBatchForTest(t, mp), invalidFake, increments, nil, mp,
	), "fake primary key definition is invalid")

	missingFake := lifecycleRestoreTableDefForTest()
	missingFake.Cols[1].Name = "another"
	require.ErrorContains(t, prepareLifecycleRestoreWriteBatch(
		ctx, lifecycleRestoreWriteBatchForTest(t, mp), missingFake, increments, nil, mp,
	), "unexpected hidden column")
}

func TestLifecycleRestoreAutoIncrementOffsetRejectsCorruptMaximum(t *testing.T) {
	schema := lifecyclepkg.SchemaDescriptor{Columns: []lifecyclepkg.SchemaColumn{{
		Name: "id", TypeID: int32(types.T_uint64), AutoIncrement: true,
	}}}
	_, _, _, err := lifecycleRestoreAutoIncrementOffset(
		context.Background(), schema, lifecyclepkg.AutoIncrementMax{ColumnOrdinal: 1, Value: "1"},
	)
	require.ErrorContains(t, err, "maximum is corrupt")
	schema.Columns[0].AutoIncrement = false
	_, _, _, err = lifecycleRestoreAutoIncrementOffset(
		context.Background(), schema, lifecyclepkg.AutoIncrementMax{ColumnOrdinal: 0, Value: "1"},
	)
	require.ErrorContains(t, err, "maximum is corrupt")
	schema.Columns[0].AutoIncrement = true
	_, _, _, err = lifecycleRestoreAutoIncrementOffset(
		context.Background(), schema, lifecyclepkg.AutoIncrementMax{ColumnOrdinal: 0, Value: "invalid"},
	)
	require.Error(t, err)
}

func TestSQLRestoreRequestPurgeRejectsInvalidStateAndCatalogFailure(t *testing.T) {
	ctx := context.Background()
	dataset := lifecyclepkg.RestoreDataset{
		DatasetID: "22222222-2222-2222-2222-222222222222",
		State:     "PUBLISHED",
		Version:   3,
	}
	failure := errors.New("catalog unavailable")

	err := (SQLRestoreRepository{}).RequestPurge(ctx, lifecyclepkg.RestoreDataset{DatasetID: "invalid"}, time.Now())
	require.ErrorContains(t, err, "invalid Lifecycle Catalog UUID")
	err = (SQLRestoreRepository{AccountID: 17, Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, failure },
	)}).RequestPurge(ctx, dataset, time.Now())
	require.ErrorIs(t, err, failure)
	dataset.State = "VERIFYING"
	err = (SQLRestoreRepository{}).RequestPurge(ctx, dataset, time.Now())
	require.ErrorContains(t, err, "cannot be purged")
	dataset.State = "DELETE_PENDING"
	require.NoError(t, (SQLRestoreRepository{}).RequestPurge(ctx, dataset, time.Now()))
}

func TestSQLRestoreRepositoryIdentityReadersRejectMissingAndCorruptRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	failure := errors.New("catalog unavailable")
	repository := SQLRestoreRepository{AccountID: 17, TargetDatabaseName: "history"}
	errorTxn := restoreTxnExecutor{execute: func(
		string,
		executor.StatementOption,
	) (executor.Result, error) {
		return executor.Result{}, failure
	}}

	_, err := repository.lookupHiddenTableID(errorTxn, 7, "hidden")
	require.ErrorIs(t, err, failure)
	missingTxn := restoreTxnExecutor{execute: func(
		string,
		executor.StatementOption,
	) (executor.Result, error) {
		return executor.Result{Mp: mp}, nil
	}}
	_, err = repository.lookupHiddenTableID(missingTxn, 7, "hidden")
	require.ErrorContains(t, err, "was not created")
	zeroTxn := restoreTxnExecutor{execute: func(
		string,
		executor.StatementOption,
	) (executor.Result, error) {
		result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{0}))
		return result.GetResult(), nil
	}}
	_, err = repository.lookupHiddenTableID(zeroTxn, 7, "hidden")
	require.ErrorContains(t, err, "was not created")

	_, _, _, _, err = repository.lookupTableIdentity(errorTxn, 88)
	require.ErrorIs(t, err, failure)
	_, _, _, found, err := repository.lookupTableIdentity(missingTxn, 88)
	require.NoError(t, err)
	require.False(t, found)

	_, _, err = repository.getChunkReceipt(errorTxn, "invalid", 0)
	require.ErrorContains(t, err, "invalid Lifecycle Catalog UUID")
	_, _, err = repository.getChunkReceipt(
		errorTxn,
		"11111111-1111-1111-1111-111111111111",
		0,
	)
	require.ErrorIs(t, err, failure)
	malformedTxn := restoreTxnExecutor{execute: func(
		string,
		executor.StatementOption,
	) (executor.Result, error) {
		return lifecycleRestoreTableIdentityRows(t, mp, 7, "hidden", 88), nil
	}}
	_, _, err = repository.getChunkReceipt(
		malformedTxn,
		"11111111-1111-1111-1111-111111111111",
		0,
	)
	require.ErrorContains(t, err, "Chunk row is invalid")
	badDigestTxn := restoreTxnExecutor{execute: func(
		string,
		executor.StatementOption,
	) (executor.Result, error) {
		return lifecycleRestoreStringRows(t, mp, "bad"), nil
	}}
	_, _, err = repository.getChunkReceipt(
		badDigestTxn,
		"11111111-1111-1111-1111-111111111111",
		0,
	)
	require.ErrorContains(t, err, "invalid Lifecycle digest")

	repository.Executor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		return lifecycleRestoreAttemptRows(t, mp, "DONE"), nil
	})
	require.NoError(t, repository.CleanupHidden(
		ctx,
		"11111111-1111-1111-1111-111111111111",
	))
}

func TestSQLRestoreInitializeIsIdempotentAndStopsBeforeNewSideEffects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	request := lifecycleRestoreInitializeRequestForTest(10)
	request.Attempt.HiddenName = lifecycleRestoreAttemptForTest("IMPORTING").HiddenName

	t.Run("matching-attempt", func(t *testing.T) {
		calls := 0
		repository := lifecycleRestoreRepositoryForAdmissionTest(mp, &restoreTxnSQLExecutor{
			execute: func(sql string, _ executor.StatementOption) (executor.Result, error) {
				calls++
				switch calls {
				case 1:
					return lifecycleRestoreUint64Rows(t, mp, 17), nil
				case 2:
					return lifecycleRestoreAttemptRows(t, mp, "IMPORTING"), nil
				default:
					t.Fatalf("resumable initialization must not create another side effect: %s", sql)
					return executor.Result{}, nil
				}
			},
		}, 100)
		attempt, err := repository.Initialize(ctx, request)
		require.NoError(t, err)
		require.Equal(t, request.Attempt.RestoreID, attempt.RestoreID)
		require.Equal(t, 2, calls)
	})

	t.Run("identity-mismatch", func(t *testing.T) {
		calls := 0
		repository := lifecycleRestoreRepositoryForAdmissionTest(mp, &restoreTxnSQLExecutor{
			execute: func(sql string, _ executor.StatementOption) (executor.Result, error) {
				calls++
				if calls == 1 {
					return lifecycleRestoreUint64Rows(t, mp, 17), nil
				}
				if calls == 2 {
					return lifecycleRestoreAttemptRows(t, mp, "IMPORTING"), nil
				}
				t.Fatalf("identity mismatch must stop before mutation: %s", sql)
				return executor.Result{}, nil
			},
		}, 100)
		mismatch := request
		mismatch.Attempt.TargetName = "other_target"
		_, err := repository.Initialize(ctx, mismatch)
		require.ErrorContains(t, err, "initialization identity mismatch")
		require.Equal(t, 2, calls)
	})

	t.Run("dataset-lease-cas", func(t *testing.T) {
		calls := 0
		repository := lifecycleRestoreRepositoryForAdmissionTest(mp, &restoreTxnSQLExecutor{
			execute: func(sql string, _ executor.StatementOption) (executor.Result, error) {
				calls++
				switch calls {
				case 1:
					return lifecycleRestoreUint64Rows(t, mp, 17), nil
				case 2:
					return executor.Result{Mp: mp}, nil
				case 3:
					return lifecycleRestoreUint64Rows(t, mp, 0), nil
				case 4:
					return executor.Result{AffectedRows: 0, Mp: mp}, nil
				default:
					t.Fatalf("failed Dataset lease CAS must precede hidden table creation: %s", sql)
					return executor.Result{}, nil
				}
			},
		}, 100)
		_, err := repository.Initialize(ctx, request)
		require.ErrorIs(t, err, lifecyclepkg.ErrRestoreInProgress)
		require.Equal(t, 4, calls)
	})
}

func TestSQLRestoreImportChunkRejectsConflictingReceiptAndAttempt(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attempt := lifecycleRestoreAttemptForTest("IMPORTING")
	receipt := lifecyclepkg.RestoreChunkReceipt{
		RestoreID:    attempt.RestoreID,
		ChunkOrdinal: attempt.NextChunkOrdinal,
		ChunkDigest:  [32]byte{1},
	}

	t.Run("conflicting-receipt", func(t *testing.T) {
		repository := SQLRestoreRepository{
			AccountID:          17,
			TargetDatabaseName: "history",
			Engine:             lifecycleRestoreEngineStub{},
			MPool:              mp,
			Executor: &restoreTxnSQLExecutor{execute: func(
				string,
				executor.StatementOption,
			) (executor.Result, error) {
				return lifecycleRestoreStringRows(t, mp, stringsRepeatHex("02")), nil
			}},
		}
		_, err := repository.ImportChunk(ctx, attempt, receipt, lifecyclepkg.SchemaDescriptor{}, nil)
		require.ErrorContains(t, err, "Chunk digest corruption")
	})

	t.Run("missing-attempt", func(t *testing.T) {
		calls := 0
		repository := SQLRestoreRepository{
			AccountID:          17,
			TargetDatabaseName: "history",
			Engine:             lifecycleRestoreEngineStub{},
			MPool:              mp,
			Executor: &restoreTxnSQLExecutor{execute: func(
				string,
				executor.StatementOption,
			) (executor.Result, error) {
				calls++
				return executor.Result{Mp: mp}, nil
			}},
		}
		_, err := repository.ImportChunk(ctx, attempt, receipt, lifecyclepkg.SchemaDescriptor{}, nil)
		require.ErrorContains(t, err, "lease or ordinal CAS failed")
		require.Equal(t, 2, calls)
	})

	t.Run("stale-attempt-state", func(t *testing.T) {
		calls := 0
		repository := SQLRestoreRepository{
			AccountID:          17,
			TargetDatabaseName: "history",
			Engine:             lifecycleRestoreEngineStub{},
			MPool:              mp,
			Executor: &restoreTxnSQLExecutor{execute: func(
				string,
				executor.StatementOption,
			) (executor.Result, error) {
				calls++
				if calls == 1 {
					return executor.Result{Mp: mp}, nil
				}
				return lifecycleRestoreAttemptRows(t, mp, "PUBLISHING"), nil
			}},
		}
		_, err := repository.ImportChunk(ctx, attempt, receipt, lifecyclepkg.SchemaDescriptor{}, nil)
		require.ErrorContains(t, err, "lease or ordinal CAS failed")
		require.Equal(t, 2, calls)
	})
}

func TestSQLRestorePublishFailsClosedBeforeOrDuringCatalogHandoff(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	verified := [32]byte{7, 8, 9}
	attempt := lifecycleRestoreAttemptForTest("IMPORTING")

	tests := []struct {
		name       string
		current    executor.Result
		table      executor.Result
		publishCAS uint64
		doneCAS    uint64
		leaseCAS   uint64
		failSQL    int
		maxima     []lifecyclepkg.AutoIncrementMax
		match      string
	}{
		{name: "missing-attempt", current: executor.Result{Mp: mp}, match: "Attempt disappeared"},
		{name: "stale-attempt", current: lifecycleRestoreAttemptRows(t, mp, "PUBLISHING"), match: "publish identity changed"},
		{
			name:    "missing-hidden-table",
			current: lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:   executor.Result{Mp: mp},
			match:   "table identity is unknown",
		},
		{
			name:    "renamed-hidden-table",
			current: lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table: lifecycleRestoreTableIdentityRows(
				t, mp, attempt.StagingDatabaseID, attempt.TargetName, attempt.StagingTableID,
			),
			match: "hidden table identity changed",
		},
		{
			name:       "publishing-cas",
			current:    lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:      lifecycleRestoreTableIdentityRows(t, mp, 7, attempt.HiddenName, 88),
			publishCAS: 0,
			match:      "publish CAS failed",
		},
		{
			name:       "auto-increment-service",
			current:    lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:      lifecycleRestoreTableIdentityRows(t, mp, 7, attempt.HiddenName, 88),
			publishCAS: 1,
			maxima:     []lifecyclepkg.AutoIncrementMax{{ColumnOrdinal: 0, Value: "1"}},
			match:      "auto-increment service is unavailable",
		},
		{
			name:       "rename-error",
			current:    lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:      lifecycleRestoreTableIdentityRows(t, mp, 7, attempt.HiddenName, 88),
			publishCAS: 1,
			failSQL:    4,
			match:      "injected catalog failure",
		},
		{
			name:       "done-cas",
			current:    lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:      lifecycleRestoreTableIdentityRows(t, mp, 7, attempt.HiddenName, 88),
			publishCAS: 1,
			doneCAS:    0,
			match:      "DONE CAS failed",
		},
		{
			name:       "dataset-lease-release",
			current:    lifecycleRestoreAttemptRows(t, mp, "IMPORTING"),
			table:      lifecycleRestoreTableIdentityRows(t, mp, 7, attempt.HiddenName, 88),
			publishCAS: 1,
			doneCAS:    1,
			leaseCAS:   0,
			match:      "Dataset lease release failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			repository := SQLRestoreRepository{
				AccountID:          17,
				TargetDatabaseName: "history",
				Executor: &restoreTxnSQLExecutor{execute: func(
					_ string,
					_ executor.StatementOption,
				) (executor.Result, error) {
					calls++
					if calls == test.failSQL {
						return executor.Result{}, errors.New("injected catalog failure")
					}
					switch calls {
					case 1:
						return test.current, nil
					case 2:
						return test.table, nil
					case 3:
						return executor.Result{AffectedRows: test.publishCAS, Mp: mp}, nil
					case 4:
						if test.publishCAS == 0 {
							return lifecycleRestoreAttemptRows(t, mp, "IMPORTING"), nil
						}
						return executor.Result{Mp: mp}, nil
					case 5:
						return executor.Result{AffectedRows: test.doneCAS, Mp: mp}, nil
					case 6:
						return executor.Result{AffectedRows: test.leaseCAS, Mp: mp}, nil
					default:
						t.Fatalf("unexpected publish SQL call %d", calls)
						return executor.Result{}, nil
					}
				}},
			}
			err := repository.Publish(
				ctx,
				attempt,
				verified,
				lifecyclepkg.SchemaDescriptor{Columns: []lifecyclepkg.SchemaColumn{{
					Name: "id", TypeID: int32(types.T_uint64), AutoIncrement: true,
				}}},
				test.maxima,
			)
			require.ErrorContains(t, err, test.match)
		})
	}
}

func lifecycleRestoreWriteBatchForTest(t *testing.T, mp *mpool.MPool) *batch.Batch {
	t.Helper()
	value := batch.NewWithSize(1)
	value.SetAttributes([]string{"payload"})
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(value.Vecs[0], int64(1), false, mp))
	value.SetRowCount(1)
	return value
}

func lifecycleRestoreChunkReceiptRows(t *testing.T, mp *mpool.MPool) executor.Result {
	t.Helper()
	result := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(),
		types.T_uint32.ToType(),
		types.T_uint32.ToType(),
		types.T_varchar.ToType(),
		types.T_uint64.ToType(),
		types.T_uint64.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 1, []uint32{0}))
	require.NoError(t, executor.AppendFixedRows(result, 2, []uint32{0}))
	require.NoError(t, executor.AppendStringRows(result, 3, []string{stringsRepeatHex("11")}))
	require.NoError(t, executor.AppendFixedRows(result, 4, []uint64{10}))
	require.NoError(t, executor.AppendFixedRows(result, 5, []uint64{100}))
	require.NoError(t, executor.AppendStringRows(result, 6, []string{stringsRepeatHex("22")}))
	return result.GetResult()
}

func stringsRepeatHex(value string) string {
	result := ""
	for range 32 {
		result += value
	}
	return result
}
