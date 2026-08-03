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
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_incr "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_incr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestPrepareLifecycleRestoreWriteBatchGeneratesExistingFakePrimaryKey(t *testing.T) {
	mp := mpool.MustNewZero()
	value := batch.NewWithSize(1)
	value.SetAttributes([]string{"payload"})
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		value.Vecs[0],
		[]int64{11, 22},
		[]bool{false, false},
		mp,
	))
	value.SetRowCount(2)
	defer value.Clean(mp)

	tableDef := &plan.TableDef{
		TblId:         88,
		AutoIncrEpoch: 3,
		Cols: []*plan.ColDef{
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{
				Name:   catalog.FakePrimaryKeyColName,
				Hidden: true,
				Typ: plan.Type{
					Id:       int32(types.T_uint64),
					AutoIncr: true,
				},
			},
			{
				Name:   catalog.Row_ID,
				Hidden: true,
				Typ:    plan.Type{Id: int32(types.T_Rowid)},
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
		},
	}
	controller := gomock.NewController(t)
	increments := mock_incr.NewMockAutoIncrementService(controller)
	increments.EXPECT().InsertValues(
		gomock.Any(),
		uint64(88),
		uint32(3),
		gomock.Nil(),
		gomock.Any(),
		2,
		int64(2),
	).DoAndReturn(func(
		_ context.Context,
		_ uint64,
		_ uint32,
		_ client.TxnOperator,
		vecs []*vector.Vector,
		rows int,
		_ int64,
	) (uint64, error) {
		require.Len(t, vecs, 2)
		require.Equal(t, rows, vecs[1].GetNulls().Count())
		for row := 0; row < rows; row++ {
			vector.SetFixedAtNoTypeCheck(vecs[1], row, uint64(row+1))
		}
		vecs[1].SetNulls(nil)
		return uint64(rows), nil
	})

	err := prepareLifecycleRestoreWriteBatch(
		context.Background(),
		value,
		tableDef,
		increments,
		nil,
		mp,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"payload", catalog.FakePrimaryKeyColName}, value.Attrs)
	require.NotContains(t, value.Attrs, catalog.Row_ID)
	require.Equal(t, []int64{11, 22}, vector.MustFixedColWithTypeCheck[int64](value.Vecs[0]))
	require.Equal(t, []uint64{1, 2}, vector.MustFixedColWithTypeCheck[uint64](value.Vecs[1]))
}

func TestLifecycleRestoreAutoIncrementOffsetValidatesColumnTypeLimit(t *testing.T) {
	schema := lifecyclepkg.SchemaDescriptor{Columns: []lifecyclepkg.SchemaColumn{{
		Name:          "id",
		TypeID:        int32(types.T_uint8),
		AutoIncrement: true,
	}}}
	name, offset, err := lifecycleRestoreAutoIncrementOffset(
		context.Background(),
		schema,
		lifecyclepkg.AutoIncrementMax{ColumnOrdinal: 0, Value: "255"},
	)
	require.NoError(t, err)
	require.Equal(t, "id", name)
	require.Equal(t, uint64(255), offset)

	_, _, err = lifecycleRestoreAutoIncrementOffset(
		context.Background(),
		schema,
		lifecyclepkg.AutoIncrementMax{ColumnOrdinal: 0, Value: "256"},
	)
	require.Error(t, err)
}

func TestSQLRestoreInitializeOwnsLeaseTableAndAttemptInOneTransaction(t *testing.T) {
	mp := mpool.MustNewZero()
	var statements []restoreSQLCall
	sqlExecutor := &restoreTxnSQLExecutor{execute: func(
		sql string,
		option executor.StatementOption,
	) (executor.Result, error) {
		statements = append(statements, restoreSQLCall{
			sql:       strings.ToLower(sql),
			accountID: option.AccountID(),
		})
		switch len(statements) {
		case 1:
			return lifecycleRestoreUint64Rows(t, mp, 17), nil
		case 2:
			return executor.Result{Mp: mp}, nil
		case 3:
			return lifecycleRestoreUint64Rows(t, mp, 20), nil
		case 4:
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		case 5:
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		case 6:
			value := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
			value.NewBatch()
			require.NoError(t, executor.AppendFixedRows(
				value,
				0,
				[]uint64{88},
			))
			return value.GetResult(), nil
		case 7:
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		default:
			t.Fatalf("unexpected SQL %s", sql)
			return executor.Result{}, nil
		}
	}}
	repository := SQLRestoreRepository{
		AccountID:                        17,
		TargetDatabaseName:               "history",
		Executor:                         sqlExecutor,
		Engine:                           lifecycleRestoreEngineStub{},
		MPool:                            mp,
		MaxRestoreStagingBytesPerAccount: 100,
	}
	attempt, err := repository.Initialize(
		context.Background(),
		lifecyclepkg.RestoreInitializeRequest{
			Dataset: lifecyclepkg.RestoreDataset{
				DatasetID:    "22222222-2222-2222-2222-222222222222",
				Version:      3,
				LogicalBytes: 10,
			},
			Attempt: lifecyclepkg.RestoreAttempt{
				RestoreID:         "11111111-1111-1111-1111-111111111111",
				LeaseID:           "33333333-3333-3333-3333-333333333333",
				Deadline:          time.Now().Add(time.Minute),
				StagingDatabaseID: 7,
				HiddenName:        "__mo_lifecycle_restore_1",
				TargetDatabaseID:  7,
				TargetName:        "events_history",
			},
			HiddenCreateSQL: "create table history.__mo_lifecycle_restore_1(id bigint)",
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(88), attempt.StagingTableID)
	require.Equal(t, "IMPORTING", attempt.State)
	require.Contains(t, statements[0].sql, "from mo_catalog.mo_account")
	require.Contains(t, statements[0].sql, "for update")
	require.Equal(t, uint32(0), statements[0].accountID)
	require.Contains(t, statements[2].sql, "sum(d.logical_bytes)")
	require.Equal(t, uint32(17), statements[2].accountID)
	require.Contains(t, statements[3].sql, "restore_lease_id")
	require.Contains(t, statements[3].sql, "and restore_lease_id is null")
	require.NotContains(t, statements[3].sql, "or restore_deadline")
	require.Contains(t, statements[4].sql, "create table")
	require.Contains(t, statements[6].sql, "insert into mo_catalog.mo_lifecycle_restore_attempts")
	for _, statement := range statements {
		require.NotContains(t, statement.sql, "mo_feature_registry")
		require.NotContains(t, statement.sql, "mo_lifecycle_cleanup_roots")
	}
}

func TestSQLRestoreInitializeFailsClosedWhenOwnerAccountIsMissing(t *testing.T) {
	mp := mpool.MustNewZero()
	sqlExecutor := &restoreTxnSQLExecutor{execute: func(
		sql string,
		option executor.StatementOption,
	) (executor.Result, error) {
		require.Equal(t, uint32(0), option.AccountID())
		require.Contains(t, strings.ToLower(sql), "from mo_catalog.mo_account")
		return executor.Result{Mp: mp}, nil
	}}
	repository := lifecycleRestoreRepositoryForAdmissionTest(mp, sqlExecutor, 100)
	_, err := repository.Initialize(
		context.Background(),
		lifecycleRestoreInitializeRequestForTest(10),
	)
	require.ErrorContains(t, err, "owner account lock row is missing")
}

func TestSQLRestoreInitializeEnforcesTransactionalStagingCaps(t *testing.T) {
	tests := []struct {
		name         string
		active       uint64
		accountCap   uint64
		errorMessage string
	}{
		{
			name:         "account",
			active:       95,
			accountCap:   100,
			errorMessage: "account Restore staging bytes exhausted",
		},
		{
			name:         "overflow",
			active:       math.MaxUint64,
			accountCap:   math.MaxUint64,
			errorMessage: "account Restore staging bytes exhausted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			sqlExecutor := &restoreTxnSQLExecutor{execute: func(
				sql string,
				_ executor.StatementOption,
			) (executor.Result, error) {
				lower := strings.ToLower(sql)
				switch {
				case strings.Contains(lower, "from mo_catalog.mo_account"):
					return lifecycleRestoreUint64Rows(t, mp, 17), nil
				case strings.Contains(lower, "where restore_id=unhex"):
					return executor.Result{Mp: mp}, nil
				case strings.Contains(lower, "sum(d.logical_bytes)"):
					return lifecycleRestoreUint64Rows(t, mp, test.active), nil
				default:
					t.Fatalf("admission rejection must precede mutation: %s", sql)
					return executor.Result{}, nil
				}
			}}
			repository := lifecycleRestoreRepositoryForAdmissionTest(
				mp,
				sqlExecutor,
				test.accountCap,
			)
			_, err := repository.Initialize(
				context.Background(),
				lifecycleRestoreInitializeRequestForTest(10),
			)
			require.ErrorContains(t, err, test.errorMessage)
		})
	}
}

func TestSQLRestorePurgeRequiresLeaseFullyReleased(t *testing.T) {
	mp := mpool.MustNewZero()
	calls := 0
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		calls++
		lower := strings.ToLower(sql)
		require.Contains(t, lower, "restore_lease_id is null")
		require.NotContains(t, lower, "restore_deadline")
		return executor.Result{AffectedRows: 0, Mp: mp}, nil
	})
	repository := SQLRestoreRepository{
		AccountID: 17,
		Executor:  sqlExecutor,
	}
	err := repository.RequestPurge(
		context.Background(),
		lifecyclepkg.RestoreDataset{
			DatasetID: "22222222-2222-2222-2222-222222222222",
			State:     "PUBLISHED",
			Version:   3,
		},
		time.Now(),
	)
	require.ErrorIs(t, err, lifecyclepkg.ErrRestoreInProgress)
	require.Equal(t, 1, calls)
}

func TestSQLRestoreFindsResumableAttemptByDatasetAndTarget(t *testing.T) {
	mp := mpool.MustNewZero()
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		require.Contains(t, lower, "a.state='importing'")
		require.Contains(t, lower, "a.deadline>utc_timestamp()")
		require.Contains(t, lower, "a.state='done'")
		require.Contains(t, lower, "a.verified_content_hash is not null")
		require.Contains(t, lower, "t.rel_id=a.staging_table_id")
		require.Contains(t, lower, "t.relname=a.target_name")
		require.Contains(t, lower, "h.relname=a.hidden_name")
		require.Contains(t, strings.ToLower(sql), "target_database_id=7")
		require.Contains(t, strings.ToLower(sql), "target_name='events_history'")
		value := executor.NewMemResult([]types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
		}, mp)
		value.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendStringRows(value, 0, []string{
			"11111111111111111111111111111111",
		}))
		require.NoError(t, executor.AppendStringRows(value, 1, []string{
			"22222222222222222222222222222222",
		}))
		require.NoError(t, executor.AppendStringRows(value, 2, []string{
			"33333333333333333333333333333333",
		}))
		require.NoError(t, executor.AppendStringRows(value, 3, []string{
			"2026-08-01 09:00:00.000000",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 4, []uint64{7}))
		require.NoError(t, executor.AppendFixedRows(value, 5, []uint64{88}))
		require.NoError(t, executor.AppendStringRows(value, 6, []string{
			"__mo_lifecycle_restore_1",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 7, []uint64{7}))
		require.NoError(t, executor.AppendStringRows(value, 8, []string{
			"events_history",
		}))
		require.NoError(t, executor.AppendStringRows(value, 9, []string{
			"DONE",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 10, []uint64{4}))
		require.NoError(t, executor.AppendFixedRows(value, 11, []uint64{100}))
		require.NoError(t, executor.AppendStringRows(value, 12, []string{""}))
		return value.GetResult(), nil
	})
	repository := SQLRestoreRepository{
		AccountID:          17,
		TargetDatabaseName: "history",
		Executor:           sqlExecutor,
	}
	attempt, found, err := repository.FindResumable(
		context.Background(),
		"22222222-2222-2222-2222-222222222222",
		7,
		"events_history",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(
		t,
		"11111111-1111-1111-1111-111111111111",
		attempt.RestoreID,
	)
	require.Equal(t, uint64(4), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(100), attempt.RestoredRows)
	require.Equal(t, "DONE", attempt.State)
	require.Equal(t, "history", attempt.TargetDatabaseName)
}

func TestValidateLifecycleRestoreHiddenIdentityIncludesDatabase(t *testing.T) {
	attempt := lifecyclepkg.RestoreAttempt{
		StagingDatabaseID: 7,
		StagingTableID:    88,
		HiddenName:        "__mo_lifecycle_restore_1",
	}
	require.NoError(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		7,
		"__mo_lifecycle_restore_1",
		88,
	))
	require.Error(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		8,
		"__mo_lifecycle_restore_1",
		88,
	))
	require.Error(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		7,
		"events_history",
		88,
	))
}

func TestSQLRestorePublishRetryStopsAtDoneBeforeHiddenIdentityLookup(t *testing.T) {
	mp := mpool.MustNewZero()
	verified := [32]byte{1, 2, 3}
	calls := 0
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		calls++
		require.Contains(
			t,
			strings.ToLower(sql),
			"from mo_catalog.mo_lifecycle_restore_attempts",
		)
		value := executor.NewMemResult([]types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
		}, mp)
		value.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendStringRows(
			value, 0, []string{"11111111111111111111111111111111"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 1, []string{"22222222222222222222222222222222"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 2, []string{"33333333333333333333333333333333"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 3, []string{"2026-08-01 09:00:00.000000"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 4, []uint64{7}))
		require.NoError(t, executor.AppendFixedRows(value, 5, []uint64{88}))
		require.NoError(t, executor.AppendStringRows(
			value, 6, []string{"events_history"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 7, []uint64{7}))
		require.NoError(t, executor.AppendStringRows(
			value, 8, []string{"events_history"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 9, []string{"DONE"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 10, []uint64{4}))
		require.NoError(t, executor.AppendFixedRows(value, 11, []uint64{100}))
		require.NoError(t, executor.AppendStringRows(
			value, 12, []string{hex.EncodeToString(verified[:])},
		))
		return value.GetResult(), nil
	})
	repository := SQLRestoreRepository{
		AccountID: 17,
		Executor:  sqlExecutor,
	}
	require.NoError(t, repository.Publish(
		context.Background(),
		lifecyclepkg.RestoreAttempt{
			RestoreID:    "11111111-1111-1111-1111-111111111111",
			DatasetID:    "22222222-2222-2222-2222-222222222222",
			LeaseID:      "33333333-3333-3333-3333-333333333333",
			VerifiedHash: verified,
		},
		verified,
		lifecyclepkg.SchemaDescriptor{},
		nil,
	))
	require.Equal(t, 1, calls)
}

type restoreSQLCall struct {
	sql       string
	accountID uint32
}

type restoreTxnSQLExecutor struct {
	execute func(string, executor.StatementOption) (executor.Result, error)
}

func (sqlExecutor *restoreTxnSQLExecutor) Exec(
	_ context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	return sqlExecutor.execute(sql, options.StatementOption())
}

func (sqlExecutor *restoreTxnSQLExecutor) ExecTxn(
	_ context.Context,
	execFunc func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	return execFunc(restoreTxnExecutor{execute: sqlExecutor.execute})
}

type restoreTxnExecutor struct {
	execute func(string, executor.StatementOption) (executor.Result, error)
}

func (restoreTxnExecutor) Use(string) {}

func (restoreTxnExecutor) LockTable(string) error { return nil }

func (txn restoreTxnExecutor) Exec(
	sql string,
	options executor.StatementOption,
) (executor.Result, error) {
	return txn.execute(sql, options)
}

func (restoreTxnExecutor) Txn() client.TxnOperator { return nil }

func lifecycleRestoreUint64Rows(
	t *testing.T,
	mp *mpool.MPool,
	values ...uint64,
) executor.Result {
	t.Helper()
	result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
	if len(values) != 0 {
		result.NewBatchWithRowCount(len(values))
		require.NoError(t, executor.AppendFixedRows(result, 0, values))
	}
	return result.GetResult()
}

func lifecycleRestoreRepositoryForAdmissionTest(
	mp *mpool.MPool,
	sqlExecutor executor.SQLExecutor,
	accountCap uint64,
) SQLRestoreRepository {
	return SQLRestoreRepository{
		AccountID:                        17,
		TargetDatabaseName:               "history",
		Executor:                         sqlExecutor,
		Engine:                           lifecycleRestoreEngineStub{},
		MPool:                            mp,
		MaxRestoreStagingBytesPerAccount: accountCap,
	}
}

func lifecycleRestoreInitializeRequestForTest(
	logicalBytes uint64,
) lifecyclepkg.RestoreInitializeRequest {
	return lifecyclepkg.RestoreInitializeRequest{
		Dataset: lifecyclepkg.RestoreDataset{
			DatasetID:    "22222222-2222-2222-2222-222222222222",
			Version:      3,
			LogicalBytes: logicalBytes,
		},
		Attempt: lifecyclepkg.RestoreAttempt{
			RestoreID:         "11111111-1111-1111-1111-111111111111",
			LeaseID:           "33333333-3333-3333-3333-333333333333",
			Deadline:          time.Now().Add(time.Minute),
			StagingDatabaseID: 7,
			HiddenName:        "__mo_lifecycle_restore_1",
			TargetDatabaseID:  7,
			TargetName:        "events_history",
		},
		HiddenCreateSQL: "create table history.__mo_lifecycle_restore_1(id bigint)",
	}
}

type lifecycleRestoreEngineStub struct{}

func (lifecycleRestoreEngineStub) GetRelationById(
	context.Context,
	client.TxnOperator,
	uint64,
) (string, string, engine.Relation, error) {
	panic("not used by initialization")
}
