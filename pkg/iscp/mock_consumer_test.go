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

package iscp

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type flushErrorSQLExecutor struct {
	err error
}

func (e flushErrorSQLExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	return executor.Result{}, e.err
}

func (e flushErrorSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	return e.err
}

func TestInternalSQLConsumerFlushReturnsError(t *testing.T) {
	expected := errors.New("flush failed")
	consumer := &interalSqlConsumer{
		internalSqlExecutor: flushErrorSQLExecutor{err: expected},
		dataRetriever:       &DataRetrieverImpl{accountID: 7},
		tableInfo:           &plan.TableDef{Name: "source"},
	}

	require.ErrorIs(t, consumer.tryFlushSqlBuf(context.Background(), nil, []byte("insert")), expected)
}

type targetLookupRetriever struct {
	MockRetriever
}

func (*targetLookupRetriever) GetAccountID() uint32 { return 42 }

type targetDDLExecutor struct {
	executor.SQLExecutor
	exec func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e targetDDLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return e.exec(ctx, sql, opts)
}

func TestInternalSQLConsumerTargetReadiness(t *testing.T) {
	injected := errors.New("injected target failure")
	for _, tc := range []struct {
		name      string
		exists    []bool
		failure   string
		missingDB bool
		ddlCalls  int
	}{
		{name: "fresh consumers reuse warm target", exists: []bool{true, true}},
		{name: "cold database", exists: []bool{false}, missingDB: true, ddlCalls: 2},
		{name: "cold table", exists: []bool{false}, ddlCalls: 2},
		{name: "target dropped between iterations", exists: []bool{true, false}, ddlCalls: 2},
		{name: "transaction acquisition", exists: []bool{true}, failure: "new"},
		{name: "engine initialization", exists: []bool{true}, failure: "engine"},
		{name: "database lookup", exists: []bool{true}, failure: "database"},
		{name: "relation lookup", exists: []bool{true}, failure: "relation"},
		{name: "lookup commit", exists: []bool{true}, failure: "commit"},
		{name: "lookup cancellation", exists: []bool{true}, failure: "cancel"},
		{name: "database DDL", exists: []bool{false}, missingDB: true, failure: "ddl1", ddlCalls: 1},
		{name: "table DDL", exists: []bool{false}, failure: "ddl2", ddlCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			txnClient := mock_frontend.NewMockTxnClient(ctrl)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(0)))
			defer cancel()
			checkTenant := func(ctx context.Context) {
				require.Equal(t, uint32(42), ctx.Value(defines.TenantIDKey{}))
			}
			ddlCalls := 0
			for _, exists := range tc.exists {
				txn := mock_frontend.NewMockTxnOperator(ctrl)
				finished := false
				eng.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{})
				var newErr error
				if tc.failure == "new" {
					newErr = injected
				}
				// Even a partially acquired operator must be rolled back.
				txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txn, newErr)
				if tc.failure != "new" {
					var engineErr error
					if tc.failure == "engine" {
						engineErr = injected
					}
					eng.EXPECT().New(gomock.Any(), txn).DoAndReturn(func(ctx context.Context, _ any) error {
						checkTenant(ctx)
						return engineErr
					})
				}
				if tc.failure != "new" && tc.failure != "engine" {
					db := mock_frontend.NewMockDatabase(ctrl)
					eng.EXPECT().Database(gomock.Any(), TargetDbName, txn).DoAndReturn(func(ctx context.Context, _ string, _ any) (engine.Database, error) {
						checkTenant(ctx)
						if tc.failure == "database" {
							return nil, injected
						}
						if tc.missingDB {
							return nil, moerr.GetOkExpectedEOB()
						}
						return db, nil
					})
					if !tc.missingDB && tc.failure != "database" {
						db.EXPECT().RelationExists(gomock.Any(), "test_table_7_job", nil).DoAndReturn(func(ctx context.Context, _ string, _ any) (bool, error) {
							checkTenant(ctx)
							if tc.failure == "relation" {
								return false, injected
							}
							if tc.failure == "cancel" {
								cancel()
							}
							return exists, nil
						})
					}
				}
				finish := func(ctx context.Context) error {
					checkTenant(ctx)
					require.NoError(t, ctx.Err(), "cleanup remains usable after cancellation")
					finished = true
					if tc.failure == "commit" {
						return injected
					}
					return nil
				}
				switch tc.failure {
				case "new", "engine", "database", "relation", "cancel":
					txn.EXPECT().Rollback(gomock.Any()).DoAndReturn(finish)
				default:
					txn.EXPECT().Commit(gomock.Any()).DoAndReturn(finish)
				}
				s := &interalSqlConsumer{
					cnEngine: eng, cnTxnClient: txnClient,
					targetTableName: "test_table_7_job",
					tableInfo:       &plan.TableDef{Createsql: "create table src (id int primary key)"},
					internalSqlExecutor: targetDDLExecutor{exec: func(ctx context.Context, sql string, _ executor.Options) (executor.Result, error) {
						checkTenant(ctx)
						require.False(t, exists, "a warm target must not execute DDL")
						require.True(t, finished, "lookup transaction must close before DDL")
						ddlCalls++
						if ddlCalls == 1 {
							require.Equal(t, "create database if not exists "+TargetDbName, sql)
						} else {
							require.Contains(t, sql, "create table if not exists "+TargetDbName+".test_table_7_job")
						}
						if tc.failure == "ddl1" || tc.failure == "ddl2" && ddlCalls == 2 {
							return executor.Result{}, injected
						}
						return executor.Result{}, nil
					}},
				}
				retriever := &targetLookupRetriever{MockRetriever: MockRetriever{dtype: ISCPDataType_Snapshot, noMoreData: true}}
				err := s.Consume(ctx, retriever)
				if tc.failure == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else if tc.failure != "" {
					require.ErrorIs(t, err, injected)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.failure == "", s.inited)
				require.True(t, finished)
			}
			require.Equal(t, tc.ddlCalls, ddlCalls)
		})
	}
}
