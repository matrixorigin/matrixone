// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type databaseDefaultsExecutor struct {
	executor.SQLExecutor
	exec func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e *databaseDefaultsExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return e.exec(ctx, sql, opts)
}

func installDatabaseDefaultsExecutor(t *testing.T, proc *process.Process, exec executor.SQLExecutor, version int64) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	for key, value := range map[string]any{moruntime.InternalSQLExecutor: exec, moruntime.MOProtocolVersion: version} {
		old, had := rt.GetGlobalVariables(key)
		rt.SetGlobalVariables(key, value)
		t.Cleanup(func() {
			if had {
				rt.SetGlobalVariables(key, old)
			} else {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
}
func compileDefaultsResult(t *testing.T, proc *process.Process, collation string, version uint64) executor.Result {
	t.Helper()
	if version == 0 {
		return executor.Result{}
	}
	r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}, proc.Mp())
	r.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(r, 0, []string{"utf8mb4"}))
	require.NoError(t, executor.AppendStringRows(r, 1, []string{collation}))
	require.NoError(t, executor.AppendFixedRows(r, 2, []uint64{version}))
	return r.GetResult()
}

func TestAlterDatabaseDefaultsExecution(t *testing.T) {
	injected := errors.New("catalog failure")
	for _, tc := range []struct {
		name, dbName, id, current                     string
		version                                       uint64
		oldProtocol, subscription, missingDefaults    bool
		lockErr, dbErr, readErr, deleteErr, insertErr error
		wantError                                     string
		wantWrites                                    int
	}{
		{name: "legacy first alter", current: "", wantWrites: 1},
		{name: "replace pair atomically", current: "utf8mb4_general_ci", version: 3, wantWrites: 2},
		{name: "idempotent", current: "utf8mb4_bin", version: 3},
		{name: "read failure", readErr: injected, wantError: "catalog failure"},
		{name: "delete failure", current: "utf8mb4_general_ci", version: 3, deleteErr: injected, wantError: "catalog failure", wantWrites: 1},
		{name: "insert failure", insertErr: injected, wantError: "catalog failure", wantWrites: 1},
		{name: "lock cancellation", lockErr: context.Canceled, wantError: "context canceled"},
		{name: "unknown database", dbErr: moerr.GetOkExpectedEOB(), wantError: "Unknown database"},
		{name: "storage error", dbErr: injected, wantError: "catalog failure"},
		{name: "subscription", subscription: true, wantError: "subscription"},
		{name: "system", dbName: "mo_catalog", wantError: "system database"},
		{name: "old protocol", oldProtocol: true, wantError: "protocol version 95"},
		{name: "invalid database ID", id: "bad", wantError: "invalid syntax"},
		{name: "missing defaults", missingDefaults: true, wantError: "missing database defaults"},
		{name: "version exhausted", current: "utf8mb4_general_ci", version: math.MaxUint64, wantError: "version exhausted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			proc := testutil.NewProcess(t)
			proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
			proc.Ctx = defines.AttachAccountId(t.Context(), 7)
			proc.ReplaceTopCtx(proc.Ctx)
			if tc.dbName == "" {
				tc.dbName = "d"
			}
			if tc.id == "" {
				tc.id = "42"
			}
			locked := false
			stub := gostub.Stub(&lockMoDatabase, func(c *Compile, name string, mode lock.LockMode) error {
				require.Equal(t, tc.dbName, name)
				require.Equal(t, lock.LockMode_Exclusive, mode)
				locked = true
				return tc.lockErr
			})
			defer stub.Reset()
			reachedLookup := !tc.oldProtocol && tc.dbName != "mo_catalog" && tc.lockErr == nil
			if reachedLookup {
				eng.EXPECT().Database(gomock.Any(), tc.dbName, proc.Base.TxnOperator).Return(db, tc.dbErr)
			}
			if reachedLookup && tc.dbErr == nil {
				db.EXPECT().IsSubscription(gomock.Any()).Return(tc.subscription)
				if !tc.subscription {
					db.EXPECT().GetDatabaseId(gomock.Any()).Return(tc.id).AnyTimes()
				}
			}
			writes := []string{}
			exec := &databaseDefaultsExecutor{exec: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
				require.True(t, locked)
				require.Same(t, proc.Base.TxnOperator, opts.Txn())
				require.True(t, opts.DisableIncrStatement())
				accountID, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(7), accountID)
				if strings.HasPrefix(sql, "select ") {
					require.Equal(t, "select character_set, collation_name, version from mo_catalog.mo_database_defaults where account_id = 7 and database_id = 42", sql)
					if tc.readErr != nil {
						return executor.Result{}, tc.readErr
					}
					return compileDefaultsResult(t, proc, tc.current, tc.version), nil
				}
				writes = append(writes, sql)
				if strings.HasPrefix(sql, "delete ") {
					require.Equal(t, "delete from mo_catalog.mo_database_defaults where account_id=7 and database_id=42", sql)
					return executor.Result{}, tc.deleteErr
				}
				require.Equal(t, fmt.Sprintf("insert into mo_catalog.mo_database_defaults (account_id,database_id,character_set,collation_name,version) values (7,42,'utf8mb4','utf8mb4_bin',%d)", tc.version+1), sql)
				return executor.Result{}, tc.insertErr
			}}
			protocol := defines.MORPCVersion95
			if tc.oldProtocol {
				protocol = defines.MORPCVersion94
			}
			installDatabaseDefaultsExecutor(t, proc, exec, protocol)
			defaults := &planpb.DatabaseDefaults{CharacterSet: "utf8mb4", Collation: "utf8mb4_bin", Version: 1}
			if tc.missingDefaults {
				defaults = nil
			}
			s := &Scope{Magic: AlterDatabase, Plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{DdlType: planpb.DataDefinition_ALTER_DATABASE, Definition: &planpb.DataDefinition_AlterDatabase{AlterDatabase: &planpb.AlterDatabase{Database: tc.dbName, Defaults: defaults}}}}}}
			c := &Compile{e: eng, proc: proc, affectRows: new(atomic.Uint64)}
			before := proc.Mp().CurrNB()
			err := c.run(s)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, writes, tc.wantWrites)
			require.Zero(t, c.getAffectedRows())
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

func TestDatabaseDefaultsPlanGenerationFence(t *testing.T) {
	for _, tc := range []struct {
		name, id, collation string
		version             uint64
		expected            *planpb.DatabaseDefaults
		retry               bool
		fail                bool
	}{
		{name: "explicit table override", expected: nil},
		{name: "same generation", id: "42", collation: "utf8mb4_bin", version: 2, expected: &planpb.DatabaseDefaults{DatabaseId: 42, CharacterSet: "utf8mb4", Collation: "utf8mb4_bin", Version: 2}},
		{name: "concurrent alter", id: "42", collation: "utf8mb4_general_ci", version: 3, expected: &planpb.DatabaseDefaults{DatabaseId: 42, CharacterSet: "utf8mb4", Collation: "utf8mb4_bin", Version: 2}, retry: true},
		{name: "drop recreate", id: "43", collation: "utf8mb4_bin", version: 2, expected: &planpb.DatabaseDefaults{DatabaseId: 42, CharacterSet: "utf8mb4", Collation: "utf8mb4_bin", Version: 2}, retry: true},
		{name: "legacy unchanged", id: "42", expected: &planpb.DatabaseDefaults{DatabaseId: 42}},
		{name: "legacy first alter", id: "42", collation: "utf8mb4_bin", version: 1, expected: &planpb.DatabaseDefaults{DatabaseId: 42}, retry: true},
		{name: "read failure", id: "42", expected: &planpb.DatabaseDefaults{DatabaseId: 42}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			db := mock_frontend.NewMockDatabase(ctrl)
			proc := testutil.NewProcess(t)
			if tc.expected != nil {
				db.EXPECT().GetDatabaseId(gomock.Any()).Return(tc.id)
			}
			injected := errors.New("cannot read metadata")
			exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if tc.fail {
					return executor.Result{}, injected
				}
				return compileDefaultsResult(t, proc, tc.collation, tc.version), nil
			})
			installDatabaseDefaultsExecutor(t, proc, exec, defines.MORPCVersion95)
			before := proc.Mp().CurrNB()
			err := (&Compile{proc: proc}).validateDatabaseDefaults(db, tc.expected)
			if tc.fail {
				require.ErrorIs(t, err, injected)
			} else if tc.retry {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

func TestDatabaseDefaultsCreateRebuildsAfterConcurrentAlter(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng, db := mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockDatabase(ctrl)
	proc := testutil.NewProcess(t)
	ctx := defines.AttachAccountId(t.Context(), 0)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)
	proc.GetSessionInfo().Buf = buffer.New()
	defer proc.GetSessionInfo().Buf.Free()
	txnClient, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Base.TxnClient, proc.Base.TxnOperator = txnClient, txnOp
	// The plan is bound before ALTER commits. The first shared-lock acquisition
	// is the phase boundary that exposes the newer committed database default.
	locks, reads, rebuilds := 0, 0, 0
	stub := gostub.Stub(&lockMoDatabase, func(_ *Compile, name string, mode lock.LockMode) error {
		require.Equal(t, "d", name)
		require.Equal(t, lock.LockMode_Shared, mode)
		locks++
		return nil
	})
	defer stub.Reset()
	eng.EXPECT().Database(gomock.Any(), "d", gomock.Any()).Return(db, nil).Times(2)
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").Times(2)
	installDatabaseDefaultsExecutor(t, proc, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		require.Greater(t, locks, reads, "read must follow catalog lock")
		reads++
		return compileDefaultsResult(t, proc, "utf8mb4_bin", 2), nil
	}), defines.MORPCVersion95)
	makePlan := func(charset uint32, version uint64, collation string) *planpb.Plan {
		return &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{DdlType: planpb.DataDefinition_CREATE_TABLE, Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{
			Database: "d", IfNotExists: true, DatabaseDefaults: &planpb.DatabaseDefaults{DatabaseId: 42, CharacterSet: "utf8mb4", Collation: collation, Version: version},
			TableDef: &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: charset}}}},
		}}}}}
	}
	c := NewCompile("test", "d", "create table if not exists t(v varchar(8))", "", "", eng, proc, nil, false, nil, time.Now())
	defer c.Release()
	require.NoError(t, c.Compile(ctx, makePlan(uint32(types.CharsetUTF8), 1, "utf8mb4_general_ci"), nil))
	c.buildPlanFunc = func(context.Context) (*planpb.Plan, error) {
		rebuilds++
		require.Equal(t, 1, reads)
		return makePlan(uint32(types.CharsetUTF8MB4Bin), 2, "utf8mb4_bin"), nil
	}
	// IF NOT EXISTS ends execution only after the generation fence. The stale
	// attempt must never reach this operation; the retry must expose new types.
	db.EXPECT().RelationExists(gomock.Any(), "t", gomock.Any()).DoAndReturn(func(context.Context, string, any) (bool, error) {
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), c.pn.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.Charset)
		return true, nil
	}).Times(1)
	_, err := c.Run(0)
	require.NoError(t, err)
	require.Equal(t, 1, rebuilds)
	require.Equal(t, 2, reads)
	require.Equal(t, 2, locks)
}
