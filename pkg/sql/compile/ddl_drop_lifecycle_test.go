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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type dropDDLExecutor struct {
	executor.SQLExecutor
	exec func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e *dropDDLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return e.exec(ctx, sql, opts)
}

func newDropDDLCompile(t *testing.T, ctrl *gomock.Controller, exec *dropDDLExecutor) (*Compile, *mock_frontend.MockEngine) {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	proc.Base.SessionInfo.TimeZone = time.UTC
	proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithPessimistic(ctrl)
	proc.Base.TxnOperator.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	installDropDDLExecutor(t, proc, exec)
	eng := mock_frontend.NewMockEngine(ctrl)
	c := NewCompile("test", "db", "", "", "", eng, proc, nil, false, nil, time.Now())
	c.disableLock, c.ignorePublish = true, true
	t.Cleanup(c.Release)
	return c, eng
}

func installDropDDLExecutor(t *testing.T, proc *process.Process, exec executor.SQLExecutor) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
		}
	})
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
}

func dropTableScope(tables ...*plan.DropTable) *Scope {
	q := &plan.DropTable{Tables: tables}
	if len(tables) == 1 {
		q = tables[0]
	}
	return &Scope{Plan: &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		DdlType:    plan.DataDefinition_DROP_TABLE,
		Definition: &plan.DataDefinition_DropTable{DropTable: q},
	}}}}
}

func TestDropDatabaseRelationsStopsAfterMemberFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	wantErr := errors.New("second table cleanup failed")
	var fkCleanups, mergeCleanups, indexCleanups int
	exec := &dropDDLExecutor{}
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.Contains(sql, "mo_foreign_keys") {
			fkCleanups++
		}
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_indexes") {
			indexCleanups++
		}
		if strings.Contains(sql, "mo_merge_settings") {
			mergeCleanups++
		}
		return executor.Result{}, nil
	}
	c, _ := newDropDDLCompile(t, ctrl, exec)
	c.ignorePublish = false
	c.skipDataBranchReclaim = true
	c.disableDropAutoIncrement = true
	originalCtx := c.proc.Ctx
	db := mock_frontend.NewMockDatabase(ctrl)
	resolved := make(map[string]engine.Relation)
	for i, name := range []string{"first", "second"} {
		def := &plan.TableDef{TblId: uint64(i + 1), Name: name, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().GetExtraInfo().Return(nil).AnyTimes()
		resolved[name] = rel
		db.EXPECT().Delete(gomock.Any(), name).DoAndReturn(func(context.Context, string) error {
			if name == "second" {
				return wantErr
			}
			return nil
		})
	}
	tables := []*plan.DropTable{
		{Database: "db", Table: "first", TableId: 1, TableDef: &plan.TableDef{TblId: 1, Name: "first", Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}},
		{Database: "db", Table: "second", TableId: 2, TableDef: &plan.TableDef{TblId: 2, Name: "second", Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}},
		{Database: "db", Table: "third", TableId: 3, TableDef: &plan.TableDef{TblId: 3, Name: "third"}},
	}
	err := c.dropDatabaseRelations(db, tables, resolved, true)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, fkCleanups)
	require.Zero(t, indexCleanups, "database recursion must leave index catalog cleanup to its owner")
	require.Equal(t, 1, mergeCleanups)
	require.Same(t, originalCtx, c.proc.Ctx)
	require.False(t, c.ignorePublish)
}

func TestDropDatabaseIndexCleanupUsesResolvedIdentityAndPropagatesFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	wantErr := errors.New("database index cleanup failed")
	var indexDeletes []string
	exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_indexes") {
			indexDeletes = append(indexDeletes, sql)
			return executor.Result{}, wantErr
		}
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	stubs := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(stubs.Reset)
	c.db = "session_default"
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").AnyTimes()
	db.EXPECT().Relations(gomock.Any()).Return(nil, nil)
	eng.EXPECT().Delete(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(nil)
	pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		DdlType:    plan.DataDefinition_DROP_DATABASE,
		Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", DatabaseId: 17}},
	}}}
	c.pn = pn
	require.ErrorIs(t, (&Scope{Plan: pn}).DropDatabase(c), wantErr)
	require.Equal(t, []string{fmt.Sprintf(deleteMoIndexesWithDatabaseIdFormat, 42)}, indexDeletes)
}

func TestDropDatabasePhysicalTemporaryRunsAllocatorCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	exec := &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
		return executor.Result{}, nil
	}}
	c, _ := newDropDDLCompile(t, ctrl, exec)
	c.skipDataBranchReclaim = true
	def := &plan.TableDef{
		TblId:     17,
		Name:      "__mo_tmp_db_t",
		TableType: catalog.SystemTemporaryTable,
		Cols:      []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), AutoIncr: true}}},
	}
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().GetExtraInfo().Return(nil).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Delete(gomock.Any(), def.Name).Return(nil)
	auto := mock_frontend.NewMockAutoIncrementService(ctrl)
	auto.EXPECT().Delete(gomock.Any(), def.TblId, c.proc.GetTxnOperator()).Return(nil)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	oldAuto, hadAuto := rt.GetGlobalVariables(moruntime.AutoIncrementService)
	t.Cleanup(func() {
		if hadAuto {
			rt.SetGlobalVariables(moruntime.AutoIncrementService, oldAuto)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.AutoIncrementService, auto)
		}
	})
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), auto)

	tables := []*plan.DropTable{{
		Database: "db",
		Table:    def.Name,
		TableId:  def.TblId,
		TableDef: def,
	}}
	require.NoError(t, c.dropDatabaseRelations(db, tables, map[string]engine.Relation{def.Name: rel}, true))
}

func TestDropTableLifecycleAdmission(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("persistent=%d", count), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var order []string
			var indexCleanups int
			failGate := true
			gateErr := errors.New("admission failed")
			exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
				if strings.HasPrefix(sql, "delete from mo_catalog.mo_indexes") {
					indexCleanups++
				}
				if sql == databranchutils.LineageOwnerLifecycleLockSQL() {
					order = append(order, "admit")
					if failGate {
						return executor.Result{}, gateErr
					}
				}
				if strings.Contains(sql, "mo_branch_metadata") {
					order = append(order, "reclaim")
				}
				return executor.Result{}, nil
			}}
			c, eng := newDropDDLCompile(t, ctrl, exec)
			// Internal replacement bypasses FK/publication checks but still owns
			// its table-level index cleanup.
			c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
			db := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
			var tables []*plan.DropTable
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("t%d", i)
				def := &plan.TableDef{TblId: uint64(i + 1), Name: name, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
				rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
				rel.EXPECT().GetExtraInfo().Return(nil).AnyTimes()
				db.EXPECT().Relation(gomock.Any(), name, nil).DoAndReturn(func(context.Context, string, any) (engine.Relation, error) {
					order = append(order, name)
					return rel, nil
				}).Times(2)
				db.EXPECT().Delete(gomock.Any(), name).Return(nil).Times(2)
				tables = append(tables, &plan.DropTable{Database: "db", Table: name, TableId: def.TblId, TableDef: def})
			}
			s := dropTableScope(tables...)
			require.ErrorIs(t, s.DropTable(c), gateErr)
			require.Equal(t, []string{"admit"}, order)
			failGate, order = false, nil
			for range 2 {
				require.NoError(t, s.DropTable(c))
				want := []string{"admit"}
				for i := 0; i < count; i++ {
					// The drop probes branch lineage for the pk-move link
					// reclaim decision before the lifecycle finish flips
					// table_deleted, so each member records two
					// mo_branch_metadata statements.
					want = append(want, fmt.Sprintf("t%d", i), "reclaim", "reclaim")
				}
				require.Equal(t, want, order, "each invocation admits once and finishes a member before the next")
				order = nil
			}
			require.Equal(t, 2*count, indexCleanups)
		})
	}
}

func TestDropTableMixedTemporaryFailureOrder(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		tempFirst, failGate, executorTxn bool
	}{
		{name: "temporary prefix retires before failed admission", tempFirst: true, failGate: true},
		{name: "executor temporary prefix uses parent transaction", tempFirst: true, failGate: true, executorTxn: true},
		{name: "persistent reclaim fails before temporary retirement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			wantErr := errors.New("persistent cleanup failed")
			var order []string
			exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
				if sql == databranchutils.LineageOwnerLifecycleLockSQL() {
					order = append(order, "admit")
					if tc.failGate {
						return executor.Result{}, wantErr
					}
				}
				if strings.Contains(sql, "mo_branch_metadata") {
					order = append(order, "reclaim")
					return executor.Result{}, wantErr
				}
				return executor.Result{}, nil
			}}
			c, eng := newDropDDLCompile(t, ctrl, exec)
			c.proc.Base.IsFrontend = true
			c.temporaryDDLInExecutorTxn = tc.executorTxn
			owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"}}}
			c.proc.Session = owner
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if exists {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
				}
			})
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
			tempDB := newStubDatabase(catalog.MO_CATALOG)
			tempDB.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
			if tc.tempFirst {
				eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).DoAndReturn(func(context.Context, string, any) (engine.Database, error) {
					order = append(order, "temporary")
					return tempDB, nil
				})
			} else {
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{TblId: 1}).AnyTimes()
				eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "p", nil).Return(rel, nil)
				db.EXPECT().Delete(gomock.Any(), "p").DoAndReturn(func(context.Context, string) error {
					order = append(order, "delete persistent")
					return nil
				})
			}
			temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", TableDef: &plan.TableDef{IsTemporary: true}}
			persistent := &plan.DropTable{Database: "db", Table: "p", TableId: 1, TableDef: &plan.TableDef{TblId: 1}}
			tables := []*plan.DropTable{persistent, temp}
			if tc.tempFirst {
				tables[0], tables[1] = temp, persistent
			}
			require.ErrorIs(t, dropTableScope(tables...).DropTable(c), wantErr)
			_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
			require.Equal(t, !tc.tempFirst, present)
			require.Equal(t, "tmp", temp.Table, "execution must not mutate the reusable plan")
			if tc.tempFirst {
				require.Equal(t, []string{"temporary", "admit"}, order)
				if tc.executorTxn {
					require.Empty(t, owner.retired)
					require.NotContains(t, tempDB.rels, "physical")
				} else {
					require.Equal(t, []string{"physical"}, owner.retired)
					require.Contains(t, tempDB.rels, "physical")
				}
			} else {
				require.Equal(t, []string{"admit", "delete persistent", "reclaim"}, order)
				require.Empty(t, owner.retired)
			}
		})
	}
}

func TestDropTableMixedTemporaryRCAdmissionFailureOrder(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		tempFirst, executorTxn bool
		retry                  bool
	}{
		{name: "temporary first", tempFirst: true},
		{name: "temporary first retry keeps alias for replanning", tempFirst: true, retry: true},
		{name: "persistent first"},
		{name: "executor temporary first", tempFirst: true, executorTxn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var wantErr error = errors.New("persistent domain discovery failed")
			if tc.retry {
				wantErr = moerr.NewTxnNeedRetryWithDefChangedNoCtx()
			}
			c, eng := newDropDDLCompile(t, ctrl, &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
				return executor.Result{}, nil
			}})
			c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
				ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC,
			)
			c.proc.Base.IsFrontend = true
			c.temporaryDDLInExecutorTxn = tc.executorTxn
			owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{
				tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"},
			}}
			c.proc.Session = owner
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if exists {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
				}
			})
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
			tempDB := newStubDatabase(catalog.MO_CATALOG)
			tempDB.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
			if tc.tempFirst {
				eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(tempDB, nil)
			}
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(nil, wantErr)
			temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", TableDef: &plan.TableDef{IsTemporary: true}}
			persistent := &plan.DropTable{Database: "db", Table: "p", TableId: 1, TableDef: &plan.TableDef{TblId: 1}}
			tables := []*plan.DropTable{persistent, temp}
			if tc.tempFirst {
				tables[0], tables[1] = temp, persistent
			}
			err := dropTableScope(tables...).DropTable(c)
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, tc.retry, c.canRetry(err))
			_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
			require.Equal(t, !tc.tempFirst || tc.retry, present)
			if tc.tempFirst && tc.executorTxn {
				require.Empty(t, owner.retired)
				require.NotContains(t, tempDB.rels, "physical")
			} else if tc.tempFirst && !tc.retry {
				require.Equal(t, []string{"physical"}, owner.retired)
				require.Contains(t, tempDB.rels, "physical")
			} else {
				require.Empty(t, owner.retired)
				require.Contains(t, tempDB.rels, "physical")
			}
			require.Equal(t, "tmp", temp.Table, "execution must not mutate the reusable plan")
			if tc.retry {
				// A failed retry transition or plan rebuild terminalizes Run.
				// The statement owner then publishes the reached prefix once.
				c.finishTemporaryDropRetry()
				c.finishTemporaryDropRetry()
				require.Equal(t, []string{"physical"}, owner.retired)
				_, present = owner.GetTempTable(catalog.MO_CATALOG, "tmp")
				require.False(t, present)
			}
		})
	}
}

func TestDropTableRCStagedTemporaryDuplicateName(t *testing.T) {
	for _, ifExists := range []bool{false, true} {
		name := "without if exists"
		if ifExists {
			name = "with if exists"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, eng := newDropDDLCompile(t, ctrl, nil)
			c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
				ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC,
			)
			c.proc.Base.IsFrontend = true
			owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{
				tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"},
			}}
			c.proc.Session = owner
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if exists {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
				}
			})
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
			tempDB := newStubDatabase(catalog.MO_CATALOG)
			tempDB.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
			eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(tempDB, nil)
			temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", IfExists: ifExists, TableDef: &plan.TableDef{IsTemporary: true}}
			err := dropTableScope(temp, temp).DropTable(c)
			if ifExists {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), "%v", err)
			}
			require.Equal(t, []string{"physical"}, owner.retired)
			_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
			require.False(t, present)
			require.Equal(t, "tmp", temp.Table)
		})
	}
}

func TestCompileRunDropTableRetryPlanFailurePublishesTemporaryPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	retryErr := moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	planErr := errors.New("persistent table disappeared during replan")
	c, eng := newDropDDLCompile(t, ctrl, &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
		return executor.Result{}, nil
	}})
	c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
		ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC,
	)
	c.proc.Base.TxnOperator.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	c.proc.Base.IsFrontend = true
	owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{
		tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"},
	}}
	c.proc.Session = owner
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
	tempDB := newStubDatabase(catalog.MO_CATALOG)
	tempDB.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(tempDB, nil)
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(nil, retryErr)
	temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", TableDef: &plan.TableDef{IsTemporary: true}}
	persistent := &plan.DropTable{Database: "db", Table: "p", TableId: 1, TableDef: &plan.TableDef{TblId: 1}}
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) {
		_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
		require.True(t, present, "retry planning must still resolve the temporary prefix")
		require.Empty(t, owner.retired)
		return nil, planErr
	})
	require.NoError(t, c.Compile(c.proc.Ctx, dropTableScope(temp, persistent).Plan, nil))
	_, err := c.Run(0)
	require.ErrorIs(t, err, planErr)
	require.Equal(t, 1, c.retryTimes)
	require.Equal(t, []string{"physical"}, owner.retired)
	_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
	require.False(t, present)
}

func TestCompileRunDropTablePublishesTemporaryOnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, eng := newDropDDLCompile(t, ctrl, nil)
	c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
		ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC,
	)
	c.proc.Base.IsFrontend = true
	owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{
		tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"},
	}}
	c.proc.Session = owner
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
	tempDB := newStubDatabase(catalog.MO_CATALOG)
	tempDB.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(tempDB, nil)
	temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", TableDef: &plan.TableDef{IsTemporary: true}}
	require.NoError(t, c.Compile(c.proc.Ctx, dropTableScope(temp).Plan, nil))
	_, err := c.Run(0)
	require.NoError(t, err)
	require.Equal(t, []string{"physical"}, owner.retired)
	_, present := owner.GetTempTable(catalog.MO_CATALOG, "tmp")
	require.False(t, present)
}

func TestDropDatabaseSelectsParentOwnedTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	var dropped []string
	exec := &dropDDLExecutor{exec: func(_ context.Context, _ string, _ executor.Options) (executor.Result, error) {
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	c.proc.Session = &trackingTempTableSession{tables: map[string]string{defines.TempTableAliasKey("db", "shadowed"): "__mo_tmp_db_shadowed"}}
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
	c.skipDataBranchReclaim = true
	stubs := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(stubs.Reset)
	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").AnyTimes()
	// The legacy child comes before its parent and has no feature marker.
	names := []string{"legacy", "partition", "index", "parent", "legacy_lookalike", "shadowed", "temporary", "tail"}
	db.EXPECT().Relations(gomock.Any()).Return(names, nil)
	for i, name := range names {
		rel := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), name, nil).Return(rel, nil).AnyTimes()
		extra := &api.SchemaExtra{}
		switch name {
		case "partition":
			extra.FeatureFlag = features.Partition
		case "index":
			extra.FeatureFlag = features.IndexTable
		}
		if name == "partition" || name == "index" {
			rel.EXPECT().GetExtraInfo().Return(extra).AnyTimes()
			continue
		}
		extraCalls := 0
		rel.EXPECT().GetExtraInfo().DoAndReturn(func() *api.SchemaExtra {
			extraCalls++
			if extraCalls <= 2 {
				return extra
			}
			return nil
		}).AnyTimes()
		var defs []engine.TableDef
		if name == "parent" {
			defs = []engine.TableDef{&engine.ConstraintDef{Cts: []engine.Constraint{&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexTableName: "legacy"}}}}}}
		}
		rel.EXPECT().TableDefs(gomock.Any()).Return(defs, nil).AnyTimes()
		def := &plan.TableDef{TblId: uint64(i + 1), Name: name}
		if name == "temporary" {
			def.TableType = catalog.SystemTemporaryTable
		}
		rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		if name != "legacy" && name != "partition" && name != "index" {
			db.EXPECT().Delete(gomock.Any(), name).DoAndReturn(func(context.Context, string) error {
				dropped = append(dropped, name)
				return nil
			})
		}
	}
	stopErr := errors.New("engine tail reached")
	eng.EXPECT().Delete(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(stopErr)
	pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE,
		Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", DatabaseId: 42}},
	}}}
	c.pn = pn
	require.ErrorIs(t, (&Scope{Plan: pn}).DropDatabase(c), stopErr)
	require.Equal(t, []string{"parent", "legacy_lookalike", "shadowed", "temporary", "tail"}, dropped)
}

func TestDropDatabaseRejectsSparseIndexMetadataBeforeDeletion(t *testing.T) {
	for _, tc := range []struct {
		name        string
		defs        []engine.TableDef
		planIndexes []*plan.IndexDef
	}{
		{
			name: "engine index",
			defs: []engine.TableDef{&engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexTableName: "valid"}, nil}},
			}}},
		},
		{
			name:        "plan index",
			planIndexes: []*plan.IndexDef{{IndexTableName: "valid"}, nil},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			exec := &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
				return executor.Result{}, nil
			}}
			c, eng := newDropDDLCompile(t, ctrl, exec)
			c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
			stubs := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
			t.Cleanup(stubs.Reset)
			db := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
			db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
			db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").AnyTimes()
			db.EXPECT().Relations(gomock.Any()).Return([]string{"parent"}, nil)
			rel := mock_frontend.NewMockRelation(ctrl)
			db.EXPECT().Relation(gomock.Any(), "parent", nil).Return(rel, nil)
			rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()
			rel.EXPECT().TableDefs(gomock.Any()).Return(tc.defs, nil)
			if tc.planIndexes != nil {
				rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(7))
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: "parent", Indexes: tc.planIndexes})
			}
			pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", DatabaseId: 42}},
			}}}
			c.pn = pn
			require.ErrorContains(t, (&Scope{Plan: pn}).DropDatabase(c), "nil index metadata")
		})
	}
}

func TestDropDatabaseRejectsIncomingFKBeforeTableWork(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queryErr error
	}{
		{name: "referenced"},
		{name: "query error", queryErr: errors.New("FK query failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			locked, checked := false, false
			const fkSQL = "select incoming_fk"
			var c *Compile
			exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
				if sql != fkSQL {
					return executor.Result{}, nil
				}
				require.True(t, locked, "the database lock must precede the FK check")
				checked = true
				if tc.queryErr != nil {
					return executor.Result{}, tc.queryErr
				}
				return newAlterCopyFixedResult(t, c.proc.Mp(), types.T_bool.ToType(), []bool{true}), nil
			}}
			var eng *mock_frontend.MockEngine
			c, eng = newDropDDLCompile(t, ctrl, exec)
			stubs := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error {
				locked = true
				return nil
			})
			t.Cleanup(stubs.Reset)
			db := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).Times(2)
			db.EXPECT().IsSubscription(gomock.Any()).Return(true).AnyTimes()
			pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", CheckFKSql: fkSQL}},
			}}}
			c.pn = pn
			err := (&Scope{Plan: pn}).DropDatabase(c)
			require.True(t, checked)
			if tc.queryErr != nil {
				require.ErrorIs(t, err, tc.queryErr)
			} else {
				require.ErrorContains(t, err, "referenced by foreign keys")
			}
		})
	}
}

func TestDropDatabaseRechecksIncomingFKAfterLateLockRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	const fkSQL = "select incoming_fk"
	var fkChecks, lockCalls int
	var c *Compile
	exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if sql != fkSQL {
			return executor.Result{}, nil
		}
		fkChecks++
		// The first attempt sees no incoming reference. A later table-lock
		// conflict aborts the statement; its retry must execute the live FK
		// predicate again rather than reuse the old decision.
		return newAlterCopyFixedResult(t, c.proc.Mp(), types.T_bool.ToType(), []bool{fkChecks > 1}), nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	c.disableLock = false
	c.proc.Base.IsFrontend = false
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
	// Use a pessimistic operator so the real late table-lock branch is taken.
	txnClient, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
	txnOp.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	c.proc.Base.TxnClient, c.proc.Base.TxnOperator = txnClient, txnOp
	lockDB := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error {
		lockCalls++
		return nil
	})
	defer lockDB.Reset()
	lockDatabaseTable := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error {
		return nil
	})
	defer lockDatabaseTable.Reset()
	physicalLock := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		if lockCalls == 1 {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		return nil
	})
	defer physicalLock.Reset()
	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
	db.EXPECT().IsSubscription(gomock.Any()).Return(true).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").AnyTimes()
	db.EXPECT().Relations(gomock.Any()).Return([]string{"p"}, nil).Times(1)
	rel := mock_frontend.NewMockRelation(ctrl)
	def := &plan.TableDef{TblId: 1, Name: "p"}
	rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()
	rel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
	rel.EXPECT().GetPrimaryKeys(gomock.Any()).Return([]*engine.Attribute{{Type: types.T_int32.ToType()}}, nil).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "p", nil).Return(rel, nil).Times(1)
	pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE,
		Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", DatabaseId: 42, CheckFKSql: fkSQL}},
	}}}
	s := &Scope{Plan: pn}
	firstErr := s.DropDatabase(c)
	require.True(t, moerr.IsMoErrCode(firstErr, moerr.ErrTxnNeedRetry), firstErr)
	secondErr := s.DropDatabase(c)
	require.ErrorContains(t, secondErr, "referenced by foreign keys")
	require.Equal(t, 2, fkChecks)
	require.Equal(t, 2, lockCalls)
}

func TestDropTableTemporaryAndNoopMembersDoNotAdmit(t *testing.T) {
	for _, rc := range []bool{false, true} {
		name := "SI"
		if rc {
			name = "RC"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			exec := &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
				t.Fatal("temporary/no-op drop entered the persistent lifecycle")
				return executor.Result{}, nil
			}}
			c, eng := newDropDDLCompile(t, ctrl, exec)
			if rc {
				c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
					ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC,
				)
			}
			c.proc.Session = &trackingTempTableSession{tables: map[string]string{defines.TempTableAliasKey(catalog.MO_CATALOG, "tmp"): "physical"}}
			db := newStubDatabase(catalog.MO_CATALOG)
			db.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
			eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(db, nil)
			temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", IfExists: true, TableDef: &plan.TableDef{IsTemporary: true}}
			s := dropTableScope(nil, &plan.DropTable{}, &plan.DropTable{Database: "db", Table: "missing", IfExists: true}, temp, temp)
			require.NoError(t, s.DropTable(c))
			require.NotContains(t, db.rels, "physical")
			// The retained plan sees the now-absent temporary alias on the next call.
			require.NoError(t, s.DropTable(c))
			require.Equal(t, "tmp", temp.Table)
		})
	}
}

func TestDropTableReclaimFailureStopsLaterHooks(t *testing.T) {
	ctrl := gomock.NewController(t)
	wantErr := errors.New("branch reclaim failed before the next table")
	var gates, branchProbes int
	exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if sql == databranchutils.LineageOwnerLifecycleLockSQL() {
			gates++
		}
		if strings.Contains(sql, "mo_branch_metadata") {
			branchProbes++
			return executor.Result{}, wantErr
		}
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{TblId: 1}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "plain", nil).Return(rel, nil)
	db.EXPECT().Delete(gomock.Any(), "plain").Return(nil)
	// No lookup of the second member is allowed after the first reclaim fails.
	s := dropTableScope(
		&plan.DropTable{Database: "db", Table: "plain", TableId: 1, TableDef: &plan.TableDef{TblId: 1}},
		&plan.DropTable{Database: "db", Table: "later", TableId: 2, TableDef: &plan.TableDef{TblId: 2}},
	)
	require.ErrorIs(t, s.DropTable(c), wantErr)
	require.Equal(t, 1, gates)
	require.Equal(t, 1, branchProbes)
}

// Row-less lineage parents (ultimate source tables) accumulate pk-move
// update links as merge destinations but hold no mo_branch_metadata row, so
// the DAG reclaim set can never name them. When the last descendant row of
// such a parent retires, the parent's links retire with it — unless a live
// descendant survives anywhere in its lineage graph (possibly behind a
// deleted intermediate generation), or a probe failure keeps them.
func TestDeletePkMoveUpdateLinksReclaimsRowlessParents(t *testing.T) {
	ctrl := gomock.NewController(t)
	var linkDeletes []string
	probeErr := errors.New("metadata probe failed")
	type child struct {
		id      uint64
		deleted bool
	}
	// children models mo_branch_metadata adjacency per probed parent id.
	children := map[uint64][]child{
		800: {{id: 2, deleted: true}},
		2:   nil,
		900: {{id: 3, deleted: true}, {id: 30, deleted: false}},
		810: {{id: 5, deleted: true}, {id: 51, deleted: true}},
		5:   {{id: 50, deleted: false}},
		820: {{id: 6, deleted: true}},
		6:   {{id: 60, deleted: true}},
		60:  nil,
	}
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
			linkDeletes = append(linkDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_data_branch_update_link") {
			// Every orphan parent in this scenario carries genuine links:
			// the funnel's presence probe (r51 review F2) must report one
			// row so the KEEP/retire decisions below stay on their paths.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"1"}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = ") {
			var pid uint64
			_, err := fmt.Sscanf(sql,
				"select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = %d", &pid)
			require.NoError(t, err)
			if pid == 901 {
				return executor.Result{}, probeErr
			}
			kids := children[pid]
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(len(kids))
			ids := make([]uint64, 0, len(kids))
			dels := make([]bool, 0, len(kids))
			for _, k := range kids {
				ids = append(ids, k.id)
				dels = append(dels, k.deleted)
			}
			require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
			require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
			return memRes.GetResult(), nil
		}
		// Snapshot-grounding gate: name resolution succeeds for every
		// parent and no user snapshot covers them, so the gate does not
		// hold any links back.
		if strings.HasPrefix(sql, "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = ") {
			var relID uint64
			_, err := fmt.Sscanf(sql,
				"select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = %d", &relID)
			require.NoError(t, err)
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"db1"}))
			require.NoError(t, executor.AppendStringRows(memRes, 1, []string{fmt.Sprintf("t%d", relID)}))
			require.NoError(t, executor.AppendFixedRows(memRes, 2, []uint64{relID}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots") {
			memRes := executor.NewMemResult(
				[]types.Type{
					types.T_varchar.ToType(), types.T_varchar.ToType(),
					types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(0)
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	// Retired leaves: 2 hangs off row-less root 800 (no live descendants
	// anywhere), 3 off row-less 900 (a live sibling keeps the links), 4
	// off row-less 901 (probe fails: links stay, error is tolerated).
	// 51 hangs off 810; 810's other child 5 was retired EARLIER but its
	// live descendant 50 survives behind it, so 810's links stay (the
	// direct-children probe previously lost them). 6 hangs off 820; its
	// own descendant chain 6 -> 60 is fully deleted, so 820's links
	// retire through the transitive walk.
	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 3, PTableID: 900, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 30, PTableID: 900, Creator: 7, Level: "table", TableDeleted: false},
		{TableID: 4, PTableID: 901, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 5, PTableID: 810, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 50, PTableID: 5, Creator: 7, Level: "table", TableDeleted: false},
		{TableID: 51, PTableID: 810, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 6, PTableID: 820, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 60, PTableID: 6, Creator: 7, Level: "table", TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)
	require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2, 3, 4, 51, 6},
		map[uint64]pkMoveTableIdentity{
			2: {db: "db1", tbl: "t2"}, 3: {db: "db1", tbl: "t3"},
			4: {db: "db1", tbl: "t4"}, 51: {db: "db1", tbl: "t51"},
			6: {db: "db1", tbl: "t6"},
		}))

	lowerBounds := make(map[uint64]bool, len(linkDeletes))
	for _, sql := range linkDeletes {
		var lo uint64
		_, err := fmt.Sscanf(sql,
			"delete from mo_catalog.mo_data_branch_update_link where link_id >= x'%016x'", &lo)
		require.NoError(t, err, sql)
		lowerBounds[lo] = true
	}
	// Retired rows 2, 3, 4, 51, 6 and row-less parents 800 and 820. (60 is
	// a descendant of root 6, so it retires with its own drop, not here.)
	// Parents 900 (live direct child), 901 (failed probe), and 810 (live
	// descendant behind the deleted intermediate 5) keep their links.
	require.Equal(t, map[uint64]bool{
		2: true, 3: true, 4: true, 51: true, 6: true,
		800: true, 820: true,
	}, lowerBounds)
}

// pkMoveSnapshotCoversTable matches snapshot scope rows against a parent
// table identity: cluster/account rows ground every table of the account,
// database rows the named database, table rows the named table or the
// recorded physical table id — a snapshot created before a rename still
// grounds the renamed table (r24 review F2).
func TestPkMoveSnapshotCoversTable(t *testing.T) {
	rows := []pkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "t1", ObjId: 501},
		{Level: "database", DatabaseName: "db2"},
		{Level: "table", DatabaseName: "db1", TableName: "t9", ObjId: 509},
	}
	// Exact table-scope hit and its near misses.
	require.True(t, pkMoveSnapshotCoversTable(rows, 501, 0, "db1", "t1"))
	require.False(t, pkMoveSnapshotCoversTable(rows, 502, 0, "db1", "t2"))
	require.False(t, pkMoveSnapshotCoversTable(rows, 9, 0, "db9", "t1"))
	// A table renamed after snapshot creation keeps its physical id, so
	// the pre-rename scope row still grounds it by objId even though the
	// names no longer match.
	require.True(t, pkMoveSnapshotCoversTable(rows, 501, 0, "db1", "t1_renamed"))
	require.True(t, pkMoveSnapshotCoversTable(rows, 501, 0, "db9", "zz"),
		"the recorded identity survives even a database-scope name drift")
	// The pre-existing name rule is untouched: matching scope names
	// ground the caller's table even when its id differs from the
	// snapshot row's recorded objId.
	require.True(t, pkMoveSnapshotCoversTable(rows, 502, 0, "db1", "t1"))
	// Database scope grounds any table of that database only.
	require.True(t, pkMoveSnapshotCoversTable(rows, 42, 0, "db2", "anything"))
	require.False(t, pkMoveSnapshotCoversTable(rows, 42, 0, "db3", "anything"))
	// Cluster and account scopes ground everything; their scope columns
	// are not consulted.
	require.True(t, pkMoveSnapshotCoversTable(
		[]pkMoveSnapshotRow{{Level: "cluster"}}, 42, 0, "db9", "zz"))
	require.True(t, pkMoveSnapshotCoversTable(
		[]pkMoveSnapshotRow{{Level: "account"}}, 42, 0, "db9", "zz"))
	// No snapshot rows: nothing is grounded.
	require.False(t, pkMoveSnapshotCoversTable(nil, 501, 0, "db1", "t1"))
	// Unknown levels ground nothing.
	require.False(t, pkMoveSnapshotCoversTable(
		[]pkMoveSnapshotRow{{Level: "branch", DatabaseName: "db1", TableName: "t1", ObjId: 501}},
		501, 0, "db1", "t1"))
}

// A retained user snapshot whose scope covers a row-less lineage parent
// can still ground a historical DATA BRANCH over that table
// (DATA BRANCH CREATE ... FROM t{snapshot="..."}), so the parent's
// pk-move links must survive the last-descendant reclaim for as long as
// the covering snapshot does.
func TestDeletePkMoveUpdateLinksKeepsSnapshotGroundedParents(t *testing.T) {
	ctrl := gomock.NewController(t)
	var linkDeletes []string
	children := map[uint64]map[uint64]bool{
		800: {2: true},
		2:   nil,
		820: {6: true},
		6:   nil,
	}
	// One retained table-level snapshot grounds parent 800 by physical
	// identity: it was created when the table still had its pre-rename
	// name, and name matching alone would miss it (r24 review F2).
	snapshotRows := []pkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "t800_before_rename", ObjId: 800},
	}
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
			linkDeletes = append(linkDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_data_branch_update_link") {
			// Both orphan parents carry genuine links (r51 review F2 gate):
			// 800 keeps them behind the pre-rename snapshot, 820 retires.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"1"}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = ") {
			var pid uint64
			_, err := fmt.Sscanf(sql,
				"select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = %d", &pid)
			require.NoError(t, err)
			kids := children[pid]
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(len(kids))
			ids := make([]uint64, 0, len(kids))
			dels := make([]bool, 0, len(kids))
			for id, deleted := range kids {
				ids = append(ids, id)
				dels = append(dels, deleted)
			}
			require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
			require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = ") {
			var relID uint64
			_, err := fmt.Sscanf(sql,
				"select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = %d", &relID)
			require.NoError(t, err)
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"db1"}))
			require.NoError(t, executor.AppendStringRows(memRes, 1, []string{fmt.Sprintf("t%d", relID)}))
			require.NoError(t, executor.AppendFixedRows(memRes, 2, []uint64{relID}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots") {
			memRes := executor.NewMemResult(
				[]types.Type{
					types.T_varchar.ToType(), types.T_varchar.ToType(),
					types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(len(snapshotRows))
			levels := make([]string, 0, len(snapshotRows))
			dbs := make([]string, 0, len(snapshotRows))
			tbls := make([]string, 0, len(snapshotRows))
			objs := make([]uint64, 0, len(snapshotRows))
			for _, row := range snapshotRows {
				levels = append(levels, row.Level)
				dbs = append(dbs, row.DatabaseName)
				tbls = append(tbls, row.TableName)
				objs = append(objs, row.ObjId)
			}
			require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
			require.NoError(t, executor.AppendStringRows(memRes, 1, dbs))
			require.NoError(t, executor.AppendStringRows(memRes, 2, tbls))
			require.NoError(t, executor.AppendFixedRows(memRes, 3, objs))
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 6, PTableID: 820, Creator: 7, Level: "table", TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)
	require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2, 6},
		map[uint64]pkMoveTableIdentity{2: {db: "db1", tbl: "t2"}, 6: {db: "db1", tbl: "t6"}}))

	lowerBounds := make(map[uint64]bool, len(linkDeletes))
	for _, sql := range linkDeletes {
		var lo uint64
		_, err := fmt.Sscanf(sql,
			"delete from mo_catalog.mo_data_branch_update_link where link_id >= x'%016x'", &lo)
		require.NoError(t, err, sql)
		lowerBounds[lo] = true
	}
	// Retired rows 2 and 6 plus row-less parent 820 (no covering
	// snapshot). Parent 800 keeps its links: the retained table-level
	// snapshot on its pre-rename scope still grounds it by objId.
	require.Equal(t, map[uint64]bool{
		2: true, 6: true, 820: true,
	}, lowerBounds)
}

// r50 review P1: a snapshot-grounded orphan parent's KEEP verdict must
// carry the resolved identity into the retention ledger. The by-id
// grounding probe resolves the parent's names from mo_tables (or the
// ledger itself), and a database-level owner can only match by name — an
// empty-name row would survive this funnel yet be undeletable-proof
// garbage a later owner-removal reap could never ground by its database
// scope. Seed a database-level owner and assert the ledger INSERT for
// the grounded parent carries its resolved names.
func TestDeletePkMoveUpdateLinksCarriesIdentityIntoGroundedParentKeep(t *testing.T) {
	ctrl := gomock.NewController(t)
	var linkDeletes []string
	var ledgerWrites []string
	children := map[uint64]map[uint64]bool{
		800: {2: true},
		2:   nil,
	}
	// A database-level snapshot grounds db1: it matches the parent's
	// resolved names but never an empty-name residue row.
	snapshotLevels := []string{"database"}
	snapshotDbs := []string{"db1"}
	snapshotTbls := []string{""}
	snapshotObjs := []uint64{0}
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
			linkDeletes = append(linkDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_data_branch_update_link") {
			// The grounded orphan parent carries genuine links; the
			// funnel's presence probe (r51 review F2) reports one row so
			// the identity-carrying KEEP below still fires.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"1"}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "insert into mo_catalog.mo_data_branch_link_retention") ||
			strings.HasPrefix(sql, "update mo_catalog.mo_data_branch_link_retention") {
			ledgerWrites = append(ledgerWrites, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = ") {
			var pid uint64
			_, err := fmt.Sscanf(sql,
				"select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = %d", &pid)
			require.NoError(t, err)
			kids := children[pid]
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(len(kids))
			ids := make([]uint64, 0, len(kids))
			dels := make([]bool, 0, len(kids))
			for id, deleted := range kids {
				ids = append(ids, id)
				dels = append(dels, deleted)
			}
			require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
			require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = ") {
			var relID uint64
			_, err := fmt.Sscanf(sql,
				"select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = %d", &relID)
			require.NoError(t, err)
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			// Only the parent's row survives; the retired member is gone.
			if relID == 800 {
				memRes.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"db1"}))
				require.NoError(t, executor.AppendStringRows(memRes, 1, []string{"t800"}))
				require.NoError(t, executor.AppendFixedRows(memRes, 2, []uint64{800}))
			} else {
				memRes.NewBatchWithRowCount(0)
			}
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots") {
			memRes := executor.NewMemResult(
				[]types.Type{
					types.T_varchar.ToType(), types.T_varchar.ToType(),
					types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(len(snapshotLevels))
			require.NoError(t, executor.AppendStringRows(memRes, 0, snapshotLevels))
			require.NoError(t, executor.AppendStringRows(memRes, 1, snapshotDbs))
			require.NoError(t, executor.AppendStringRows(memRes, 2, snapshotTbls))
			require.NoError(t, executor.AppendFixedRows(memRes, 3, snapshotObjs))
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)
	// Member 2 lives in an uncovered database so it retires; parent 800
	// sits under the database-level snapshot's scope and must KEEP with
	// its resolved identity.
	require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2},
		map[uint64]pkMoveTableIdentity{2: {db: "db2", tbl: "t2"}}))

	require.Len(t, linkDeletes, 1)
	require.Contains(t, linkDeletes[0], "x'0000000000000002")

	// Exactly one ledger write, for the grounded parent, and it carries
	// the names the by-id probe resolved — before the fix this write was
	// values(800, '', '', ...) and the owner-removal reap deleted the
	// provenance even though the database-level owner survived.
	require.Len(t, ledgerWrites, 1)
	require.Contains(t, ledgerWrites[0], "values(0, 800, 'db1', 't800'")
}

// A retained user snapshot whose scope covers a RECLAIMED BRANCH MEMBER
// grounds a later historical DATA BRANCH over it exactly like a row-less
// parent, so a covered member's pk-move links must survive its own
// subtree reclaim (r23 review F2). A member whose catalog identity was
// never resolvable (retired by an earlier transaction) keeps its links
// fail-closed, and uncovered members still retire.
func TestDeletePkMoveUpdateLinksKeepsSnapshotGroundedMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	var linkDeletes []string
	children := map[uint64]map[uint64]bool{
		800: {2: true, 6: true, 9: true, 12: true},
		2:   nil,
		6:   nil,
		9:   nil,
		12:  nil,
	}
	// One retained table-level snapshot names db1.t2; a second grounds
	// member 6 by physical identity: it was created before the table's
	// rename and names the pre-rename scope (r24 review F2).
	snapshotRows := []pkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "t2"},
		{Level: "table", DatabaseName: "db1", TableName: "t6", ObjId: 6},
	}
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
			linkDeletes = append(linkDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_data_branch_update_link") {
			// Orphan parent 800 carries genuine links; the funnel's
			// presence probe (r51 review F2) reports one row so its
			// uncovered retire below still fires.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"1"}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = ") {
			var pid uint64
			_, err := fmt.Sscanf(sql,
				"select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = %d", &pid)
			require.NoError(t, err)
			kids := children[pid]
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(len(kids))
			ids := make([]uint64, 0, len(kids))
			dels := make([]bool, 0, len(kids))
			for id, deleted := range kids {
				ids = append(ids, id)
				dels = append(dels, deleted)
			}
			require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
			require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = ") {
			var relID uint64
			_, err := fmt.Sscanf(sql,
				"select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = %d", &relID)
			require.NoError(t, err)
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"db1"}))
			require.NoError(t, executor.AppendStringRows(memRes, 1, []string{fmt.Sprintf("t%d", relID)}))
			require.NoError(t, executor.AppendFixedRows(memRes, 2, []uint64{relID}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots") {
			memRes := executor.NewMemResult(
				[]types.Type{
					types.T_varchar.ToType(), types.T_varchar.ToType(),
					types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(len(snapshotRows))
			levels := make([]string, 0, len(snapshotRows))
			dbs := make([]string, 0, len(snapshotRows))
			tbls := make([]string, 0, len(snapshotRows))
			objs := make([]uint64, 0, len(snapshotRows))
			for _, row := range snapshotRows {
				levels = append(levels, row.Level)
				dbs = append(dbs, row.DatabaseName)
				tbls = append(tbls, row.TableName)
				objs = append(objs, row.ObjId)
			}
			require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
			require.NoError(t, executor.AppendStringRows(memRes, 1, dbs))
			require.NoError(t, executor.AppendStringRows(memRes, 2, tbls))
			require.NoError(t, executor.AppendFixedRows(memRes, 3, objs))
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 6, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 9, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 12, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)
	// Member 9 carries no captured identity: its names were already
	// unresolvable at admission, so it must keep its links fail-closed.
	// Member 6's captured identity carries its POST-rename name, which no
	// snapshot scope names; only the physical id still grounds it.
	require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2, 6, 9, 12},
		map[uint64]pkMoveTableIdentity{2: {db: "db1", tbl: "t2"}, 6: {db: "db1", tbl: "t6_renamed"}, 12: {db: "db1", tbl: "t12"}}))

	lowerBounds := make(map[uint64]bool, len(linkDeletes))
	for _, sql := range linkDeletes {
		var lo uint64
		_, err := fmt.Sscanf(sql,
			"delete from mo_catalog.mo_data_branch_update_link where link_id >= x'%016x'", &lo)
		require.NoError(t, err, sql)
		lowerBounds[lo] = true
	}
	// Only member 12 (uncovered) and row-less parent 800 (uncovered after
	// its own name-resolution probe) retire. Member 2 keeps its links
	// behind the name-matched snapshot; member 6 keeps them behind the
	// pre-rename snapshot by physical identity; member 9 keeps them
	// fail-closed.
	require.Equal(t, map[uint64]bool{12: true, 800: true}, lowerBounds)
}

// captureBranchReclaimMemberIdentities names same-transaction retirees
// from mo_tables (pass 1) and earlier retirees from any retained
// `__mo_branch_*` protect snapshot of one of their lineage children
// (pass 2), leaving catalog-inconsistent ids unresolved so the
// snapshot-grounding gate keeps their links fail-closed (r23 review F2).
func TestCaptureBranchReclaimMemberIdentities(t *testing.T) {
	ctrl := gomock.NewController(t)
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "select rel_id, reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id in ") {
			// Same-transaction retirees: only 2 and 6 still have rows.
			memRes := executor.NewMemResult(
				[]types.Type{
					types.T_uint64.ToType(), types.T_varchar.ToType(),
					types.T_varchar.ToType(), types.T_uint64.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(2)
			require.NoError(t, executor.AppendFixedRows(memRes, 0, []uint64{2, 6}))
			require.NoError(t, executor.AppendStringRows(memRes, 1, []string{"db1", "db1"}))
			require.NoError(t, executor.AppendStringRows(memRes, 2, []string{"t2", "t6"}))
			// The coalesce answer: each member's durable rel_logical_id.
			require.NoError(t, executor.AppendFixedRows(memRes, 3, []uint64{202, 206}))
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select obj_id, database_name, table_name from mo_catalog.mo_snapshots where kind = 'branch' and obj_id in ") {
			// Earlier retiree 5: TWO children retained protect snapshots
			// naming it identically (dedupe). 9 names nothing.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
				c.proc.Mp())
			memRes.NewBatchWithRowCount(2)
			require.NoError(t, executor.AppendFixedRows(memRes, 0, []uint64{5, 5}))
			require.NoError(t, executor.AppendStringRows(memRes, 1, []string{"db1", "db1"}))
			require.NoError(t, executor.AppendStringRows(memRes, 2, []string{"t5", "t5"}))
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	names, err := captureBranchReclaimMemberIdentities(c, []uint64{2, 5, 6, 9})
	require.NoError(t, err)
	require.Equal(t, map[uint64]pkMoveTableIdentity{
		2: {db: "db1", tbl: "t2", logicalID: 202},
		5: {db: "db1", tbl: "t5"},
		6: {db: "db1", tbl: "t6", logicalID: 206},
	}, names)
	_, hasNine := names[9]
	require.False(t, hasNine)

	// The empty candidate set issues no catalog reads at all.
	names, err = captureBranchReclaimMemberIdentities(c, nil)
	require.NoError(t, err)
	require.Empty(t, names)
}

// TestReclaimUnbranchedDropPkMoveLinks pins the direct DROP/TRUNCATE
// reclamation gate: an unbranched table's pk-move links retire with the
// drop UNLESS a live user snapshot can still ground a historical DATA
// BRANCH over that table (DATA BRANCH CREATE ... FROM t{snapshot="..."}
// instantiates a descendant whose diffs span the dropped table's
// recorded history). Probe failures keep the links and never fail the
// drop (fail alive).
func TestReclaimUnbranchedDropPkMoveLinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	probeErr := errors.New("snapshot probe failed")
	cases := []struct {
		name string
		// callerDB/callerTable model the plan identity the drop flow
		// carries; empty values force the by-id fallback probe.
		callerDB, callerTable string
		nameRows              int // mo_tables rows the fallback name resolution returns
		snapshotRows          []pkMoveSnapshotRow
		snapshotErr           error
		wantDelete            bool
		// wantNameProbe: the by-id mo_tables probe is expected to run —
		// only in the fallback shape. Every caller-identity case must
		// resolve coverage WITHOUT re-reading the retired relation row.
		wantNameProbe bool
	}{
		{name: "no snapshots reclaim", callerDB: "db1", callerTable: "t800", wantDelete: true},
		{name: "caller identity bypasses the retired relation row", callerDB: "db1", callerTable: "t800",
			nameRows: 0, wantDelete: true},
		{name: "unrelated snapshots reclaim", callerDB: "db1", callerTable: "t800", wantDelete: true,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "table", DatabaseName: "db1", TableName: "other"},
			}},
		{name: "table snapshot keeps", callerDB: "db1", callerTable: "t800", wantDelete: false,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "table", DatabaseName: "db1", TableName: "t800"},
			}},
		// r24 review F2: the snapshot was created before a rename, so its
		// scope names no longer match the caller identity; the recorded
		// physical table id still grounds the drop (800 is the dropped
		// table's rel_id).
		{name: "pre-rename table snapshot keeps by identity", callerDB: "db1", callerTable: "t800_renamed", wantDelete: false,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "table", DatabaseName: "db1", TableName: "t800", ObjId: 800},
			}},
		{name: "pre-rename snapshot of another table reclaims", callerDB: "db1", callerTable: "t800_renamed", wantDelete: true,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "table", DatabaseName: "db1", TableName: "t801", ObjId: 801},
			}},
		{name: "database snapshot keeps", callerDB: "db1", callerTable: "t800", wantDelete: false,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "database", DatabaseName: "db1"},
			}},
		{name: "cluster snapshot keeps", callerDB: "db1", callerTable: "t800", wantDelete: false,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "cluster"},
			}},
		{name: "account snapshot keeps", callerDB: "db1", callerTable: "t800", wantDelete: false,
			snapshotRows: []pkMoveSnapshotRow{
				{Level: "account"},
			}},
		{name: "gone mo_tables row keeps", nameRows: 0, wantDelete: false, wantNameProbe: true},
		{name: "fallback by-id probe resolves coverage", nameRows: 1, wantDelete: true, wantNameProbe: true},
		{name: "snapshot probe failure keeps", callerDB: "db1", callerTable: "t800", wantDelete: false,
			snapshotErr: probeErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var linkDeletes []string
			nameProbeRan := false
			exec := &dropDDLExecutor{}
			var c *Compile
			exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
				if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
					linkDeletes = append(linkDeletes, sql)
					return executor.Result{}, nil
				}
				if strings.HasPrefix(sql, "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = ") {
					nameProbeRan = true
					memRes := executor.NewMemResult(
						[]types.Type{
							types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
						c.proc.Mp())
					memRes.NewBatchWithRowCount(tc.nameRows)
					if tc.nameRows > 0 {
						require.NoError(t, executor.AppendStringRows(memRes, 0, []string{"db1"}))
						require.NoError(t, executor.AppendStringRows(memRes, 1, []string{"t800"}))
						require.NoError(t, executor.AppendFixedRows(memRes, 2, []uint64{800}))
					}
					return memRes.GetResult(), nil
				}
				if strings.HasPrefix(sql, "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots") {
					if tc.snapshotErr != nil {
						return executor.Result{}, tc.snapshotErr
					}
					memRes := executor.NewMemResult(
						[]types.Type{
							types.T_varchar.ToType(), types.T_varchar.ToType(),
							types.T_varchar.ToType(), types.T_uint64.ToType()},
						c.proc.Mp())
					memRes.NewBatchWithRowCount(len(tc.snapshotRows))
					levels := make([]string, 0, len(tc.snapshotRows))
					dbs := make([]string, 0, len(tc.snapshotRows))
					tbls := make([]string, 0, len(tc.snapshotRows))
					objs := make([]uint64, 0, len(tc.snapshotRows))
					for _, row := range tc.snapshotRows {
						levels = append(levels, row.Level)
						dbs = append(dbs, row.DatabaseName)
						tbls = append(tbls, row.TableName)
						objs = append(objs, row.ObjId)
					}
					if len(tc.snapshotRows) > 0 {
						require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
						require.NoError(t, executor.AppendStringRows(memRes, 1, dbs))
						require.NoError(t, executor.AppendStringRows(memRes, 2, tbls))
						require.NoError(t, executor.AppendFixedRows(memRes, 3, objs))
					}
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}
			c, _ = newDropDDLCompile(t, ctrl, exec)
			// Probe failures are tolerated inside the gate: the drop must
			// never fail because a snapshot catalog probe did.
			require.NoError(t, reclaimUnbranchedDropPkMoveLinks(c, 7, 800, 0, tc.callerDB, tc.callerTable))
			require.Equal(t, tc.wantNameProbe, nameProbeRan,
				"caller identity must reach coverage without re-reading the retired relation row")
			if tc.wantDelete {
				require.Len(t, linkDeletes, 1, "the ungrounded table's links retire with the drop")
			} else {
				require.Empty(t, linkDeletes, "the grounded or unproven table keeps its links")
			}
		})
	}
}

// r51 review F2: an orphan parent that NEVER carried provenance links —
// the ordinary create/branch/drop-parent/drop-branch sequence — must
// produce no retention-ledger residue at all. The funnel's presence
// probe succeeds empty, so the parent's handling ends there: no KEEP
// insert (the empty-name O1 row D154-D would keep forever), no link
// delete, no retention delete. The member's own retire must still run
// (it carries a named identity and no owner), proving the funnel was
// reached and only the link-free parent was skipped.
func TestDeletePkMoveUpdateLinksSkipsLinkFreeOrphanParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	var linkDeletes []string
	var retentionDeletes []string
	var ledgerWrites []string
	children := map[uint64]map[uint64]bool{
		800: {2: true},
		2:   nil,
	}
	exec := &dropDDLExecutor{}
	var c *Compile
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_update_link") {
			linkDeletes = append(linkDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_data_branch_link_retention") {
			retentionDeletes = append(retentionDeletes, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "insert into mo_catalog.mo_data_branch_link_retention") ||
			strings.HasPrefix(sql, "update mo_catalog.mo_data_branch_link_retention") {
			ledgerWrites = append(ledgerWrites, sql)
			return executor.Result{}, nil
		}
		if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_data_branch_update_link") {
			// Link-free parent: the presence probe succeeds empty.
			memRes := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(0)
			return memRes.GetResult(), nil
		}
		if strings.HasPrefix(sql, "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = ") {
			var pid uint64
			_, err := fmt.Sscanf(sql,
				"select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = %d", &pid)
			require.NoError(t, err)
			kids := children[pid]
			memRes := executor.NewMemResult(
				[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
			memRes.NewBatchWithRowCount(len(kids))
			ids := make([]uint64, 0, len(kids))
			dels := make([]bool, 0, len(kids))
			for id, deleted := range kids {
				ids = append(ids, id)
				dels = append(dels, deleted)
			}
			require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
			require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
			return memRes.GetResult(), nil
		}
		// mo_tables, the retention ledger lookup, and the snapshot probes
		// all hit the default empty result: nothing resolves and nothing
		// covers, so without the presence gate the parent would mint the
		// permanent empty-name residue row this test forbids.
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)

	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)
	require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2},
		map[uint64]pkMoveTableIdentity{2: {db: "db1", tbl: "t2"}}))

	linkLower := make(map[uint64]bool, len(linkDeletes))
	for _, sql := range linkDeletes {
		var lo uint64
		_, err := fmt.Sscanf(sql,
			"delete from mo_catalog.mo_data_branch_update_link where link_id >= x'%016x'", &lo)
		require.NoError(t, err, sql)
		linkLower[lo] = true
	}
	require.Equal(t, map[uint64]bool{2: true}, linkLower,
		"only the member's links retire; the link-free parent is skipped entirely")
	require.Len(t, retentionDeletes, 1, "only the member's name lookup row retires")
	require.Contains(t, retentionDeletes[0], "table_id = 2")
	require.Empty(t, ledgerWrites,
		"a link-free orphan parent mints no retention-ledger residue")
}

// The compile-side coverage funnel rides the same D158 logical lane:
// non-zero logical ids match the recorded table-scope obj id regardless of
// the member's current physical id; the zero marker keeps the legacy lane.
func TestPkMoveSnapshotCoversTableLogicalLane(t *testing.T) {
	rows := []pkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "old_t", ObjId: 700},
	}
	require.True(t, pkMoveSnapshotCoversTable(rows, 900, 700, "db1", "t_new"))
	require.False(t, pkMoveSnapshotCoversTable(rows, 700, 701, "db1", "t_new"))
	require.True(t, pkMoveSnapshotCoversTable(rows, 700, 0, "db1", "t_new"))
	require.False(t, pkMoveSnapshotCoversTable(rows, 900, 0, "db1", "t_new"))
}
