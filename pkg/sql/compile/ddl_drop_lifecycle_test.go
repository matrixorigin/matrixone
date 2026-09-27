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
	var fkCleanups, mergeCleanups int
	exec := &dropDDLExecutor{}
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.Contains(sql, "mo_foreign_keys") {
			fkCleanups++
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
		def := &plan.TableDef{TblId: uint64(i + 1), Name: name}
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
		{Database: "db", Table: "first", TableId: 1, UpdateFkSqls: []string{dropDatabaseTableFkCleanupSQL("db", "first")}, TableDef: &plan.TableDef{TblId: 1, Name: "first"}},
		{Database: "db", Table: "second", TableId: 2, UpdateFkSqls: []string{dropDatabaseTableFkCleanupSQL("db", "second")}, TableDef: &plan.TableDef{TblId: 2, Name: "second"}},
		{Database: "db", Table: "third", TableId: 3, TableDef: &plan.TableDef{TblId: 3, Name: "third"}},
	}
	err := c.dropDatabaseRelations(db, tables, resolved, true)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, fkCleanups)
	require.Equal(t, 1, mergeCleanups)
	require.Same(t, originalCtx, c.proc.Ctx)
	require.False(t, c.ignorePublish)
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

func TestDropDatabaseTableFkCleanupSQLEscapesNames(t *testing.T) {
	require.Equal(t,
		"delete from `mo_catalog`.`mo_foreign_keys` where db_name = 'db\\'name' and table_name = 'table\\\\name'",
		dropDatabaseTableFkCleanupSQL("db'name", `table\name`),
	)
}

func TestDropTableLifecycleAdmission(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("persistent=%d", count), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var order []string
			failGate := true
			gateErr := errors.New("admission failed")
			exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
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
			db := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).AnyTimes()
			var tables []*plan.DropTable
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("t%d", i)
				def := &plan.TableDef{TblId: uint64(i + 1), Name: name}
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
					want = append(want, fmt.Sprintf("t%d", i), "reclaim")
				}
				require.Equal(t, want, order, "each invocation admits once and finishes a member before the next")
				order = nil
			}
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
			owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{tables: map[string]string{catalog.MO_CATALOG + ".tmp": "physical"}}}
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

func TestDropDatabaseSelectsParentOwnedTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	var dropped []string
	exec := &dropDDLExecutor{exec: func(_ context.Context, _ string, _ executor.Options) (executor.Result, error) {
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	c.proc.Session = &trackingTempTableSession{tables: map[string]string{"db.shadowed": "__mo_tmp_db_shadowed"}}
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
			eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil)
			db.EXPECT().IsSubscription(gomock.Any()).Return(true)
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
	ctrl := gomock.NewController(t)
	exec := &dropDDLExecutor{exec: func(context.Context, string, executor.Options) (executor.Result, error) {
		t.Fatal("temporary/no-op drop entered the persistent lifecycle")
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	c.proc.Session = &trackingTempTableSession{tables: map[string]string{catalog.MO_CATALOG + ".tmp": "physical"}}
	db := newStubDatabase(catalog.MO_CATALOG)
	db.rels["physical"] = &stubRelation{tableDef: &plan.TableDef{IsTemporary: true}}
	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, c.proc.GetTxnOperator()).Return(db, nil)
	temp := &plan.DropTable{Database: catalog.MO_CATALOG, Table: "tmp", IfExists: true, TableDef: &plan.TableDef{IsTemporary: true}}
	s := dropTableScope(nil, &plan.DropTable{}, &plan.DropTable{Database: "db", Table: "missing", IfExists: true}, temp)
	require.NoError(t, s.DropTable(c))
	require.NotContains(t, db.rels, "physical")
	// The retained plan sees the now-absent temporary alias on the next call.
	require.NoError(t, s.DropTable(c))
	require.Equal(t, "tmp", temp.Table)
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
