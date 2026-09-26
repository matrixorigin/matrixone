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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOp(ctrl)
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

func parseDropTableNames(t *testing.T, sql, dbName string) []string {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 0)
	require.NoError(t, err)
	defer stmt.Free()
	drop, ok := stmt.(*tree.DropTable)
	require.True(t, ok)
	require.True(t, drop.IfExists)
	names := make([]string, 0, len(drop.Names))
	for _, name := range drop.Names {
		require.Equal(t, dbName, string(name.SchemaName))
		names = append(names, string(name.ObjectName))
	}
	return names
}

func TestDropDatabaseTableBatches(t *testing.T) {
	for _, tc := range []struct {
		n       int
		failAt  int
		wantErr error
	}{
		{n: 0}, {n: 1}, {n: 32}, {n: 33},
		{n: 65, failAt: 2, wantErr: errors.New("second group failed")},
		{n: 33, failAt: 1, wantErr: context.Canceled},
	} {
		t.Run(fmt.Sprintf("n=%d/error=%v", tc.n, tc.wantErr), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mp := mpool.MustNewZero()
			const dbName = "库`db"
			names := make([]string, tc.n)
			for i := range names {
				names[i] = fmt.Sprintf("表`%d", i)
			}
			var got []string
			calls := 0
			var c *Compile
			exec := &dropDDLExecutor{exec: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
				calls++
				require.Zero(t, mp.CurrNB(), "previous group result must be closed")
				require.Same(t, c.proc.GetTxnOperator(), opts.Txn())
				require.True(t, opts.DisableIncrStatement())
				require.True(t, opts.IsFrontend())
				require.Same(t, time.UTC, opts.GetTimeZone())
				require.Equal(t, int64(1), opts.LowerCaseTableNames())
				require.False(t, opts.HasAccountID(), "retain the originating tenant context")
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(7), account)
				require.Equal(t, c.proc.Ctx.Done(), ctx.Done())
				require.True(t, opts.StatementOption().IgnoreForeignKey())
				require.True(t, opts.StatementOption().IgnorePublish())
				require.True(t, opts.StatementOption().DisableLog())
				group := parseDropTableNames(t, sql, dbName)
				require.LessOrEqual(t, len(group), 32)
				got = append(got, group...)
				if calls == tc.failAt {
					if tc.wantErr == context.Canceled {
						require.ErrorIs(t, ctx.Err(), context.Canceled)
					}
					return executor.Result{}, tc.wantErr
				}
				return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{1}), nil
			}}
			c, _ = newDropDDLCompile(t, ctrl, exec)
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			if tc.wantErr == context.Canceled {
				cancel()
			}
			c.proc.Ctx, c.proc.Base.IsFrontend = ctx, true
			c.pn = &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE}}}
			err := c.dropDatabaseTables(dbName, names)
			require.ErrorIs(t, err, tc.wantErr)
			wantCalls := (tc.n + 31) / 32
			if tc.failAt > 0 {
				wantCalls = tc.failAt
			}
			require.Equal(t, wantCalls, calls)
			require.Equal(t, names[:min(tc.n, wantCalls*32)], append([]string{}, got...))
			require.Zero(t, mp.CurrNB())
		})
	}
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
	exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		if strings.HasPrefix(sql, "drop table") {
			dropped = append(dropped, parseDropTableNames(t, sql, "db")...)
		}
		return executor.Result{}, nil
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.IgnoreForeignKey{}, true)
	stubs := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(stubs.Reset)
	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(db, nil).Times(2)
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("42").AnyTimes()
	// The legacy child comes before its parent and has no feature marker.
	names := []string{"legacy", "partition", "index", "parent", "legacy_lookalike", "tail"}
	db.EXPECT().Relations(gomock.Any()).Return(names, nil)
	for _, name := range names {
		rel := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), name, nil).Return(rel, nil)
		extra := &api.SchemaExtra{}
		switch name {
		case "partition":
			extra.FeatureFlag = features.Partition
		case "index":
			extra.FeatureFlag = features.IndexTable
		}
		rel.EXPECT().GetExtraInfo().Return(extra).AnyTimes()
		if name == "partition" || name == "index" {
			continue
		}
		var defs []engine.TableDef
		if name == "parent" {
			defs = []engine.TableDef{&engine.ConstraintDef{Cts: []engine.Constraint{&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexTableName: "legacy"}}}}}}
		}
		rel.EXPECT().TableDefs(gomock.Any()).Return(defs, nil)
	}
	stopErr := errors.New("engine tail reached")
	eng.EXPECT().Delete(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(stopErr)
	pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_DROP_DATABASE,
		Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db", DatabaseId: 42}},
	}}}
	c.pn = pn
	require.ErrorIs(t, (&Scope{Plan: pn}).DropDatabase(c), stopErr)
	require.Equal(t, []string{"parent", "legacy_lookalike", "tail"}, dropped)
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
