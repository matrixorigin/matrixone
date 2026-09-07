// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sessionTemporaryDDLTestOwner struct {
	trackingTempTableSession
	retired []string
}

func (s *sessionTemporaryDDLTestOwner) OwnsTemporaryTable(db, physical string) bool {
	for key, name := range s.tables {
		if name == physical && strings.HasPrefix(key, db+".") {
			return true
		}
	}
	return false
}
func (*sessionTemporaryDDLTestOwner) CheckTemporaryTableCapacity(context.Context) error { return nil }
func (s *sessionTemporaryDDLTestOwner) PublishTemporaryTable(db, alias, name string) {
	s.AddTempTable(db, alias, name)
}
func (s *sessionTemporaryDDLTestOwner) RetireTemporaryTable(db, alias, name string, _ []string) {
	s.retired = append(s.retired, name)
	if current, ok := s.GetTempTable(db, alias); ok && current == name {
		s.RemoveTempTable(db, alias)
	}
}

type temporaryDDLTestTxn struct {
	executor.TxnExecutor
	op client.TxnOperator
}

func (tx temporaryDDLTestTxn) Txn() client.TxnOperator { return tx.op }

type temporaryDDLTestExecutor struct {
	t                  *testing.T
	schema, parent     client.TxnOperator
	owner              *sessionTemporaryDDLTestOwner
	commitErr, dataErr error
	inserts            int
	dataPanic          bool
}

func (e *temporaryDDLTestExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	require.False(e.t, opts.HasExistsTxn())
	require.True(e.t, opts.WaitCommittedLogApplied())
	err := fn(temporaryDDLTestTxn{op: e.schema})
	require.Empty(e.t, e.owner.tables, "schema work must not publish before commit")
	if err != nil {
		return err
	}
	return e.commitErr
}
func (e *temporaryDDLTestExecutor) Exec(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
	require.Same(e.t, e.parent, opts.Txn(), "CTAS data belongs to the parent transaction")
	require.Contains(e.t, sql, "insert into")
	e.inserts++
	if e.dataPanic {
		panic("CTAS panic")
	}
	return executor.Result{AffectedRows: 1}, e.dataErr
}

func TestSessionTemporaryDDLCompile(t *testing.T) {
	for _, scenario := range []string{"create", "ctas", "schema failure", "commit failure", "ctas failure", "schema panic", "ctas panic"} {
		t.Run(scenario, func(t *testing.T) {
			stubs := gostub.New()
			defer stubs.Reset()
			stubs.Stub(&engine.PlanDefsToExeDefs, func(*plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) { return nil, &api.SchemaExtra{}, nil })
			stubs.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
			stubs.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
			stubs.Stub(&checkIndexInitializable, func(string, string) bool { return false })
			stubs.Stub(&maybeCreateAutoIncrement, func(context.Context, string, engine.Database, *plan.TableDef, client.TxnOperator, func() string) error {
				if scenario == "schema panic" {
					panic("schema panic")
				}
				if scenario == "schema failure" {
					return assert.AnError
				}
				return nil
			})
			ctrl := gomock.NewController(t)
			parent, schema := mock_frontend.NewMockTxnOperator(ctrl), mock_frontend.NewMockTxnOperator(ctrl)
			parent.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
			schema.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
			owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{tables: make(map[string]string)}}
			exec := &temporaryDDLTestExecutor{t: t, schema: schema, parent: parent, owner: owner}
			if scenario == "commit failure" {
				exec.commitErr = assert.AnError
			}
			exec.dataPanic = scenario == "ctas panic"
			if scenario == "ctas failure" {
				exec.dataErr = assert.AnError
			}
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
			proc.Session, proc.Base.TxnOperator = owner, parent
			proc.Base.IsFrontend = true
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, _ := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			previousVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
			t.Cleanup(func() {
				rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, previousVersion)
			})
			qry := &plan.CreateTable{Database: "test", Temporary: true, TableDef: &plan.TableDef{Name: "t"}}
			if scenario == "ctas" || scenario == "ctas failure" || scenario == "ctas panic" {
				qry.CreateAsSelectSql = "insert into `test`.`t` select 1"
			}
			pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: qry}}}}
			scope := &Scope{Plan: pn}
			eng := newStubEngine()
			eng.dbs["test"] = newStubDatabase("test")
			c := NewCompile("test", "test", "create temporary table t (a int)", "", "", eng, proc, nil, false, nil, time.Now())
			c.pn = pn
			var err error
			if scenario == "ctas panic" {
				require.Panics(t, func() { _ = scope.CreateTable(c) })
			} else {
				err = scope.CreateTable(c)
			}
			if scenario == "create" || scenario == "ctas" {
				require.NoError(t, err)
				name, ok := owner.GetTempTable("test", "t")
				require.True(t, ok)
				require.True(t, defines.IsTempTableName(name))
				require.Contains(t, eng.dbs["test"].rels, name)
				require.Empty(t, owner.retired)
			} else {
				if scenario == "schema panic" {
					require.ErrorContains(t, err, "schema panic")
				} else if scenario != "ctas panic" {
					require.ErrorIs(t, err, assert.AnError)
				}
				require.Empty(t, owner.tables)
				require.Len(t, owner.retired, 1)
			}
			require.Equal(t, "t", qry.TableDef.Name)
			require.Same(t, pn, c.pn)
			require.Same(t, parent, proc.GetTxnOperator())
			require.Same(t, owner, proc.Session)
			require.False(t, c.isInternal)
			if qry.CreateAsSelectSql != "" {
				require.Equal(t, 1, exec.inserts)
			} else {
				require.Zero(t, exec.inserts)
			}
		})
	}
}

func TestSessionTemporaryDDLRollout(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion54)
	require.False(t, supportsSessionTemporaryDDL(proc.GetService()))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
	require.True(t, supportsSessionTemporaryDDL(proc.GetService()))
}

func TestSessionTemporaryDDLRejectsInvalidExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
	proc.Base.IsFrontend = true
	owner := &sessionTemporaryDDLTestOwner{trackingTempTableSession: trackingTempTableSession{tables: make(map[string]string)}}
	proc.Session = owner
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, _ := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	previousVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, "invalid")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion55)
	t.Cleanup(func() {
		rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, previousVersion)
	})

	qry := &plan.CreateTable{Database: "test", Temporary: true, TableDef: &plan.TableDef{Name: "t"}}
	pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: qry}}}}
	eng := newStubEngine()
	eng.dbs["test"] = newStubDatabase("test")
	c := NewCompile("test", "test", "create temporary table t (a int)", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = pn

	err := (&Scope{Plan: pn}).CreateTable(c)
	require.ErrorContains(t, err, "temporary DDL executor has an invalid type")
	require.Empty(t, owner.tables)
	require.Empty(t, owner.retired)
}

func TestTemporaryDDLGenerationPreservesIndexIdentity(t *testing.T) {
	s := &temporaryDDLSession{sessionID: uuid.New(), generation: uuid.NewString()}
	unique := catalog.UniqueIndexTableNamePrefix + uuid.NewString()
	secondary := catalog.SecondaryIndexTableNamePrefix + uuid.NewString()
	require.True(t, catalog.IsUniqueIndexTable(s.TemporaryTableName("db", unique)))
	require.True(t, catalog.IsSecondaryIndexTable(s.TemporaryTableName("db", secondary)))
	first := s.TemporaryTableName("db", "t")
	s.generation = uuid.NewString()
	require.NotEqual(t, first, s.TemporaryTableName("db", "t"))
}
