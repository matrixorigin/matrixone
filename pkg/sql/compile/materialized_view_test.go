// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/mv"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewExecutionCapability(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("execution")}).AnyTimes()
	proc.Base.TxnOperator = op
	c := &Compile{proc: proc}
	def := &plan.TableDef{TableType: catalog.SystemMaterializedViewRel, TblId: 99}
	require.Error(t, c.validateMaterializedViewWrite(def))
	proc.Ctx = defines.WithMaterializedViewWrite(proc.Ctx, 7, 99, []byte("binding"))
	require.Error(t, c.validateMaterializedViewWrite(def))
	proc.Ctx = defines.WithMaterializedViewWrite(proc.Ctx, 7, 99, []byte("execution"))
	require.NoError(t, c.validateMaterializedViewWrite(def))
	def.TblId = 100
	require.Error(t, c.validateMaterializedViewWrite(def))
	def.TableType = catalog.SystemOrdinaryRel
	require.NoError(t, c.validateMaterializedViewWrite(def))
}

func TestMaterializedViewCompileProtocolAdmission(t *testing.T) {
	for _, tc := range []struct {
		name            string
		protocol, floor int64
		register        bool
		ordinary        bool
		wantErr         bool
	}{
		{"old scan protocol", defines.MORPCVersion112, defines.MORPCVersion113, false, false, true},
		{"current scan protocol", defines.MORPCVersion113, defines.MORPCVersion113, false, false, false},
		{"ordinary scan", defines.MORPCVersion112, defines.MORPCVersion112, false, true, false},
		{"old registration protocol", defines.MORPCVersion112, defines.MORPCVersion113, true, false, true},
		{"old registration floor", defines.MORPCVersion113, defines.MORPCVersion112, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("execution")}).AnyTimes()
			proc.Base.TxnOperator = op
			proc.Ctx = defines.WithMaterializedViewWrite(proc.Ctx, 7, 99, []byte("execution"))
			rt := moruntime.ServiceRuntime(proc.GetService())
			for key, value := range map[string]int64{
				moruntime.MOProtocolVersion: tc.protocol, moruntime.PersistedExpressionProtocolAuthoringFloor: tc.floor,
			} {
				old, present := rt.GetGlobalVariables(key)
				rt.SetGlobalVariables(key, value)
				t.Cleanup(func() {
					if present {
						rt.SetGlobalVariables(key, old)
					} else {
						rt.CompareAndDeleteGlobalVariables(key, value)
					}
				})
			}
			def := &plan.TableDef{TableType: catalog.SystemMaterializedViewRel, TblId: 99,
				Props: []*plan.PropertyDef{{Key: catalog.MaterializedViewProperty,
					Value: `{"format_version":1,"query":"select * from t","sources":[{"table":"t"}]}`}}}
			if tc.ordinary {
				def.TableType = catalog.SystemOrdinaryRel
			}
			c := &Compile{proc: proc}
			var err error
			if tc.register {
				// Rejection must precede relation lookup and catalog publication.
				err = c.registerMaterializedView(nil, "db", "mv", def)
			} else {
				err = c.validateMaterializedViewScan(def)
			}
			if tc.wantErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type materializedViewNestedExecutor struct {
	executor.SQLExecutor
	t  *testing.T
	op client.TxnOperator
}

func (e *materializedViewNestedExecutor) ExecTxn(_ context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
	require.True(e.t, opts.DisableIncrStatement(), "nested catalog work must not start a second active statement")
	require.Same(e.t, e.op, opts.Txn())
	return f(executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }, e.op))
}
func TestMaterializedViewNestedStatementOwnership(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	proc.Base.TxnOperator = op
	inner := &materializedViewNestedExecutor{t: t, op: op}
	installDropDDLExecutor(t, proc, inner)
	c := &Compile{proc: proc, db: "db"}
	require.NoError(t, c.mvTxn(func(tx executor.TxnExecutor) error {
		require.Same(t, op, tx.Txn())
		r, err := tx.Exec("select 1", executor.StatementOption{})
		if err == nil {
			r.Close()
		}
		return err
	}))
}

func TestMaterializedViewSourceNameAdmissionDomain(t *testing.T) {
	definition := catalog.MVDefinition{FormatVersion: 1, Query: "select * from a", Sources: []catalog.MVSource{
		{Database: "db", Table: "b"}, {Database: "db", Table: "a"}, {Database: "db", Table: "a"},
	}}
	data, err := json.Marshal(definition)
	require.NoError(t, err)
	q := &plan.CreateTable{Database: "db", TableDef: &plan.TableDef{Name: "mv", TableType: catalog.SystemMaterializedViewRel,
		Props: []*plan.PropertyDef{{Key: catalog.MaterializedViewProperty, Value: string(data)}}}}
	domain := createNameDomain(q, "")
	require.Len(t, domain, 3)
	for _, name := range []string{"mv", "a", "b"} {
		require.Contains(t, domain, plan2.FkReferKey{Db: "db", Tbl: name})
	}
	changed := plan2.DeepCopyCreateTable(q)
	definition.Sources[0].Table = "other"
	data, err = json.Marshal(definition)
	require.NoError(t, err)
	changed.TableDef.Props[0].Value = string(data)
	require.False(t, sameCreateNameDomain(q, changed), "replanning cannot silently change the locked source domain")
	q.TableDef.TableType = catalog.SystemOrdinaryRel
	require.Len(t, createNameDomain(q, ""), 1, "ordinary CREATE must not pin MV source names")
}

func TestMaterializedViewDDLGuardDoesNotWriteAccountFence(t *testing.T) {
	for _, dependent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "dependent"}[dependent], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC}).AnyTimes()
			proc.Base.TxnOperator = op
			eng := mock_frontend.NewMockEngine(ctrl)
			database := mock_frontend.NewMockDatabase(ctrl)
			relation := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(proc.Ctx, "db", op).Return(database, nil)
			database.EXPECT().Relation(proc.Ctx, "source", nil).Return(relation, nil)
			relation.EXPECT().GetTableDef(proc.Ctx).Return(&plan.TableDef{TableType: catalog.SystemOrdinaryRel, TblId: 99})
			calls := 0
			exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				if sql == mv.CatalogReadinessSQL {
					res := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, proc.Mp())
					res.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(res, 0, []int64{4}))
					return res.GetResult(), nil
				}
				require.True(t, strings.HasPrefix(sql, "select mv_id from "), "ordinary DDL must not introduce DML or retained locks: %s", sql)
				require.Contains(t, sql, "account_id=7")
				calls++
				if !dependent {
					return executor.Result{}, nil
				}
				res := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, proc.Mp())
				res.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(res, 0, []uint64{100}))
				return res.GetResult(), nil
			}, op)
			installDropDDLExecutor(t, proc, exec)
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, existed := rt.GetGlobalVariables(mv.RuntimeKey)
			marker := &mv.Service{}
			rt.SetGlobalVariables(mv.RuntimeKey, marker)
			t.Cleanup(func() {
				if existed {
					rt.SetGlobalVariables(mv.RuntimeKey, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(mv.RuntimeKey, marker)
				}
			})
			c := &Compile{proc: proc, e: eng, db: "db"}
			err := c.guardMaterializedViewDDL("db", "source", false)
			if dependent {
				require.ErrorContains(t, err, "dependent materialized views")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls)
		})
	}
}

func TestMaterializedViewDDLReadiness(t *testing.T) {
	for _, mode := range []txn.TxnMode{txn.TxnMode_Pessimistic, txn.TxnMode_Optimistic} {
		for _, count := range []int64{0, 4} {
			for _, dependent := range []bool{false, true} {
				for _, tableDDL := range []bool{false, true} {
					t.Run(fmt.Sprintf("%d/catalog=%d/dependent=%v/table=%v", mode, count, dependent, tableDDL), func(t *testing.T) {
						proc := testutil.NewProcess(t)
						proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
						op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
						op.EXPECT().Txn().Return(txn.TxnMeta{Mode: mode}).AnyTimes()
						proc.Base.TxnOperator = op
						downstream := 0
						exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
							if sql == mv.CatalogReadinessSQL {
								r := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, proc.Mp())
								r.NewBatchWithRowCount(1)
								require.NoError(t, executor.AppendFixedRows(r, 0, []int64{count}))
								return r.GetResult(), nil
							}
							downstream++
							require.Equal(t, int64(4), count, "pre-migration DDL touched missing MV catalogs")
							if strings.HasPrefix(sql, "select mv_id") && dependent {
								r := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, proc.Mp())
								r.NewBatchWithRowCount(1)
								require.NoError(t, executor.AppendFixedRows(r, 0, []uint64{99}))
								return r.GetResult(), nil
							}
							return executor.Result{}, nil
						}, op)
						installDropDDLExecutor(t, proc, exec)
						rt := moruntime.ServiceRuntime(proc.GetService())
						old, present := rt.GetGlobalVariables(mv.RuntimeKey)
						marker := &mv.Service{}
						rt.SetGlobalVariables(mv.RuntimeKey, marker)
						t.Cleanup(func() {
							if present {
								rt.SetGlobalVariables(mv.RuntimeKey, old)
							} else {
								rt.CompareAndDeleteGlobalVariables(mv.RuntimeKey, marker)
							}
						})
						c := &Compile{proc: proc, db: "db"}
						var err error
						if tableDDL {
							ctrl := gomock.NewController(t)
							eng := mock_frontend.NewMockEngine(ctrl)
							database := mock_frontend.NewMockDatabase(ctrl)
							relation := mock_frontend.NewMockRelation(ctrl)
							eng.EXPECT().Database(proc.Ctx, "db", op).Return(database, nil)
							database.EXPECT().Relation(proc.Ctx, "source", nil).Return(relation, nil)
							relation.EXPECT().GetTableDef(proc.Ctx).Return(&plan.TableDef{TableType: catalog.SystemOrdinaryRel, TblId: 99})
							c.e = eng
							err = c.guardMaterializedViewDDL("db", "source", false)
						} else {
							err = c.guardMaterializedViewDatabaseDDL("db")
						}
						if count == 4 && dependent {
							require.ErrorContains(t, err, "materialized views")
						} else {
							require.NoError(t, err)
						}
						if count == 0 {
							require.Zero(t, downstream)
						} else {
							require.Positive(t, downstream)
						}
					})
				}
			}
		}
	}
}
