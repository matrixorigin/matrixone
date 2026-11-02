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

package frontend

import (
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

type fieldRetryTestCompiler struct {
	*preparedStatsTestCompiler
	resolveErr error
	missing    bool
	resolve    func(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error)
}

func (c *fieldRetryTestCompiler) Resolve(db, table string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	if c.resolve != nil {
		return c.resolve(db, table, snapshot)
	}
	if c.resolveErr != nil || c.missing {
		return nil, nil, c.resolveErr
	}
	ref, def, err := c.preparedStatsTestCompiler.Resolve(db, table, snapshot)
	if err == nil && ref != nil && def != nil {
		ref = proto.Clone(ref).(*plan.ObjectRef)
		ref.Db, ref.Obj, ref.Server = int64(def.DbId), int64(def.TblId), int64(def.Version)
	}
	return ref, def, err
}

func TestPreparedFieldRetryDeclarationGeneration(t *testing.T) {
	for _, name := range []string{"unchanged", "version", "object", "database", "publication", "missing", "resolve error", "captured facts", "captured nested facts", "nil dependency", "without history"} {
		t.Run(name, func(t *testing.T) {
			base := plan2.NewMockCompilerContext(false, newPlanTestProcess(t))
			_, table, err := base.Resolve("tpch", "nation", nil)
			require.NoError(t, err)
			table = plan2.DeepCopyTableDef(table, true)
			table.DbId = 73
			ctx := &fieldRetryTestCompiler{preparedStatsTestCompiler: &preparedStatsTestCompiler{
				preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()}, table: table}}
			sql := "select field(case when ? then n_name else ? end,'A','a') from nation"
			if name == "without history" {
				sql = "select field(n_name,'A','a') from nation where ? and n_name = ?"
			}
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQLWithCompilerContext(t, 311, sql, ctx)
			prepared.proc, ctx.proc = cw.proc, cw.proc
			prepared.defaultDatabase = "tpch"
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			prepared.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("a"), false, cw.proc.Mp()))
			prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(defines.MYSQL_TYPE_VAR_STRING), 0}
			_, initial, clone, _, owned, err := initExecuteStmtParamWithResolverInSession(ec, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
			if owned && clone != nil {
				clone.Free()
			}
			require.NoError(t, err)
			if name == "without history" {
				require.Nil(t, prepared.fieldTypes)
			} else {
				require.NotNil(t, prepared.fieldTypes)
			}
			history := prepared.fieldTypes
			schemas := prepared.PreparePlan.GetDcl().GetPrepare().Schemas
			require.NotEmpty(t, schemas)
			require.Equal(t, int64(73), schemas[0].Db)
			if name == "captured nested facts" {
				schemas[0].Snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 7}}
				schemas[0].PubInfo = &plan.PubInfo{TenantId: 71}
				schemas[0].SubscriptionName = "sub"
			}
			if name == "nil dependency" {
				prepared.PreparePlan.GetDcl().GetPrepare().Schemas = append(prepared.PreparePlan.GetDcl().GetPrepare().Schemas, nil)
			}
			retry := cw.preparedExecutionRetry()
			require.NotNil(t, retry)
			if history != nil {
				require.Same(t, history, retry.fieldTypes)
			} else {
				require.Nil(t, retry.fieldTypes)
				require.Nil(t, retry.fieldSchemas)
			}
			sentinel := errors.New("catalog lookup failed")
			switch name {
			case "version", "captured facts", "without history":
				for _, col := range table.Cols {
					if col.Name == "n_name" {
						col.Typ.Id = int32(types.T_varbinary)
						col.Typ.Charset = uint32(types.CharsetBinary)
					}
				}
				table.Version++
				if name == "captured facts" {
					for _, ref := range prepared.PreparePlan.GetDcl().GetPrepare().Schemas {
						ref.Server = int64(table.Version)
					}
				}
			case "object":
				table.TblId++
			case "database":
				table.DbId++
			case "publication":
				ctx.resolve = func(db, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
					ref, def, err := ctx.preparedStatsTestCompiler.Resolve(db, name, snapshot)
					ref = proto.Clone(ref).(*plan.ObjectRef)
					ref.PubInfo = &plan.PubInfo{TenantId: 72}
					return ref, def, err
				}
			case "captured nested facts":
				captured := retry.fieldSchemas[0]
				want := proto.Clone(captured).(*plan.ObjectRef)
				require.NotSame(t, schemas[0], captured)
				require.NotSame(t, schemas[0].PubInfo, captured.PubInfo)
				require.NotSame(t, schemas[0].Snapshot, captured.Snapshot)
				require.NotSame(t, schemas[0].Snapshot.TS, captured.Snapshot.TS)
				schemas[0].Snapshot.TS.PhysicalTime = 99
				schemas[0].PubInfo.TenantId = 99
				schemas[0].SubscriptionName = "mutated"
				require.Equal(t, want, captured)
				ctx.resolve = func(db, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
					require.Equal(t, plan2.DbNameOfObjRef(want), db)
					require.Equal(t, want.Snapshot, snapshot, "validation must use the captured snapshot, not mutable handle metadata")
					return nil, nil, sentinel
				}
			case "missing":
				ctx.missing = true
			case "resolve error":
				ctx.resolveErr = sentinel
			}
			originalContext := ctx.GetContext()
			rebuilt, err := buildPlanForCompileRetry(ec.reqCtx, ses, ctx, prepared.PrepareStmt, false, retry)
			require.Equal(t, originalContext, ctx.GetContext(), "retry must restore the compiler context, including on errors")
			if history != nil {
				require.Same(t, history, prepared.fieldTypes)
				require.Same(t, history, retry.fieldTypes)
			} else {
				require.Nil(t, prepared.fieldTypes)
				require.Nil(t, retry.fieldTypes)
			}
			if name == "unchanged" || name == "without history" {
				require.NoError(t, err)
				require.NotNil(t, rebuilt)
				return
			}
			if name == "resolve error" || name == "captured nested facts" {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "must reject stale FIELD history, got %v", err)
				code, state, message := RewriteError(err, "")
				require.Equal(t, moerr.ErrTxnNeedRetryWithDefChanged, code)
				require.Equal(t, "HY000", state)
				require.Contains(t, message, "def changed")
			}
			require.Nil(t, rebuilt, "no stale-history rows may execute")
			wrapper := &TxnComputationWrapper{preparedStmt: prepared}
			wrapper.completeCompileExecution(&mockCompile{planGenerationRebuilt: true, getPlanFunc: func() *plan.Plan { return initial }}, err)
			require.True(t, prepared.needsRebuild)
			require.True(t, prepared.compileNeedsRebuild)
			require.Same(t, history, prepared.fieldTypes, "only the normal execution owner may publish replacement history")
			if name != "version" {
				return
			}
			// Supply catalog observations to the session's real compiler context:
			// ordinary EXECUTE rebuilds its PREPARE seed through that context.
			ctrl := gomock.NewController(t)
			storage := mock_frontend.NewMockEngine(ctrl)
			database := mock_frontend.NewMockDatabase(ctrl)
			relation := mock_frontend.NewMockRelation(ctrl)
			storage.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(database, nil).AnyTimes()
			database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
			database.EXPECT().Relation(gomock.Any(), "nation", gomock.Any()).Return(relation, nil).AnyTimes()
			database.EXPECT().GetDatabaseId(gomock.Any()).Return("73").AnyTimes()
			relation.EXPECT().GetTableDef(gomock.Any()).Return(table).AnyTimes()
			relation.EXPECT().GetTableID(gomock.Any()).Return(table.TblId).AnyTimes()
			relation.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			pu := getPu(ses.GetService())
			oldEngine, oldStorage := pu.StorageEngine, ses.txnHandler.storage
			pu.StorageEngine, ses.txnHandler.storage = storage, storage
			t.Cleanup(func() { pu.StorageEngine, ses.txnHandler.storage = oldEngine, oldStorage })
			// The next ordinary EXECUTE, not the retry callback, owns publication.
			_, next, clone, _, owned, err := initExecuteStmtParamWithResolverInSession(ec, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
			if owned && clone != nil {
				clone.Free()
			}
			require.NoError(t, err)
			require.NotNil(t, next)
			require.NotNil(t, prepared.fieldTypes)
			require.NotSame(t, history, prepared.fieldTypes)
			require.False(t, prepared.fieldTypesResetPending)
			require.False(t, prepared.needsRebuild)
			nextRetry := cw.preparedExecutionRetry()
			require.Same(t, prepared.fieldTypes, nextRetry.fieldTypes)
			require.Same(t, history, retry.fieldTypes)
			require.Equal(t, int64(table.Version), nextRetry.fieldSchemas[0].Server)
			require.NotEqual(t, nextRetry.fieldSchemas[0].Server, retry.fieldSchemas[0].Server)
			// Prove why unchanged UINT64 output metadata alone is insufficient.
			ctx.SetContext(plan2.WithPreparedFieldTypes(ec.reqCtx, nil))
			defer ctx.SetContext(originalContext)
			fresh, err := plan2.BuildPreparedExecutionPlan(ctx, prepared.PrepareStmt, retry.bindings, retry.paramVals)
			require.NoError(t, err)
			require.NotSame(t, history, fresh.FieldTypes)
			require.Equal(t, plan2.GetResultColumnsFromPlan(initial), plan2.GetResultColumnsFromPlan(fresh.Plan))
			require.Equal(t, plan2.GetResultColumnsFromPlan(fresh.Plan), plan2.GetResultColumnsFromPlan(next))
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(cw.proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("a"), false, cw.proc.Mp()))
			input.SetRowCount(1)
			var expr *plan.Expr
			require.NoError(t, plan.VisitExpressionsInOwner(next, func(e *plan.Expr) error {
				return plan.VisitExprTree(e, func(e *plan.Expr) error {
					if e.GetF() != nil && e.GetF().Func.ObjName == "field" {
						expr = e
					}
					return nil
				})
			}))
			require.NotNil(t, expr)
			executor, err := colexec.NewExpressionExecutor(cw.proc, expr)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](result, 0), "the next execution generation consumes the new binary declaration")
		})
	}
}
