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
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedCalendarTypedSources(t *testing.T) {
	for _, protocol := range []string{"sql", "binary"} {
		for _, source := range []struct {
			name string
			oid  types.T
			wire defines.MysqlType
		}{
			{"date", types.T_date, defines.MYSQL_TYPE_DATE},
			{"datetime", types.T_datetime, defines.MYSQL_TYPE_DATETIME},
			{"timestamp", types.T_timestamp, defines.MYSQL_TYPE_TIMESTAMP},
		} {
			for _, operation := range []struct{ expression, want string }{
				{"date_add(?, interval 1 day)", "2024-02-29"},
				{"date_sub(?, interval 1 month)", "2024-01-28"},
			} {
				t.Run(protocol+"/"+source.name+"/"+operation.expression, func(t *testing.T) {
					// Each source starts with a fresh handle: an earlier DATETIME
					// execution must not hide incorrect first-execution inference.
					query := "select " + operation.expression + " as v"
					ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25304, query)
					prepared.proc = cw.proc
					cw.proc.GetSessionInfo().TimeZone = time.UTC
					t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
					var execute *plan.Execute
					if protocol == "binary" {
						proto, _, scratch := newBinaryPrepareProtocolTestCase(t, query)
						t.Cleanup(scratch.Close)
						packet := buildDateExecutePacketForParams([]defines.MysqlType{source.wire}, 2024, 2, 28)
						if source.oid != types.T_date {
							packet[9] = 11
							packet = append(packet, 12, 34, 56)
							packet = binary.LittleEndian.AppendUint32(packet, 123456)
						}
						require.NoError(t, proto.ParseExecuteData(ec.reqCtx, cw.proc, prepared, packet, 0))
					} else {
						ec.input.isBinaryProtExecute = false
						cw.binaryPrepare = false
						value := "2024-02-28"
						if source.oid != types.T_date {
							value += " 12:34:56.123456"
						}
						// SELECT INTO stores the scalar representation together with
						// its source type; exercise the same session variable owner.
						require.NoError(t, ses.setUserDefinedVarWithType("calendar_source", value, "", false, plan.Type{Id: int32(source.oid), Scale: 6}))
						execute = &plan.Execute{Name: prepared.Name, Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "calendar_source"}}}}}
					}
					_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, execute, prepared.Name)
					if owned && stmt != nil {
						defer stmt.Free()
					}
					require.NoError(t, err)
					require.Len(t, cw.paramBindings, 1)
					require.Equal(t, source.oid, cw.paramBindings[0].Type.Oid)
					q := p.GetQuery()
					expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
					wantType, want := types.T_date, operation.want
					if source.oid != types.T_date {
						wantType = types.T_datetime
						want += " 12:34:56.123456"
					}
					require.Equal(t, wantType, types.T(expr.Typ.Id))
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					if wantType == types.T_date {
						require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Date](result, 0).String())
					} else {
						require.Equal(t, int32(6), expr.Typ.Scale)
						require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
					}
				})
			}
		}
	}
}

func TestPreparedTemporalHistoryPublicationAndCache(t *testing.T) {
	ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25302, "select date_add(?, interval 1 day) as v")
	prepared.proc = cw.proc
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
	original := prepared.PreparePlan.String()
	setParams := func(typ defines.MysqlType, value string) {
		cw.discardRuntimeCacheCandidate()
		cw.proc.SetPrepareParams(nil)
		if prepared.params != nil {
			prepared.params.Free(cw.proc.Mp())
		}
		prepared.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), false, cw.proc.Mp()))
		prepared.ParamTypes = []byte{byte(typ), 0}
	}
	bind := func() (*plan.Plan, error) {
		_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
		if owned && stmt != nil {
			stmt.Free()
		}
		return p, err
	}
	assertResult := func(p *plan.Plan, oid types.T, want string) {
		q := p.GetQuery()
		expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
		require.Equal(t, oid, types.T(expr.Typ.Id))
		result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err)
		defer free()
		if oid == types.T_date {
			require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Date](result, 0).String())
		} else {
			require.Equal(t, int32(6), expr.Typ.Scale)
			require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
		}
	}
	writer := ec.resper.MysqlRrWr().(*testMysqlWriter)
	setParams(defines.MYSQL_TYPE_VAR_STRING, "2024-02-28 00:00:00")
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("temporal metadata failure")
	}
	_, err := bind()
	require.ErrorContains(t, err, "temporal metadata failure")
	require.Nil(t, prepared.temporalTypes)
	writer.makeColumnDefDataFunc = nil

	setParams(defines.MYSQL_TYPE_DATE, "2024-02-28")
	datePlan, err := bind()
	require.NoError(t, err)
	assertResult(datePlan, types.T_date, "2024-02-29")
	// Install through the same logical-cache owner and exercise its lookup.
	require.NotNil(t, cw.runtimeCachePlan)
	prepared.installRuntimeSpecializationCache(cw.runtimeCacheKey, cw.runtimeCachePlan, nil, cw.runtimeCacheDiagnostics, cw.runtimeCacheDecimalFloatProofs, cw.runtimeCacheColDefData)
	cw.discardRuntimeCacheCandidate()
	cached, err := bind()
	require.NoError(t, err)
	require.Same(t, datePlan, cached)

	setParams(defines.MYSQL_TYPE_DATETIME, "2024-02-28 00:00:00")
	p, err := bind()
	require.NoError(t, err)
	assertResult(p, types.T_datetime, "2024-02-29 00:00:00.000000")
	history := prepared.temporalTypes
	require.Equal(t, uint64(1), history.Revision())
	retry := cw.preparedExecutionRetry()
	require.Same(t, history, retry.temporalTypes)
	require.Nil(t, retry.fieldTypes)
	setParams(defines.MYSQL_TYPE_DATE, "2024-02-28")
	p, err = bind()
	require.NoError(t, err)
	require.NotSame(t, datePlan, p, "a pre-widening typed-DATE cache entry is stale")
	assertResult(p, types.T_datetime, "2024-02-29 00:00:00.000000")
	require.Equal(t, original, prepared.PreparePlan.String())

	prepared.NativeMode = !prepared.NativeMode
	metadataCalls := 0
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		metadataCalls++
		if metadataCalls == 2 {
			return nil, errors.New("temporal generation metadata failure")
		}
		return nil, nil
	}
	t.Cleanup(func() { writer.makeColumnDefDataFunc = nil })
	_, err = bind()
	require.ErrorContains(t, err, "temporal generation metadata failure")
	require.Same(t, history, prepared.temporalTypes)
	require.True(t, prepared.temporalTypesResetPending)
	writer.makeColumnDefDataFunc = nil
	p, err = bind()
	require.NoError(t, err)
	assertResult(p, types.T_date, "2024-02-29")
	require.Nil(t, prepared.temporalTypes)
	require.False(t, prepared.temporalTypesResetPending)
	prepared.Close()
	require.Equal(t, uint64(1), retry.temporalTypes.Revision(), "Close cannot mutate a borrowed snapshot")
}

func TestPreparedTemporalRetryDeclarationGeneration(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed"}[changed], func(t *testing.T) {
			base := plan2.NewMockCompilerContext(false, newPlanTestProcess(t))
			_, table, err := base.Resolve("tpch", "nation", nil)
			require.NoError(t, err)
			table = plan2.DeepCopyTableDef(table, true)
			table.DbId = 73
			ctx := &fieldRetryTestCompiler{preparedStatsTestCompiler: &preparedStatsTestCompiler{preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()}, table: table}}
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQLWithCompilerContext(t, 25303, "select date_add(?, interval 1 day) as v from nation", ctx)
			prepared.proc, ctx.proc = cw.proc, cw.proc
			prepared.defaultDatabase = "tpch"
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			prepared.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("2024-02-28 00:00:00"), false, cw.proc.Mp()))
			prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
			_, _, stmt, _, owned, err := initExecuteStmtParamWithResolverInSession(ec, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
			if owned && stmt != nil {
				stmt.Free()
			}
			require.NoError(t, err)
			retry := cw.preparedExecutionRetry()
			require.NotNil(t, retry.temporalTypes)
			require.Nil(t, retry.fieldTypes)
			require.NotEmpty(t, retry.fieldSchemas, "temporal-only history also needs a declaration proof")
			require.NotSame(t, prepared.PreparePlan.GetDcl().GetPrepare().Schemas[0], retry.fieldSchemas[0])
			if changed {
				table.Version++
			}
			previous := ctx.GetContext()
			rebuilt, err := buildPlanForCompileRetry(ec.reqCtx, ses, ctx, prepared.PrepareStmt, false, retry)
			require.Equal(t, previous, ctx.GetContext())
			if changed {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
				require.Nil(t, rebuilt)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rebuilt)
			}
			require.Same(t, retry.temporalTypes, prepared.temporalTypes, "retry is not a publisher")
		})
	}
}
