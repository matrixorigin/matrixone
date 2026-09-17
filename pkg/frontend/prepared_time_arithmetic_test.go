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

package frontend

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func formatPreparedTimeDecimalResult(t *testing.T, result *vector.Vector) string {
	t.Helper()
	switch result.GetType().Oid {
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](result, 0).Format(result.GetType().Scale)
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0).Format(result.GetType().Scale)
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0).Format(result.GetType().Scale)
	default:
		require.Failf(t, "unexpected prepared TIME result type", "%v", result.GetType())
		return ""
	}
}

// TestCOMStmtPreparedTimeArithmeticRebindsDecodedNumericTypes exercises the
// production COM_STMT_EXECUTE decoder and execute-time initializer. The TCP
// client test in pkg/embed covers the complete public path; this component
// test additionally covers NEWDECIMAL, which database/sql's standard driver
// cannot send as a parameter type.
func TestCOMStmtPreparedTimeArithmeticRebindsDecodedNumericTypes(t *testing.T) {
	const query = "select cast('00:00:01' as time(0)) * ? as result"
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28963, query)
	proto, _, scratchPrepare := newBinaryPrepareProtocolTestCase(t, query)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
		scratchPrepare.Close()
	}()

	cachedPlan, err := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		packet    func() []byte
		wantType  types.T
		wantWidth int32
		wantScale int32
		wantValue string
		wantNull  bool
	}{
		{
			name: "integer",
			packet: func() []byte {
				return buildLongLongExecutePacket(10, false)
			},
			wantType: types.T_decimal128, wantWidth: 38, wantScale: 0, wantValue: "10",
		},
		{
			name: "newdecimal high scale",
			packet: func() []byte {
				return buildStringExecutePacket(proto, defines.MYSQL_TYPE_NEWDECIMAL, "1.2345678901234")
			},
			wantType: types.T_decimal128, wantWidth: 38, wantScale: 13, wantValue: "1.2345678901234",
		},
		{
			name: "newdecimal lower scale",
			packet: func() []byte {
				return buildStringExecutePacket(proto, defines.MYSQL_TYPE_NEWDECIMAL, "1.25")
			},
			wantType: types.T_decimal128, wantWidth: 38, wantScale: 2, wantValue: "1.25",
		},
		{
			name: "null resets to prepare domain",
			packet: func() []byte {
				return buildNullExecutePacket(defines.MYSQL_TYPE_NULL)
			},
			wantType: types.T_decimal128, wantWidth: 38, wantScale: 0, wantNull: true,
		},
		{
			name: "integer after decimal",
			packet: func() []byte {
				return buildLongLongExecutePacket(10, false)
			},
			wantType: types.T_decimal128, wantWidth: 38, wantScale: 0, wantValue: "10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, proto.ParseExecuteData(
				execCtx.reqCtx, cw.proc, prepareStmt, tc.packet(), 0))
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.NotNil(t, runtimePlan)
			t.Cleanup(func() {
				if owned && executionStmt != nil {
					executionStmt.Free()
				}
				prepareStmt.clearBinaryParamState(cw.proc)
			})

			queryPlan := runtimePlan.GetQuery()
			project := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(cw.proc, project)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.wantType, result.GetType().Oid)
			require.Equal(t, tc.wantWidth, result.GetType().Width)
			require.Equal(t, tc.wantScale, result.GetType().Scale)
			if tc.wantNull {
				require.True(t, result.IsNull(0))
			} else {
				require.False(t, result.IsNull(0))
				require.Equal(t, tc.wantValue, formatPreparedTimeDecimalResult(t, result))
			}
			after, err := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
			require.NoError(t, err)
			require.Equal(t, cachedPlan, after, "execute-time rebinding must not mutate the cached plan")
		})
	}
}

func TestCOMStmtPreparedTimeArithmeticPreservesTemporalIntegerDomain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    string
		want  string
		value uint64
	}{
		{name: "add", op: "+", want: "11", value: 10},
		{name: "subtract", op: "-", want: "-9", value: 10},
		{name: "mod", op: "%", want: "1", value: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := "select cast('00:00:01' as time(0)) " + tc.op + " ? as result"
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28963, query)
			proto, _, scratchPrepare := newBinaryPrepareProtocolTestCase(t, query)
			t.Cleanup(func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
				scratchPrepare.Close()
			})

			require.NoError(t, proto.ParseExecuteData(
				execCtx.reqCtx, cw.proc, prepareStmt,
				buildLongLongExecutePacket(tc.value, false), 0))
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.NotNil(t, runtimePlan)
			t.Cleanup(func() {
				if owned && executionStmt != nil {
					executionStmt.Free()
				}
				prepareStmt.clearBinaryParamState(cw.proc)
			})

			queryPlan := runtimePlan.GetQuery()
			project := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]].ProjectList[0]
			require.Equal(t, types.T_decimal64, types.T(project.Typ.Id))
			require.Equal(t, int32(18), project.Typ.Width)
			require.Equal(t, int32(0), project.Typ.Scale)
			executor, err := colexec.NewExpressionExecutor(cw.proc, project)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.Equal(t, tc.want, formatPreparedTimeDecimalResult(t, result))
		})
	}

	t.Run("add/int64-overflow-matches-decimal64", func(t *testing.T) {
		query := "select cast('00:00:01' as time(0)) + ? as result"
		ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28963, query)
		proto, _, scratchPrepare := newBinaryPrepareProtocolTestCase(t, query)
		t.Cleanup(func() {
			cw.proc.SetPrepareParams(nil)
			prepareStmt.Close()
			scratchPrepare.Close()
		})
		require.NoError(t, proto.ParseExecuteData(
			execCtx.reqCtx, cw.proc, prepareStmt,
			buildLongLongExecutePacket(9223372036854775807, false), 0))
		_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.NotNil(t, runtimePlan)
		t.Cleanup(func() {
			if owned && executionStmt != nil {
				executionStmt.Free()
			}
			prepareStmt.clearBinaryParamState(cw.proc)
		})
		queryPlan := runtimePlan.GetQuery()
		project := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]].ProjectList[0]
		executor, err := colexec.NewExpressionExecutor(cw.proc, project)
		require.NoError(t, err)
		result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		executor.Free()
		require.Error(t, err)
		require.Nil(t, result)
	})
}

func TestCOMStmtPreparedTimeArithmeticPreservesExplicitDecimalBoundary(t *testing.T) {
	const query = "select cast(cast('00:00:01' as time(0)) as decimal(10,2)) + ? as result"
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28963, query)
	proto, _, scratchPrepare := newBinaryPrepareProtocolTestCase(t, query)
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
		scratchPrepare.Close()
	})

	cachedPlan, err := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
	require.NoError(t, err)
	for _, tc := range []struct {
		value uint64
	}{
		{value: 10},
		{value: 9223372036854775807},
	} {
		value := tc.value
		t.Run(fmt.Sprintf("value-%d", value), func(t *testing.T) {
			require.NoError(t, proto.ParseExecuteData(
				execCtx.reqCtx, cw.proc, prepareStmt,
				buildLongLongExecutePacket(value, false), 0))
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.NotNil(t, runtimePlan)
			t.Cleanup(func() {
				if owned && executionStmt != nil {
					executionStmt.Free()
				}
				prepareStmt.clearBinaryParamState(cw.proc)
			})

			queryPlan := runtimePlan.GetQuery()
			project := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]].ProjectList[0]
			require.Equal(t, types.T_decimal128, types.T(project.Typ.Id))
			require.Equal(t, int32(38), project.Typ.Width)
			require.Equal(t, int32(2), project.Typ.Scale)
			executor, err := colexec.NewExpressionExecutor(cw.proc, project)
			require.NoError(t, err)
			_, err = executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			executor.Free()
			require.NoError(t, err)
			after, err := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
			require.NoError(t, err)
			require.Equal(t, cachedPlan, after, "execute-time rebinding must not mutate the cached plan")
		})
	}
}
