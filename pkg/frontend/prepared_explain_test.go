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

package frontend

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planPb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TestPreparedExplainUsesBinaryParameterValues(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 104, "explain select ?")
	defer prepareStmt.Close()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("42"), false, cw.proc.Mp()))

	_, queryPlan, savedStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.IsType(t, &tree.ExplainStmt{}, savedStmt)
	require.Equal(t, []any{plan2.ParamValue{Value: "42", IsBin: false}}, cw.ParamVals())

	cw.plan = queryPlan
	cw.ifIsExeccute = true
	filled, err := preparedExplainPlan(execCtx.reqCtx, cw)
	require.NoError(t, err)
	require.NotSame(t, queryPlan, filled, "parameter substitution must not mutate the reusable prepared plan")
	require.NotNil(t, filled.GetQuery())
}

func TestUnwrapExecutableExplainStatement(t *testing.T) {
	tests := []struct {
		name string
		wrap func(tree.Statement) tree.Statement
	}{
		{
			name: "explain",
			wrap: func(stmt tree.Statement) tree.Statement {
				return tree.NewExplainStmt(stmt, "text")
			},
		},
		{
			name: "explain analyze",
			wrap: func(stmt tree.Statement) tree.Statement {
				return tree.NewExplainAnalyze(stmt, "text")
			},
		},
		{
			name: "explain phyplan",
			wrap: func(stmt tree.Statement) tree.Statement {
				return tree.NewExplainPhyPlan(stmt, "text")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inner := &tree.Select{}
			wrapped := tc.wrap(inner)
			require.Same(t, inner, unwrapExecutableExplainStatement(wrapped))
			wrapped.Free()
			inner.Free()
		})
	}

	inner := &tree.Select{}
	child := tree.NewExplainAnalyze(inner, "text")
	nested := tree.NewExplainStmt(child, "text")
	require.Same(t, inner, unwrapExecutableExplainStatement(nested))
	nested.Free()
	child.Free()
	inner.Free()

	plain := &tree.Select{}
	require.Same(t, plain, unwrapExecutableExplainStatement(plain))
	plain.Free()
}

func TestHandlePreparedExplainDoesNotRebuildUnderlyingStatement(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 106, "explain select ?")
	defer prepareStmt.Close()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("42"), false, cw.proc.Mp()))
	_, queryPlan, savedStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)

	explainStmt := savedStmt.(*tree.ExplainStmt)
	cw.plan = queryPlan
	cw.ifIsExeccute = true
	execCtx.cw = cw
	ses.SetMysqlResultSet(&MysqlResultSet{})

	originalBuild := buildPlanWithAuthorization
	defer func() { buildPlanWithAuthorization = originalBuild }()
	buildPlanWithAuthorization = func(context.Context, FeSession, plan2.CompilerContext, tree.Statement) (*planPb.Plan, error) {
		return nil, errors.New("prepared EXPLAIN must consume the initialized plan")
	}

	require.NoError(t, handleExplainStmt(ses, execCtx, explainStmt))
	require.Greater(t, ses.GetMysqlResultSet().GetRowCount(), uint64(0))
}

func TestSendPrepareResponseForExplainUsesExplainColumn(t *testing.T) {
	ctx := context.Background()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())

	testCases := []struct {
		name          string
		sql           string
		expectedTitle func(*planPb.Query) string
	}{
		{
			name: "explain",
			sql:  "explain select ?",
			expectedTitle: func(query *planPb.Query) string {
				return plan2.GetPlanTitle(query, false)
			},
		},
		{
			name: "explain analyze",
			sql:  "explain analyze select ?",
			expectedTitle: func(query *planPb.Query) string {
				return plan2.GetPlanTitle(query, false)
			},
		},
		{
			name: "explain phyplan",
			sql:  "explain phyplan select ?",
			expectedTitle: func(query *planPb.Query) string {
				return plan2.GetPhyPlanTitle(query, false)
			},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &prepareResponseCaptureConn{}
			ioses, err := NewIOSession(conn, pu, "")
			require.NoError(t, err)
			proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
			proto.capability &^= CLIENT_DEPRECATE_EOF
			proto.SetSession(&Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}})

			prepare := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(uint32(105+i))), tc.sql)
			stmts, err := mysql.Parse(ctx, prepare.Sql, 1)
			require.NoError(t, err)
			preparePlan, err := buildPlan(ctx, nil, plan2.NewEmptyCompilerContext(), prepare)
			require.NoError(t, err)
			prepareStmt := &PrepareStmt{
				Name:        preparePlan.GetDcl().GetPrepare().GetName(),
				PreparePlan: preparePlan,
				PrepareStmt: stmts[0],
			}
			defer prepareStmt.Close()

			require.NoError(t, proto.SendPrepareResponse(ctx, prepareStmt))
			packets := splitProtocolPackets(t, conn.writes)
			require.Len(t, packets, 5)
			require.Equal(t, uint16(1), binary.LittleEndian.Uint16(packets[0][5:]), "EXPLAIN returns one result column")
			require.Equal(t, uint16(1), binary.LittleEndian.Uint16(packets[0][7:]), "the inner SELECT keeps its parameter")

			resultColumn := parsePrepareColumnDefinition(t, packets[3])
			expectedTitle := tc.expectedTitle(preparePlan.GetDcl().GetPrepare().GetPlan().GetQuery())
			require.Equal(t, expectedTitle, resultColumn.name)
			require.Equal(t, defines.MYSQL_TYPE_VAR_STRING, resultColumn.typ)
		})
	}
}

func TestRebuildPreparedExplainAnalyzeKeepsExplainColumn(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 108, "explain analyze select 1")
	defer prepareStmt.Close()

	var rebuiltColumns []*planPb.ColDef
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(_ context.Context, columns []*planPb.ColDef) ([][]byte, error) {
		rebuiltColumns = columns
		return [][]byte{[]byte("explain-column")}, nil
	}

	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, rebuiltPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Len(t, rebuiltColumns, 1)
	require.Equal(t, int32(types.T_varchar), rebuiltColumns[0].Typ.Id)
	require.Equal(t, plan2.GetPlanTitle(rebuiltPlan.GetQuery(), false), rebuiltColumns[0].Name)
	require.Equal(t, rebuiltColumns[0].Name, rebuiltColumns[0].OriginName)
}

func TestCompileOutputCallbackSuppressesExplainPipelineRows(t *testing.T) {
	testCases := []struct {
		name       string
		stmt       tree.Statement
		wantCalled bool
	}{
		{name: "select", stmt: &tree.Select{}, wantCalled: true},
		{name: "explain analyze", stmt: &tree.ExplainAnalyze{}, wantCalled: false},
		{name: "explain phyplan", stmt: &tree.ExplainPhyPlan{}, wantCalled: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			fill := func(*batch.Batch, *perfcounter.CounterSet) error {
				called = true
				return nil
			}
			require.NoError(t, compileOutputCallback(tc.stmt, fill)(nil, nil))
			require.Equal(t, tc.wantCalled, called)
		})
	}
}
