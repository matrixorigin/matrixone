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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planPb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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

	conn := &prepareResponseCaptureConn{}
	ioses, err := NewIOSession(conn, pu, "")
	require.NoError(t, err)
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
	proto.capability &^= CLIENT_DEPRECATE_EOF
	proto.SetSession(&Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}})

	prepare := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(105)), "explain select ?")
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
	expectedTitle := plan2.GetPlanTitle(preparePlan.GetDcl().GetPrepare().GetPlan().GetQuery(), false)
	require.Equal(t, expectedTitle, resultColumn.name)
	require.Equal(t, defines.MYSQL_TYPE_VAR_STRING, resultColumn.typ)
}
