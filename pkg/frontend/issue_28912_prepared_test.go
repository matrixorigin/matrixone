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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type issue28912ObservedResult struct {
	value  string
	isNull bool
	typ    types.Type
	binary bool
}

func evalIssue28912PreparedProject(
	t *testing.T,
	proc *process.Process,
	runtimePlan *plan.Plan,
) (issue28912ObservedResult, error) {
	t.Helper()
	query := runtimePlan.GetQuery()
	if query == nil || len(query.Steps) == 0 {
		t.Fatalf("prepared execution has no query projection: %s", runtimePlan)
	}
	projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.Len(t, projectList, 1)
	executor, err := colexec.NewExpressionExecutor(proc, projectList[0])
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return issue28912ObservedResult{}, err
	}
	require.Equal(t, 1, result.Length())
	observed := issue28912ObservedResult{
		isNull: result.GetNulls().Contains(0),
		typ:    *result.GetType(),
		binary: result.GetIsBinaryStringAt(0),
	}
	if !observed.isNull {
		observed.value = result.GetStringAt(0)
	}
	return observed, nil
}

func requireIssue28912Value(
	t *testing.T,
	observed issue28912ObservedResult,
	want string,
	wantType types.T,
	wantBinary bool,
) {
	t.Helper()
	require.False(t, observed.isNull)
	require.Equal(t, want, observed.value)
	require.Equal(t, wantType, observed.typ.Oid)
	require.Equal(t, wantBinary, observed.binary)
}

func TestIssue28912COMStringParametersUseTextSemantics(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		mysqlType defines.MysqlType
		value     string
		want      string
	}{
		{name: "string SOUNDEX", query: "select soundex(?)", mysqlType: defines.MYSQL_TYPE_STRING, value: "\xc3\xa9", want: "é000"},
		{name: "var-string SOUNDEX", query: "select soundex(?)", mysqlType: defines.MYSQL_TYPE_VAR_STRING, value: "\xc3\xa9", want: "é000"},
		{name: "string QUOTE", query: "select quote(?)", mysqlType: defines.MYSQL_TYPE_STRING, value: "A\xffB", want: ""},
		{name: "var-string QUOTE", query: "select quote(?)", mysqlType: defines.MYSQL_TYPE_VAR_STRING, value: "A\xffB", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28912, tc.query)
			proto, _, scratch := newBinaryPrepareProtocolTestCase(t, tc.query)
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
				scratch.Close()
			}()
			originalPlan := prepareStmt.PreparePlan.String()
			require.NoError(t, proto.ParseExecuteData(
				execCtx.reqCtx,
				cw.proc,
				prepareStmt,
				buildStringExecutePacket(proto, tc.mysqlType, tc.value),
				0,
			))
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			if owned && executionStmt != nil {
				defer executionStmt.Free()
			}
			require.Equal(t, originalPlan, prepareStmt.PreparePlan.String(),
				"execute-time specialization must not mutate the cached PREPARE plan")
			observed, err := evalIssue28912PreparedProject(t, cw.proc, runtimePlan)
			require.NoError(t, err)
			requireIssue28912Value(t, observed, tc.want, types.T_text, false)
		})
	}
}

func TestIssue28912COMPreparedPlanRebindsStringDomainOnReuse(t *testing.T) {
	const query = "select soundex(?)"
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28912, query)
	proto, _, scratch := newBinaryPrepareProtocolTestCase(t, query)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
		scratch.Close()
	}()
	originalPlan := prepareStmt.PreparePlan.String()

	for _, tc := range []struct {
		name      string
		mysqlType defines.MysqlType
		want      string
		wantType  types.T
		binary    bool
	}{
		{name: "text parameter", mysqlType: defines.MYSQL_TYPE_STRING, want: "é000", wantType: types.T_text},
		{name: "binary parameter", mysqlType: defines.MYSQL_TYPE_LONG_BLOB, want: "", wantType: types.T_blob, binary: true},
		{name: "text parameter after binary", mysqlType: defines.MYSQL_TYPE_VAR_STRING, want: "é000", wantType: types.T_text},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, proto.ParseExecuteData(
				execCtx.reqCtx,
				cw.proc,
				prepareStmt,
				buildStringExecutePacket(proto, tc.mysqlType, "\xc3\xa9"),
				0,
			))
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			if owned && executionStmt != nil {
				defer executionStmt.Free()
			}
			require.Equal(t, originalPlan, prepareStmt.PreparePlan.String(),
				"reusing a statement must not mutate the cached PREPARE plan")
			observed, err := evalIssue28912PreparedProject(t, cw.proc, runtimePlan)
			require.NoError(t, err)
			requireIssue28912Value(t, observed, tc.want, tc.wantType, tc.binary)
		})
	}
}

func TestIssue28912BLOBProtocolRemainsBinary(t *testing.T) {
	t.Run("SOUNDEX result domain", func(t *testing.T) {
		ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28912, "select soundex(?)")
		proto, _, scratch := newBinaryPrepareProtocolTestCase(t, "select soundex(?)")
		defer func() {
			cw.proc.SetPrepareParams(nil)
			prepareStmt.Close()
			scratch.Close()
		}()
		require.NoError(t, proto.ParseExecuteData(
			execCtx.reqCtx, cw.proc, prepareStmt,
			buildStringExecutePacket(proto, defines.MYSQL_TYPE_LONG_BLOB, "\xc3\xa9"), 0))
		_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		observed, err := evalIssue28912PreparedProject(t, cw.proc, runtimePlan)
		require.NoError(t, err)
		requireIssue28912Value(t, observed, "", types.T_blob, true)
	})

	t.Run("QUOTE invalid bytes still errors", func(t *testing.T) {
		ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28912, "select quote(?)")
		proto, _, scratch := newBinaryPrepareProtocolTestCase(t, "select quote(?)")
		defer func() {
			cw.proc.SetPrepareParams(nil)
			prepareStmt.Close()
			scratch.Close()
		}()
		require.NoError(t, proto.ParseExecuteData(
			execCtx.reqCtx, cw.proc, prepareStmt,
			buildStringExecutePacket(proto, defines.MYSQL_TYPE_LONG_BLOB, "A\xffB"), 0))
		_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		_, err = evalIssue28912PreparedProject(t, cw.proc, runtimePlan)
		var conversionErr *moerr.Error
		require.ErrorAs(t, err, &conversionErr)
		require.Equal(t, moerr.ER_CANNOT_CONVERT_STRING, conversionErr.MySQLCode())
	})
}

func evalIssue28912SQLExecute(
	t *testing.T,
	stmtID uint32,
	sql string,
	variableName string,
	value string,
	variableType plan.Type,
) (issue28912ObservedResult, error) {
	t.Helper()
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, stmtID, sql)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	originalPlan := prepareStmt.PreparePlan.String()
	return evalIssue28912SQLExecuteInEnv(t, ses, prepareStmt, cw, execCtx, originalPlan,
		variableName, value, variableType)
}

func evalIssue28912SQLExecuteInEnv(
	t *testing.T,
	ses *Session,
	prepareStmt *PrepareStmt,
	cw *TxnComputationWrapper,
	execCtx *ExecCtx,
	originalPlan string,
	variableName string,
	value string,
	variableType plan.Type,
) (issue28912ObservedResult, error) {
	t.Helper()
	require.NoError(t, ses.setUserDefinedVarWithType(
		variableName, value, "", false, variableType))
	execCtx.input.isBinaryProtExecute = false
	cw.binaryPrepare = false
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: variableName}}}},
	}
	_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	if owned && executionStmt != nil {
		defer executionStmt.Free()
	}
	require.Equal(t, originalPlan, prepareStmt.PreparePlan.String(),
		"execute-time specialization must not mutate the cached PREPARE plan")
	return evalIssue28912PreparedProject(t, cw.proc, runtimePlan)
}

func TestIssue28912SQLExecutePreparedPlanRebindsVariableDomainOnReuse(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28912, "select soundex(?)")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	originalPlan := prepareStmt.PreparePlan.String()
	binaryType := plan.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
	textType := plan.Type{Id: int32(types.T_varchar), Width: 64, Charset: uint32(types.CharsetUTF8)}

	for _, tc := range []struct {
		name  string
		value string
		typ   plan.Type
		want  string
	}{
		{name: "binary variable", value: "\xc3\xa9", typ: binaryType, want: "é000"},
		{name: "text variable", value: "Pfister", typ: textType, want: "P236"},
		{name: "binary variable after text", value: "\xc3\xa9", typ: binaryType, want: "é000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed, err := evalIssue28912SQLExecuteInEnv(
				t, ses, prepareStmt, cw, execCtx, originalPlan,
				"reuse_binary_param", tc.value, tc.typ)
			require.NoError(t, err)
			requireIssue28912Value(t, observed, tc.want, types.T_text, false)
		})
	}
}

func TestIssue28912SQLExecuteBinaryVariableUsesTextFunctionContext(t *testing.T) {
	binaryType := plan.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
	for _, tc := range []struct {
		name  string
		sql   string
		value string
		want  string
	}{
		{name: "SOUNDEX", sql: "select soundex(?)", value: "\xc3\xa9", want: "é000"},
		{name: "QUOTE", sql: "select quote(?)", value: "A\xffB", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed, err := evalIssue28912SQLExecute(t, 28912, tc.sql,
				"binary_param", tc.value, binaryType)
			require.NoError(t, err)
			requireIssue28912Value(t, observed, tc.want, types.T_text, false)
		})
	}
}

func TestIssue28912ExplicitBinaryAndOtherConsumersKeepBinaryDomain(t *testing.T) {
	binaryType := plan.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
	for _, tc := range []struct {
		name          string
		sql           string
		value         string
		want          string
		wantType      types.T
		anyBinaryType bool
		wantError     bool
	}{
		{name: "explicit binary SOUNDEX", sql: "select soundex(cast(? as binary))", value: "\xc3\xa9", want: "", anyBinaryType: true},
		{name: "explicit binary QUOTE", sql: "select quote(cast(? as binary))", value: "A\xffB", wantError: true},
		{name: "CONCAT remains binary", sql: "select concat(?)", value: "\xc3\xa9", want: "é", wantType: types.T_varbinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed, err := evalIssue28912SQLExecute(t, 28912, tc.sql,
				"binary_param", tc.value, binaryType)
			if tc.wantError {
				var conversionErr *moerr.Error
				require.ErrorAs(t, err, &conversionErr)
				require.Equal(t, moerr.ER_CANNOT_CONVERT_STRING, conversionErr.MySQLCode())
				return
			}
			require.NoError(t, err)
			if tc.anyBinaryType {
				require.False(t, observed.isNull)
				require.Equal(t, tc.want, observed.value)
				require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(observed.typ))
				require.True(t, observed.binary)
			} else {
				requireIssue28912Value(t, observed, tc.want, tc.wantType, true)
			}
		})
	}
}
