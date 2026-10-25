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
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// jqBinaryProtocolEnv pairs the execute-time prepared-statement environment
// with a protocol that can decode COM_STMT_EXECUTE packets for the same
// statement, so a test can bind binary parameters exactly the way the
// production mysql_cmd_executor COM_STMT_EXECUTE handler does.
type jqBinaryProtocolEnv struct {
	ses         *Session
	prepareStmt *PrepareStmt
	cw          *TxnComputationWrapper
	execCtx     *ExecCtx
	proto       *MysqlProtocolImpl
	scratch     *PrepareStmt
	cachedPlan  []byte
}

func newJqBinaryProtocolEnv(t *testing.T, stmtID uint32, call string) *jqBinaryProtocolEnv {
	t.Helper()
	query := "select " + call + "(?, ?) as result"
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, stmtID, query)
	proto, _, scratchPrepare := newBinaryPrepareProtocolTestCase(t, query)
	cachedPlan, err := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
	require.NoError(t, err)
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
		scratchPrepare.Close()
	})
	return &jqBinaryProtocolEnv{
		ses:         ses,
		prepareStmt: prepareStmt,
		cw:          cw,
		execCtx:     execCtx,
		proto:       proto,
		scratch:     scratchPrepare,
		cachedPlan:  cachedPlan,
	}
}

// bindAndEval decodes one COM_STMT_EXECUTE packet carrying the given string
// parameters through the production decoder, binds them at execute time, and
// evaluates the prepared projection. It returns the scalar result, whether it
// is NULL, and the evaluation error. The cached prepare plan must survive the
// rebinding untouched.
func (e *jqBinaryProtocolEnv) bindAndEval(t *testing.T, packet []byte) (string, bool, error) {
	t.Helper()
	require.NoError(t, e.proto.ParseExecuteData(
		e.execCtx.reqCtx, e.cw.proc, e.prepareStmt, packet, 0))
	_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
		e.execCtx, e.ses, e.cw, nil, e.prepareStmt.Name)
	require.NoError(t, err)
	if owned && executionStmt != nil {
		defer executionStmt.Free()
	}

	queryPlan := runtimePlan.GetQuery()
	projects := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]].ProjectList
	require.Len(t, projects, 1)
	executor, err := colexec.NewExpressionExecutor(e.cw.proc, projects[0])
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(e.cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	after, marshalErr := e.prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
	require.NoError(t, marshalErr)
	require.Equal(t, e.cachedPlan, after, "execute-time rebinding must not mutate the cached plan")
	if err != nil {
		return "", false, err
	}
	require.Equal(t, 1, result.Length())
	if result.IsNull(0) {
		return "", true, nil
	}
	return result.GetStringAt(0), false, nil
}

func (e *jqBinaryProtocolEnv) bindStringsAndEval(t *testing.T, jsonArg, queryArg string) (string, bool, error) {
	t.Helper()
	packet := buildStringExecutePacketForParams(e.proto,
		[]defines.MysqlType{defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING},
		[]string{jsonArg, queryArg})
	return e.bindAndEval(t, packet)
}

// buildNullExecutePacketForParams builds a COM_STMT_EXECUTE payload that
// binds NULL for every parameter of the statement and re-binds the parameter
// types, matching the wire shape a driver sends for SQL NULL arguments.
func buildNullExecutePacketForParams(tp defines.MysqlType, count int) []byte {
	data := make([]byte, 7+count*2)
	data[1] = 1 // iteration count
	for i := 0; i < count; i++ {
		data[5] |= 1 << byte(i)
	}
	data[6] = 1 // new parameters bound
	for i := 0; i < count; i++ {
		data[7+2*i] = byte(tp)
	}
	return data
}

// TestCOMStmtJqPrepareExecuteRebindsExactParameterStrings covers the binary
// prepared protocol for jq()/try_jq() (issues #30924 and #30929): the same
// prepared statement is repeatedly re-bound through the production COM_STMT
// execute parameter decoder and execute-time initializer, and every binding
// must keep jq()'s exact contract. Integer 2^53 boundaries, multi-result
// framing, error and NULL handling are probed by re-binding string parameters
// on one statement, including ordinary controls and a final rebind that keeps
// stale parameter state visible.
func TestCOMStmtJqPrepareExecuteRebindsExactParameterStrings(t *testing.T) {
	for _, fn := range []struct{ name, call string }{
		{"jq", "jq"},
		{"try_jq", "try_jq"},
	} {
		t.Run(fn.name, func(t *testing.T) {
			env := newJqBinaryProtocolEnv(t, 30929, fn.call)
			for _, tc := range []struct {
				name       string
				jsonArg    string
				queryArg   string
				nullParams bool
				want       string
				strictErr  string
			}{
				{name: "positive 2^53+1", jsonArg: `{"n":9007199254740993}`, queryArg: ".n", want: "9007199254740993"},
				{name: "ordinary single control", jsonArg: `{"foo":128}`, queryArg: ".foo", want: "128"},
				{name: "negative 2^53+1", jsonArg: `{"n":-9007199254740993}`, queryArg: ".n", want: "-9007199254740993"},
				{name: "framed multi results", jsonArg: `{"a":1,"b":2}`, queryArg: ".a, .b", want: "1\n2"},
				{name: "22-digit integer", jsonArg: `{"n":4722366482869645213696}`, queryArg: ".n", want: "4722366482869645213696"},
				{name: "invalid json kept strict", jsonArg: "oops", queryArg: ".n", strictErr: "invalid character 'o'"},
				{name: "empty json kept strict", jsonArg: "", queryArg: ".n", strictErr: "unexpected end of JSON input"},
				{name: "overflow rejects like legacy decode", jsonArg: `{"n":1e309}`, queryArg: ".n", strictErr: "cannot unmarshal number 1e309"},
				{name: "positive 2^53+1 after rebinds", jsonArg: `{"n":9007199254740993}`, queryArg: ".n", want: "9007199254740993"},
				// A NULL wire binding keeps jq's NULL contract on every
				// engine: SQL NULL inputs stay SQL NULL. This case runs last
				// because re-binding a concrete value after a NULL bitmap
				// binding is separate, pre-existing frontend parameter-decode
				// behavior outside this issue group's scope.
				{name: "null parameters", nullParams: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var got string
					var isNull bool
					var err error
					if tc.nullParams {
						got, isNull, err = env.bindAndEval(t,
							buildNullExecutePacketForParams(defines.MYSQL_TYPE_VAR_STRING, 2))
					} else {
						got, isNull, err = env.bindStringsAndEval(t, tc.jsonArg, tc.queryArg)
					}
					if fn.name == "jq" && tc.strictErr != "" {
						require.ErrorContains(t, err, tc.strictErr)
						return
					}
					require.NoError(t, err)
					if tc.strictErr != "" || tc.nullParams {
						// try_jq swallows every strict rejection and NULL inputs stay NULL.
						require.True(t, isNull)
						return
					}
					require.False(t, isNull)
					require.Equal(t, tc.want, got)
				})
			}
		})
	}
}

// TestCOMStmtJqCursorFetchRetainsExactBinaryRows covers the server-side
// prepared cursor path for jq()/try_jq() (issues #30924 and #30929): rows
// produced by genuinely bound COM_STMT executes are materialized through the
// production cursor capture, retained independently of their source vectors,
// and returned by COM_STMT_FETCH with binary result rows whose exact bytes
// encode newline framing and integers beyond 2^53 without loss.
func TestCOMStmtJqCursorFetchRetainsExactBinaryRows(t *testing.T) {
	ctx := context.Background()

	// Every retained row is produced by a real binary parameter binding and
	// evaluation of the prepared query, so the cursor stores genuine jq output.
	rows := []struct {
		call   string
		json   string
		query  string
		want   string
		isNull bool
	}{
		{call: "jq", json: `{"n":9007199254740993}`, query: ".n", want: "9007199254740993"},
		{call: "jq", json: `{"n":-9007199254740993}`, query: ".n", want: "-9007199254740993"},
		{call: "jq", json: `{"n":4722366482869645213696}`, query: ".n", want: "4722366482869645213696"},
		{call: "jq", json: `{"a":1,"b":2}`, query: ".a, .b", want: "1\n2"},
		{call: "jq", json: `{"foo":128}`, query: ".foo", want: "128"},
		{call: "try_jq", json: "oops", query: ".n", isNull: true},
	}
	values := make([]string, len(rows))
	stmtIDs := map[string]uint32{"jq": 30924, "try_jq": 30929}
	for call, stmtID := range stmtIDs {
		env := newJqBinaryProtocolEnv(t, stmtID, call)
		for i, row := range rows {
			if row.call != call {
				continue
			}
			got, isNull, err := env.bindStringsAndEval(t, row.json, row.query)
			require.NoError(t, err, "row %d", i)
			if row.isNull {
				require.True(t, isNull, "row %d must be NULL", i)
				continue
			}
			require.False(t, isNull, "row %d", i)
			require.Equal(t, row.want, got, "row %d", i)
			values[i] = got
		}
	}

	// Cursor fixture: the session owns one prepared statement whose cursor
	// retains the rows above until COM_STMT_FETCH asks for them.
	conn := &prepareResponseCaptureConn{}
	proto, proc, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, "select jq(?, ?) as result", conn)
	cleanupGeometryWireFixture(t, proto, stmt)
	ses := proto.GetSession()
	ses.proc = proc
	ses.respr = NewMysqlResp(proto)
	ses.SetCmd(COM_STMT_FETCH)
	ses.prepareStmts = map[string]*PrepareStmt{stmt.Name: stmt}

	column := new(MysqlColumn)
	column.SetName("result")
	require.NoError(t, setMysqlColumnTypeInfo(ctx, types.T_varchar.ToType(), column))

	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	for i, row := range rows {
		if row.isNull {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], nil, true, proc.Mp()))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(values[i]), false, proc.Mp()))
		}
	}
	bat.SetRowCount(len(rows))

	stmt.cursor = &preparedStmtCursor{
		result:   &MysqlResultSet{Columns: []Column{column}},
		maxRows:  uint64(len(rows) + 2),
		maxBytes: 4096,
	}
	require.NoError(t, capturePreparedCursorBatch(ses, &ExecCtx{reqCtx: ctx, prepareStmt: stmt}, bat))
	require.Positive(t, ses.preparedCursorBytes.Load())

	// Retained rows own their bytes: mutating the source vector must not
	// change what the client receives. COM_STMT_EXECUTE also uses binary
	// result rows, so encode the undelivered cursor rows through that
	// command as well and compare every packet byte for byte.
	bat.Vecs[0].GetBytesAt(0)[0] = 9
	wantBinaryRow := func(i int) []byte {
		// Binary result rows offset the null bitmap by two reserved bits
		// (column i marks bit (i+2)%8), so one column yields a 0x04 marker
		// byte for a NULL row and 0x00 otherwise.
		packet := []byte{defines.OKHeader, 0}
		if rows[i].isNull {
			packet[1] = 4
			return packet
		}
		packet = append(packet, byte(len(rows[i].want)))
		return append(packet, rows[i].want...)
	}
	ses.SetCmd(COM_STMT_EXECUTE)
	undelivered := &MysqlResultSet{Columns: []Column{column}, Data: stmt.cursor.result.Data}
	require.NoError(t, proto.WriteResultSetRow(undelivered, uint64(len(rows))))
	require.NoError(t, proto.flush())
	packets := splitProtocolPackets(t, conn.writes)
	require.Len(t, packets, len(rows))
	for i := range rows {
		require.Equal(t, wantBinaryRow(i), packets[i])
	}

	// COM_STMT_FETCH hands out the retained rows across two fetches; the
	// cursor terminates the first response with CURSOR_EXISTS and the last
	// one with LAST_ROW_SENT.
	conn.writes = nil
	ses.SetCmd(COM_STMT_FETCH)
	request := make([]byte, 8)
	binary.LittleEndian.PutUint32(request[:4], 1)
	binary.LittleEndian.PutUint32(request[4:], 4)
	response, err := executeStmtFetch(ctx, ses, request)
	require.NoError(t, err)
	require.Nil(t, response)
	require.NotNil(t, stmt.cursor)
	require.Equal(t, uint64(4), stmt.cursor.offset)
	require.NoError(t, proto.flush())
	packets = splitProtocolPackets(t, conn.writes)
	require.Len(t, packets, 5)
	for i := 0; i < 4; i++ {
		require.Equal(t, wantBinaryRow(i), packets[i])
	}
	require.Equal(t, byte(defines.EOFHeader), packets[4][0])
	status := binary.LittleEndian.Uint16(packets[4][3:5])
	require.NotZero(t, status&SERVER_STATUS_CURSOR_EXISTS)
	require.Zero(t, status&SERVER_STATUS_LAST_ROW_SENT)

	conn.writes = nil
	response, err = executeStmtFetch(ctx, ses, request)
	require.NoError(t, err)
	require.Nil(t, response)
	require.Nil(t, stmt.cursor)
	require.NoError(t, proto.flush())
	packets = splitProtocolPackets(t, conn.writes)
	require.Len(t, packets, 3)
	require.Equal(t, wantBinaryRow(4), packets[0])
	require.Equal(t, wantBinaryRow(5), packets[1])
	require.Equal(t, byte(defines.EOFHeader), packets[2][0])
	status = binary.LittleEndian.Uint16(packets[2][3:5])
	require.NotZero(t, status&SERVER_STATUS_LAST_ROW_SENT)
	require.Zero(t, status&SERVER_STATUS_CURSOR_EXISTS)
	require.Zero(t, ses.preparedCursorBytes.Load())
}
