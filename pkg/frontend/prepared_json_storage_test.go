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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedJSONStorageRejectsEnumAndRecovers(t *testing.T) {
	for _, functionName := range []string{"json_storage_size", "json_storage_free"} {
		t.Run(functionName, func(t *testing.T) {
			query := "select " + functionName + "(?)"
			wantText := int64(9)
			if functionName == "json_storage_free" {
				wantText = 0
			}
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28034, query)
			setSessionAlloc("", NewLeakCheckAllocator())
			ioses, err := NewIOSession(&testConn{}, getPu(""), "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = ioses.Close() })
			proto := NewMysqlClientProtocol("", 0, ioses, 1024, getPu("").SV)
			proto.SetSession(ses)
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
			}()

			evaluate := func(mysqlType defines.MysqlType) (int64, error) {
				err := proto.ParseExecuteData(execCtx.reqCtx, cw.proc, prepareStmt,
					buildStringExecutePacket(proto, mysqlType, "1"), 0)
				if err != nil {
					return 0, err
				}
				require.Equal(t, []byte{byte(mysqlType), 0}, prepareStmt.ParamTypes)
				_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(
					execCtx, ses, cw, nil, prepareStmt.Name)
				if owned && executionStmt != nil {
					defer executionStmt.Free()
				}
				if err != nil {
					return 0, err
				}
				queryPlan := runtimePlan.GetQuery()
				projectNode := queryPlan.Nodes[queryPlan.Steps[len(queryPlan.Steps)-1]]
				executor, err := colexec.NewExpressionExecutor(cw.proc, projectNode.ProjectList[0])
				if err != nil {
					return 0, err
				}
				defer executor.Free()
				result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				if err != nil {
					return 0, err
				}
				return vector.GetFixedAtNoTypeCheck[int64](result, 0), nil
			}

			got, err := evaluate(defines.MYSQL_TYPE_VAR_STRING)
			require.NoError(t, err)
			require.Equal(t, wantText, got, "ordinary text remains valid for %s", functionName)
			require.Equal(t, types.T_any, cw.proc.GetPrepareParamType(0))
			require.Equal(t, vector.PrepareParamNone, cw.proc.GetPrepareParamKind(0))
			cw.proc.SetPrepareParams(nil)
			prepareStmt.clearBinaryParamState(cw.proc)

			got, err = evaluate(defines.MYSQL_TYPE_ENUM)
			require.Equal(t, int64(0), got)
			require.Error(t, err, "ENUM must not be accepted as text by %s", functionName)
			require.ErrorContains(t, err, "invalid argument "+functionName)
			require.Equal(t, types.T_enum, cw.proc.GetPrepareParamType(0),
				"COM_STMT metadata must retain ENUM source domain")
			cw.proc.SetPrepareParams(nil)
			prepareStmt.clearBinaryParamState(cw.proc)

			got, err = evaluate(defines.MYSQL_TYPE_VAR_STRING)
			require.NoError(t, err, "a failed execute must not poison the next valid call")
			require.Equal(t, wantText, got)
		})
	}
}
