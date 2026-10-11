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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedBinaryFieldCaseNumericTypedNull(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 1, "select field(case when ? then cast(null as signed) else ? end,?)")
	defer func() { cw.proc.SetPrepareParams(nil); prepared.Close() }()
	// Connector/J 8.0.15 sends both setString and setBytes with wire type 253;
	// this is not an independently declared MYSQL_TYPE_BLOB parameter.
	for index, wireType := range []defines.MysqlType{defines.MYSQL_TYPE_NULL, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING} {
		func() {
			cw.proc.SetPrepareParams(nil)
			if prepared.params != nil {
				prepared.params.Free(cw.proc.Mp())
			}
			prepared.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("0"), false, cw.proc.Mp()))
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("A"), wireType == defines.MYSQL_TYPE_NULL, cw.proc.Mp()))
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("a"), false, cw.proc.Mp()))
			prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(wireType), 0, byte(defines.MYSQL_TYPE_VAR_STRING), 0}
			_, runtime, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
			require.NoError(t, err)
			if owned && stmt != nil {
				defer stmt.Free()
			}
			query := runtime.GetQuery()
			project := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(cw.proc, project)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1, 1}[index], vector.GetFixedAtWithTypeCheck[int64](result, 0), "step %d: %s", index, project.String())
		}()
	}
}
