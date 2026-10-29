// Copyright 2021 - 2026 MatrixOne
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// TestExecuteParamCountMismatchMapsToMySQL1210 pins issue #31454: a parameter
// count mismatch on EXECUTE must surface MySQL's ER_WRONG_ARGUMENTS class
// (errno 1210, HY000) instead of MO's 20301 "invalid input", and the failed
// validation must not unregister the statement: the same handle has to accept
// a corrected binding afterwards.
func TestExecuteParamCountMismatchMapsToMySQL1210(t *testing.T) {
	ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 31454, "select ?+0 AS x")
	prepared.proc = cw.proc
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })

	// Binary-protocol mismatch: two wire parameters for a one-placeholder plan.
	cw.discardRuntimeCacheCandidate()
	prepared.clearBinaryParamState(cw.proc)
	prepared.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("7"), false, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("9"), false, cw.proc.Mp()))
	prepared.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
	}

	_, _, _, _, _, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongArguments), err)
	require.ErrorContains(t, err, "Incorrect arguments to EXECUTE")

	// The failed validation must keep the statement registered (issue #31454).
	got, serr := ses.GetPrepareStmt(context.Background(), prepared.Name)
	require.NoError(t, serr)
	require.NotNil(t, got)

	// Corrected binding succeeds on the same handle.
	cw.discardRuntimeCacheCandidate()
	prepared.clearBinaryParamState(cw.proc)
	prepared.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("7"), false, cw.proc.Mp()))
	prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0}
	_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	require.NoError(t, err)
	require.NotNil(t, p)
	if owned && stmt != nil {
		stmt.Free()
	}
}
