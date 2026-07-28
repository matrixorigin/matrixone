// Copyright 2021 - 2026 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestEffectiveExternalStrictModeForLoadDataIgnore(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Load", tree.QueryTypeDML, true)

	loadParam := &tree.ExternParam{ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD)}}
	require.False(t, effectiveExternalStrictMode(proc, loadParam, true))
	require.False(t, effectiveExternalStrictMode(proc, loadParam, false))

	externalScanParam := &tree.ExternParam{ExParam: tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)}}
	require.True(t, effectiveExternalStrictMode(proc, externalScanParam, true))

	proc.GetStmtProfile().SetStatementRuntimeProfile("Load", tree.QueryTypeDML, false)
	require.True(t, effectiveExternalStrictMode(proc, loadParam, true))
}

func TestStrictSqlModeTraditional(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return "TRADITIONAL", nil
	})

	err, strict := StrictSqlMode(proc)
	require.NoError(t, err)
	require.True(t, strict)
}
