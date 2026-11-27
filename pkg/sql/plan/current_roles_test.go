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

package plan

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCurrentRolesProtocolGate(t *testing.T) {
	ctx := context.Background()
	require.Error(t, requireCurrentRolesProtocol(ctx, nil))
	require.Error(t, requireCurrentRolesProtocol(ctx, &process.Process{}))
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion11)
	require.Error(t, requireCurrentRolesProtocol(ctx, proc))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	require.NoError(t, requireCurrentRolesProtocol(ctx, proc))
}

func TestCurrentRolesSQLPlanning(t *testing.T) {
	mock := NewMockOptimizer(false)
	proc := mock.ctxt.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion11)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	_, err := buildSingleStmt(mock, t, "select role_id from mo_current_roles() as r")
	require.NoError(t, err)
	_, err = buildSingleStmt(mock, t, "select role_id from mo_current_roles(1) as r")
	require.Error(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion11)
	_, err = buildSingleStmt(mock, t, "select role_id from mo_current_roles() as r")
	require.Error(t, err)
}
