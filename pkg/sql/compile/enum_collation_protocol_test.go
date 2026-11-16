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

package compile

import (
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestRemoteEnumCollationProtocolFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	features, err := plan.RequiredRemoteExpressionFeatures(&plan.TableDef{EnumUnsupportedDefault: "utf8mb4_0900_ai_ci"})
	require.NoError(t, err)
	require.True(t, features.EnumCollation)
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	require.ErrorContains(t, validateRemoteExpressionFeatures(proc, features), "version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	require.NoError(t, validateRemoteExpressionFeatures(proc, features))
}

func TestRemoteEnumCollationDestinationAndStreamFloor(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: types.EnumCollationUTF8Bin},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion134
	var floor int64
	_, err := encodeRemoteScopeWithProtocol(scope, c.proc, &floor)
	require.ErrorContains(t, err, "MORPC protocol version 135")
	require.Equal(t, defines.MORPCVersion135, floor)
	client.version = defines.MORPCVersion135
	floor = 0
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &floor)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion135, floor)
	p := new(pipeline.Pipeline)
	require.NoError(t, p.Unmarshal(data))
	features, err := pipelineExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.EnumCollation)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion) })
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "protocol version 135")
}
