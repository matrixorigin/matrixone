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
	"errors"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func statementDigestProtocolScope(proc *process.Process) *Scope {
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			Obj:     int64(planfunction.STATEMENT_DIGEST) << 32,
			ObjName: "statement_digest",
		}}},
	}}
	return &Scope{
		Magic:    Remote,
		Proc:     proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestStatementDigestDestinationProtocolValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	scope := statementDigestProtocolScope(c.proc)
	defer scope.RootOp.Release()

	client.version = defines.MORPCVersion72
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support STATEMENT_DIGEST")

	client.version = defines.MORPCVersion73
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// The destination is probed for every send, so a worker downgrade after
	// compile-time placement is fenced before a sender is created.
	client.version = defines.MORPCVersion72
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support STATEMENT_DIGEST")
	client.version = defines.MORPCVersion73
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, client.calls, client.releases)
}

func TestStatementDigestDestinationRejectsUnknownOrFailedProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	scope := statementDigestProtocolScope(c.proc)
	defer scope.RootOp.Release()

	client.version = defines.MORPCVersion73
	scope.NodeInfo.Id = "missing-worker"
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support STATEMENT_DIGEST")

	scope.NodeInfo.Id = "old-worker"
	client.customResponse = true
	client.response = nil
	client.sendErr = errors.New("capability probe failed")
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support STATEMENT_DIGEST")

	// A missing pipeline destination is also fail-closed when validation is
	// called directly (fillPipeline normally materializes an empty NodeInfo).
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion73)
	err = validateStatementDigestDestination(c.proc, &pipeline.Pipeline{})
	require.ErrorContains(t, err, "requires a versioned remote destination")

	// A coordinator below v73 is rejected by the coordinator gate before any
	// worker probe; a worker version cannot make the sender safe by itself.
	client.customResponse = false
	client.sendErr = nil
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion72)
	probeCalls := client.calls
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "STATEMENT_DIGEST remote execution requires MORPC protocol version 73")
	require.Equal(t, probeCalls, client.calls, "coordinator gate must reject before probing the worker")
}
