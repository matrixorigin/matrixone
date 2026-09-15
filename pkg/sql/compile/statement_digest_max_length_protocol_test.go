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

package compile

import (
	"errors"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestRemoteStatementDigestRejectsMaxDigestLengthResolutionFailure guards the
// sender-side protocol gate. A remote digest pipeline must not be dispatched
// with a default token budget when the initiating statement cannot resolve its
// max_digest_length setting.
func TestRemoteStatementDigestRejectsMaxDigestLengthResolutionFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "max_digest_length" {
			return nil, errors.New("max_digest_length unavailable")
		}
		return "", nil
	})
	defer proc.Free()

	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion73)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion73)

	digest := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{
			Obj:     int64(planfunction.STATEMENT_DIGEST) << 32,
			ObjName: "statement_digest",
		},
	}}}
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{digest}}}}

	err := validateRemoteExpressionPipelineProtocol(proc, p)
	require.ErrorContains(t, err, "resolve max_digest_length: max_digest_length unavailable")
	require.False(t, proc.GetSessionInfo().MaxDigestLengthSet,
		"a failed resolution must not publish a default before dispatch")
	require.Zero(t, proc.GetSessionInfo().MaxDigestLength)
}
