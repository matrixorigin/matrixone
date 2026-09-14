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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestRemoteIPFunctionProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	makePipeline := func(functionID, overloadID int32) *pipeline.Pipeline {
		return &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
			ProjectList: []*planpb.Expr{{
				Typ: planpb.Type{Id: 10},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{
						Obj:     function.EncodeOverloadID(functionID, overloadID),
						ObjName: "ip-function",
					},
				}},
			}},
		}}}
	}

	for _, functionID := range []int32{
		392, // INET6_ATON
		393, // INET6_NTOA
		394, // INET_ATON
		395, // INET_NTOA
		396, // IS_IPV4
		397, // IS_IPV6
		398, // IS_IPV4_COMPAT
		399, // IS_IPV4_MAPPED
	} {
		t.Run("function-"+strconv.Itoa(int(functionID)), func(t *testing.T) {
			remotePipeline := makePipeline(functionID, 0)

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
			err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
			require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion72)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
		})
	}

	t.Run("new INET_NTOA overload", func(t *testing.T) {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
		err := validateRemoteExpressionPipelineProtocol(proc, makePipeline(function.INET_NTOA, 8))
		require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")
	})

	t.Run("ordinary function is not fenced", func(t *testing.T) {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
		require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, makePipeline(function.ABS, 0)))
	})
}
