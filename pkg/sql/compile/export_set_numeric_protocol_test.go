// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestExportSetNumericProtocolSenderAndReceiver(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	cast5 := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(21)<<32 | 5, ObjName: "cast"},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_decimal128)}}, {Typ: planpb.Type{Id: int32(types.T_int64)}}},
	}}}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{cast5}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{cast5}
	scope := &Scope{Magic: Remote, Proc: c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}

	features, err := planpb.RequiredRemoteExpressionFeatures(qry)
	require.NoError(t, err)
	require.True(t, features.ExportSetNumericContracts)
	for _, tc := range []struct {
		typ  types.T
		want bool
	}{{types.T_float64, true}, {types.T_int64, false}} {
		exportSet := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: int64(385) << 32, ObjName: "export_set"},
			Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(tc.typ)}}},
		}}}
		direct, featureErr := planpb.RequiredRemoteExpressionFeatures(exportSet)
		require.NoError(t, featureErr)
		require.Equal(t, tc.want, direct.ExportSetNumericContracts)
	}

	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 1}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainExportSetNumericWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 1}}
	client.version = defines.MORPCVersion82
	require.NoError(t, c.constrainExportSetNumericWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)

	client.version = defines.MORPCVersion80
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc,
		&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{cast5}}}}), "version 82")
}
