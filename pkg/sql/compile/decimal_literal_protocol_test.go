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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func remoteDecimalLiteralExpr() *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{
			Id:    int32(types.T_decimal256),
			Width: 40,
			Scale: 1,
		},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			// The field name is retained for protobuf source compatibility; the
			// feature is admitted at MORPC v88.
			DecimalLiteralRequiresV82: true,
			Value:                     &planpb.Literal_Sval{Sval: "12345678901234567890123456789012345678.1"},
		}},
	}
}

func remoteDecimalLiteralPipeline() *pipeline.Pipeline {
	return &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		ProjectList: []*planpb.Expr{remoteDecimalLiteralExpr()},
	}}}
}

func TestRemoteDecimalLiteralProtocolValidation(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := runtime.ServiceRuntime(c.proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion86)
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion88)
		}
	})

	p := remoteDecimalLiteralPipeline()
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.DecimalLiteralSemantics)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion86)
	err = validateRemoteExpressionPipelineProtocol(c.proc, p)
	require.ErrorContains(t, err, "exact DECIMAL256 literal semantics require MORPC protocol version 88")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion88)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}

func TestRemoteDecimalLiteralPlacementAndDestinationValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := remoteDecimalLiteralExpr()
	query := &planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}},
		Steps: []int32{0},
	}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}

	client.version = defines.MORPCVersion86
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainDecimalLiteralWorkers(query))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	client.version = defines.MORPCVersion88
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainDecimalLiteralWorkers(query))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, client.calls, client.releases)
}
