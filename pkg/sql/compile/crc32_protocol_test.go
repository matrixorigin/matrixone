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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func crc32JSONTextProtocolExpr(t *testing.T, pos int32) *planpb.Expr {
	t.Helper()
	arg := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "crc32", []*planpb.Expr{arg})
	require.NoError(t, err)
	return expr
}

func TestCRC32JSONProtocolIncludesMergedMediumIntBounds(t *testing.T) {
	mediumInt := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int32), Width: 24},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_ASSIGN, 0), ObjName: "cast_assign"},
			Args: []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: planpb.Type{Id: int32(types.T_int32), Width: 24}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
			},
		}},
	}
	// Preserve actual main's widened-result/width-24 assignment-target contract.
	mediumInt.Typ.Width = 32
	crc32JSON := crc32JSONTextProtocolExpr(t, 1)
	floatCast := floatConversionProtocolExpr(function.CAST, types.T_varchar, types.T_float64)
	for _, tc := range []struct {
		name            string
		exprs           []*planpb.Expr
		mediumInt       bool
		crc32JSON       bool
		floatConversion bool
		version         int64
	}{
		{"mediumint_only", []*planpb.Expr{mediumInt}, true, false, false, defines.MORPCVersion110},
		{"crc32_only", []*planpb.Expr{crc32JSON}, false, true, false, defines.MORPCVersion129},
		{"mediumint_then_crc32", []*planpb.Expr{mediumInt, crc32JSON}, true, true, false, defines.MORPCVersion129},
		{"crc32_then_mediumint", []*planpb.Expr{crc32JSON, mediumInt}, true, true, false, defines.MORPCVersion129},
		{"float_only", []*planpb.Expr{floatCast}, false, false, true, defines.MORPCVersion132},
		{"float_then_crc32", []*planpb.Expr{floatCast, crc32JSON}, false, true, true, defines.MORPCVersion132},
		{"crc32_then_float", []*planpb.Expr{crc32JSON, floatCast}, false, true, true, defines.MORPCVersion132},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: tc.exprs}}, Steps: []int32{0}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.Equal(t, tc.mediumInt, features.MediumIntAssignmentBounds)
			containsText, err := planpb.ContainsCRC32JSONTextBytes(qry)
			require.NoError(t, err)
			require.Equal(t, tc.crc32JSON, containsText)
			require.Equal(t, tc.floatConversion, features.FloatConversionSemantics)
			require.Equal(t, tc.version, remoteExpressionProtocolVersion(features))
			owner := &planpb.TableDef{}
			for _, expr := range tc.exprs {
				owner.Cols = append(owner.Cols, &planpb.ColDef{GeneratedCol: &planpb.GeneratedCol{Expr: expr}})
			}
			persisted, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, tc.version, persisted)

			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = tc.exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			client.version = tc.version - 1
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "remote destination")
			require.ErrorContains(t, err, fmt.Sprintf("MORPC version %d", tc.version))

			client.version = tc.version
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			encoded, err := encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, tc.version-1)
			decoded, err := decodeScope(encoded, c.proc, true, nil)
			require.ErrorContains(t, err, fmt.Sprintf("protocol version %d", tc.version))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			require.Nil(t, decoded)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, tc.version)
			decoded, err = decodeScope(encoded, c.proc, true, nil)
			require.NoError(t, err)
			decoded.release()
			// Analyze the decoded expression owner as well as the source query.
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: tc.exprs}}}
			wire, err := p.Marshal()
			require.NoError(t, err)
			var restored pipeline.Pipeline
			require.NoError(t, restored.Unmarshal(wire))
			restoredFeatures, err := planpb.RequiredRemoteExpressionFeatures(&restored)
			require.NoError(t, err)
			require.Equal(t, features, restoredFeatures)
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestCRC32JSONWithIndexSearchRetainsExistingGate(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	require.Equal(t, int64(defines.MORPCVersion111), int64(vectorscan.IndexSearchScanProtocolVersion))
	expr := crc32JSONTextProtocolExpr(t, 1)
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
		DataSource: &pipeline.Source{Node: &planpb.Node{NodeType: planpb.Node_INDEX_SEARCH_SCAN,
			IndexSearchScan: &planpb.IndexSearchScan{AlgoOptions: []byte(`{}`), AlgoExprs: []*planpb.Expr{expr}}}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.StringNumericResultContracts)
	require.Equal(t, int64(defines.MORPCVersion129), remoteExpressionProtocolVersion(features))
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	client.version = defines.MORPCVersion110
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion110)
	require.Error(t, validateRemoteVectorPartitionProtocol(c.proc, p))
	require.Error(t, validateVectorPartitionDestination(c.proc, p))
	client.version = defines.MORPCVersion111
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
	require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, p))
	require.NoError(t, validateVectorPartitionDestination(c.proc, p))
	client.version = defines.MORPCVersion129
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, p, features))
	require.Equal(t, client.calls, client.releases)
}
