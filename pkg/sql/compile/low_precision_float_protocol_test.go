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
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

var lowPrecisionFloatContractTypes = []types.T{
	types.T_bf16, types.T_float16, types.T_float8, types.T_float4,
	types.T_array_float8, types.T_array_float4,
}

func lowPrecisionColExpr(typ types.T) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(typ), Width: 4}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
}

func lowPrecisionFuncExpr(id int32, result types.T, args ...*planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(result)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
		Args: args,
	}}}
}

func TestLowPrecisionFloatContractFeatures(t *testing.T) {
	vecf32 := lowPrecisionColExpr(types.T_array_float32)
	cases := map[string]any{}
	for _, typ := range lowPrecisionFloatContractTypes {
		cases["column "+typ.String()] = lowPrecisionColExpr(typ)
		cases["table column "+typ.String()] = &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(typ)}}}}
		cases["group key "+typ.String()] = &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.Group), Agg: &pipeline.Group{Types: []planpb.Type{{Id: int32(typ)}}},
		}}}
	}
	cases["public cast to bf16"] = lowPrecisionFuncExpr(function.CAST, types.T_bf16,
		lowPrecisionColExpr(types.T_float32), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bf16)}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}})
	cases["vector_matmul over vecf32"] = lowPrecisionFuncExpr(function.VECTOR_MATMUL, types.T_json, lowPrecisionColExpr(types.T_int64), vecf32)
	cases["distinct vector_matmul"] = func() *planpb.Expr {
		expr := lowPrecisionFuncExpr(function.VECTOR_MATMUL, types.T_json, lowPrecisionColExpr(types.T_int64), vecf32)
		expr.GetF().Func.Obj = int64(uint64(expr.GetF().Func.Obj) | function.Distinct)
		return expr
	}()
	cases["vecblock_json over vecf32"] = lowPrecisionFuncExpr(function.VECBLOCK_JSON, types.T_varchar, vecf32)
	cases["vecblock_binary over vecf32"] = lowPrecisionFuncExpr(function.VECBLOCK_BINARY, types.T_varbinary, vecf32)
	for name, owner := range cases {
		t.Run(name, func(t *testing.T) {
			features, err := planpb.RequiredRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.LowPrecisionFloatContracts)
			required, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, plan2.LowPrecisionFloatProtocolVersion, required)
		})
	}

	for name, owner := range map[string]any{
		"float32 column": lowPrecisionColExpr(types.T_float32),
		"vecf32 column":  vecf32,
		"vecbf16 table":  &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_array_bf16)}}}},
		"l2_distance":    lowPrecisionFuncExpr(function.L2_DISTANCE, types.T_float64, vecf32, vecf32),
	} {
		t.Run("control "+name, func(t *testing.T) {
			features, err := planpb.RequiredRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.False(t, features.LowPrecisionFloatContracts)
		})
	}
}

// vector_matmul lowered to a Group descriptor keeps no function expression.
func TestLowPrecisionFloatContractVectorMatmulDescriptor(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		Op: int32(vm.Group),
		Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{{
			Op:   aggexec.AggIdOfVectorMatmul,
			Expr: []*planpb.Expr{lowPrecisionColExpr(types.T_int64), lowPrecisionColExpr(types.T_array_float32)},
		}}},
	}}}
	features, err := requiredRemotePipelineExpressionFeatures(&pipeline.Pipeline{Children: []*pipeline.Pipeline{p}})
	require.NoError(t, err)
	require.True(t, features.LowPrecisionFloatContracts)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, plan2.LowPrecisionFloatProtocolVersion-1)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), fmt.Sprintf("version %d", plan2.LowPrecisionFloatProtocolVersion))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, plan2.LowPrecisionFloatProtocolVersion)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))

	p.InstructionList[0].Agg.Aggs[0].Op = aggexec.AggIdOfCountColumn
	features, err = requiredRemotePipelineExpressionFeatures(p)
	require.NoError(t, err)
	require.False(t, features.LowPrecisionFloatContracts)
}

func TestLowPrecisionFloatContractRemoteBoundaries(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	exprs := map[string]*planpb.Expr{
		"vector_matmul over vecf32": lowPrecisionFuncExpr(function.VECTOR_MATMUL, types.T_json,
			lowPrecisionColExpr(types.T_int64), lowPrecisionColExpr(types.T_array_float32)),
	}
	for _, typ := range lowPrecisionFloatContractTypes {
		exprs[typ.String()] = lowPrecisionColExpr(typ)
	}
	for name, expr := range exprs {
		t.Run(name, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}
			place := func(version int64) {
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			}
			place(plan2.LowPrecisionFloatProtocolVersion - 1)
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			place(plan2.LowPrecisionFloatProtocolVersion)
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)

			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			client.version = plan2.LowPrecisionFloatProtocolVersion - 1
			_, err := encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "remote destination")
			client.version = plan2.LowPrecisionFloatProtocolVersion
			_, err = encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)

			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, plan2.LowPrecisionFloatProtocolVersion-1)
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), fmt.Sprintf("version %d", plan2.LowPrecisionFloatProtocolVersion))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, plan2.LowPrecisionFloatProtocolVersion)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
		})
	}
}
