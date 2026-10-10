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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func stringNumericResultPipeline(t *testing.T) (*planpb.Query, *Scope) {
	t.Helper()
	args := []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	}
	expr, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "find_in_set", args)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int32), expr.Typ.Id)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{expr}
	return qry, &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func strictStringNumericCompatibilityPipeline(t *testing.T, sourceType types.T, overload int64) (*planpb.Query, *Scope) {
	t.Helper()
	stringValue := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(sourceType)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	// Use each serialized CAST identity that can reach the mode-aware
	// string-to-FLOAT executor; the protocol fence must not depend on overload
	// selection.
	cast := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: overload, ObjName: "cast"},
			Args: []*planpb.Expr{stringValue},
		}},
	}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{cast}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{cast}
	return qry, &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestStringNumericResultPlacementAndDestinationProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry, scope := stringNumericResultPipeline(t)
	scope.Proc = c.proc
	t.Cleanup(scope.RootOp.Release)

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion79
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a worker below the result-contract version must not receive a multi-CN plan")
	require.Equal(t, c.addr, c.cnList[0].Addr)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"a v80 worker keeps the multi-CN placement")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	client.version = defines.MORPCVersion79
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination",
		"a destination downgrade after placement must still be rejected")

	// An unaddressable worker is an unknown capability and must fail closed to
	// one-CN placement rather than assuming the current protocol is sufficient.
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
	require.Equal(t, c.addr, c.cnList[0].Addr)

	rt := runtime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion79)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc,
		&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
			ProjectList: []*planpb.Expr{qry.Nodes[0].ProjectList[0]},
		}}}), "version 80")
}

func makeStringLiteralExpr(value string, form planpb.StringLiteralForm) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value:       &planpb.Literal_Sval{Sval: value},
			IsBin:       form == planpb.StringLiteralForm_STRING_LITERAL_HEX || form == planpb.StringLiteralForm_STRING_LITERAL_BIT,
			LiteralForm: form,
		}},
	}
}

func makeStringNumericFlowExpr(name string, functionID int64, args ...*planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: functionID << 32, ObjName: name}, Args: args,
		}},
	}
}

func makeStringNumericCastExpr(value *planpb.Expr, typ types.T) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(typ)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: int64(21) << 32, ObjName: "cast"}, Args: []*planpb.Expr{value},
		}},
	}
}

func makeNumericBinaryLiteralProvenanceExpr(producer string) *planpb.Expr {
	if producer != "case" {
		panic("unknown numeric binary literal flow producer: " + producer)
	}
	condition := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
	}
	hex := makeStringLiteralExpr("1", planpb.StringLiteralForm_STRING_LITERAL_HEX)
	text := makeStringLiteralExpr("1", planpb.StringLiteralForm_STRING_LITERAL_TEXT)
	return makeStringNumericCastExpr(
		makeStringNumericFlowExpr("case", 71, condition, hex, text), types.T_int64)
}

func numericBinaryLiteralProtocolVersion(
	t *testing.T,
	c *Compile,
	client *expressionVersionClient,
	qry *planpb.Query,
	scope *Scope,
	wirePipeline *pipeline.Pipeline,
	version int64,
) {
	t.Helper()
	rt := runtime.ServiceRuntime(c.proc.GetService())
	oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
	// The coordinator is on the new contract while the fake worker is varied
	// across protocol versions to prove mixed-version admission.
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	client.version = version
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))

	if version < defines.MORPCVersion107 {
		require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
			"pre-v107 placement must fall back locally")
		require.Equal(t, c.addr, c.cnList[0].Addr)
		_, err := encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion107),
			"a destination downgrade after placement must be rejected before send")
		rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), fmt.Sprintf("version %d", defines.MORPCVersion107),
			"pre-v107 receivers must fail closed")
		return
	}

	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"v107 placement must retain distributed execution")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline))
}

func TestNumericBinaryLiteralProvenanceAdmissionEveryMode(t *testing.T) {
	for _, mode := range []struct {
		name  string
		mysql bool
	}{
		{name: "default"},
		{name: "mysql_compatibility", mysql: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
			scope.Proc = c.proc
			t.Cleanup(scope.RootOp.Release)
			projects := []*planpb.Expr{makeNumericBinaryLiteralProvenanceExpr("case")}
			qry.Nodes[0].ProjectList = projects
			scope.RootOp.(*projection.Projection).ProjectList = projects
			wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: projects}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.True(t, features.NumericBinaryLiteralProvenance)

			info := c.proc.GetSessionInfo()
			info.MySQLNumericCompatibilityMode = mode.mysql
			info.LegacyNumericCompatibilityMode = false
			for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107} {
				numericBinaryLiteralProtocolVersion(t, c, client, qry, scope, wirePipeline, version)
			}

			info.LegacyNumericCompatibilityMode = true
			c.execType = plan.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			client.version = defines.MORPCVersion107
			rt := runtime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
			require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "legacy session contract")
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "legacy session contract")
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "legacy session contract")
		})
	}
}

func TestStringNumericCompatibilityAdmissionBoundaries(t *testing.T) {
	cases := []struct {
		name            string
		mysql           bool
		requiresCurrent bool
		kind            string
	}{
		{name: "strict_cast", requiresCurrent: true, kind: "cast"},
		{name: "explicit_mysql_cast", mysql: true, kind: "cast"},
		{name: "historical_ceil", mysql: true, requiresCurrent: true, kind: "historical_ceil"},
		{name: "ordinary_float_int64", requiresCurrent: true, kind: "float_int64"},
		{name: "scalar_ceil_precision", mysql: true, requiresCurrent: true, kind: "ceil_scalar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
			scope.Proc = c.proc
			t.Cleanup(scope.RootOp.Release)
			expr := qry.Nodes[0].ProjectList[0]
			switch tc.kind {
			case "historical_ceil":
				expr = &planpb.Expr{
					Typ: planpb.Type{Id: int32(types.T_float64)},
					Expr: &planpb.Expr_F{F: &planpb.Function{
						Func: &planpb.ObjectRef{Obj: int64(72)<<32 | 12, ObjName: "ceil"},
						Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_varchar)},
							Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1.5tail"}}}}},
					}},
				}
			case "float_int64":
				bound, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "round", []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 12.345}}}},
					{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				})
				require.NoError(t, err)
				expr = bound
			case "ceil_scalar":
				bound, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "ceil", []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "2"}}}},
				})
				require.NoError(t, err)
				expr = bound
			}
			qry.Nodes[0].ProjectList = []*planpb.Expr{expr}
			scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{expr}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.False(t, features.NumericBinaryLiteralProvenance)
			switch tc.kind {
			case "historical_ceil":
				require.True(t, features.HistoricalStringMathCompatibility)
			case "float_int64":
				require.True(t, features.OrdinaryFloatInt64Bounds)
			case "ceil_scalar":
				require.True(t, features.ScalarMathPrecisionCompatibility)
			default:
				require.True(t, features.StrictStringNumericCompatibility)
			}
			info := c.proc.GetSessionInfo()
			info.MySQLNumericCompatibilityMode = tc.mysql
			info.LegacyNumericCompatibilityMode = false
			rt := runtime.ServiceRuntime(c.proc.GetService())
			oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
			t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
			wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107} {
				// The coordinator uses v107 while worker placement varies; then
				// receiver validation is run at the worker's actual version.
				rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
				client.version = version
				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				denied := tc.requiresCurrent && version == defines.MORPCVersion106
				if denied {
					require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
					require.Equal(t, c.addr, c.cnList[0].Addr)
					_, err = encodeRemoteScope(scope, c.proc)
					require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion107))
				} else {
					require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType)
					data, sendErr := encodeRemoteScope(scope, c.proc)
					require.NoError(t, sendErr)
					require.NotEmpty(t, data)
				}
				rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
				err = validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline)
				if denied {
					require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion107))
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
