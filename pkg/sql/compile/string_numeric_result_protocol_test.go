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
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
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

func makeNumericValueProgramExpr() *planpb.Expr {
	int64Literal := func(value int64) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}},
		}}
	}
	text := makeStringLiteralExpr("1", planpb.StringLiteralForm_STRING_LITERAL_HEX)
	text.Typ.Id = int32(types.T_varchar)
	scope := int64Literal(1)
	definition := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_any)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(605) << 32, ObjName: "_scalar_numeric_program"},
		Args: []*planpb.Expr{scope, text},
	}}}
	value := &planpb.Expr{Typ: text.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(606) << 32, ObjName: "_scalar_numeric_program_ref"},
		Args: []*planpb.Expr{definition, int64Literal(1), int64Literal(0)},
	}}}
	kind := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(606) << 32, ObjName: "_scalar_numeric_program_ref"},
		Args: []*planpb.Expr{int64Literal(1), int64Literal(1)},
	}}}
	target := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
	policy := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_Lit{
		Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 0}},
	}}
	return &planpb.Expr{Typ: target.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(21)<<32 | 9, ObjName: "cast"},
		Args: []*planpb.Expr{value, target, kind, policy},
	}}}
}

func numericValueProgramProtocolVersion(
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
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
	client.version = version
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))

	if version < defines.MORPCVersion130 {
		require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
			"pre-v130 placement must fall back locally")
		require.Equal(t, c.addr, c.cnList[0].Addr)
		_, err := encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion130),
			"a destination downgrade after placement must be rejected before send")
		rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), fmt.Sprintf("version %d", defines.MORPCVersion130),
			"pre-v130 receivers must fail closed")
		return
	}

	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"v130 placement must retain distributed execution")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline))
}

func TestNumericValueProgramAdmissionEveryMode(t *testing.T) {
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
			projects := []*planpb.Expr{makeNumericValueProgramExpr()}
			qry.Nodes[0].ProjectList = projects
			scope.RootOp.(*projection.Projection).ProjectList = projects
			wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: projects}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.True(t, features.NumericValuePrograms)
			combinedFeatures := features
			combinedFeatures.JSONStringConsumerOverload = true
			require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(combinedFeatures),
				"numeric programs raise the independent JSON v129 floor to v130")

			info := c.proc.GetSessionInfo()
			info.MySQLNumericCompatibilityMode = mode.mysql
			info.LegacyNumericCompatibilityMode = false
			for _, version := range []int64{
				defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112,
				defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116,
				defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119,
				defines.MORPCVersion120, defines.MORPCVersion121,
			} {
				numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, version)
			}
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion122)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion123)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion124)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion125)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion126)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion127)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion128)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion129)
			numericValueProgramProtocolVersion(t, c, client, qry, scope, wirePipeline, defines.MORPCVersion130)

		})
	}
}

func TestNumericValueProgramUnknownWorkerCapabilityFailsClosed(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
	scope.Proc = c.proc
	t.Cleanup(scope.RootOp.Release)
	projects := []*planpb.Expr{makeNumericValueProgramExpr()}
	qry.Nodes[0].ProjectList = projects
	scope.RootOp.(*projection.Projection).ProjectList = projects
	wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: projects}}}
	c.proc.GetSessionInfo().LegacyNumericCompatibilityMode = false

	rt := runtime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
	client.customResponse = true
	client.response = &querypb.Response{}
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"unknown worker capability must not admit numeric value programs")
	require.Equal(t, c.addr, c.cnList[0].Addr)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 130",
		"the final destination fence must reject an unknown worker for numeric value programs")
	require.Equal(t, client.calls, client.releases)

	client.customResponse = false
	client.version = defines.MORPCVersion128
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a known v128 worker must not receive v130 numeric program IDs")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion128)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "version 130",
		"a v128 receiver must reject the v130 numeric program IDs")
	client.version = defines.MORPCVersion129
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"the JSON-only v129 worker must not receive numeric program IDs")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion129)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "version 130",
		"a v129 receiver must reject the v130 numeric program IDs")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, "unknown")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "version 130",
		"the receiver fence must reject an unknown local protocol for numeric value programs")
}

func TestSelectIntoKindLayoutProtocolAdmission(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	query := &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"v"},
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Typ: planpb.Type{Id: int32(types.T_bool)}},
		}}},
		IntoUserVariableKindColumns: []int32{1},
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(query)
	require.NoError(t, err)
	require.True(t, features.NumericValuePrograms)
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features),
		"Query metadata needs the same independent v130 sender floor as expression programs")

	wire := &pipeline.Pipeline{
		Qry:  &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}},
		Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
	}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion128)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 130",
		"a receiver must inspect Query-level metadata even without an expression marker")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, wire, features), "version 130",
		"the final destination fence must include Query-level metadata")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion129)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 130",
		"v129 JSON support alone must not admit a SELECT INTO kind map")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
	client.version = defines.MORPCVersion128
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"the sender must keep the SELECT INTO kind map away from pre-v130 workers")
	client.version = defines.MORPCVersion129
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"the sender must keep the SELECT INTO kind map away from JSON-only v129 workers")
	client.version = defines.MORPCVersion130
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"the v130 worker accepts the SELECT INTO kind map")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wire),
		"the v130 receiver accepts the SELECT INTO kind map")
}

func TestDependentScalarKindResultRequiresV130(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	query := &planpb.Query{Correlation: &planpb.CorrelationSpec{Programs: []*planpb.CorrelationProgram{{
		OutputTypes: []planpb.Type{
			{Id: int32(types.T_varchar)},
			{Id: int32(types.T_bool)},
		},
		ScalarResultKindColumn: 1,
	}}}}
	pipeline := &pipeline.Pipeline{Qry: &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(query)
	require.NoError(t, err)
	require.True(t, features.NumericValuePrograms)
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features),
		"the descriptor alone carries an ordinary BOOL passenger without private expression nodes")

	rt := runtime.ServiceRuntime(c.proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	for _, version := range []int64{defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130} {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
		err := validateRemoteExpressionPipelineProtocol(c.proc, pipeline)
		if version < defines.MORPCVersion130 {
			require.ErrorContains(t, err, "version 130")
		} else {
			require.NoError(t, err)
		}
	}

	client.version = defines.MORPCVersion128
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a v128 worker must not receive a correlated scalar kind layout")
	client.version = defines.MORPCVersion129
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a JSON-only v129 worker must not receive a correlated scalar kind layout")
	client.version = defines.MORPCVersion130
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"a v130 worker accepts the correlated scalar kind layout")

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "unknown-worker", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(query))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"an unaddressable worker has unknown capability and must fail closed")
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
			require.False(t, features.NumericValuePrograms)
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
			for _, version := range []int64{
				defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112,
				defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116,
				defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119,
				defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122,
				defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125,
				defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128,
				defines.MORPCVersion129,
				defines.MORPCVersion130,
			} {
				// The coordinator uses v130 while worker placement varies; then
				// receiver validation is run at the worker's actual version.
				rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion130)
				client.version = version
				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				denied := tc.requiresCurrent && version < defines.MORPCVersion130
				if denied {
					require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
					require.Equal(t, c.addr, c.cnList[0].Addr)
					_, err = encodeRemoteScope(scope, c.proc)
					require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion130))
				} else {
					require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType)
					data, sendErr := encodeRemoteScope(scope, c.proc)
					require.NoError(t, sendErr)
					require.NotEmpty(t, data)
				}
				rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
				err = validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline)
				if denied {
					require.ErrorContains(t, err, fmt.Sprintf("version %d", defines.MORPCVersion130))
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
