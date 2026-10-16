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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Pin the semantic capability independently of the current latest version.
const exactJSONTestProtocol int64 = 119

func boundExactJSONProtocolExpressions(t *testing.T) map[string]*planpb.Expr {
	t.Helper()
	col := func(oid types.T) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(oid)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(t.Context(), name, args)
		require.NoError(t, err)
		return expr
	}
	parse := bind("json_parse_exact", col(types.T_varchar))
	nested := bind("json_extract_string", parse, plan2.MakePlan2StringConstExprWithType("$.x"))
	cast := func(oid types.T) *planpb.Expr {
		return bind("cast", col(types.T_json), &planpb.Expr{
			Typ: planpb.Type{Id: int32(oid), Width: 18, Scale: 3}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
		})
	}
	return map[string]*planpb.Expr{"parse": parse, "nested scan filter": nested,
		"stored JSON decimal64": cast(types.T_decimal64), "stored JSON decimal128": cast(types.T_decimal128),
		"division of stored JSON":           bind("div", cast(types.T_decimal128), plan2.MakePlan2Int64ConstExprWithType(1)),
		"decimal variance of stored JSON":   bind("var_pop", cast(types.T_decimal128)),
		"calendar interval over exact JSON": bind("to_calendar_interval", nested, plan2.MakePlan2Int64ConstExprWithType(int64(types.Day)))}
}

func TestExactJSONProtocolPlacementSendAndReceive(t *testing.T) {
	for name, expr := range boundExactJSONProtocolExpressions(t) {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			if name == "division of stored JSON" {
				c.proc.SetStmtProfile(&process.StmtProfile{})
				c.proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
				c.proc.GetSessionInfo().SqlMode = "TRADITIONAL"
				require.True(t, c.proc.DivisionByZeroWarningsEnabled())
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol)
			qry := &planpb.Query{Nodes: []*planpb.Node{{FilterList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			if name == "decimal variance of stored JSON" {
				require.True(t, features.ExactJSONDecimalCast)
				require.True(t, features.DecimalStatisticalResultContracts)
			}
			if name == "calendar interval over exact JSON" {
				require.True(t, features.ExactJSONParsing)
				require.True(t, features.CalendarIntervalNormalization)
			}
			if name == "division of stored JSON" {
				require.True(t, features.ExactJSONDecimalCast)
				require.True(t, features.ScalarDivision)
			}
			require.Equal(t, exactJSONTestProtocol, executionRemoteExpressionProtocolVersion(c.proc, features))
			require.Equal(t, exactJSONTestProtocol, remoteExpressionProtocolVersion(features))
			required, err := plan2.RequiredPersistedExpressionProtocolVersion(qry)
			require.NoError(t, err)
			require.Equal(t, exactJSONTestProtocol, required)

			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc,
				NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			place := func(version int64) {
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			}
			for _, old := range []int64{defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118} {
				place(old)
				require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
				require.Len(t, c.cnList, 1)
				require.Equal(t, c.addr, c.cnList[0].Addr)
				data, err := encodeRemoteScope(scope, c.proc)
				require.Nil(t, data, "no executable payload may escape to an old worker")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			}
			place(exactJSONTestProtocol)
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			data, err := encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			require.NotEmpty(t, data)
			wire := new(pipeline.Pipeline)
			require.NoError(t, wire.Unmarshal(data))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol-1)
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 119")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wire))
			// Replace the worker after successful placement: no observation is cached.
			client.version = exactJSONTestProtocol - 1
			data, err = encodeRemoteScope(scope, c.proc)
			require.Error(t, err)
			require.Nil(t, data)
			require.Positive(t, client.calls)
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestExactJSONProtocolUnknownChangedAndCanceledDestination(t *testing.T) {
	expr := boundExactJSONProtocolExpressions(t)["stored JSON decimal128"]
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	for _, name := range []string{"missing node", "missing client", "stale endpoint", "wrong identity",
		"nil response", "missing version", "failed response", "floor lowered", "cancel before", "cancel during"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol)
			client.version = exactJSONTestProtocol
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			c.proc.Ctx = ctx
			switch name {
			case "missing node":
				p.Node = nil
			case "missing client":
				c.proc.Base.QueryClient = nil
			case "stale endpoint":
				p.Node.Addr = "stale:6001"
			case "wrong identity":
				p.Node.Id = "wrong-worker"
			case "nil response":
				client.customResponse = true
			case "missing version":
				client.customResponse, client.response = true, &querypb.Response{}
			case "failed response":
				client.customResponse, client.response = true, &querypb.Response{}
				client.sendErr = errors.New("probe failed")
			case "floor lowered":
				client.onSend = func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol-1) }
			case "cancel before":
				cancel()
			case "cancel during":
				client.onSend = cancel
			}
			err := validateRemoteExpressionDestination(c.proc, p, features)
			if name == "cancel before" || name == "cancel during" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			}
			if client.response != nil || !client.customResponse {
				require.Equal(t, client.calls, client.releases)
			} else {
				require.Zero(t, client.releases)
			}
		})
	}
}

func TestExactJSONProtocolPlacementProbeTransitions(t *testing.T) {
	expr := boundExactJSONProtocolExpressions(t)["parse"]
	for _, cancelProbe := range []bool{false, true} {
		t.Run(map[bool]string{false: "floor lowered", true: "parent canceled"}[cancelProbe], func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol)
			client.version = exactJSONTestProtocol
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			c.proc.Ctx = ctx
			client.onSend = func() {
				if cancelProbe {
					cancel()
				} else {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol-1)
				}
			}
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			err := c.constrainRemoteExpressionWorkers(&planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}})
			if cancelProbe {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			}
			require.Equal(t, 1, client.calls)
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestExactJSONLocalAndCachedAdmission(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	expressions := boundExactJSONProtocolExpressions(t)
	owners := map[string]*planpb.Plan{}
	for name, expr := range expressions {
		owners[name] = &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}}}
	}
	owners["DDL expression"] = &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expressions["parse"]}}}}}}}}}
	owners["Parquet scan"] = &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{ExternScan: &planpb.ExternScan{Type: int32(planpb.ExternType_LOAD)}, TableDef: &planpb.TableDef{Createsql: `{"Format":"parquet"}`}}}}}}
	for name, pn := range owners {
		t.Run(name, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, exactJSONTestProtocol)
			require.NoError(t, validateExactJSONProtocol(c.proc, pn))
			require.Zero(t, testing.AllocsPerRun(10, func() { require.NoError(t, validateExactJSONProtocol(c.proc, pn)) }))
			c.pn = pn
			for _, version := range []any{exactJSONTestProtocol - 1, nil, "119"} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				require.ErrorContains(t, c.Compile(t.Context(), pn, nil), "protocol version 119")
				_, err := c.Run(0)
				require.ErrorContains(t, err, "protocol version 119", "cached execution must recheck a closed floor")
			}
		})
	}
	plain := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}}}}}}
	require.NoError(t, validateExactJSONProtocol(nil, plain), "unrelated local plans retain their original contract")
	require.ErrorContains(t, validateExactJSONProtocol(nil, owners["parse"]), "protocol version 119")
}
