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
const exactJSONTestProtocol int64 = 129

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
	unicodeType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	unicodeType.CollationVersion = 1
	unicodeCol := &planpb.Expr{Typ: unicodeType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	least := bind("least", unicodeCol, unicodeCol)
	parse := bind("json_parse_exact", col(types.T_varchar))
	nested := bind("json_extract_string", parse, plan2.MakePlan2StringConstExprWithType("$.x"))
	cast := func(oid types.T) *planpb.Expr {
		return bind("cast", col(types.T_json), &planpb.Expr{
			Typ: planpb.Type{Id: int32(oid), Width: 18, Scale: 3}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
		})
	}
	return map[string]*planpb.Expr{"parse": parse, "nested scan filter": nested,
		"stored JSON decimal64": cast(types.T_decimal64), "stored JSON decimal128": cast(types.T_decimal128),
		"collation least over exact JSON":   bind("concat", nested, least),
		"table collation over exact JSON":   bind("internal_table_collation", nested),
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
			if name == "collation least over exact JSON" {
				require.True(t, features.ExactJSONParsing)
				require.True(t, features.LeastGreatestCollationSemantics)
			}
			if name == "table collation over exact JSON" {
				require.True(t, features.ExactJSONParsing)
				require.True(t, features.TableCollationFunction)
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
			for _, old := range []int64{defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
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
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 129")
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
			} else if name == "failed response" {
				// Main preserves transport failures at required send gates.
				require.ErrorIs(t, err, client.sendErr)
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

// The new floor must not relabel or lower independent main contracts.
func TestExactJSONProtocolKeepsUpstreamFloors(t *testing.T) {
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	typ.CollationVersion = 1
	col := &planpb.Expr{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(t.Context(), name, args)
		require.NoError(t, err)
		return expr
	}
	least := bind("least", col, col)
	collation := bind("internal_table_collation", col)
	exact := bind("json_parse_exact", col)
	jsonColumn := &planpb.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	jsonNumeric := bind("=", jsonColumn, plan2.MakePlan2Int64ConstExprWithType(1))
	exactNumeric := bind("=", exact, plan2.MakePlan2Int64ConstExprWithType(1))
	dependent := &planpb.Expr{Typ: types.T_int64.ToType().PlanType(),
		Expr: &planpb.Expr_Dependent{Dependent: &planpb.DependentExpr{ProgramId: 1}}}
	// Dependent expressions are coordinator-owned: they add no scalar worker
	// floor here, while their durable catalog owner retains main's v119 floor.
	for _, tc := range []struct {
		name                        string
		expressions                 []*planpb.Expr
		workerFloor, persistedFloor int64
	}{
		{"least", []*planpb.Expr{least}, defines.MORPCVersion117, defines.MORPCVersion117},
		{"table collation", []*planpb.Expr{collation}, defines.MORPCVersion118, defines.MORPCVersion118},
		{"both upstream", []*planpb.Expr{least, collation}, defines.MORPCVersion118, defines.MORPCVersion118},
		{"dependent program", []*planpb.Expr{dependent}, 0, defines.MORPCVersion119},
		{"main JSON numeric comparison", []*planpb.Expr{jsonNumeric}, defines.MORPCVersion128, defines.MORPCVersion128},
		{"JSON numeric comparison with exact", []*planpb.Expr{exactNumeric}, exactJSONTestProtocol, exactJSONTestProtocol},
		{"dependent with exact", []*planpb.Expr{dependent, exact}, exactJSONTestProtocol, exactJSONTestProtocol},
		{"both with exact", []*planpb.Expr{least, collation, exact}, exactJSONTestProtocol, exactJSONTestProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: tc.expressions}}}
			encoded, err := owner.Marshal()
			require.NoError(t, err)
			decoded := new(planpb.Query)
			require.NoError(t, decoded.Unmarshal(encoded))
			features, err := planpb.RequiredRemoteExpressionFeatures(decoded)
			require.NoError(t, err)
			require.Equal(t, tc.workerFloor, remoteExpressionProtocolVersion(features))
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(decoded)
			require.NoError(t, err)
			require.Equal(t, tc.persistedFloor, floor)
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
			for _, version := range []any{exactJSONTestProtocol - 1, nil, "129"} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				require.ErrorContains(t, c.Compile(t.Context(), pn, nil), "protocol version 129")
				_, err := c.Run(0)
				require.ErrorContains(t, err, "protocol version 129", "cached execution must recheck a closed floor")
			}
		})
	}
	plain := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}}}}}}
	require.NoError(t, validateExactJSONProtocol(nil, plain), "unrelated local plans retain their original contract")
	require.ErrorContains(t, validateExactJSONProtocol(nil, owners["parse"]), "protocol version 129")
}
