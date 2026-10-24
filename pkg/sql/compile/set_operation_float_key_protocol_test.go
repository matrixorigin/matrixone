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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetOperationFloatKeyDependentCompilationProtocol(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)})
	require.NoError(t, err)
	shared := &planpb.TableDef{Defs: []*planpb.TableDef_DefType{{Def: &planpb.TableDef_DefType_Properties{
		Properties: &planpb.PropertiesDef{Properties: []*planpb.Property{{Key: catalog.PropFromPublication, Value: "true"}}},
	}}}}
	q := &planpb.Query{Correlation: &planpb.CorrelationSpec{}, Steps: []int32{0}, Nodes: []*planpb.Node{{
		NodeType: planpb.Node_INSERT, InsertCtx: &planpb.InsertCtx{TableDef: shared}, ProjectList: []*planpb.Expr{key},
	}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	_, err = c.compileQuery(q)
	require.ErrorContains(t, err, "version 131")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	_, err = c.compileQuery(q)
	// At127 compilation reaches the independent target-admission owner; no
	// engine/source work is needed to prove that the expression fence passed.
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCCPRReadOnly), "%v", err)
}

func TestSetOperationFloatKeyProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.SetOperationFloatKeys)
	require.Equal(t, defines.MORPCVersion131, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{PhysicalEqualityKeyList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64, want plan2.ExecType) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
	}
	place(defines.MORPCVersion125, plan2.ExecTypeAP_ONECN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 131")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	place(defines.MORPCVersion131, plan2.ExecTypeAP_MULTICN)
	var streamRequirement int64
	_, err = encodeRemoteScopeWithProtocol(scope, c.proc, &streamRequirement)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion131, streamRequirement)
	client.version = defines.MORPCVersion125
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 131", "send checks changed actual destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 131")
	require.Equal(t, client.calls, client.releases)
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		validationErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		if version < defines.MORPCVersion131 {
			require.ErrorContains(t, validationErr, "version 131")
			require.ErrorContains(t, decodeErr, "version 131")
		} else {
			require.NoError(t, validationErr)
			require.NoError(t, decodeErr)
		}
	}
}

// Exercise the real compiler producer: the key is hidden in SetOp.KeyExprs or
// Group.GroupBy, rather than an artificial visible projection.
func TestCompiledSetOperationFloatKeyProtocol(t *testing.T) {
	for _, kind := range []planpb.Node_NodeType{planpb.Node_INTERSECT_ALL, planpb.Node_UNION} {
		t.Run(kind.String(), func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
			c.execType = plan2.ExecTypeAP_ONECN
			c.isPrepare = true
			visible := plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)
			key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.DeepCopyExpr(visible)})
			require.NoError(t, err)
			node := &planpb.Node{NodeType: kind, ProjectList: []*planpb.Expr{visible}, PhysicalEqualityKeyList: []*planpb.Expr{key}}
			input := func() *Scope {
				return &Scope{Magic: Normal, NodeInfo: engine.Node{Mcpu: 1}, Proc: c.proc.NewNoContextChildProc(0), RootOp: projection.NewArgument()}
			}
			left, right := []*Scope{input()}, []*Scope{input()}
			var result []*Scope
			if kind == planpb.Node_UNION {
				result = c.compileUnion(node, left, right)
			} else {
				result = c.compileMinusAndIntersect(node, left, right, kind)
			}
			t.Cleanup(func() {
				for _, scope := range result {
					scope.FreeOperator(c)
					scope.release()
				}
				c.proc.Free()
			})
			require.Len(t, result, 1)
			scope := result[0]
			if kind == planpb.Node_UNION {
				require.Len(t, scope.RootOp.(*group.Group).GroupBy, 2)
			} else {
				require.Len(t, scope.RootOp.(*intersectall.IntersectAll).KeyExprs, 1)
			}
			scope.Magic = Remote
			scope.NodeInfo.Id = "old-worker"
			scope.NodeInfo.Addr = "remote:6001"
			client.version = defines.MORPCVersion125
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 131")
			require.Positive(t, client.calls, "actual stage must probe its destination")
			client.version = defines.MORPCVersion131
			data, err := encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			var wire pipeline.Pipeline
			require.NoError(t, wire.Unmarshal(data))
			features, err := planpb.RequiredRemoteExpressionFeatures(&wire)
			require.NoError(t, err)
			require.True(t, features.SetOperationFloatKeys)
			require.Equal(t, client.calls, client.releases)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
			_, err = decodeScope(data, c.proc, true, nil)
			require.ErrorContains(t, err, "version 131")
		})
	}
}

// A set-key floor must compose with SELECT-only division diagnostics without
// making either capability depend on the other's session policy.
func TestSetOperationFloatKeyAndDivisionProtocol(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	c.proc.SetStmtProfile(&process.StmtProfile{})
	c.proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
	c.proc.SetResolveVariableFunc(nil)
	key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{
		plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0),
	})
	require.NoError(t, err)
	division, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
		plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 1),
		plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 2),
	})
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, tc := range []struct {
		name   string
		exprs  []*planpb.Expr
		hasKey bool
	}{
		{"division", []*planpb.Expr{division}, false},
		{"set-key", []*planpb.Expr{key}, true},
		{"combined", []*planpb.Expr{division, key}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: tc.exprs}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.Equal(t, tc.hasKey, features.SetOperationFloatKeys)
			for _, mode := range []string{"ERROR_FOR_DIVISION_BY_ZERO", ""} {
				c.proc.GetSessionInfo().SqlMode = mode
				want := remoteExpressionProtocolVersion(features)
				if tc.hasKey {
					require.Equal(t, defines.MORPCVersion131, want)
				} else {
					require.Less(t, want, defines.MORPCVersion116, "division diagnostics are not a persisted expression contract")
					if mode != "" {
						want = defines.MORPCVersion116
					}
				}
				require.Equal(t, want, executionRemoteExpressionProtocolVersion(c.proc, features))
				floor := defines.MORPCVersion116
				if tc.hasKey {
					floor = defines.MORPCVersion118
				}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, floor)
				err = validateRemoteExpressionPipelineProtocol(c.proc, p)
				if tc.hasKey {
					require.ErrorContains(t, err, "version 131")
				} else {
					require.NoError(t, err)
				}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
			}
		})
	}
}

// Official main's collation capability keeps its own floor when a set key
// advances. Composition requires the maximum; neither feature hides the other.
func TestSetOperationFloatKeyAndCollationProtocol(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	typ.CollationVersion = 1
	collation, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "least", []*planpb.Expr{plan2.GetColExpr(typ.PlanType(), 0, 0), plan2.GetColExpr(typ.PlanType(), 0, 1)})
	require.NoError(t, err)
	key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 2)})
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, tc := range []struct {
		name   string
		exprs  []*planpb.Expr
		hasKey bool
	}{
		{"collation", []*planpb.Expr{collation}, false},
		{"combined", []*planpb.Expr{collation, key}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: tc.exprs}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, features.LeastGreatestCollationSemantics)
			require.Equal(t, tc.hasKey, features.SetOperationFloatKeys)
			want := defines.MORPCVersion117
			if tc.hasKey {
				want = defines.MORPCVersion131
			}
			require.Equal(t, want, remoteExpressionProtocolVersion(features))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion117)
			err = validateRemoteExpressionPipelineProtocol(c.proc, p)
			if tc.hasKey {
				require.ErrorContains(t, err, "version 131")
			} else {
				require.NoError(t, err)
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
		})
	}
}

// Table collation keeps its released identity and floor when set keys advance.
// A mixed pipeline must require the newer capability before decoding owners.
func TestSetOperationFloatKeyAndTableCollationProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	arg := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: ""}}}}
	tableCollation, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "internal_table_collation", []*planpb.Expr{arg})
	require.NoError(t, err)
	key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)})
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, tc := range []struct {
		name     string
		exprs    []*planpb.Expr
		hasTable bool
		hasKey   bool
	}{
		{"table-collation", []*planpb.Expr{tableCollation}, true, false},
		{"set-key", []*planpb.Expr{key}, false, true},
		{"combined", []*planpb.Expr{tableCollation, key}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: tc.exprs}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.Equal(t, tc.hasTable, features.TableCollationFunction)
			require.Equal(t, tc.hasKey, features.SetOperationFloatKeys)
			want := defines.MORPCVersion118
			if tc.hasKey {
				want = defines.MORPCVersion131
			}
			require.Equal(t, want, remoteExpressionProtocolVersion(features))
			persisted, err := plan2.RequiredPersistedExpressionProtocolVersion(p)
			require.NoError(t, err)
			require.Equal(t, want, persisted)
			data, err := p.Marshal()
			require.NoError(t, err)
			for _, floor := range []int64{defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, floor)
				client.version = floor
				sendErr := validateRemoteExpressionDestination(c.proc, &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}, features)
				decoded, decodeErr := decodeScope(data, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				if floor < want {
					require.ErrorContains(t, sendErr, "version 131")
					require.ErrorContains(t, decodeErr, "version 131")
					require.Nil(t, decoded)
				} else {
					require.NoError(t, sendErr)
					require.NoError(t, decodeErr)
				}
			}
		})
	}
	require.Equal(t, client.calls, client.releases)
}

func TestSetOperationFloatKeyCapabilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		key, marked, temporal, second bool
		floor                         int64
	}{
		{"DIV-only", false, false, false, false, defines.MORPCVersion121},
		{"DIV-and-key", true, false, false, false, defines.MORPCVersion131},
		{"DIV-and-local-marker", false, true, false, false, defines.MORPCVersion131},
		{"timestampadd-only", false, false, false, true, defines.MORPCVersion124},
		{"timestampadd-and-key", true, false, false, true, defines.MORPCVersion131},
		{"timestampadd-and-local-marker", false, true, false, true, defines.MORPCVersion131},
		{"timestamp-only", false, false, true, false, defines.MORPCVersion122},
		{"timestamp-and-key", true, false, true, false, defines.MORPCVersion131},
		{"timestamp-and-local-marker", false, true, true, false, defines.MORPCVersion131},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			division, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
				plan2.GetColExpr(types.New(types.T_decimal64, 10, 3).PlanType(), 0, 0),
				plan2.GetColExpr(types.T_uint64.ToType().PlanType(), 0, 1),
			})
			require.NoError(t, err)
			if tc.temporal {
				division, err = plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "timestamp", []*planpb.Expr{
					plan2.GetColExpr(types.T_datetime.ToTypeWithScale(3).PlanType(), 0, 0),
				})
				require.NoError(t, err)
			}
			if tc.second {
				division, err = plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "timestampadd_second_interval", []*planpb.Expr{plan2.GetColExpr(types.New(types.T_decimal64, 18, 7).PlanType(), 0, 0)})
				require.NoError(t, err)
			}
			exprs := []*planpb.Expr{division}
			if tc.key {
				key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 2)})
				require.NoError(t, err)
				exprs = append(exprs, key)
			}
			node := &planpb.Node{NodeType: planpb.Node_PROJECT, ProjectList: exprs, ScalarSetReaggregation: tc.marked}
			if tc.marked {
				node.NodeType = planpb.Node_WINDOW
			}
			owner := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
			features, err := planpb.RequiredRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.Equal(t, !tc.temporal && !tc.second, features.DecimalIntegerDivisionSemantics)
			require.Equal(t, tc.temporal, features.TimestampDatetimeResultContracts)
			require.Equal(t, tc.second, features.TimestampAddSecondIntervals)
			require.Equal(t, tc.key, features.SetOperationFloatKeys)
			require.Equal(t, tc.marked, features.ScalarSetReaggregation)
			require.Equal(t, tc.floor, remoteExpressionProtocolVersion(features))
			persisted, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, tc.floor, persisted)
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: exprs, ScalarSetReaggregation: tc.marked}}}
			wire, err := p.Marshal()
			require.NoError(t, err)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			if tc.marked {
				// Scalar placement has its own owner, independently of whether
				// a remote worker could satisfy the expression version floor.
				coordinator := engine.Node{Id: "coordinator", Addr: "local:6001", Mcpu: 1}
				require.NoError(t, validateScalarSetScopePlacement(owner, coordinator, []*Scope{{NodeInfo: coordinator}}))
				require.Error(t, validateScalarSetScopePlacement(owner, coordinator, []*Scope{scope}))
			}
			for _, floor := range []int64{defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
				client.version = floor
				// Receive rollback from the previous round must not downgrade
				// the current sender before independently probing its worker.
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				local := floor < tc.floor || tc.marked
				if !tc.marked {
					c.execType = plan2.ExecTypeAP_MULTICN
					c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
					require.NoError(t, c.constrainRemoteExpressionWorkers(owner))
					if local {
						require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					} else {
						require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					}
				}
				// A current sender must check the actual destination independently
				// of its own floor and any earlier placement decision.
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				if tc.marked {
					require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "coordinator-local")
				} else {
					var streamRequirement int64
					_, sendErr := encodeRemoteScopeWithProtocol(scope, c.proc, &streamRequirement)
					if local {
						require.Error(t, sendErr)
					} else {
						require.NoError(t, sendErr)
						if tc.key || tc.temporal || tc.second {
							require.Equal(t, tc.floor, streamRequirement)
						}
					}
				}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, floor)
				localErr := validateRemoteExpressionFeatures(c.proc, features)
				if floor < tc.floor {
					require.Error(t, localErr)
				} else {
					require.NoError(t, localErr)
				}
				decoded, receiveErr := decodeScope(wire, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				if local {
					require.Error(t, receiveErr)
				} else {
					require.NoError(t, receiveErr)
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestLoadIgnoreAndSetKeyCapabilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key, marked bool
		floor       int64
	}{
		{"load-only", false, false, defines.MORPCVersion125},
		{"load-and-key", true, false, defines.MORPCVersion131},
		{"load-and-marker", false, true, defines.MORPCVersion131},
		{"load-key-and-marker", true, true, defines.MORPCVersion131},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			load, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "_load_fk_ignore_check", []*planpb.Expr{plan2.GetColExpr(types.T_bool.ToType().PlanType(), 0, 0)})
			require.NoError(t, err)
			exprs := []*planpb.Expr{load}
			if tc.key {
				key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 1)})
				require.NoError(t, err)
				exprs = append(exprs, key)
			}
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: exprs, ScalarSetReaggregation: tc.marked}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, features.LoadFKIgnoreCheck)
			require.Equal(t, tc.key, features.SetOperationFloatKeys)
			require.Equal(t, tc.marked, features.ScalarSetReaggregation)
			require.Equal(t, tc.floor, remoteExpressionProtocolVersion(features))
			wire, err := p.Marshal()
			require.NoError(t, err)
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			owner := &planpb.Query{Nodes: []*planpb.Node{{NodeType: planpb.Node_PROJECT, ProjectList: exprs, ScalarSetReaggregation: tc.marked}}, Steps: []int32{0}}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			for _, floor := range []int64{0, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				client.version = floor
				client.customResponse = floor == 0
				client.response = &query.Response{}
				if tc.marked {
					require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "coordinator-local")
				} else {
					c.execType, c.cnList = plan2.ExecTypeAP_MULTICN, engine.Nodes{scope.NodeInfo}
					require.NoError(t, c.constrainRemoteExpressionWorkers(owner))
					if floor < tc.floor {
						require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					} else {
						require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					}
					var required int64
					data, sendErr := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
					if floor < tc.floor {
						require.Error(t, sendErr)
						require.Empty(t, data)
					} else {
						require.NoError(t, sendErr)
						require.Equal(t, tc.floor, required)
					}
				}
				if floor == 0 {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				} else {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, floor)
				}
				localErr := validateRemoteExpressionFeatures(c.proc, features)
				if floor < tc.floor {
					require.Error(t, localErr)
				} else {
					require.NoError(t, localErr)
				}
				decoded, receiveErr := decodeScope(wire, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				if floor < tc.floor || tc.marked {
					require.Error(t, receiveErr)
				} else {
					require.NoError(t, receiveErr)
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestViewMetadataAndSetKeyCapabilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key, marked bool
		floor       int64
	}{
		{"view-only", false, false, defines.MORPCVersion126},
		{"view-and-key", true, false, defines.MORPCVersion131},
		{"view-and-marker", false, true, defines.MORPCVersion131},
		{"view-key-and-marker", true, true, defines.MORPCVersion131},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			load, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "mo_view_definition", []*planpb.Expr{plan2.GetColExpr(types.T_varchar.ToType().PlanType(), 0, 0)})
			require.NoError(t, err)
			exprs := []*planpb.Expr{load}
			if tc.key {
				key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 1)})
				require.NoError(t, err)
				exprs = append(exprs, key)
			}
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: exprs, ScalarSetReaggregation: tc.marked}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, features.ViewMetadataFunctions)
			require.False(t, features.LoadFKIgnoreCheck)
			require.Equal(t, tc.key, features.SetOperationFloatKeys)
			require.Equal(t, tc.marked, features.ScalarSetReaggregation)
			require.Equal(t, tc.floor, remoteExpressionProtocolVersion(features))
			wire, err := p.Marshal()
			require.NoError(t, err)
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			owner := &planpb.Query{Nodes: []*planpb.Node{{NodeType: planpb.Node_PROJECT, ProjectList: exprs, ScalarSetReaggregation: tc.marked}}, Steps: []int32{0}}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			for _, floor := range []int64{0, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				client.version = floor
				client.customResponse = floor == 0
				client.response = &query.Response{}
				if tc.marked {
					require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "coordinator-local")
				} else {
					c.execType, c.cnList = plan2.ExecTypeAP_MULTICN, engine.Nodes{scope.NodeInfo}
					require.NoError(t, c.constrainRemoteExpressionWorkers(owner))
					if floor < tc.floor {
						require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					} else {
						require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					}
					var required int64
					data, sendErr := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
					if floor < tc.floor {
						require.Error(t, sendErr)
						require.Empty(t, data)
					} else {
						require.NoError(t, sendErr)
						require.Equal(t, tc.floor, required)
					}
				}
				if floor == 0 {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
				} else {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, floor)
				}
				localErr := validateRemoteExpressionFeatures(c.proc, features)
				if floor < tc.floor {
					require.Error(t, localErr)
				} else {
					require.NoError(t, localErr)
				}
				decoded, receiveErr := decodeScope(wire, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				if floor < tc.floor || tc.marked {
					require.Error(t, receiveErr)
				} else {
					require.NoError(t, receiveErr)
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}
