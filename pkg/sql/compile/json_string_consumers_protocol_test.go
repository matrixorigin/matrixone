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
	"testing"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func bindJSONStringConsumerExpr(t *testing.T, name string) *planpb.Expr {
	t.Helper()
	jsonColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	stringConstant := func(value string) *planpb.Expr {
		return plan2.MakePlan2StringConstExprWithType(value)
	}

	if name == "binary_cast" {
		target := planpb.Type{Id: int32(types.T_json)}
		expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_blob)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: target, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		})
		require.NoError(t, err)
		return expr
	}
	var args []*planpb.Expr
	switch name {
	case "json_row":
		args = []*planpb.Expr{jsonColumn}
	case "json_schema_valid", "json_schema_validation_report":
		args = []*planpb.Expr{stringConstant(`{"type":"string","maxLength":95}`), jsonColumn}
	case "concat":
		args = []*planpb.Expr{jsonColumn, stringConstant("!")}
	case "concat_ws":
		args = []*planpb.Expr{stringConstant("|"), jsonColumn, stringConstant("!")}
	case "elt":
		args = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1), jsonColumn, stringConstant("!")}
	default:
		t.Fatalf("unsupported JSON string consumer %q", name)
	}

	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, args)
	require.NoError(t, err)
	_, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
	wantOverload := int32(0)
	if name == "elt" {
		wantOverload = 1
	}
	if name == "concat" || name == "concat_ws" || name == "elt" {
		require.Equal(t, wantOverload, overloadID)
	}
	return expr
}

func jsonStringConsumerCreateTablePlan(expr *planpb.Expr, owner string) *planpb.Plan {
	tableDef := &planpb.TableDef{}
	switch owner {
	case "check":
		tableDef.Checks = []*planpb.CheckDef{{Check: expr}}
	case "default":
		tableDef.Cols = []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}
	case "generated":
		tableDef.Cols = []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}
	case "on_update":
		tableDef.Cols = []*planpb.ColDef{{OnUpdate: &planpb.OnUpdate{Expr: expr}}}
	}
	return &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{TableDef: tableDef}},
	}}}
}

func TestJSONStringConsumerProtocolPlanAdmissionAndCachedRun(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
		}
	})

	for _, name := range []string{"elt", "concat", "concat_ws", "binary_cast"} {
		t.Run(name, func(t *testing.T) {
			expr := bindJSONStringConsumerExpr(t, name)
			for _, owner := range []string{"check", "default", "generated", "on_update"} {
				t.Run(owner, func(t *testing.T) {
					queryPlan := jsonStringConsumerCreateTablePlan(expr, owner)
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
					require.NoError(t, validateJSONStringConsumerProtocol(proc, queryPlan))

					for _, version := range []int64{112, 113, 114, 115, 116, 117, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
						rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
						c := &Compile{proc: proc, pn: queryPlan}
						require.ErrorContains(t,
							c.Compile(context.Background(), queryPlan, nil),
							"protocol version 129")
						_, err := c.Run(0)
						require.ErrorContains(t, err, "protocol version 129")
					}
				})
			}
		})
	}
}

// This component test simulates reported worker and receiver versions. It
// checks placement and serialized-scope admission, without starting an old
// binary or executing a remote pipeline.
func TestJSONStringConsumerSimulatedVersionSerializationFences(t *testing.T) {
	for _, name := range []string{"elt", "concat", "concat_ws", "binary_cast"} {
		t.Run(name, func(t *testing.T) { testJSONStringConsumerSerializationFences(t, name) })
	}
}

func testJSONStringConsumerSerializationFences(t *testing.T, name string) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	expr := bindJSONStringConsumerExpr(t, name)
	qry := &planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}},
		Steps: []int32{0},
	}
	project := projection.NewArgument()
	defer project.Release()
	project.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   project,
	}
	workers := engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}

	// The released v122-v128 prefix forces one-CN placement and prevents
	// encoding a pipeline carrying overload 1 for those destinations.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	for _, version := range []int64{defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = workers
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		_, err := encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "remote destination")
	}

	// A simulated v129 worker response retains multi-CN placement and permits
	// encoding. This does not prove remote pipeline execution.
	client.version = defines.MORPCVersion129
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = workers
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion129, minimum, "the execution stream must confirm the selected JSON overload")
	require.NotEmpty(t, data)

	// Released receivers reject the serialized overload; a simulated
	// v129 receiver accepts decoding. Neither case executes the decoded scope.
	for _, version := range []int64{defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		_, err = decodeScope(data, c.proc, true, nil)
		require.ErrorContains(t, err, "protocol version 129")
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Positive(t, client.calls)
	require.Equal(t, client.calls, client.releases)
}

func TestJSONStringConsumerActualOwnerProtocolFloors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		floor    int64
		overload int32
	}{
		{"concat", defines.MORPCVersion129, 0},
		{"concat_ws", defines.MORPCVersion129, 0},
		{"elt", defines.MORPCVersion129, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := bindJSONStringConsumerExpr(t, tc.name)
			require.Equal(t, tc.overload, int32(expr.GetF().GetFunc().GetObj()))
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.Equal(t, tc.name == "elt", features.JSONStringConsumerOverload)
			require.Equal(t, tc.floor, remoteExpressionProtocolVersion(features))
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, tc.floor, floor)
		})
	}
}

// MEDIUMINT semantics retain their actual main110 owner independently of JSON ELT129.
// This component test validates two independently derived requirements and
// both expression orders; it does not run an old executable.
func TestJSONStringConsumerAndActualMediumIntFloors(t *testing.T) {
	elt := bindJSONStringConsumerExpr(t, "elt")
	medium := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int32), Width: 24}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0)},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}, {Typ: planpb.Type{Id: int32(types.T_int32), Width: 24}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}},
	}}}
	// The ordinary cast is not an assignment fence. Use the inherited strict
	// assignment identity with the explicit width24 target.
	medium.GetF().Func.Obj = function.EncodeOverloadID(function.CAST_STRICT, 0)
	for _, owner := range [][]*planpb.Expr{{elt, medium}, {medium, elt}} {
		features, err := planpb.RequiredRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		require.True(t, features.JSONStringConsumerOverload)
		require.True(t, features.MediumIntAssignmentBounds)
		require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion129, floor)
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(medium)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion110, remoteExpressionProtocolVersion(features))
}

// IndexSearchScan is actual main111 code. Its structured expression fields
// share the existing expression walker; this test covers admission, not an
// executed vector/fulltext search or a real mixed-binary topology.
func TestJSONStringConsumerIndexSearchExpressionOwners(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
		}
	})
	expr := bindJSONStringConsumerExpr(t, "elt")
	for _, tc := range []struct {
		name string
		put  func(*planpb.IndexSearchScan)
	}{
		{"query_payload", func(s *planpb.IndexSearchScan) { s.QueryPayload = expr }},
		{"algorithm_expression", func(s *planpb.IndexSearchScan) { s.AlgoExprs = []*planpb.Expr{expr} }},
		{"candidate_limit", func(s *planpb.IndexSearchScan) { s.CandidateLimit = expr }},
		{"distance_bound", func(s *planpb.IndexSearchScan) { s.DistanceRange = &planpb.DistRange{LowerBound: expr} }},
		{"source_generated_expression", func(s *planpb.IndexSearchScan) {
			s.SourceTableDef = &planpb.TableDef{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}}
		}},
		{"hidden_table_default", func(s *planpb.IndexSearchScan) {
			s.HiddenTables = []*planpb.IndexHiddenTableRef{{Table: &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			search := &planpb.IndexSearchScan{}
			tc.put(search)
			owner := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Nodes: []*planpb.Node{{NodeType: planpb.Node_INDEX_SEARCH_SCAN, IndexSearchScan: search}}, Steps: []int32{0},
			}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.JSONStringConsumerOverload)
			require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion129, floor)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion122)
			require.ErrorContains(t, validateJSONStringConsumerProtocol(proc, owner), "protocol version 129")
			c := &Compile{proc: proc, pn: owner}
			_, err = c.Run(0)
			require.ErrorContains(t, err, "protocol version 129")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
			require.NoError(t, validateJSONStringConsumerProtocol(proc, owner))
		})
	}
	// The new operator's existence alone is not a JSON expression requirement.
	features, err := planpb.RequiredRemoteExpressionFeatures(&planpb.Node{
		NodeType: planpb.Node_INDEX_SEARCH_SCAN, IndexSearchScan: &planpb.IndexSearchScan{},
	})
	require.NoError(t, err)
	require.False(t, features.JSONStringConsumerOverload)
	// Main's operator gate stays111; it must not be raised with the ELT family.
	require.Equal(t, defines.MORPCVersion111, vectorscan.IndexSearchScanProtocolVersion)
}

// Main protocol112 implements signed date results, but not ELT overload1.
// The two independently executable contracts must never share admission.
func TestJSONStringConsumerAfterMainDateProtocol(t *testing.T) {
	dateOnly := planpb.RemoteExpressionFeatures{DateNumericResultContracts: true}
	require.Equal(t, defines.MORPCVersion112, remoteExpressionProtocolVersion(dateOnly))
	elt := planpb.RemoteExpressionFeatures{JSONStringConsumerOverload: true}
	require.Greater(t, remoteExpressionProtocolVersion(elt), defines.MORPCVersion112)
	both := planpb.RemoteExpressionFeatures{JSONStringConsumerOverload: true, DateNumericResultContracts: true}
	require.Equal(t, remoteExpressionProtocolVersion(elt), remoteExpressionProtocolVersion(both))
}

// Main's published protocol identities retain their independent contracts;
// none admits the JSON execution semantics added after protocol 128.
func TestJSONStringConsumerAfterPublishedMainProtocols(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, tc := range []struct {
		version  int64
		features planpb.RemoteExpressionFeatures
	}{
		{defines.MORPCVersion113, planpb.RemoteExpressionFeatures{DecimalStatisticalResultContracts: true}},
		{defines.MORPCVersion115, planpb.RemoteExpressionFeatures{CalendarIntervalNormalization: true}},
		{defines.MORPCVersion117, planpb.RemoteExpressionFeatures{LeastGreatestCollationSemantics: true}},
		{defines.MORPCVersion118, planpb.RemoteExpressionFeatures{TableCollationFunction: true}},
		{defines.MORPCVersion121, planpb.RemoteExpressionFeatures{DecimalIntegerDivisionSemantics: true}},
		{defines.MORPCVersion122, planpb.RemoteExpressionFeatures{TimestampDatetimeResultContracts: true}},
		{defines.MORPCVersion124, planpb.RemoteExpressionFeatures{TimestampAddSecondIntervals: true}},
		{defines.MORPCVersion125, planpb.RemoteExpressionFeatures{LoadFKIgnoreCheck: true}},
		{defines.MORPCVersion128, planpb.RemoteExpressionFeatures{JSONNumericComparisons: true}},
	} {
		require.Equal(t, tc.version, remoteExpressionProtocolVersion(tc.features))
		tc.features.JSONStringConsumerOverload = true
		require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(tc.features))
	}
	for _, name := range []string{"elt", "concat", "concat_ws", "binary_cast"} {
		expr := bindJSONStringConsumerExpr(t, name)
		for _, version := range []int64{defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			require.ErrorContains(t, validateJSONStringConsumerProtocol(c.proc, expr), "protocol version 129")
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
		require.NoError(t, validateJSONStringConsumerProtocol(c.proc, expr))
	}
}

func TestJSONStringConsumerAndLeastGreatestFloors(t *testing.T) {
	elt := bindJSONStringConsumerExpr(t, "elt")
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	typ.CollationVersion = 1
	for _, name := range []string{"least", "greatest"} {
		t.Run(name, func(t *testing.T) {
			collated, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
			})
			require.NoError(t, err)
			for _, expressions := range [][]*planpb.Expr{{elt, collated}, {collated, elt}} {
				query := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: expressions}}, Steps: []int32{0}}
				data, err := query.Marshal()
				require.NoError(t, err)
				decoded := new(planpb.Query)
				require.NoError(t, decoded.Unmarshal(data))
				features, err := planpb.RequiredRemoteExpressionFeatures(decoded)
				require.NoError(t, err)
				require.True(t, features.JSONStringConsumerOverload)
				require.True(t, features.LeastGreatestCollationSemantics)
				require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
				floor, err := plan2.RequiredPersistedExpressionProtocolVersion(decoded)
				require.NoError(t, err)
				require.Equal(t, defines.MORPCVersion129, floor)
			}
			features, err := planpb.RequiredRemoteExpressionFeatures(collated)
			require.NoError(t, err)
			require.False(t, features.JSONStringConsumerOverload)
			require.Equal(t, defines.MORPCVersion117, remoteExpressionProtocolVersion(features))
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(collated)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion117, floor)
		})
	}
}

// Existing stored opaque values need no new CAST/ELT producer in the plan.
func TestJSONOpaqueExistingConsumerFences(t *testing.T) {
	for _, name := range []string{"json_row", "json_schema_valid", "json_schema_validation_report"} {
		t.Run(name, func(t *testing.T) {
			expr := bindJSONStringConsumerExpr(t, name)
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.JSONOpaqueTextContracts)
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion129, floor)
			testJSONStringConsumerSerializationFences(t, name)
		})
	}
}

func TestLocalSourceFenceFastScanRetainsCompleteMixedFeaturePriority(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	trigger := bindJSONStringConsumerExpr(t, "concat")
	invalidDecimal := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(180) << 32},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}},
	}}}
	week := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: int64(216) << 32},
	}}}
	for _, extra := range []*planpb.Expr{invalidDecimal, week} {
		owner := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{trigger, extra}}}}
		features, err := planpb.RequiredRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		require.True(t, features.RequiresLocalSourceFence())
		expected := validateRemoteExpressionFeatures(proc, features)
		require.Error(t, expected)
		actual := validateJSONStringConsumerProtocol(proc, owner)
		require.IsType(t, expected, actual)
		require.EqualError(t, actual, expected.Error())
	}
}
