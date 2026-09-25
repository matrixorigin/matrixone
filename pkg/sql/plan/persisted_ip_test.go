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

package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	planrule "github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedDecimalLiteralUsesDedicatedEpochInMixedOwner(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	decimalExpr, err := makePlan2DecimalExprWithType(proc.Ctx,
		"12345678901234567890123456789012345678.1")
	require.NoError(t, err)
	spatialExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			Obj: function.EncodeOverloadID(function.ST_DISTANCE, 4),
		}}},
	}
	temporalExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.EXTRACT, 0)},
	}}}
	intervalExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL_MICROSECOND, 0)},
	}}}
	weekExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint8)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.WEEK, 0)},
	}}}
	for _, tc := range []struct {
		name  string
		exprs []*planpb.Expr
		want  int64
	}{
		{"decimal only", []*planpb.Expr{decimalExpr}, defines.MORPCVersion89},
		{"spatial only", []*planpb.Expr{spatialExpr}, defines.MORPCVersion90},
		{"decimal then spatial", []*planpb.Expr{decimalExpr, spatialExpr}, defines.MORPCVersion90},
		{"spatial then decimal", []*planpb.Expr{spatialExpr, decimalExpr}, defines.MORPCVersion90},
		{"temporal result", []*planpb.Expr{temporalExpr}, defines.MORPCVersion97},
		{"temporal then spatial", []*planpb.Expr{temporalExpr, spatialExpr}, defines.MORPCVersion97},
		{"normalized interval", []*planpb.Expr{intervalExpr}, defines.MORPCVersion98},
		{"week session default", []*planpb.Expr{weekExpr}, defines.MORPCVersion98},
		{"temporal then interval", []*planpb.Expr{temporalExpr, intervalExpr}, defines.MORPCVersion98},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &planpb.TableDef{}
			for _, expr := range tc.exprs {
				owner.Cols = append(owner.Cols, &planpb.ColDef{Default: &planpb.Default{Expr: expr}})
			}
			required, err := RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, tc.want, required, "mixed contracts take the maximum independently of order")
		})
	}
	legacyTemporal := DeepCopyExpr(temporalExpr)
	legacyTemporal.Typ.Id = int32(types.T_varchar)
	_, err = RequiredPersistedExpressionProtocolVersion(legacyTemporal)
	require.ErrorContains(t, err, "legacy temporal")
}

func TestPersistedDecimalDivisionRequiresV97(t *testing.T) {
	division := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 16, Scale: 6},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			Obj: function.EncodeOverloadID(function.DIV, 0),
		}}}}
	owner := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: division}}}}
	required, err := RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion97, required)
}

func TestPersistedDecimalDivisionViewAdmissionBeforeFold(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	const createSQL = "create view v_decimal_division as select 1.00 / 3.00 as quotient"
	build := func(floor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
	}
	_, err := build(defines.MORPCVersion96)
	require.ErrorContains(t, err, "protocol version 97")
	created, err := build(defines.MORPCVersion97)
	require.NoError(t, err)
	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &viewData))
	require.NotNil(t, viewData.RequiredProtocolVersion)
	require.Equal(t, defines.MORPCVersion97, *viewData.RequiredProtocolVersion)
}

func TestPersistedDecimalLiteralProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldFloor)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, value)
		}
	})

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select 12345678901234567890123456789012345678.1", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, nil, nil)
	expr, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.DecimalLiteralSemantics, expr.String())

	for _, version := range []int64{
		defines.MORPCVersion81,
		defines.MORPCVersion83,
		defines.MORPCVersion84,
		defines.MORPCVersion85,
		defines.MORPCVersion86,
		defines.MORPCVersion87,
		defines.MORPCVersion88,
		defines.MORPCVersion89,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, version)
		err = RequirePersistedExpressionProtocol(proc.Ctx, proc, expr)
		if version < defines.MORPCVersion89 {
			require.ErrorContains(t, err, "protocol version 89")
		} else {
			require.NoError(t, err)
		}
	}

	plain := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_Lit{
		Lit: &planpb.Literal{Value: &planpb.Literal_Decimal64Val{
			Decimal64Val: &planpb.Decimal64{A: 125},
		}},
	}}
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, plain))
}

func TestPersistedDecimalLiteralTargetTypedDefaultAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldFloor)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, value)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(
				moruntime.PersistedExpressionProtocolAuthoringFloor, value)
		}
	})

	// This spelling is numerically just 1.25, but a target-typed default is
	// bound through the destination DECIMAL type rather than the untyped path.
	// The provenance marker must still fence the persisted default from pre-v89
	// readers and writers.
	source := strings.Repeat("0", 100) + "1.25"
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t(a decimal(10,2) default ("+source+"))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	typ := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	for _, floor := range []int64{defines.MORPCVersion81, defines.MORPCVersion83} {
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, floor)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		_, err = buildDefaultExpr(proc.Ctx, col, typ, proc)
		require.ErrorContains(t, err, "protocol version 89")
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion89))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion89))
	defaultExpr, err := buildDefaultExpr(proc.Ctx, col, typ, proc)
	require.NoError(t, err)
	require.NotNil(t, defaultExpr)
	require.Equal(t, int64(defines.MORPCVersion89), func() int64 {
		version, versionErr := RequiredPersistedExpressionProtocolVersion(defaultExpr)
		require.NoError(t, versionErr)
		return version
	}())

	plain := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_Lit{
		Lit: &planpb.Literal{Value: &planpb.Literal_Decimal64Val{
			Decimal64Val: &planpb.Decimal64{A: 125},
		}},
	}}
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, plain))
	// A temporal/non-DECIMAL target keeps the original spelling for the cast,
	// so it does not depend on the exact DECIMAL literal carrier introduced by
	// v89 and remains admissible at the v81 floor.
	timeSource := "0.001"
	timeStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t_time(a time(3) default ("+timeSource+"))", 1)
	require.NoError(t, err)
	defer timeStmt.Free()
	timeCol := timeStmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	timeTyp := planpb.Type{Id: int32(types.T_time), Scale: 3}
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion81))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion81))
	timeDefault, err := buildDefaultExpr(proc.Ctx, timeCol, timeTyp, proc)
	require.NoError(t, err)
	require.NotNil(t, timeDefault)
	timeVersion, err := RequiredPersistedExpressionProtocolVersion(timeDefault)
	require.NoError(t, err)
	require.Zero(t, timeVersion)
}

func TestPersistedDecimalLiteralMarkerSurvivesDeepCopyAndListFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	wide, err := makePlan2DecimalExprWithType(proc.Ctx,
		"000000000000000000000000000000000000000001.25")
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(wide)
	require.NoError(t, err)
	require.True(t, features.DecimalLiteralSemantics)
	ordinary, err := makePlan2DecimalExprWithType(proc.Ctx, "1.25")
	require.NoError(t, err)
	ordinaryFeatures, err := planpb.RequiredRemoteExpressionFeatures(ordinary)
	require.NoError(t, err)
	require.False(t, ordinaryFeatures.DecimalLiteralSemantics)
	ordinaryVersion, err := RequiredPersistedExpressionProtocolVersion(ordinary)
	require.NoError(t, err)
	require.Zero(t, ordinaryVersion)

	copied := DeepCopyExpr(wide)
	require.True(t, copied.GetF().GetArgs()[0].GetLit().GetDecimalLiteralRequiresV82())
	copiedFeatures, err := planpb.RequiredRemoteExpressionFeatures(copied)
	require.NoError(t, err)
	require.True(t, copiedFeatures.DecimalLiteralSemantics)
	list := &planpb.Expr{
		Typ: wide.Typ,
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			wide,
			ordinary,
		}}},
	}
	foldedByAPI, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(list), proc, false, true)
	require.NoError(t, err)
	node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(list)}}
	planrule.NewConstantFold(false).Apply(node, nil, proc)
	for name, folded := range map[string]*planpb.Expr{
		"public-api": foldedByAPI,
		"rule":       node.ProjectList[0],
	} {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, folded.GetVec(), "decimal provenance should not disable LiteralVec folding")
			require.True(t, folded.GetVec().GetDecimalLiteralRequiresV82())
			foldedFeatures, featureErr := planpb.RequiredRemoteExpressionFeatures(folded)
			require.NoError(t, featureErr)
			require.True(t, foldedFeatures.DecimalLiteralSemantics)
			require.Equal(t, int64(defines.MORPCVersion89), func() int64 {
				version, versionErr := RequiredPersistedExpressionProtocolVersion(folded)
				require.NoError(t, versionErr)
				return version
			}())
			copiedVec := DeepCopyExpr(folded)
			require.True(t, copiedVec.GetVec().GetDecimalLiteralRequiresV82())
			payload, marshalErr := proto.Marshal(folded)
			require.NoError(t, marshalErr)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.True(t, decoded.GetVec().GetDecimalLiteralRequiresV82())
		})
	}
	legacyExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL, 0)},
	}}}
	_, err = RequiredPersistedExpressionProtocolVersion(legacyExpr)
	require.ErrorContains(t, err, "legacy interval")
}

func TestPersistedDecimalLiteralMarkerSurvivesConstantFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	wide, err := makePlan2DecimalExprWithType(proc.Ctx,
		"12345678901234567890123456789012345678.1")
	require.NoError(t, err)
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "+", []*planpb.Expr{
		wide, makePlan2Int64ConstExprWithType(1),
	})
	require.NoError(t, err)
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(folded)
	require.NoError(t, err)
	require.True(t, features.DecimalLiteralSemantics, folded.String())
}

func TestPersistedIPFunctionProtocolAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	statements := []string{
		"create table t(a bigint, b varchar(32) default (inet_ntoa(a)))",
		"create table t(a bigint, b varchar(32) generated always as (inet_ntoa(a)) stored)",
		"create table t(a bigint, check (inet_ntoa(a) <> ''))",
	}
	for _, version := range []int64{
		defines.MORPCVersion70,
		defines.MORPCVersion71,
		defines.MORPCVersion72,
		defines.MORPCVersion84,
		defines.MORPCVersion85,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		for _, sql := range statements {
			t.Run(fmt.Sprintf("%s/v%d", sql, version), func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				_, err = BuildPlan(ctx, stmt, false)
				if version < defines.MORPCVersion72 {
					require.ErrorContains(t, err, "protocol version 72")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestPersistedIPFunctionProtocolAdmissionAcrossOwners(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select inet_ntoa(a)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{{Id: int32(types.T_int64), Width: 64}})
	expr, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)

	for _, version := range []int64{
		defines.MORPCVersion70,
		defines.MORPCVersion71,
		defines.MORPCVersion72,
		defines.MORPCVersion84,
		defines.MORPCVersion85,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version < defines.MORPCVersion72 {
			require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr), "protocol version 72")
		} else {
			require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr))
		}
	}

	plain := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, plain))
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
	require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, table), "protocol version 72")
}

func TestPersistedProtocolVersionAdmissionRejectsFutureReader(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCLatestVersion))
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCLatestVersion+1), "protocol version")

	// A lower local runtime cannot bind an expression whose marker was already
	// persisted by a newer writer, even when the marker is not IP-specific.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion71)
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")
}

func TestPersistedProtocolVersionAdmissionHonorsCommittedFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldFloor)
		} else {
			if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
				rt.CompareAndDeleteGlobalVariables(
					moruntime.PersistedExpressionProtocolFloor, value)
			}
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(0))
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion72))
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72))
}

func TestPersistedProtocolVersionAdmissionSeparatesReadAndAuthoringFloors(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	// Phase one has committed the durable read floor but has not completed the
	// routing/catalog barrier. Existing marked views can be rebound; new
	// protocol-bearing catalog metadata must still be rejected.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion72))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72))
	require.ErrorContains(t, RequirePersistedProtocolVersionForAuthoring(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")

	// After the enabled/admitted/catalog-fenced snapshot, the write gate opens.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor,
		int64(defines.MORPCVersion72))
	require.NoError(t, RequirePersistedProtocolVersionForAuthoring(
		proc.Ctx, proc, defines.MORPCVersion72))
}

func TestPersistedIPFunctionProtocolAdmissionForCatalogBuilders(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	parseColumn := func(t *testing.T, sql string, index int) *tree.ColumnTableDef {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		return stmt.(*tree.CreateTable).Defs[index].(*tree.ColumnTableDef)
	}
	defaultCol := parseColumn(t, "create table t(a bigint, b varchar(32) default (inet_ntoa(a)))", 1)
	numericDefaultCol := parseColumn(t, "create table t(a bigint, b varchar(32) default (inet_ntoa(1.6)))", 1)
	onUpdateCol := parseColumn(t, "create table t(a bigint, b varchar(32))", 1)
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select inet_ntoa(1)", 1)
	require.NoError(t, err)
	t.Cleanup(selectStmt.Free)
	updateExpr := selectStmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	onUpdateCol.Attributes = append(onUpdateCol.Attributes, tree.NewAttributeOnUpdate(updateExpr))
	generatedCol := parseColumn(t,
		"create table t(a bigint, b varchar(32) generated always as (inet_ntoa(a)) stored)", 1)
	columns := []*ColDef{{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64}}}

	for _, version := range []int64{
		defines.MORPCVersion70,
		defines.MORPCVersion71,
		defines.MORPCVersion72,
		defines.MORPCVersion84,
		defines.MORPCVersion85,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			_, err := buildDefaultExprWithColumns(proc.Ctx, defaultCol,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, proc, columns)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)

			_, err = buildDefaultExprWithColumns(proc.Ctx, numericDefaultCol,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, proc, columns)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)

			_, err = buildOnUpdate(proc.Ctx, onUpdateCol, planpb.Type{Id: int32(types.T_varchar), Width: 32}, proc)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)

			_, err = buildGeneratedExpr(proc.Ctx, generatedCol,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, columns, proc)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)

			_, err = buildCTASDefaultFromOrigin(ctx,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, true,
				"inet_ntoa(1)", columns...)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)

			_, err = buildCTASDefaultFromOrigin(ctx,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, true,
				"inet_ntoa(1.6)", columns...)
			checkAdmissionResult(t, version, err, defines.MORPCVersion72)
		})
	}
}

func TestPersistedStringNumericResultProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL, "select strcmp('a', 'b')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, nil, nil)
	expr, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.StringNumericResultContracts)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion74)
	require.ErrorContains(t,
		RequirePersistedExpressionProtocol(proc.Ctx, proc, expr), "protocol version 80")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, expr))
	require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr),
		"legacy catalog-builder wrapper must use the shared maximum-version gate")
}

func TestPersistedDynamicIPViewProtocolSurvivesConstantFolding(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_dynamic_ip as select inet_ntoa('1.6') as ip"
	build := func(authoringFloor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
	}

	_, err := build(defines.MORPCVersion85)
	require.ErrorContains(t, err, "protocol version 86")

	created, err := build(defines.MORPCVersion86)
	require.NoError(t, err)
	var viewData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &viewData))
	require.NotNil(t, viewData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *viewData.RequiredProtocolVersion)
}

func TestPersistedViewProtocolAdmissionCapturesBindTimeBetweenFold(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_dynamic_ip_between as select 1 as x where '0.0.0.1' between inet_ntoa('1.6') and '0.0.0.1'"
	build := func(authoringFloor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
	}

	_, err := build(defines.MORPCVersion85)
	require.ErrorContains(t, err, "protocol version 86")

	created, err := build(defines.MORPCVersion86)
	require.NoError(t, err)
	var viewData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &viewData))
	require.NotNil(t, viewData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *viewData.RequiredProtocolVersion)
}

func TestPersistedMixedTemporalViewProtocolAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_mixed_temporal as select if(1 = 1, cast('2024-01-02 12:34:56.123456' as timestamp(6)), cast('2024-01-02 12:34:56.123' as datetime(3))) as value"
	build := func(authoringFloor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
	}

	_, err := build(defines.MORPCVersion85)
	require.ErrorContains(t, err, "protocol version 86")

	created, err := build(defines.MORPCVersion86)
	require.NoError(t, err)
	var viewData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &viewData))
	require.NotNil(t, viewData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *viewData.RequiredProtocolVersion)
}

func TestPersistedBoundedConditionalStringProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	binaryType := types.New(types.T_binary, 4, 0)
	varbinaryType := types.New(types.T_varbinary, 12, 0)
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "coalesce", []*planpb.Expr{
		{Typ: makePlan2Type(&binaryType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: makePlan2Type(&varbinaryType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	functionID, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
	require.Equal(t, int32(function.COALESCE), functionID)
	require.Equal(t, int32(31), overloadID)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.BoundedConditionalStringDomains)

	required, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion83), required)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	require.ErrorContains(t,
		RequirePersistedExpressionProtocol(proc.Ctx, proc, expr), "protocol version 83")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion83)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, expr))

}

func TestPersistedExpressionProtocolAdmissionForSpatialDistance(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		proc.Free()
	})

	spatialExpr := func(functionID, overloadID int32) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_float64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
				Obj: int64(functionID)<<32 | int64(overloadID),
			}}},
		}
	}
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "frechet geodetic", expr: spatialExpr(506, 4)},
		{name: "hausdorff geodetic", expr: spatialExpr(507, 4)},
		{name: "distance unit", expr: spatialExpr(421, 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, version := range []int64{defines.MORPCVersion89, defines.MORPCVersion90} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				err := RequirePersistedExpressionProtocol(proc.Ctx, proc, tc.expr)
				if version < defines.MORPCVersion90 {
					require.ErrorContains(t, err, "protocol version 90")
				} else {
					require.NoError(t, err)
				}
			}
		})
	}

	// Mixed owners use the strongest admission requirement instead of allowing
	// a v72 IP expression to mask a v90 spatial-distance expression.
	mixed := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Default: &planpb.Default{Expr: spatialExpr(394, 0)}},
		{Default: &planpb.Default{Expr: spatialExpr(421, 4)}},
	}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion89)
	require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, mixed), "protocol version 90")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion90)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, mixed))
}

func TestSpatialDistanceRequirementSurvivesConstantFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, sql := range []string{
		"select st_frechetdistance(st_geomfromtext('LINESTRING(0 0, 1 0)', 4326), st_geomfromtext('LINESTRING(0 1, 1 1)', 4326))",
		"select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'kilometre')",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			expr, err := NewGeneratedColBinder(proc.Ctx, nil, nil).
				BindExpr(ast, 0, false)
			require.NoError(t, err)
			required, err := RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion90, required)

			folded, err := ConstantFold(
				batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), proc, false, true)
			require.NoError(t, err)
			require.NotNil(t, folded.GetF(),
				"spatial capability must remain visible after constant folding")
			required, err = RequiredPersistedExpressionProtocolVersion(folded)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion90, required)

			node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(expr)}}
			planrule.NewConstantFold(false).Apply(node, nil, proc)
			require.NotNil(t, node.ProjectList[0].GetF(),
				"optimizer constant folding must preserve spatial provenance")
			required, err = RequiredPersistedExpressionProtocolVersion(node.ProjectList[0])
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion90, required)
		})
	}
}

func TestPersistedFollowupExpressionProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	column := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
	}
	cases := []struct {
		name     string
		function string
		input    types.Type
		required int64
	}{
		{name: "native INET_NTOA remains v72", function: "inet_ntoa", input: types.T_int64.ToType(), required: defines.MORPCVersion72},
		{name: "dynamic INET_NTOA is v86", function: "inet_ntoa", input: types.T_varchar.ToType(), required: defines.MORPCVersion86},
		{name: "TO_BASE64 binary result is v86", function: "to_base64", input: types.NewWithCharset(types.T_varbinary, 8, 0, types.CharsetBinary), required: defines.MORPCVersion86},
		{name: "IP predicate INT32 result is v86", function: "is_ipv4", input: types.T_varchar.ToType(), required: defines.MORPCVersion86},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, test.function, []*planpb.Expr{column(test.input)})
			require.NoError(t, err)
			got, err := RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, test.required, got)

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.required-1)
			require.ErrorContains(t,
				RequirePersistedExpressionProtocol(proc.Ctx, proc, expr),
				fmt.Sprintf("protocol version %d", test.required))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.required)
			require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, expr))
		})
	}

	metadataCases := []struct {
		name     string
		expr     *planpb.Expr
		required int64
	}{
		{
			name:     "bounded character substring is v86",
			required: defines.MORPCVersion86,
			expr: func() *planpb.Expr {
				return mustBindPersistedFollowupExpr(t, proc.Ctx, "substring", []*planpb.Expr{
					column(types.New(types.T_varchar, 64, 0)),
					makePlan2Int64ConstExprWithType(1),
					makePlan2Int64ConstExprWithType(7),
				})
			}(),
		},
		{
			name:     "fractional temporal coalesce is v86",
			required: defines.MORPCVersion86,
			expr: func() *planpb.Expr {
				return mustBindPersistedFollowupExpr(t, proc.Ctx, "coalesce", []*planpb.Expr{
					column(types.T_time.ToTypeWithScale(0)),
					column(types.T_time.ToTypeWithScale(6)),
				})
			}(),
		},
		{
			name:     "mixed temporal if is v86",
			required: defines.MORPCVersion86,
			expr: func() *planpb.Expr {
				return mustBindPersistedFollowupExpr(t, proc.Ctx, "if", []*planpb.Expr{
					column(types.T_bool.ToType()),
					column(types.T_timestamp.ToTypeWithScale(6)),
					column(types.T_datetime.ToTypeWithScale(3)),
				})
			}(),
		},
		{
			name:     "mixed temporal case without else is v86",
			required: defines.MORPCVersion86,
			expr: func() *planpb.Expr {
				return mustBindPersistedFollowupExpr(t, proc.Ctx, "case", []*planpb.Expr{
					column(types.T_bool.ToType()),
					column(types.T_timestamp.ToTypeWithScale(6)),
					column(types.T_bool.ToType()),
					column(types.T_datetime.ToTypeWithScale(3)),
				})
			}(),
		},
	}
	for _, test := range metadataCases {
		t.Run(test.name, func(t *testing.T) {
			got, err := RequiredPersistedExpressionProtocolVersion(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.required, got)
			if test.required == 0 {
				require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, test.expr))
				return
			}

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion85)
			require.ErrorContains(t,
				RequirePersistedExpressionProtocol(proc.Ctx, proc, test.expr), "protocol version 86")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
			require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, test.expr))
		})
	}
}

func TestInetNtoaLiteralDomains(t *testing.T) {
	for _, test := range []struct {
		name       string
		sql        string
		argType    types.T
		overloadID int32
	}{
		{name: "hex literal is numeric", sql: "select inet_ntoa(0x0102)", argType: types.T_uint64, overloadID: 0},
		{name: "X hex literal is numeric", sql: "select inet_ntoa(X'31')", argType: types.T_uint64, overloadID: 0},
		{name: "bit literal is numeric", sql: "select inet_ntoa(b'100000010')", argType: types.T_uint64, overloadID: 0},
		{name: "binary string keeps text prefix", sql: "select inet_ntoa(_binary '1.6')", argType: types.T_varbinary, overloadID: 20},
		{name: "binary hex keeps text prefix", sql: "select inet_ntoa(_binary X'31')", argType: types.T_varbinary, overloadID: 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			proc := testutil.NewProcess(t)
			defer proc.Free()
			binder := NewGeneratedColBinder(proc.Ctx, nil, nil)
			expr, err := binder.BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(test.argType), expr.GetF().Args[0].Typ.Id, expr.String())
			functionID, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
			require.Equal(t, int32(function.INET_NTOA), functionID)
			require.Equal(t, test.overloadID, overloadID, expr.String())
		})
	}
}

func TestInetNtoaNestedLiteralDomains(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "concat keeps nested hex as string prefix", sql: "select inet_ntoa(concat(X'31'))"},
		{name: "cast keeps nested hex as string prefix", sql: "select inet_ntoa(cast(X'31' as char))"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			proc := testutil.NewProcess(t)
			defer proc.Free()
			binder := NewGeneratedColBinder(proc.Ctx, nil, nil)
			expr, err := binder.BindExpr(ast, 0, false)
			require.NoError(t, err)
			_, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
			require.GreaterOrEqual(t, overloadID, int32(9), expr.String())
			require.NotEqual(t, int32(types.T_uint64), expr.GetF().Args[0].Typ.Id, expr.String())
		})
	}
}

func mustBindPersistedFollowupExpr(t *testing.T, ctx context.Context, name string, args []*planpb.Expr) *planpb.Expr {
	t.Helper()
	expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
	require.NoError(t, err)
	return expr
}

func checkAdmissionResult(t *testing.T, version int64, err error, required int64) {
	t.Helper()
	if version < required {
		require.ErrorContains(t, err, fmt.Sprintf("protocol version %d", required))
	} else {
		require.NoError(t, err)
	}
}
