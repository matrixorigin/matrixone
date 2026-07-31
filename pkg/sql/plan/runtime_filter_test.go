// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func makeRuntimeFilterTestEq(typ planpb.Type, leftTag, rightTag, leftPos, rightPos int32) *planpb.Expr {
	return makeRuntimeFilterTestEqTypes(typ, typ, leftTag, rightTag, leftPos, rightPos)
}

func makeRuntimeFilterTestEqTypes(
	leftType, rightType planpb.Type,
	leftTag, rightTag, leftPos, rightPos int32,
) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{
				GetColExpr(leftType, leftTag, leftPos),
				GetColExpr(rightType, rightTag, rightPos),
			},
		}},
	}
}

func makeMixedSideRuntimeFilterResidual(typ planpb.Type, leftTag, rightTag int32) *planpb.Expr {
	mixedArg := &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.PLUS), 0), "+"),
			Args: []*planpb.Expr{
				GetColExpr(typ, leftTag, 0),
				GetColExpr(typ, rightTag, 0),
			},
		}},
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{mixedArg, GetColExpr(typ, rightTag, 0)},
		}},
	}
}

func newRuntimeFilterSingleTestBuilder(rightSingle bool) *QueryBuilder {
	pkType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	return &QueryBuilder{
		compCtx: NewMockCompilerContext(true),
		qry: &planpb.Query{Nodes: []*planpb.Node{
			{
				NodeType:    planpb.Node_TABLE_SCAN,
				NodeId:      0,
				BindingTags: []int32{1},
				TableDef: &planpb.TableDef{
					Name:          "discardable_probe",
					Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
					Name2ColIndex: map[string]int32{"id": 0},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats: &planpb.Stats{Cost: 1_000_000, Outcnt: 1_000_000, TableCnt: 1_000_000, BlockNum: 123},
			},
			{
				NodeType:    planpb.Node_TABLE_SCAN,
				NodeId:      1,
				BindingTags: []int32{2},
				TableDef: &planpb.TableDef{
					Name:          "preserved_build",
					Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
					Name2ColIndex: map[string]int32{"id": 0},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats: &planpb.Stats{Cost: 3, Outcnt: 3, TableCnt: 3, BlockNum: 1, Selectivity: 1},
			},
			{
				NodeType:    planpb.Node_JOIN,
				NodeId:      2,
				Children:    []int32{0, 1},
				JoinType:    planpb.Node_SINGLE,
				IsRightJoin: rightSingle,
				OnList:      []*planpb.Expr{makeRuntimeFilterTestEq(pkType, 1, 2, 0, 0)},
				Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{
					HashmapSize: 3,
					HashOnPK:    true,
				}},
			},
		}},
	}
}

func configureRuntimeFilterCompositePK(builder *QueryBuilder) (*planpb.Node, *planpb.Node) {
	probe := builder.qry.Nodes[0]
	build := builder.qry.Nodes[1]
	pkType := probe.TableDef.Cols[0].Typ
	cpType := planpb.Type{Id: int32(types.T_varchar), NotNullable: true}
	probe.TableDef.Cols = []*planpb.ColDef{
		{Name: "a", Typ: pkType},
		{Name: "b", Typ: pkType},
		{Name: catalog.CPrimaryKeyColName, Typ: cpType},
	}
	probe.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1, catalog.CPrimaryKeyColName: 2}
	probe.TableDef.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: catalog.CPrimaryKeyColName,
		Names:       []string{"a", "b"},
	}
	build.TableDef.Cols = []*planpb.ColDef{{Name: "a", Typ: pkType}, {Name: "b", Typ: pkType}}
	build.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1}
	build.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	builder.qry.Nodes[2].OnList = []*planpb.Expr{
		makeRuntimeFilterTestEq(pkType, 1, 2, 0, 0),
		makeRuntimeFilterTestEq(pkType, 1, 2, 1, 1),
	}
	return probe, build
}

func TestRightSingleRuntimeFilterSemanticAndDeliveryContract(t *testing.T) {
	t.Run("right single filters only the discardable probe and is colocated", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probeStatsBefore := DeepCopyStats(builder.qry.Nodes[0].Stats)

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		join := builder.qry.Nodes[2]
		probe := builder.qry.Nodes[0]
		build := builder.qry.Nodes[1]
		require.Len(t, join.RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.Equal(t, join.RuntimeFilterBuildList[0].Tag, probe.RuntimeFilterProbeList[0].Tag)
		require.Empty(t, build.RuntimeFilterProbeList)
		require.True(t, probe.Stats.ForceOneCN)
		require.True(t, build.Stats.ForceOneCN)
		require.Equal(t, probeStatsBefore.Cost, probe.Stats.Cost)
		require.Equal(t, probeStatsBefore.Outcnt, probe.Stats.Outcnt)
		require.Equal(t, probeStatsBefore.BlockNum, probe.Stats.BlockNum)
	})

	t.Run("left single preserves unmatched probe rows", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(false)
		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		require.False(t, builder.qry.Nodes[0].Stats.ForceOneCN)
	})

	t.Run("residual condition remains on the join", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes[2].OnList = append(builder.qry.Nodes[2].OnList, MakeFalseExpr())

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[2].OnList, 2)
		require.True(t, IsFalseExpr(builder.qry.Nodes[2].OnList[1]))
	})

	t.Run("mixed-side equality remains residual and is not used as an RF key", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		pkType := builder.qry.Nodes[0].TableDef.Cols[0].Typ
		mixed := makeMixedSideRuntimeFilterResidual(pkType, 1, 2)
		builder.qry.Nodes[2].OnList = append(builder.qry.Nodes[2].OnList, mixed)

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[2].OnList, 2)
		require.Same(t, mixed, builder.qry.Nodes[2].OnList[1])
	})
}

func TestFloatRuntimeFilterUsesOnlySoundEncoding(t *testing.T) {
	tests := []struct {
		name     string
		typ      planpb.Type
		want     bool
		encoding planpb.RuntimeFilterKeyEncoding
	}{
		{
			name: "scaled FLOAT32 is omitted",
			typ:  planpb.Type{Id: int32(types.T_float32), Width: 5, Scale: 2},
		},
		{
			name:     "unscaled FLOAT32 closes signed zero",
			typ:      planpb.Type{Id: int32(types.T_float32)},
			want:     true,
			encoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		},
		{
			name:     "FLOAT64 closes signed zero",
			typ:      planpb.Type{Id: int32(types.T_float64)},
			want:     true,
			encoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(true)
			builder.qry.Nodes[0].TableDef.Cols[0].Typ = test.typ
			builder.qry.Nodes[1].TableDef.Cols[0].Typ = test.typ
			builder.qry.Nodes[2].OnList = []*planpb.Expr{
				makeRuntimeFilterTestEq(test.typ, 1, 2, 0, 0),
			}

			builder.generateRuntimeFilters(2)

			if !test.want {
				require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
				require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
				return
			}
			require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
			require.Len(t, builder.qry.Nodes[0].RuntimeFilterProbeList, 1)
			require.Equal(t, test.encoding,
				builder.qry.Nodes[2].RuntimeFilterBuildList[0].KeyEncoding)
			require.Equal(t, test.typ,
				*builder.qry.Nodes[2].RuntimeFilterBuildList[0].ProbeType)
		})
	}

	t.Run("different decimal scales", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probeType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
		buildType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 3}
		builder.qry.Nodes[0].TableDef.Cols[0].Typ = probeType
		builder.qry.Nodes[1].TableDef.Cols[0].Typ = buildType
		builder.qry.Nodes[2].OnList = []*planpb.Expr{
			makeRuntimeFilterTestEqTypes(probeType, buildType, 1, 2, 0, 0),
		}
		require.True(t, isEquiCond(builder.qry.Nodes[2].OnList[0],
			map[int32]bool{1: true}, map[int32]bool{2: true}))

		builder.generateRuntimeFilters(2)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
	})

	t.Run("same decimal scale carries explicit raw pair contract", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		decimalType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 3}
		builder.qry.Nodes[0].TableDef.Cols[0].Typ = decimalType
		builder.qry.Nodes[1].TableDef.Cols[0].Typ = decimalType
		builder.qry.Nodes[2].OnList = []*planpb.Expr{
			makeRuntimeFilterTestEq(decimalType, 1, 2, 0, 0),
		}

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		spec := builder.qry.Nodes[2].RuntimeFilterBuildList[0]
		require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			spec.KeyEncoding)
		require.NotNil(t, spec.ProbeType)
		require.Equal(t, decimalType, *spec.ProbeType)
		require.Nil(t, spec.Expr)
		require.Equal(t, decimalType, spec.BuildExpr.Typ)
	})

	t.Run("different varchar widths remain raw-compatible", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probeType := planpb.Type{Id: int32(types.T_varchar), Width: 10}
		buildType := planpb.Type{Id: int32(types.T_varchar), Width: 20}
		builder.qry.Nodes[0].TableDef.Cols[0].Typ = probeType
		builder.qry.Nodes[1].TableDef.Cols[0].Typ = buildType
		builder.qry.Nodes[2].OnList = []*planpb.Expr{
			makeRuntimeFilterTestEqTypes(probeType, buildType, 1, 2, 0, 0),
		}

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[0].RuntimeFilterProbeList, 1)
		spec := builder.qry.Nodes[2].RuntimeFilterBuildList[0]
		require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			spec.KeyEncoding)
		require.Equal(t, probeType, *spec.ProbeType)
		require.NotNil(t, spec.Expr)
		require.Equal(t, buildType, spec.BuildExpr.Typ)
		require.True(t, exprStructuralEqual(spec.Expr, spec.BuildExpr))
	})

	t.Run("float closure follows the deployment rollout gate", func(t *testing.T) {
		build := func(version int64) *QueryBuilder {
			builder := newRuntimeFilterSingleTestBuilder(true)
			floatType := planpb.Type{Id: int32(types.T_float64)}
			builder.qry.Nodes[0].TableDef.Cols[0].Typ = floatType
			builder.qry.Nodes[1].TableDef.Cols[0].Typ = floatType
			builder.qry.Nodes[2].OnList = []*planpb.Expr{
				makeRuntimeFilterTestEq(floatType, 1, 2, 0, 0),
			}
			sid := builder.compCtx.GetProcess().GetService()
			moruntime.ServiceRuntime(sid).SetGlobalVariables(
				moruntime.MOProtocolVersion, version)
			builder.generateRuntimeFilters(2)
			return builder
		}

		probe := newRuntimeFilterSingleTestBuilder(true)
		sid := probe.compCtx.GetProcess().GetService()
		rt := moruntime.ServiceRuntime(sid)
		original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		t.Cleanup(func() {
			if hadOriginal {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		})

		gateV6 := build(defines.MORPCVersion6)
		require.Empty(t, gateV6.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, gateV6.qry.Nodes[0].RuntimeFilterProbeList)

		gateV7 := build(defines.MORPCVersion7)
		require.Len(t, gateV7.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
			gateV7.qry.Nodes[2].RuntimeFilterBuildList[0].KeyEncoding)

		loweredGate := build(defines.MORPCVersion6)
		require.Empty(t, loweredGate.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, loweredGate.qry.Nodes[0].RuntimeFilterProbeList)
	})

	t.Run("raw contract remains enabled below the rollout gate", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		sid := builder.compCtx.GetProcess().GetService()
		rt := moruntime.ServiceRuntime(sid)
		original, hadOriginal := rt.GetGlobalVariables(
			moruntime.MOProtocolVersion)
		t.Cleanup(func() {
			if hadOriginal {
				rt.SetGlobalVariables(
					moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(
					moruntime.MOProtocolVersion,
					defines.MORPCLatestVersion)
			}
		})
		typ := planpb.Type{Id: int32(types.T_int64)}
		probeExpr := GetColExpr(typ, 1, 0)
		buildExpr := GetColExpr(typ, -1, 0)

		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion6)
		_, preRollout, ok := builder.makeExactRuntimeFilterPair(
			1, false, 100, probeExpr, buildExpr, false)
		require.True(t, ok)
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			preRollout.KeyEncoding)
		require.NotNil(t, preRollout.Expr)
		require.NotNil(t, preRollout.BuildExpr)
		require.True(t,
			exprStructuralEqual(preRollout.Expr, preRollout.BuildExpr))

		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion7)
		_, versioned, ok := builder.makeExactRuntimeFilterPair(
			1, false, 100, probeExpr, buildExpr, false)
		require.True(t, ok)
		require.NotNil(t, versioned.Expr)
		require.NotNil(t, versioned.BuildExpr)
		require.True(t,
			exprStructuralEqual(versioned.Expr, versioned.BuildExpr))

		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion6)
		_, loweredGate, ok := builder.makeExactRuntimeFilterPair(
			1, false, 100, probeExpr, buildExpr, false)
		require.True(t, ok)
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			loweredGate.KeyEncoding)

		decimalType := planpb.Type{
			Id: int32(types.T_decimal64), Width: 18, Scale: 2}
		decimalProbe := GetColExpr(decimalType, 1, 0)
		decimalBuild := GetColExpr(decimalType, -1, 0)
		_, _, ok = builder.makeExactRuntimeFilterPair(
			2, false, 100, decimalProbe, decimalBuild, false)
		require.False(t, ok,
			"metadata-dependent RAW must wait for versioned producers")

		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion7)
		_, decimalV7, ok := builder.makeExactRuntimeFilterPair(
			2, false, 100, decimalProbe, decimalBuild, false)
		require.True(t, ok)
		require.Nil(t, decimalV7.Expr)
		require.NotNil(t, decimalV7.BuildExpr)
	})
}

func TestExactRuntimeFilterPairRequiresMaterializableShape(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(true)
	varcharType := planpb.Type{
		Id: int32(types.T_varchar), Width: types.MaxVarcharLen,
	}
	intType := planpb.Type{Id: int32(types.T_int32)}
	probe := GetColExpr(varcharType, 1, 0)

	for _, functionName := range []string{"serial", "serial_full"} {
		t.Run(functionName+" output type is not a composite contract", func(t *testing.T) {
			build, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				functionName,
				[]*planpb.Expr{
					GetColExpr(intType, -1, 0),
					GetColExpr(intType, -1, 1),
				},
			)
			require.NoError(t, err)
			require.Equal(t, types.T_varchar, types.T(build.Typ.Id))

			probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
				1, functionName == "serial_full", 100, probe, build, false)
			require.False(t, ok)
			require.Nil(t, probeSpec)
			require.Nil(t, buildSpec)
		})
	}

	t.Run("prefix consumer must support the probe type", func(t *testing.T) {
		probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
			1,
			true,
			100,
			GetColExpr(intType, 1, 0),
			GetColExpr(intType, -1, 0),
			false,
		)
		require.False(t, ok)
		require.Nil(t, probeSpec)
		require.Nil(t, buildSpec)
	})

	t.Run("direct producer slot is explicit", func(t *testing.T) {
		probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
			1,
			false,
			100,
			GetColExpr(intType, 1, 0),
			GetColExpr(intType, -1, 1),
			false,
		)
		require.True(t, ok)
		require.NotNil(t, probeSpec)
		require.Equal(t, int32(1),
			buildSpec.BuildExpr.GetCol().ColPos)
	})

	t.Run("negative direct producer slot is invalid", func(t *testing.T) {
		probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
			1,
			false,
			100,
			GetColExpr(intType, 1, 0),
			GetColExpr(intType, -1, -1),
			false,
		)
		require.False(t, ok)
		require.Nil(t, probeSpec)
		require.Nil(t, buildSpec)
	})
}

func TestSerializedExactRuntimeFilterPairContract(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(true)
	sid := builder.compCtx.GetProcess().GetService()
	rt := moruntime.ServiceRuntime(sid)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)

	varcharType := planpb.Type{
		Id: int32(types.T_varchar), Width: types.MaxVarcharLen,
	}
	intType := planpb.Type{Id: int32(types.T_int32)}
	finalProbe := GetColExpr(varcharType, 1, 0)
	componentProbes := []*planpb.Expr{
		GetColExpr(intType, 1, 0),
		GetColExpr(intType, 1, 1),
	}

	for _, test := range []struct {
		functionName string
		marker       planpb.RuntimeFilterKeyEncoding
		matchPrefix  bool
	}{
		{
			functionName: function.SerialFunctionName,
			marker: planpb.
				RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1,
		},
		{
			functionName: function.SerialFullFunctionName,
			marker: planpb.
				RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1,
			matchPrefix: true,
		},
	} {
		t.Run(test.functionName, func(t *testing.T) {
			build, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				test.functionName,
				[]*planpb.Expr{
					GetColExpr(intType, -1, 0),
					GetColExpr(intType, -1, 1),
				},
			)
			require.NoError(t, err)

			probeSpec, buildSpec, ok :=
				builder.makeSerializedExactRuntimeFilterPair(
					1,
					test.matchPrefix,
					100,
					finalProbe,
					build,
					componentProbes,
					false,
				)
			require.True(t, ok)
			require.NotNil(t, probeSpec)
			require.Equal(t, test.marker, buildSpec.KeyEncoding)
			require.Nil(t, buildSpec.Expr)
			require.NotNil(t, buildSpec.BuildExpr)
			require.Equal(t, varcharType, *buildSpec.ProbeType)
			require.Equal(t,
				[]planpb.Type{intType, intType},
				buildSpec.KeyComponentProbeTypes)
		})
	}

	t.Run("tuple marker must match consumer prefix semantics", func(t *testing.T) {
		for _, test := range []struct {
			name         string
			functionName string
			wrongPrefix  bool
		}{
			{
				name:         "serial cannot drive prefix in",
				functionName: function.SerialFunctionName,
				wrongPrefix:  true,
			},
			{
				name:         "serial full requires prefix in",
				functionName: function.SerialFullFunctionName,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				build, err := BindFuncExprImplByPlanExpr(
					builder.GetContext(),
					test.functionName,
					[]*planpb.Expr{GetColExpr(intType, -1, 0)},
				)
				require.NoError(t, err)
				_, _, ok := builder.makeSerializedExactRuntimeFilterPair(
					1,
					test.wrongPrefix,
					100,
					finalProbe,
					build,
					componentProbes[:1],
					false,
				)
				require.False(t, ok)
			})
		}
	})

	t.Run("tuple function identity and build slots are part of the contract", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*planpb.Expr)
		}{
			{
				name: "encoded function id drift",
				mutate: func(build *planpb.Expr) {
					build.GetF().Func.Obj++
				},
			},
			{
				name: "negative materialization slot",
				mutate: func(build *planpb.Expr) {
					build.GetF().Args[0].GetCol().ColPos = -1
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				build, err := BindFuncExprImplByPlanExpr(
					builder.GetContext(),
					function.SerialFunctionName,
					[]*planpb.Expr{GetColExpr(intType, -1, 0)},
				)
				require.NoError(t, err)
				test.mutate(build)

				probeSpec, buildSpec, ok :=
					builder.makeSerializedExactRuntimeFilterPair(
						1,
						false,
						100,
						finalProbe,
						build,
						componentProbes[:1],
						false,
					)
				require.False(t, ok)
				require.Nil(t, probeSpec)
				require.Nil(t, buildSpec)
			})
		}
	})

	t.Run("tuple result must retain the production varchar type", func(t *testing.T) {
		build, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			function.SerialFunctionName,
			[]*planpb.Expr{GetColExpr(intType, -1, 0)},
		)
		require.NoError(t, err)
		build.Typ = intType

		probeSpec, buildSpec, ok :=
			builder.makeSerializedExactRuntimeFilterPair(
				1,
				false,
				100,
				GetColExpr(intType, 1, 0),
				build,
				componentProbes[:1],
				false,
			)
		require.False(t, ok)
		require.Nil(t, probeSpec)
		require.Nil(t, buildSpec)
	})

	t.Run("component decimal scale mismatch", func(t *testing.T) {
		scale2 := planpb.Type{
			Id: int32(types.T_decimal64), Width: 18, Scale: 2,
		}
		scale3 := planpb.Type{
			Id: int32(types.T_decimal64), Width: 18, Scale: 3,
		}
		build, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			function.SerialFunctionName,
			[]*planpb.Expr{GetColExpr(scale3, -1, 0)},
		)
		require.NoError(t, err)

		probeSpec, buildSpec, ok :=
			builder.makeSerializedExactRuntimeFilterPair(
				1,
				false,
				100,
				finalProbe,
				build,
				[]*planpb.Expr{GetColExpr(scale2, 1, 0)},
				false,
			)
		require.False(t, ok)
		require.Nil(t, probeSpec)
		require.Nil(t, buildSpec)
	})

	t.Run("float component has no bounded tuple closure", func(t *testing.T) {
		floatType := planpb.Type{Id: int32(types.T_float64)}
		build, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			function.SerialFullFunctionName,
			[]*planpb.Expr{GetColExpr(floatType, -1, 0)},
		)
		require.NoError(t, err)

		_, _, ok := builder.makeSerializedExactRuntimeFilterPair(
			1,
			true,
			100,
			finalProbe,
			build,
			[]*planpb.Expr{GetColExpr(floatType, 1, 0)},
			false,
		)
		require.False(t, ok)
	})

	t.Run("pre-rollout deployment omits tuple contract", func(t *testing.T) {
		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion6)
		build, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			function.SerialFunctionName,
			[]*planpb.Expr{GetColExpr(intType, -1, 0)},
		)
		require.NoError(t, err)

		_, _, ok := builder.makeSerializedExactRuntimeFilterPair(
			1,
			false,
			100,
			finalProbe,
			build,
			componentProbes[:1],
			false,
		)
		require.False(t, ok)
	})
}

func TestFinalizeFuzzyRuntimeFilterKeepsDecisionAtomic(t *testing.T) {
	newBuilder := func(tableCost, sinkCost float64) (*QueryBuilder, *planpb.Node, *planpb.Node, *planpb.Node) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		tableScan := builder.qry.Nodes[0]
		sinkScan := builder.qry.Nodes[1]
		fuzzy := builder.qry.Nodes[2]
		fuzzy.NodeType = planpb.Node_FUZZY_FILTER
		tableScan.Stats.Cost = tableCost
		tableScan.Stats.Outcnt = 800
		tableScan.Stats.TableCnt = 1_000
		tableScan.Stats.BlockNum = 100
		tableScan.Stats.Selectivity = 0.8
		sinkScan.Stats.Cost = sinkCost
		sinkScan.Stats.Outcnt = 10
		sinkScan.Stats.TableCnt = 10

		typ := tableScan.TableDef.Cols[0].Typ
		probeSpec := MakeRuntimeFilter(
			71, false, 0, GetColExpr(typ, 1, 0), false)
		buildSpec := MakeRuntimeFilter(
			71, false, 100, GetColExpr(typ, 0, 0), false)
		buildSpec.BuildExpr = DeepCopyExpr(buildSpec.Expr)
		buildSpec.ProbeType = DeepCopyType(&typ)
		buildSpec.KeyEncoding =
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1
		tableScan.RuntimeFilterProbeList =
			[]*planpb.RuntimeFilterSpec{probeSpec}
		fuzzy.RuntimeFilterBuildList =
			[]*planpb.RuntimeFilterSpec{buildSpec}
		return builder, tableScan, sinkScan, fuzzy
	}

	t.Run("build on table clears only candidate state", func(t *testing.T) {
		builder, tableScan, _, fuzzy := newBuilder(20, 100)
		before := DeepCopyStats(tableScan.Stats)

		builder.finalizeFuzzyRuntimeFilter(fuzzy)

		require.Equal(t, planpb.Node_FUZZY_BUILD_SIDE_TABLE,
			fuzzy.FuzzyBuildSide)
		require.Empty(t, fuzzy.RuntimeFilterBuildList)
		require.Empty(t, tableScan.RuntimeFilterProbeList)
		require.Equal(t, before, tableScan.Stats)
		require.False(t, tableScan.Stats.ForceOneCN)
	})

	t.Run("malformed pair cannot publish optimistic state", func(t *testing.T) {
		builder, tableScan, _, fuzzy := newBuilder(100, 100)
		before := DeepCopyStats(tableScan.Stats)
		fuzzy.RuntimeFilterBuildList[0].Expr.GetCol().ColPos = 1

		builder.finalizeFuzzyRuntimeFilter(fuzzy)

		require.Empty(t, fuzzy.RuntimeFilterBuildList)
		require.Empty(t, tableScan.RuntimeFilterProbeList)
		require.Equal(t, before, tableScan.Stats)
		require.False(t, tableScan.Stats.ForceOneCN)
		require.Equal(t,
			planpb.Node_FUZZY_BUILD_SIDE_UNSPECIFIED,
			fuzzy.FuzzyBuildSide)
	})

	t.Run("pre rollout fuzzy transport removes both dependency ends", func(t *testing.T) {
		builder, tableScan, _, fuzzy := newBuilder(300_000, 1_000_000)
		sid := builder.compCtx.GetProcess().GetService()
		rt := moruntime.ServiceRuntime(sid)
		original, hadOriginal := rt.GetGlobalVariables(
			moruntime.MOProtocolVersion)
		rt.SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion6)
		t.Cleanup(func() {
			if hadOriginal {
				rt.SetGlobalVariables(
					moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(
					moruntime.MOProtocolVersion,
					defines.MORPCLatestVersion)
			}
		})
		before := DeepCopyStats(tableScan.Stats)

		builder.finalizeFuzzyRuntimeFilter(fuzzy)

		require.Empty(t, fuzzy.RuntimeFilterBuildList)
		require.Empty(t, tableScan.RuntimeFilterProbeList)
		require.Equal(t, before, tableScan.Stats)
		require.False(t, tableScan.Stats.ForceOneCN)
		require.Equal(t,
			planpb.Node_FUZZY_BUILD_SIDE_UNSPECIFIED,
			fuzzy.FuzzyBuildSide)
	})

	t.Run("build on sink publishes pair placement and stats together", func(t *testing.T) {
		builder, tableScan, sinkScan, fuzzy := newBuilder(300_000, 1_000_000)
		tableScan.Stats.TableCnt = 1_000_000
		sinkScan.Stats.Outcnt = 1

		builder.finalizeFuzzyRuntimeFilter(fuzzy)

		require.Equal(t, planpb.Node_FUZZY_BUILD_SIDE_SINK,
			fuzzy.FuzzyBuildSide)
		require.Len(t, fuzzy.RuntimeFilterBuildList, 1)
		require.Len(t, tableScan.RuntimeFilterProbeList, 1)
		require.True(t, tableScan.Stats.ForceOneCN)
		require.Equal(t, sinkScan.Stats.Outcnt, tableScan.Stats.Outcnt)
		require.Equal(t, sinkScan.Stats.Outcnt/tableScan.Stats.TableCnt,
			tableScan.Stats.Selectivity)
		after := DeepCopyStats(tableScan.Stats)

		// Finalization is idempotent if a later planning pass revisits the node.
		builder.finalizeFuzzyRuntimeFilter(fuzzy)
		require.Equal(t, after, tableScan.Stats)
		require.Len(t, fuzzy.RuntimeFilterBuildList, 1)
		require.Len(t, tableScan.RuntimeFilterProbeList, 1)
	})
}

func TestSingleJoinStatsUseSemanticPreservedSide(t *testing.T) {
	t.Run("left SINGLE preserves physical left stats", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(false)

		reCalcNodeStatsAfterSwap(2, builder, false, false, false)

		join := builder.qry.Nodes[2]
		require.Equal(t, builder.qry.Nodes[0].Stats.Outcnt, join.Stats.Outcnt)
		require.Equal(t, builder.qry.Nodes[0].Stats.BlockNum, join.Stats.BlockNum)
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.HashmapStats.HashmapSize)
	})

	t.Run("right SINGLE preserves physical right stats after child swap", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeType: planpb.Node_PROJECT,
			NodeId:   3,
			Children: []int32{2},
			Stats:    DefaultStats(),
		})

		reCalcNodeStatsAfterSwap(3, builder, true, false, false)

		join := builder.qry.Nodes[2]
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.Outcnt)
		require.Equal(t, builder.qry.Nodes[1].Stats.BlockNum, join.Stats.BlockNum)
		require.Equal(t, builder.qry.Nodes[1].Stats.Selectivity, join.Stats.Selectivity)
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.HashmapStats.HashmapSize)
		require.Equal(t, join.Stats.Outcnt, builder.qry.Nodes[3].Stats.Outcnt)
	})

	t.Run("right SINGLE cardinality sizes a downstream join build side", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		pkType := builder.qry.Nodes[0].TableDef.Cols[0].Typ
		downstreamProbe := &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			NodeId:      3,
			BindingTags: []int32{3},
			TableDef: &planpb.TableDef{
				Name:          "downstream_probe",
				Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
				Name2ColIndex: map[string]int32{"id": 0},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			Stats: &planpb.Stats{
				Cost:        1_000,
				Outcnt:      1_000,
				TableCnt:    1_000,
				BlockNum:    10,
				Selectivity: 1,
			},
		}
		downstreamJoin := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			NodeId:   4,
			Children: []int32{3, 2},
			JoinType: planpb.Node_INNER,
			OnList: []*planpb.Expr{
				makeRuntimeFilterTestEq(pkType, 3, 2, 0, 0),
			},
			Stats: DefaultStats(),
		}
		downstreamJoin.OnList[0].Ndv = 3
		builder.qry.Nodes = append(builder.qry.Nodes, downstreamProbe, downstreamJoin)

		reCalcNodeStatsAfterSwap(4, builder, true, false, false)

		rightSingle := builder.qry.Nodes[2]
		require.Equal(t, float64(3), rightSingle.Stats.Outcnt)
		require.Equal(t, rightSingle.Stats.Outcnt, downstreamJoin.Stats.HashmapStats.HashmapSize)
		require.Equal(t, float64(1_000), downstreamJoin.Stats.Outcnt)
	})

	t.Run("right SINGLE applies limit after selecting the preserved side", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes[2].Limit = MakePlan2Uint64ConstExprWithType(1)

		reCalcNodeStatsAfterSwap(2, builder, false, false, false)

		require.Equal(t, float64(1), builder.qry.Nodes[2].Stats.Outcnt)
	})
}

func TestRightSingleRuntimeFilterConservativeEligibility(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*QueryBuilder)
	}{
		{
			name: "probe is not a direct table scan",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].NodeType = planpb.Node_PROJECT
			},
		},
		{
			name: "probe scan has limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Limit = MakePlan2Int64ConstExprWithType(1)
			},
		},
		{
			name: "probe scan has offset",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Offset = MakePlan2Uint64ConstExprWithType(1)
			},
		},
		{
			name: "probe metadata has no primary key",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].TableDef.Pkey = nil
			},
		},
		{
			name: "build exceeds planner limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].Stats.Outcnt = 5_000_001
			},
		},
		{
			name: "preserved build is not a direct table scan",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].NodeType = planpb.Node_AGG
			},
		},
		{
			name: "build estimate exceeds exact IN limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Stats.TableCnt = 100_000
				builder.qry.Nodes[1].Stats.Outcnt = 50_000
			},
		},
		{
			name: "small filtered output still scans a build table above exact IN limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Stats.TableCnt = 100_000
				builder.qry.Nodes[1].Stats.Outcnt = 3
				builder.qry.Nodes[1].Stats.TableCnt = 50_000
			},
		},
		{
			name: "unavailable build statistics are not an exact size bound",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].Stats = DefaultStats()
			},
		},
		{
			name: "local placement override is active",
			mutate: func(builder *QueryBuilder) {
				builder.optimizerHints = &OptimizerHints{forceOneCN: 1}
			},
		},
		{
			name: "right single RF feature gate is disabled",
			mutate: func(builder *QueryBuilder) {
				builder.optimizerHints = &OptimizerHints{disableRightSingleRF: 1}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(true)
			test.mutate(builder)

			builder.generateRuntimeFilters(2)

			require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
			require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		})
	}

	t.Run("leading cluster key remains prunable with non-PK row filtering", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe := builder.qry.Nodes[0]
		probeType := probe.TableDef.Cols[0].Typ
		probe.TableDef.Cols = append(probe.TableDef.Cols, &planpb.ColDef{Name: "cluster_key", Typ: probeType})
		probe.TableDef.Name2ColIndex["cluster_key"] = 1
		probe.TableDef.ClusterBy = &planpb.ClusterByDef{Name: "cluster_key"}
		joinKey := builder.qry.Nodes[2].OnList[0].GetF().Args[0]
		joinKey.GetCol().ColPos = 1
		joinKey.GetCol().Name = "cluster_key"

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.True(t, probe.RuntimeFilterProbeList[0].NotOnPk)
		require.Equal(t, int32(1), probe.RuntimeFilterProbeList[0].Expr.GetCol().ColPos)
		require.True(t, probe.Stats.ForceOneCN)
		require.True(t, builder.qry.Nodes[1].Stats.ForceOneCN)
	})

	t.Run("non-leading composite key does not sacrifice multi-CN scan", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe := builder.qry.Nodes[0]
		pkType := probe.TableDef.Cols[0].Typ
		probe.TableDef.Cols = []*planpb.ColDef{
			{Name: "leading", Typ: pkType},
			{Name: "lookup", Typ: pkType},
			{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_varchar)}},
		}
		probe.TableDef.Name2ColIndex = map[string]int32{
			"leading":                  0,
			"lookup":                   1,
			catalog.CPrimaryKeyColName: 2,
		}
		probe.TableDef.Pkey = &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names:       []string{"leading", "lookup"},
		}
		joinKey := builder.qry.Nodes[2].OnList[0].GetF().Args[0]
		joinKey.GetCol().ColPos = 1
		joinKey.GetCol().Name = "lookup"

		builder.generateRuntimeFilters(2)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
	})

	t.Run("full composite filter is omitted until HashBuild can materialize it", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
		require.False(t, probe.Stats.ForceOneCN)
		require.False(t, builder.qry.Nodes[1].Stats.ForceOneCN)
	})

	t.Run("leading composite prefix is omitted until HashBuild can materialize it", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)
		builder.qry.Nodes[2].OnList = builder.qry.Nodes[2].OnList[:1]

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
		require.False(t, probe.Stats.ForceOneCN)
		require.False(t, builder.qry.Nodes[1].Stats.ForceOneCN)
	})

	t.Run("composite probe preserves existing runtime filters", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)
		existing := &planpb.RuntimeFilterSpec{Tag: 99, Expr: GetColExpr(probe.TableDef.Cols[0].Typ, 1, 0)}
		probe.RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{existing}

		builder.generateRuntimeFilters(2)

		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.Same(t, existing, probe.RuntimeFilterProbeList[0])
		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
	})
}

func TestAnalyzeRuntimeFilterJoinPolicy(t *testing.T) {
	tests := []struct {
		name      string
		joinType  planpb.Node_JoinType
		right     bool
		eligible  bool
		localOnly bool
	}{
		{name: "inner", joinType: planpb.Node_INNER, eligible: true},
		{name: "left", joinType: planpb.Node_LEFT},
		{name: "left single", joinType: planpb.Node_SINGLE},
		{name: "right single", joinType: planpb.Node_SINGLE, right: true, eligible: true, localOnly: true},
		{name: "right", joinType: planpb.Node_RIGHT, eligible: true, localOnly: true},
		{name: "outer", joinType: planpb.Node_OUTER},
		{name: "left semi", joinType: planpb.Node_SEMI, eligible: true},
		{name: "right semi", joinType: planpb.Node_SEMI, right: true, eligible: true, localOnly: true},
		{name: "left anti", joinType: planpb.Node_ANTI},
		{name: "right anti", joinType: planpb.Node_ANTI, right: true, eligible: true, localOnly: true},
		{name: "left dedup", joinType: planpb.Node_DEDUP, eligible: true},
		{name: "right dedup", joinType: planpb.Node_DEDUP, right: true},
		{name: "index", joinType: planpb.Node_INDEX, eligible: true, localOnly: true},
		{name: "mark", joinType: planpb.Node_MARK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := analyzeRuntimeFilterJoinPolicy(&planpb.Node{
				NodeType:    planpb.Node_JOIN,
				JoinType:    test.joinType,
				IsRightJoin: test.right,
			})
			require.Equal(t, test.eligible, policy.eligible)
			require.Equal(t, test.localOnly, policy.requiresLocalDelivery)
		})
	}
}

func TestForceJoinOnOneCNRuntimeFilterPolicy(t *testing.T) {
	tests := []struct {
		name     string
		joinType planpb.Node_JoinType
		right    bool
		shuffle  bool
		force    bool
	}{
		{name: "right join", joinType: planpb.Node_RIGHT, force: true},
		{name: "shuffle right join", joinType: planpb.Node_RIGHT, shuffle: true},
		{name: "left semi", joinType: planpb.Node_SEMI},
		{name: "right semi", joinType: planpb.Node_SEMI, right: true, force: true},
		{name: "shuffle right semi", joinType: planpb.Node_SEMI, right: true, shuffle: true},
		{name: "left anti", joinType: planpb.Node_ANTI},
		{name: "right anti", joinType: planpb.Node_ANTI, right: true, force: true},
		{name: "shuffle right anti", joinType: planpb.Node_ANTI, right: true, shuffle: true},
		{name: "right single", joinType: planpb.Node_SINGLE, right: true, force: true},
		{name: "shuffle right single", joinType: planpb.Node_SINGLE, right: true, shuffle: true},
		{name: "left dedup", joinType: planpb.Node_DEDUP, force: true},
		{name: "right dedup", joinType: planpb.Node_DEDUP, right: true, force: true},
		{name: "shuffle dedup", joinType: planpb.Node_DEDUP, shuffle: true},
		{name: "mark", joinType: planpb.Node_MARK},
		{name: "outer", joinType: planpb.Node_OUTER},
		{name: "shuffle index remains local", joinType: planpb.Node_INDEX, shuffle: true, force: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(test.right)
			join := builder.qry.Nodes[2]
			join.JoinType = test.joinType
			join.IsRightJoin = test.right
			join.Stats.HashmapStats.Shuffle = test.shuffle
			join.RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{{Tag: 1}}

			builder.forceJoinOnOneCN(2, false)

			require.Equal(t, test.force, builder.qry.Nodes[0].Stats.ForceOneCN)
			require.Equal(t, test.force, builder.qry.Nodes[1].Stats.ForceOneCN)
		})
	}
}

func TestDisableRightSingleRuntimeFilterHint(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(true)
	builder.optimizerHints = &OptimizerHints{}

	handleOptimizerHints("disableRightSingleRF=1", builder)
	builder.generateRuntimeFilters(2)

	require.Equal(t, 1, builder.optimizerHints.disableRightSingleRF)
	require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
	require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
}
