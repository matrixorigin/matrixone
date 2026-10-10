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

package substrait

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestExactDecimalProfilePreservesPhysicalDescriptor(t *testing.T) {
	profile := NewEmbeddedExportProfile(31)
	require.Equal(t, "legacy", NewEmbeddedExportProfile(1<<16).Name(), "16 is a mask, not a bit index")
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, required := range []bool{false, true} {
			typ := planpb.Type{Id: int32(oid), Width: 9, Scale: 2, NotNullable: required}
			wire, err := profile.substraitType(&typ)
			require.NoError(t, err)
			user := wire.GetUserDefined()
			require.NotNil(t, user)
			require.Equal(t, []int64{int64(exactDecimalBits(&typ)), 9, 2}, []int64{user.TypeParameters[0].GetInteger(), user.TypeParameters[1].GetInteger(), user.TypeParameters[2].GetInteger()})
			require.Equal(t, required, user.Nullability == spb.Type_NULLABILITY_REQUIRED)
		}
	}
	for _, typ := range []planpb.Type{
		{Id: int32(types.T_decimal64), Width: 19},
		{Id: int32(types.T_decimal128), Width: 39},
		{Id: int32(types.T_decimal256), Width: 77},
		{Id: int32(types.T_decimal256), Width: 15, Scale: 16},
	} {
		_, err := profile.substraitType(&typ)
		require.True(t, IsNotEligible(err))
	}
	working := planpb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 40}
	_, err := profile.substraitType(&working)
	require.NoError(t, err)
	_, err = NewEmbeddedExportProfile(11).substraitType(&working)
	require.True(t, IsNotEligible(err))
}

func TestExactColumnReferencesRetainDeclaredDescriptor(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, required := range []bool{false, true} {
			e := exporter{profile: NewEmbeddedExportProfile(31)}
			typ := planpb.Type{Id: int32(oid), Width: 15, Scale: 2, NotNullable: required}
			x := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			wire, err := e.expr(x, []int{1})
			require.NoError(t, err)
			cast := wire.GetScalarFunction()
			require.NotNil(t, cast)
			require.Equal(t, e.functions["mo_decimal_cast"], cast.FunctionReference)
			declared := cast.OutputType.GetUserDefined()
			require.Equal(t, []int64{int64(exactDecimalBits(&typ)), 15, 2}, []int64{
				declared.TypeParameters[0].GetInteger(), declared.TypeParameters[1].GetInteger(), declared.TypeParameters[2].GetInteger(),
			})
			require.Equal(t, required, declared.Nullability == spb.Type_NULLABILITY_REQUIRED)
			require.Equal(t, int32(0), cast.Arguments[0].GetValue().GetSelection().GetDirectReference().GetStructField().Field)
			legacy := exporter{}
			wire, err = legacy.expr(x, []int{1})
			require.NoError(t, err)
			require.NotNil(t, wire.GetSelection())
		}
	}
}

func TestExactCandidateRetainsProfileThroughReadAndBuild(t *testing.T) {
	query := embeddedProjectedScanQuery()
	for _, column := range query.Nodes[0].TableDef.Cols {
		column.Typ = planpb.Type{Id: int32(types.T_decimal256), Width: 15, Scale: 2}
	}
	for _, expr := range query.Nodes[0].ProjectList {
		expr.Typ = planpb.Type{Id: int32(types.T_decimal256), Width: 15, Scale: 2}
	}
	_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(11))
	require.True(t, IsNotEligible(err))
	profile := NewEmbeddedExportProfile(31)
	candidate, err := ExportEmbeddedMO(query, profile)
	require.NoError(t, err)
	profile = NewEmbeddedExportProfile(11)
	require.Equal(t, "legacy", profile.Name())
	require.Equal(t, "mo-exact-decimal-v1", candidate.NumericProfile())
	reads, err := candidate.EmbeddedMOReads()
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), reads[0].Columns[0].Type.Id)
	wire, err := candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}})
	require.NoError(t, err)
	plan := embeddedPlan(t, wire)
	require.Len(t, plan.ExtensionUrns, 1)
	require.Equal(t, MOExactDecimalV1URI, plan.ExtensionUrns[0].Urn)
	user := plan.Relations[0].GetRoot().Input.GetRead().BaseSchema.Struct.Types[0].GetUserDefined()
	require.Equal(t, int64(256), user.TypeParameters[0].GetInteger(), "precision 15 cannot narrow a 256-bit carrier")
	_, err = candidate.Build(map[int32][]byte{0: {1}})
	require.ErrorContains(t, err, "cannot be emitted as Flight")
}

func exactLiteralBytes(t *testing.T, expression *spb.Expression) []byte {
	t.Helper()
	user := expression.GetLiteral().GetUserDefined()
	require.NotNil(t, user)
	value := user.GetValue()
	require.Equal(t, exactDecimalLiteralURL, value.TypeUrl)
	number, kind, n := protowire.ConsumeTag(value.Value)
	require.Equal(t, protowire.Number(1), number)
	require.Equal(t, protowire.BytesType, kind)
	require.Greater(t, n, 0)
	bytes, consumed := protowire.ConsumeBytes(value.Value[n:])
	require.Equal(t, len(value.Value), n+consumed)
	return bytes
}

func TestExactLiteralsUseCanonicalWidthsAndWideNumericProvenance(t *testing.T) {
	e := exporter{profile: NewEmbeddedExportProfile(31)}
	for _, bits := range []int{64, 128} {
		typ := planpb.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 2, NotNullable: true}
		literal := &planpb.Literal{Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: -125}}}
		if bits == 128 {
			typ.Id = int32(types.T_decimal128)
			literal.Value = &planpb.Literal_Decimal128Val{Decimal128Val: &planpb.Decimal128{A: -125, B: -1}}
		}
		wire, err := e.literal(literal, &typ)
		require.NoError(t, err)
		bytes := exactLiteralBytes(t, wire)
		require.Len(t, bytes, bits/8)
		require.Equal(t, int64(-125), int64(binary.LittleEndian.Uint64(bytes)))
		if bits == 128 {
			require.Equal(t, ^uint64(0), binary.LittleEndian.Uint64(bytes[8:]))
		}
	}
	typ := planpb.Type{Id: int32(types.T_decimal256), Width: 39, NotNullable: true}
	for _, negative := range []bool{false, true} {
		text := "340282366920938463463374607431768211457" // 2^128 + 1
		if negative {
			text = "-" + text
		}
		call := &planpb.Function{Func: &planpb.ObjectRef{ObjName: "cast", Obj: int64(function.CAST) << 32}, Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), NotNullable: true}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: text}, DecimalLiteralRequiresV82: true}}},
			{Typ: typ, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		}}
		wire, handled, err := e.exactNumericLiteralCast(&planpb.Expr{Typ: typ}, call)
		require.True(t, handled)
		require.NoError(t, err)
		bytes := exactLiteralBytes(t, wire)
		require.Len(t, bytes, 32)
		want := [4]uint64{1, 0, 1, 0}
		if negative {
			want = [4]uint64{^uint64(0), ^uint64(0), ^uint64(1), ^uint64(0)}
		}
		for i := range want {
			require.Equal(t, want[i], binary.LittleEndian.Uint64(bytes[8*i:]))
		}
		call.Args[0].GetLit().DecimalLiteralRequiresV82 = false
		_, handled, err = e.exactNumericLiteralCast(&planpb.Expr{Typ: typ}, call)
		require.False(t, handled, "ordinary string casts cannot become numeric literals")
		require.NoError(t, err)
	}
}

func TestExactEmbeddedTPCHInventory(t *testing.T) {
	mock := planbuilder.NewMockOptimizer(false, newPlanTestProcess(t))
	for number := 1; number <= 22; number++ {
		t.Run(fmt.Sprintf("q%d", number), func(t *testing.T) {
			query := exactTPCHQuery(t, mock, number)
			candidate, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(31))
			require.NoError(t, err, "signatures: %v", numericFunctionSignatures(query))
			bindings := make(map[int32]EmbeddedReadBinding)
			for ordinal, read := range candidate.Reads() {
				bindings[read.NodeID] = EmbeddedReadBinding{BindingID: uint64(ordinal + 1), Source: EmbeddedReadMO}
			}
			wire, err := candidate.BuildEmbedded(bindings)
			require.NoError(t, err)
			var plan spb.Plan
			require.NoError(t, proto.Unmarshal(wire, &plan))
			require.Equal(t, query.Headings, plan.Relations[len(plan.Relations)-1].GetRoot().Names)
		})
	}
}

func exactTPCHQuery(t *testing.T, mock *planbuilder.MockOptimizer, number int) *planpb.Query {
	t.Helper()
	sql, err := os.ReadFile(filepath.Join("..", "tpch", fmt.Sprintf("q%d.sql", number)))
	require.NoError(t, err)
	statements, err := parsers.Parse(t.Context(), dialect.MYSQL, string(sql), 1)
	require.NoError(t, err)
	query, err := mock.Optimize(statements[0])
	require.NoError(t, err)
	for _, node := range query.Nodes {
		if node != nil && node.TableDef != nil && node.ObjRef != nil {
			node.TableDef.DbId, node.TableDef.TblId, node.ObjRef.Obj = 7, 42, 42
		}
	}
	return query
}

func TestExactScalarAndAggregateFamiliesUseBoundOverloads(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, name := range []string{"+", "-", "*", "/", "div", "mod", "unary_minus", "=", "!=", "<", "<=", ">", ">="} {
			t.Run(fmt.Sprintf("%s/%s", oid, name), func(t *testing.T) {
				n := 2
				if name == "unary_minus" {
					n = 1
				}
				inputTypes := make([]types.Type, n)
				for i := range inputTypes {
					inputTypes[i] = types.New(oid, 15, 2)
				}
				bound, err := function.GetFunctionByName(t.Context(), name, inputTypes)
				require.NoError(t, err)
				if casts, needed := bound.ShouldDoImplicitTypeCast(); needed {
					inputTypes = casts
				}
				args := make([]*planpb.Expr, len(inputTypes))
				for i, input := range inputTypes {
					args[i] = &planpb.Expr{Typ: planpb.Type{Id: int32(input.Oid), Width: input.Width, Scale: input.Scale, NotNullable: true}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(i)}}}
				}
				out := bound.GetReturnType()
				result := &planpb.Expr{Typ: planpb.Type{Id: int32(out.Oid), Width: out.Width, Scale: out.Scale, NotNullable: function.DeduceNotNullable(bound.GetEncodedOverloadID(), args)}}
				call := &planpb.Function{Func: &planpb.ObjectRef{ObjName: name, Obj: bound.GetEncodedOverloadID()}, Args: args}
				e := exporter{profile: NewEmbeddedExportProfile(31)}
				wire, err := e.exactScalarExpr(result, call, []int{n})
				require.NoError(t, err)
				id, _ := function.DecodeOverloadID(call.Func.Obj)
				require.Equal(t, e.function("mo_decimal_"+exactScalarName(id)), wire.GetScalarFunction().FunctionReference)
				call.Func.Obj++ // another overload cannot inherit the semantic declaration
				_, err = e.exactScalarExpr(result, call, []int{n})
				require.True(t, IsNotEligible(err))
			})
		}
		for _, name := range []string{"sum", "avg", "min", "max"} {
			input := types.New(oid, 15, 2)
			if oid == types.T_decimal256 && name == "sum" {
				input.Width = 53
			}
			bound, err := function.GetFunctionByName(t.Context(), name, []types.Type{input})
			require.NoError(t, err)
			args := []*planpb.Expr{{Typ: planpb.Type{Id: int32(oid), Width: input.Width, Scale: 2, NotNullable: true}}}
			out := bound.GetReturnType()
			output := planpb.Type{Id: int32(out.Oid), Width: out.Width, Scale: out.Scale}
			e := exporter{profile: NewEmbeddedExportProfile(31)}
			ok, err := e.hasExactSemanticCapability(semanticAggregate, name, &planpb.ObjectRef{ObjName: name, Obj: bound.GetEncodedOverloadID()}, args, &output)
			require.NoError(t, err)
			require.True(t, ok, "%s/%s", oid, name)
		}
	}
}

func TestExactAggregateDeclinesUnsupportedPhysicalNarrowing(t *testing.T) {
	input := types.New(types.T_decimal256, 15, 2)
	bound, err := function.GetFunctionByName(t.Context(), "sum", []types.Type{input})
	require.NoError(t, err)
	output := bound.GetReturnType()
	require.Equal(t, types.T_decimal128, output.Oid)
	e := exporter{profile: NewEmbeddedExportProfile(31)}
	ok, err := e.hasExactSemanticCapability(semanticAggregate, "sum", &planpb.ObjectRef{ObjName: "sum", Obj: bound.GetEncodedOverloadID()}, []*planpb.Expr{{Typ: planpb.Type{Id: int32(input.Oid), Width: input.Width, Scale: input.Scale}}}, &planpb.Type{Id: int32(output.Oid), Width: output.Width, Scale: output.Scale})
	require.NoError(t, err)
	require.False(t, ok, "retain the MO result and decline; do not widen it to fit a native signature")
}

func TestExactIntegerCastRequiresDomainOrProvenLiteral(t *testing.T) {
	target := planpb.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 2, NotNullable: true}
	argument := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	call := &planpb.Function{Func: &planpb.ObjectRef{ObjName: "cast", Obj: int64(function.CAST) << 32}, Args: []*planpb.Expr{argument, {Typ: target, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}}}
	e := exporter{profile: NewEmbeddedExportProfile(31)}
	_, err := e.exactScalarExpr(&planpb.Expr{Typ: target}, call, []int{1})
	require.True(t, IsNotEligible(err), "a Decimal64 target cannot contain the entire BIGINT domain")
	argument.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 12}}}
	wire, err := e.exactScalarExpr(&planpb.Expr{Typ: target}, call, []int{1})
	require.NoError(t, err)
	require.Equal(t, uint64(1200), binary.LittleEndian.Uint64(exactLiteralBytes(t, wire)))
	target.Id, target.Width = int32(types.T_decimal128), 38
	call.Args[1].Typ = target
	argument.Expr = &planpb.Expr_Col{Col: &planpb.ColRef{}}
	_, err = e.exactScalarExpr(&planpb.Expr{Typ: target}, call, []int{1})
	require.NoError(t, err)
	for _, mode := range []int64{1, 2, 3, 4} {
		call.Func.Obj = int64(function.CAST)<<32 | mode
		_, err = e.exactScalarExpr(&planpb.Expr{Typ: target}, call, []int{1})
		require.True(t, IsNotEligible(err), "only normal checked casts belong to v1")
	}
}

func TestExactDivisionRetainsBoundStatementResult(t *testing.T) {
	for _, increment := range []int32{0, 4, 10, 30} {
		inputs := []types.Type{types.New(types.T_decimal128, 10, 2), types.New(types.T_decimal128, 10, 2)}
		bound, err := function.GetFunctionByName(function.WithDivPrecisionIncrement(context.Background(), increment), "/", inputs)
		require.NoError(t, err)
		if casts, needed := bound.ShouldDoImplicitTypeCast(); needed {
			inputs = casts
		}
		args := make([]*planpb.Expr, 2)
		for i, input := range inputs {
			args[i] = &planpb.Expr{Typ: planpb.Type{Id: int32(input.Oid), Width: input.Width, Scale: input.Scale, NotNullable: true}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(i)}}}
		}
		out := bound.GetReturnType()
		result := &planpb.Expr{Typ: planpb.Type{Id: int32(out.Oid), Width: out.Width, Scale: out.Scale, NotNullable: function.DeduceNotNullable(bound.GetEncodedOverloadID(), args)}}
		e := exporter{profile: NewEmbeddedExportProfile(31)}
		wire, err := e.exactScalarExpr(result, &planpb.Function{Func: &planpb.ObjectRef{ObjName: "/", Obj: bound.GetEncodedOverloadID()}, Args: args}, []int{2})
		require.NoError(t, err)
		user := wire.GetScalarFunction().OutputType.GetUserDefined()
		require.Equal(t, int64(out.Width), user.TypeParameters[1].GetInteger())
		require.Equal(t, int64(out.Scale), user.TypeParameters[2].GetInteger())
	}
}

func TestEmbeddedIntegerArithmeticDeclinesUncheckedConsumer(t *testing.T) {
	for _, tc := range []struct{ mo, wire string }{{"+", "add"}, {"-", "subtract"}, {"*", "multiply"}} {
		t.Run(tc.wire, func(t *testing.T) {
			bound, err := function.GetFunctionByName(t.Context(), tc.mo, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
			require.NoError(t, err)
			args := []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}, {Typ: planpb.Type{Id: int32(types.T_int64)}}}
			ret := bound.GetReturnType()
			out := planpb.Type{Id: int32(ret.Oid), Width: ret.Width, Scale: ret.Scale, NotNullable: function.DeduceNotNullable(bound.GetEncodedOverloadID(), args)}
			ref := &planpb.ObjectRef{ObjName: tc.mo, Obj: bound.GetEncodedOverloadID()}
			for _, capabilities := range []uint64{11, 31} {
				e := exporter{embeddedMO: true, profile: NewEmbeddedExportProfile(capabilities)}
				ok, err := e.hasSemanticCapability(semanticScalar, tc.wire, ref, args, &out)
				require.NoError(t, err)
				require.False(t, ok, "embedded cuDF integer arithmetic does not preserve MO overflow")
			}
			flight := exporter{}
			ok, err := flight.hasSemanticCapability(semanticScalar, tc.wire, ref, args, &out)
			require.NoError(t, err)
			require.True(t, ok, "retain the separate Flight admission contract")
		})
	}
}

func TestExactCASEPreservesBoundNullableConditionDescriptor(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(fmt.Sprint(required), func(t *testing.T) {
			decimal := types.New(types.T_decimal64, 15, 2)
			bound, err := function.GetFunctionByName(t.Context(), "case", []types.Type{types.T_bool.ToType(), decimal, decimal})
			require.NoError(t, err)
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: required}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 2, NotNullable: true}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
				{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 2, NotNullable: true}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}},
			}
			result := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 2, NotNullable: function.DeduceNotNullable(bound.GetEncodedOverloadID(), args)}}
			require.Equal(t, required, result.Typ.NotNullable)
			e := exporter{embeddedMO: true, profile: NewEmbeddedExportProfile(31)}
			wire, err := e.exactScalarExpr(result, &planpb.Function{Func: &planpb.ObjectRef{ObjName: "case", Obj: bound.GetEncodedOverloadID()}, Args: args}, []int{3})
			require.NoError(t, err)
			annotation := wire.GetScalarFunction()
			require.NotNil(t, annotation, "IfThen cannot carry MO's bound descriptor")
			require.Equal(t, e.functions["mo_decimal_cast"], annotation.FunctionReference)
			declared := annotation.OutputType.GetUserDefined()
			require.Equal(t, []int64{64, 15, 2}, []int64{declared.TypeParameters[0].GetInteger(), declared.TypeParameters[1].GetInteger(), declared.TypeParameters[2].GetInteger()})
			require.Equal(t, required, declared.Nullability == spb.Type_NULLABILITY_REQUIRED)
			require.NotNil(t, annotation.Arguments[0].GetValue().GetIfThen())
		})
	}
}
